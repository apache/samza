/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.container


import org.apache.samza.Partition
import org.apache.samza.checkpoint.{Checkpoint, OffsetManager}
import org.apache.samza.config.Config
import org.apache.samza.metrics.Counter
import org.apache.samza.storage.TaskStorageManager
import org.apache.samza.system.{IncomingMessageEnvelope, SystemAdmin, SystemConsumers, SystemStream, _}
import org.apache.samza.task._
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{Matchers, Mock, MockitoAnnotations}
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class TestTaskInstance extends MockitoSugar {
  private val SYSTEM_NAME = "test-system"
  private val TASK_NAME = new TaskName("taskName")
  private val SYSTEM_STREAM_PARTITION =
    new SystemStreamPartition(new SystemStream(SYSTEM_NAME, "test-stream"), new Partition(0))
  private val SYSTEM_STREAM_PARTITIONS = Set(SYSTEM_STREAM_PARTITION)

  @Mock
  private var task: AllTask = _
  @Mock
  private var config: Config = _
  @Mock
  private var metrics: TaskInstanceMetrics = _
  @Mock
  private var systemAdmins: SystemAdmins = _
  @Mock
  private var systemAdmin: SystemAdmin = _
  @Mock
  private var consumerMultiplexer: SystemConsumers = _
  @Mock
  private var collector: TaskInstanceCollector = _
  @Mock
  private var containerContext: SamzaContainerContext = _
  @Mock
  private var offsetManager: OffsetManager = _
  @Mock
  private var taskStorageManager: TaskStorageManager = _
  // not a mock; using MockTaskInstanceExceptionHandler
  private var taskInstanceExceptionHandler: MockTaskInstanceExceptionHandler = _

  private var taskInstance: TaskInstance = _

  @Before
  def setup(): Unit = {
    MockitoAnnotations.initMocks(this)
    // not using Mockito mock since Mockito doesn't work well with the call-by-name argument in maybeHandle
    this.taskInstanceExceptionHandler = new MockTaskInstanceExceptionHandler
    this.taskInstance = new TaskInstance(this.task,
      TASK_NAME,
      this.config,
      this.metrics,
      this.systemAdmins,
      this.consumerMultiplexer,
      this.collector,
      this.containerContext,
      this.offsetManager,
      storageManager = this.taskStorageManager,
      systemStreamPartitions = SYSTEM_STREAM_PARTITIONS,
      exceptionHandler = this.taskInstanceExceptionHandler)
    when(this.systemAdmins.getSystemAdmin(SYSTEM_NAME)).thenReturn(this.systemAdmin)
  }

  @Test
  def testBasicProcess() {
    val processesCounter = mock[Counter]
    when(this.metrics.processes).thenReturn(processesCounter)
    val messagesActuallyProcessedCounter = mock[Counter]
    when(this.metrics.messagesActuallyProcessed).thenReturn(messagesActuallyProcessedCounter)
    when(this.offsetManager.getStartingOffset(TASK_NAME, SYSTEM_STREAM_PARTITION)).thenReturn(Some("0"))
    val envelope = new IncomingMessageEnvelope(SYSTEM_STREAM_PARTITION, "0", null, null)
    val coordinator = mock[ReadableCoordinator]
    this.taskInstance.process(envelope, coordinator)
    assertEquals(1, this.taskInstanceExceptionHandler.numTimesCalled)
    verify(this.task).process(envelope, this.collector, coordinator)
    verify(processesCounter).inc()
    verify(messagesActuallyProcessedCounter).inc()
  }

  @Test
  def testWindow() {
    val windowsCounter = mock[Counter]
    when(this.metrics.windows).thenReturn(windowsCounter)
    val coordinator = mock[ReadableCoordinator]
    this.taskInstance.window(coordinator)
    assertEquals(1, this.taskInstanceExceptionHandler.numTimesCalled)
    verify(this.task).window(this.collector, coordinator)
    verify(windowsCounter).inc()
  }

  @Test
  def testOffsetsAreUpdatedOnProcess() {
    when(this.metrics.processes).thenReturn(mock[Counter])
    when(this.metrics.messagesActuallyProcessed).thenReturn(mock[Counter])
    when(this.offsetManager.getStartingOffset(TASK_NAME, SYSTEM_STREAM_PARTITION)).thenReturn(Some("2"))
    this.taskInstance.process(new IncomingMessageEnvelope(SYSTEM_STREAM_PARTITION, "4", null, null),
      mock[ReadableCoordinator])
    verify(this.offsetManager).update(TASK_NAME, SYSTEM_STREAM_PARTITION, "4")
  }

  /**
   * Tests that the init() method of task can override the existing offset assignment.
   * This helps verify wiring for the task context (i.e. offset manager).
   */
  @Test
  def testManualOffsetReset() {
    when(this.task.init(any(), any())).thenAnswer(new Answer[Void] {
      override def answer(invocation: InvocationOnMock): Void = {
        val taskContext = invocation.getArgumentAt(1, classOf[TaskContext])
        taskContext.setStartingOffset(SYSTEM_STREAM_PARTITION, "10")
        null
      }
    })
    taskInstance.initTask

    verify(this.offsetManager).setStartingOffset(TASK_NAME, SYSTEM_STREAM_PARTITION, "10")
    verifyNoMoreInteractions(this.offsetManager)
  }

  @Test
  def testIgnoreMessagesOlderThanStartingOffsets() {
    val processesCounter = mock[Counter]
    when(this.metrics.processes).thenReturn(processesCounter)
    val messagesActuallyProcessedCounter = mock[Counter]
    when(this.metrics.messagesActuallyProcessed).thenReturn(messagesActuallyProcessedCounter)
    when(this.offsetManager.getStartingOffset(TASK_NAME, SYSTEM_STREAM_PARTITION)).thenReturn(Some("5"))
    when(this.systemAdmin.offsetComparator(any(), any())).thenAnswer(new Answer[Integer] {
      override def answer(invocation: InvocationOnMock): Integer = {
        val offset1 = invocation.getArgumentAt(0, classOf[String])
        val offset2 = invocation.getArgumentAt(1, classOf[String])
        offset1.toLong.compareTo(offset2.toLong)
      }
    })
    val oldEnvelope = new IncomingMessageEnvelope(SYSTEM_STREAM_PARTITION, "0", null, null)
    val newEnvelope0 = new IncomingMessageEnvelope(SYSTEM_STREAM_PARTITION, "5", null, null)
    val newEnvelope1 = new IncomingMessageEnvelope(SYSTEM_STREAM_PARTITION, "7", null, null)

    this.taskInstance.process(oldEnvelope, mock[ReadableCoordinator])
    this.taskInstance.process(newEnvelope0, mock[ReadableCoordinator])
    this.taskInstance.process(newEnvelope1, mock[ReadableCoordinator])
    verify(this.task).process(Matchers.eq(newEnvelope0), Matchers.eq(this.collector), any())
    verify(this.task).process(Matchers.eq(newEnvelope1), Matchers.eq(this.collector), any())
    verify(this.task, never()).process(Matchers.eq(oldEnvelope), any(), any())
    verify(processesCounter, times(3)).inc()
    verify(messagesActuallyProcessedCounter, times(2)).inc()
  }

  @Test
  def testCommitOrder() {
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val checkpoint = new Checkpoint(Map(SYSTEM_STREAM_PARTITION -> "4").asJava)
    when(this.offsetManager.buildCheckpoint(TASK_NAME)).thenReturn(checkpoint)

    taskInstance.commit

    val mockOrder = inOrder(this.offsetManager, this.collector, this.taskStorageManager)

    // We must first get a snapshot of the checkpoint so it doesn't change while we flush. SAMZA-1384
    mockOrder.verify(this.offsetManager).buildCheckpoint(TASK_NAME)
    // Producers must be flushed next and ideally the output would be flushed before the changelog
    // s.t. the changelog and checkpoints (state and inputs) are captured last
    mockOrder.verify(this.collector).flush
    // Local state is next, to ensure that the state (particularly the offset file) never points to a newer changelog
    // offset than what is reflected in the on disk state.
    mockOrder.verify(this.taskStorageManager).flush()
    // Finally, checkpoint the inputs with the snapshotted checkpoint captured at the beginning of commit
    mockOrder.verify(offsetManager).writeCheckpoint(TASK_NAME, checkpoint)
    verify(commitsCounter).inc()
  }

  @Test(expected = classOf[SystemProducerException])
  def testProducerExceptionsIsPropagated() {
    when(this.metrics.commits).thenReturn(mock[Counter])
    when(this.collector.flush).thenThrow(new SystemProducerException("systemProducerException"))

    try {
      taskInstance.commit // Should not swallow the SystemProducerException
    } finally {
      verify(offsetManager, never()).writeCheckpoint(any(), any())
    }
  }

  /**
    * Task type which has all task traits, which can be mocked.
    */
  trait AllTask extends StreamTask with InitableTask with WindowableTask {}

  /**
    * Mock version of [TaskInstanceExceptionHandler] which just does a passthrough execution and keeps track of the
    * number of times it is called. This is used to verify that the handler does get used to wrap the actual processing.
    */
  class MockTaskInstanceExceptionHandler extends TaskInstanceExceptionHandler {
    var numTimesCalled = 0

    override def maybeHandle(tryCodeBlock: => Unit): Unit = {
      numTimesCalled += 1
      tryCodeBlock
    }
  }
}