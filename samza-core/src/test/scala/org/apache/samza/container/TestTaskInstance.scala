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

import com.google.common.collect.{ImmutableMap, ImmutableSet}
import com.google.common.util.concurrent.MoreExecutors
import org.apache.samza.checkpoint._
import org.apache.samza.checkpoint.kafka.{KafkaChangelogSSPOffset, KafkaStateCheckpointMarker}
import org.apache.samza.config.MapConfig
import org.apache.samza.context.{TaskContext => _, _}
import org.apache.samza.job.model.TaskModel
import org.apache.samza.metrics.{Counter, Gauge, Timer}
import org.apache.samza.storage.TaskStorageCommitManager
import org.apache.samza.system.{IncomingMessageEnvelope, StreamMetadataCache, SystemAdmin, SystemConsumers, SystemStream, SystemStreamMetadata, _}
import org.apache.samza.table.TableManager
import org.apache.samza.task._
import org.apache.samza.util.FutureUtil
import org.apache.samza.{Partition, SamzaException}
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentCaptor, Matchers, Mock, MockitoAnnotations}
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar

import java.util
import java.util.Collections
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors, ForkJoinPool}
import java.util.function.Consumer
import scala.collection.JavaConverters._

class TestTaskInstance extends AssertionsForJUnit with MockitoSugar {
  private val SYSTEM_NAME = "test-system"
  private val TASK_NAME = new TaskName("taskName")
  private val SYSTEM_STREAM_PARTITION =
    new SystemStreamPartition(new SystemStream(SYSTEM_NAME, "test-stream"), new Partition(0))
  private val SYSTEM_STREAM_PARTITIONS = ImmutableSet.of(SYSTEM_STREAM_PARTITION)

  @Mock
  private var task: AllTask = null
  @Mock
  private var taskModel: TaskModel = null
  @Mock
  private var metrics: TaskInstanceMetrics = null
  @Mock
  private var systemAdmins: SystemAdmins = null
  @Mock
  private var systemAdmin: SystemAdmin = null
  @Mock
  private var consumerMultiplexer: SystemConsumers = null
  @Mock
  private var collector: TaskInstanceCollector = null
  @Mock
  private var offsetManager: OffsetManager = null
  @Mock
  private var taskCommitManager: TaskStorageCommitManager = null
  @Mock
  private var checkpointManager: CheckpointManager = null
  @Mock
  private var taskTableManager: TableManager = null
  // not a mock; using MockTaskInstanceExceptionHandler
  private var taskInstanceExceptionHandler: MockTaskInstanceExceptionHandler = null
  @Mock
  private var jobContext: JobContext = null
  @Mock
  private var containerContext: ContainerContext = null
  @Mock
  private var applicationContainerContext: ApplicationContainerContext = null
  @Mock
  private var applicationTaskContextFactory: ApplicationTaskContextFactory[ApplicationTaskContext] = null
  @Mock
  private var applicationTaskContext: ApplicationTaskContext = null
  @Mock
  private var externalContext: ExternalContext = null

  private var taskInstance: TaskInstance = null

  private val numCheckpointVersions = 2 // checkpoint versions count

  @Before
  def setup(): Unit = {
    MockitoAnnotations.initMocks(this)
    // not using Mockito mock since Mockito doesn't work well with the call-by-name argument in maybeHandle
    this.taskInstanceExceptionHandler = new MockTaskInstanceExceptionHandler
    when(this.taskModel.getTaskName).thenReturn(TASK_NAME)
    when(this.applicationTaskContextFactory.create(Matchers.eq(this.externalContext), Matchers.eq(this.jobContext),
      Matchers.eq(this.containerContext), any(), Matchers.eq(this.applicationContainerContext)))
      .thenReturn(this.applicationTaskContext)
    when(this.systemAdmins.getSystemAdmin(SYSTEM_NAME)).thenReturn(this.systemAdmin)
    val taskConfigsMap = new util.HashMap[String, String]()
    taskConfigsMap.put("task.commit.ms", "-1")
    taskConfigsMap.put("task.commit.max.delay.ms", "100000")
    when(this.jobContext.getConfig).thenReturn(new MapConfig(taskConfigsMap))
    setupTaskInstance(Some(this.applicationTaskContextFactory))
  }

  @Test
  def testProcess() {
    val processesCounter = mock[Counter]
    when(this.metrics.processes).thenReturn(processesCounter)
    val messagesActuallyProcessedCounter = mock[Counter]
    when(this.metrics.messagesActuallyProcessed).thenReturn(messagesActuallyProcessedCounter)
    when(this.offsetManager.getStartingOffset(TASK_NAME, SYSTEM_STREAM_PARTITION)).thenReturn(Some("0"))
    val envelope = new IncomingMessageEnvelope(SYSTEM_STREAM_PARTITION, "0", null, null)
    val coordinator = mock[ReadableCoordinator]
    val callbackFactory = mock[TaskCallbackFactory]
    val callback = mock[TaskCallback]
    when(callbackFactory.createCallback()).thenReturn(callback)
    this.taskInstance.process(envelope, coordinator, callbackFactory)
    assertEquals(1, this.taskInstanceExceptionHandler.numTimesCalled)
    verify(this.task).processAsync(envelope, this.collector, coordinator, callback)
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
  def testInitTask(): Unit = {
    this.taskInstance.initTask

    val contextCaptor = ArgumentCaptor.forClass(classOf[Context])
    verify(this.task).init(contextCaptor.capture())
    val actualContext = contextCaptor.getValue
    assertEquals(this.jobContext, actualContext.getJobContext)
    assertEquals(this.containerContext, actualContext.getContainerContext)
    assertEquals(this.taskModel, actualContext.getTaskContext.getTaskModel)
    assertEquals(this.applicationContainerContext, actualContext.getApplicationContainerContext)
    assertEquals(this.applicationTaskContext, actualContext.getApplicationTaskContext)
    assertEquals(this.externalContext, actualContext.getExternalContext)

    verify(this.applicationTaskContext).start()
  }

  @Test
  def testShutdownTask(): Unit = {
    this.taskInstance.shutdownTask
    verify(this.applicationTaskContext).stop()
    verify(this.task).close()
  }

  /**
   * Tests that the init() method of task can override the existing offset assignment.
   * This helps verify wiring for the task context (i.e. offset manager).
   */
  @Test
  def testManualOffsetReset() {
    when(this.task.init(any())).thenAnswer(new Answer[Void] {
      override def answer(invocation: InvocationOnMock): Void = {
        val context = invocation.getArgumentAt(0, classOf[Context])
        context.getTaskContext.setStartingOffset(SYSTEM_STREAM_PARTITION, "10")
        null
      }
    })
    taskInstance.initTask

    verify(this.offsetManager).setStartingOffset(TASK_NAME, SYSTEM_STREAM_PARTITION, "10")
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

    val mockCoordinator = mock[ReadableCoordinator]
    val mockCallback = mock[TaskCallback]
    val mockCallbackFactory = mock[TaskCallbackFactory]
    when(mockCallbackFactory.createCallback()).thenReturn(mockCallback)

    this.taskInstance.process(oldEnvelope, mockCoordinator, mockCallbackFactory)
    this.taskInstance.process(newEnvelope0, mockCoordinator, mockCallbackFactory)
    this.taskInstance.process(newEnvelope1, mockCoordinator, mockCallbackFactory)
    verify(this.task).processAsync(Matchers.eq(newEnvelope0), Matchers.eq(this.collector), Matchers.eq(mockCoordinator), Matchers.eq(mockCallback))
    verify(this.task).processAsync(Matchers.eq(newEnvelope1), Matchers.eq(this.collector), Matchers.eq(mockCoordinator), Matchers.eq(mockCallback))
    verify(this.task, never()).processAsync(Matchers.eq(oldEnvelope), any(), any(), any())
    verify(processesCounter, times(3)).inc()
    verify(messagesActuallyProcessedCounter, times(2)).inc()
  }

  @Test
  def testCommitOrder() {
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)
    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    val changelogSSP = new SystemStreamPartition(new SystemStream(SYSTEM_NAME, "test-changelog-stream"), new Partition(0))
    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    val stateCheckpointMarker = new KafkaStateCheckpointMarker(changelogSSP, "5").toString
    stateCheckpointMarkers.put("storeName", stateCheckpointMarker)
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)

    val snapshotSCMs = ImmutableMap.of(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)
    when(this.taskCommitManager.snapshot(any())).thenReturn(snapshotSCMs)
    val snapshotSCMFuture: CompletableFuture[util.Map[String, util.Map[String, String]]] =
      CompletableFuture.completedFuture(snapshotSCMs)
    when(this.taskCommitManager.upload(any(), Matchers.eq(snapshotSCMs))).thenReturn(snapshotSCMFuture) // kafka is no-op
    when(this.taskCommitManager.cleanUp(any(), any())).thenReturn(CompletableFuture.completedFuture[Void](null))


    taskInstance.commit

    val mockOrder = inOrder(this.offsetManager, this.collector, this.taskTableManager, this.taskCommitManager)

    // We must first get a snapshot of the input offsets so it doesn't change while we flush. SAMZA-1384
    mockOrder.verify(this.offsetManager).getLastProcessedOffsets(TASK_NAME)

    // Producers must be flushed next and ideally the output would be flushed before the changelog
    // s.t. the changelog and checkpoints (state and inputs) are captured last
    mockOrder.verify(this.collector).flush

    // Tables should be flushed next
    mockOrder.verify(this.taskTableManager).flush()

    // Local state should be flushed next next
    mockOrder.verify(this.taskCommitManager).snapshot(any())

    // Upload should be called next with the snapshot SCMs.
    mockOrder.verify(this.taskCommitManager).upload(any(), Matchers.eq(snapshotSCMs))

    // Stores checkpoints should be created next with the newest changelog offsets
    mockOrder.verify(this.taskCommitManager).writeCheckpointToStoreDirectories(any())

    // Input checkpoint should be written with the snapshot captured at the beginning of commit and the
    // newest changelog offset captured during storage manager flush
    val captor = ArgumentCaptor.forClass(classOf[Checkpoint])
    mockOrder.verify(offsetManager, times(numCheckpointVersions)).writeCheckpoint(any(), captor.capture)
    val cp = captor.getAllValues
    assertEquals(numCheckpointVersions, cp.size())
    cp.forEach(new Consumer[Checkpoint] {
      override def accept(c: Checkpoint): Unit = {
        assertEquals("4", c.getOffsets.get(SYSTEM_STREAM_PARTITION))
        if (c.getVersion == 2) {
          assertEquals(1, c.getOffsets.size())
          assertTrue(c.isInstanceOf[CheckpointV2])
          val checkpointedStateCheckpointMarkers = c.asInstanceOf[CheckpointV2]
            .getStateCheckpointMarkers.get(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME)
          assertTrue(checkpointedStateCheckpointMarkers.size() == 1)
          val checkpointedStateCheckpointMarker = checkpointedStateCheckpointMarkers.get("storeName")
          assertTrue(checkpointedStateCheckpointMarker.equals(stateCheckpointMarker))
          val kafkaMarker = KafkaStateCheckpointMarker.fromString(checkpointedStateCheckpointMarker)
          assertEquals(kafkaMarker.getChangelogOffset, "5")
          assertEquals(kafkaMarker.getChangelogSSP, changelogSSP)
        } else { // c.getVersion == 1
          assertEquals(2, c.getOffsets.size())
          assertTrue(c.isInstanceOf[CheckpointV1])
          assertEquals("5", KafkaChangelogSSPOffset.fromString(c.getOffsets.get(changelogSSP)).getChangelogOffset)
        }
      }
    })

    // Old checkpointed stores should be cleared
    mockOrder.verify(this.taskCommitManager).cleanUp(any(), any())
    verify(commitsCounter).inc()
    verify(uploadCounter).inc()
    verify(snapshotTimer).update(anyLong())
    verify(uploadTimer).update(anyLong())
    verify(commitTimer).update(anyLong())
    verify(skippedCounter).set(0)
  }

  @Test
  def testEmptyChangelogSSPOffsetInCommit() { // e.g. if changelog topic is empty
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = Map(SYSTEM_STREAM_PARTITION -> "4").asJava
    val changelogSSP = new SystemStreamPartition(new SystemStream(SYSTEM_NAME, "test-changelog-stream"), new Partition(0))
    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    val nullStateCheckpointMarker = new KafkaStateCheckpointMarker(changelogSSP, null).toString
    stateCheckpointMarkers.put("storeName", nullStateCheckpointMarker)
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)
    when(this.taskCommitManager.upload(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(
        Collections.singletonMap(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)))
    taskInstance.commit

    val captor = ArgumentCaptor.forClass(classOf[Checkpoint])
    verify(offsetManager, times(numCheckpointVersions)).writeCheckpoint(any(), captor.capture)
    val cp = captor.getAllValues
    assertEquals(numCheckpointVersions, cp.size())
    cp.forEach(new Consumer[Checkpoint] {
      override def accept(checkpoint: Checkpoint): Unit = {
        assertEquals("4", checkpoint.getOffsets.get(SYSTEM_STREAM_PARTITION))
        if (checkpoint.getVersion == 2) {
          assertEquals(1, checkpoint.getOffsets.size())
          assertTrue(checkpoint.isInstanceOf[CheckpointV2])
          val checkpointedStateCheckpointMarkers = checkpoint.asInstanceOf[CheckpointV2]
            .getStateCheckpointMarkers.get(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME)
          assertTrue(checkpointedStateCheckpointMarkers.size() == 1)
          val checkpointedStateCheckpointMarker = checkpointedStateCheckpointMarkers.get("storeName")
          assertTrue(checkpointedStateCheckpointMarker.equals(nullStateCheckpointMarker))
          val kafkaMarker = KafkaStateCheckpointMarker.fromString(checkpointedStateCheckpointMarker)
          assertNull(kafkaMarker.getChangelogOffset)
          assertEquals(kafkaMarker.getChangelogSSP, changelogSSP)
        } else { // c.getVersion == 1
          assertEquals(2, checkpoint.getOffsets.size())
          assertTrue(checkpoint.isInstanceOf[CheckpointV1])
          val message = checkpoint.getOffsets.get(changelogSSP)
          val checkpointedOffset = KafkaChangelogSSPOffset.fromString(message)
          assertNull(checkpointedOffset.getChangelogOffset)
          assertNotNull(checkpointedOffset.getCheckpointId)
        }
      }
    })
    verify(commitsCounter).inc()
    verify(uploadCounter).inc()
    verify(snapshotTimer).update(anyLong())
    verify(uploadTimer).update(anyLong())
  }

  @Test
  def testEmptyChangelogOffsetsInCommit() { // e.g. if stores have no changelogs
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = Map(SYSTEM_STREAM_PARTITION -> "4").asJava
    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)
    when(this.taskCommitManager.upload(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(
        Collections.singletonMap(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)))
    taskInstance.commit

    val captor = ArgumentCaptor.forClass(classOf[Checkpoint])
    // verify the write checkpoint is evoked twice, once per checkpoint version
    verify(offsetManager, times(numCheckpointVersions)).writeCheckpoint(any(), captor.capture)
    val cp = captor.getAllValues
    assertEquals(numCheckpointVersions, cp.size())
    cp.forEach(new Consumer[Checkpoint] {
      override def accept(c: Checkpoint): Unit = {
        assertEquals("4", c.getOffsets.get(SYSTEM_STREAM_PARTITION))
        assertEquals(1, c.getOffsets.size())
      }
    })
    verify(commitsCounter).inc()
    verify(uploadCounter).inc()
    verify(snapshotTimer).update(anyLong())
    verify(uploadTimer).update(anyLong())
  }

  @Test
  def testCommitFailsIfErrorGettingChangelogOffset() { // required for transactional state
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)

    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)
    when(this.taskCommitManager.snapshot(any())).thenThrow(new SamzaException("Error getting changelog offsets"))

    try {
      // sync stage exception should be caught and rethrown immediately
      taskInstance.commit

      verify(commitsCounter).inc()
      verifyZeroInteractions(snapshotTimer)
    } catch {
      case e: SamzaException =>
        val msg = e.getMessage
        // exception is expected, container should fail if could not get changelog offsets.
        return
    }

    fail("Should have failed commit if error getting newest changelog offsets")
  }

  @Test
  def testCommitFailsIfPreviousAsyncUploadFails() {
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)
    when(this.taskCommitManager.upload(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(
        Collections.singletonMap(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)))
    when(this.taskCommitManager.upload(any(), any()))
      .thenReturn(FutureUtil.failedFuture[util.Map[String, util.Map[String, String]]](new RuntimeException))

    try {
      taskInstance.commit

      verify(commitsCounter).inc()
      verifyZeroInteractions(uploadCounter)
      verify(snapshotTimer).update(anyLong())
      verifyZeroInteractions(uploadTimer)
      verifyZeroInteractions(commitTimer)
      verifyZeroInteractions(skippedCounter)

      // async stage exception in first commit should be caught and rethrown by the subsequent commit
      taskInstance.commit
      verifyNoMoreInteractions(commitsCounter)
    } catch {
      case e: SamzaException =>
        // exception is expected, container should fail if could not upload previous snapshot.
        return
    }

    fail("Should have failed commit if error uploading store contents")
  }

  @Test
  def testCommitFailsIfAsyncStoreDirCheckpointWriteFails() { // required for transactional state
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)
    when(this.taskCommitManager.upload(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(
        Collections.singletonMap(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)))
    when(this.taskCommitManager.writeCheckpointToStoreDirectories(any()))
      .thenThrow(new SamzaException("Error creating store checkpoint"))

    try {
      taskInstance.commit

      verify(commitsCounter).inc()
      verify(uploadCounter).inc()
      verify(snapshotTimer).update(anyLong())
      verify(uploadTimer).update(anyLong())
      verifyZeroInteractions(commitTimer)
      verifyZeroInteractions(skippedCounter)

      // async stage exception in first commit should be caught and rethrown by the subsequent commit
      taskInstance.commit
      verifyNoMoreInteractions(commitsCounter)
    } catch {
      case e: SamzaException =>
        // exception is expected, container should fail if could not get changelog offsets.
        return
    }

    fail("Should have failed commit if error writing checkpoint to store dirs")
  }

  @Test
  def testCommitFailsIfPreviousAsyncCheckpointTopicWriteFails() {
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)
    when(this.taskCommitManager.upload(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(
        Collections.singletonMap(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)))
    doNothing().when(this.taskCommitManager).writeCheckpointToStoreDirectories(any())
    when(this.offsetManager.writeCheckpoint(any(), any()))
      .thenThrow(new SamzaException("Error writing checkpoint"))

    try {
      taskInstance.commit

      verify(commitsCounter).inc()
      verify(uploadCounter).inc()
      verify(snapshotTimer).update(anyLong())
      verify(uploadTimer).update(anyLong())
      verifyZeroInteractions(commitTimer)
      verifyZeroInteractions(skippedCounter)

      // async stage exception in first commit should be caught and rethrown by the subsequent commit
      taskInstance.commit
      verifyNoMoreInteractions(commitsCounter)
    } catch {
      case e: SamzaException =>
        // exception is expected, container should fail if could not write previous checkpoint.
        return
    }

    fail("Should have failed commit if error writing checkpoints to checkpoint topic")
  }

  @Test
  def testCommitFailsIfPreviousAsyncCleanUpFails() { // required for blob store backend
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)
    when(this.taskCommitManager.upload(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(
        Collections.singletonMap(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)))
    doNothing().when(this.taskCommitManager).writeCheckpointToStoreDirectories(any())
    when(this.taskCommitManager.cleanUp(any(), any()))
      .thenReturn(FutureUtil.failedFuture[Void](new SamzaException("Error during cleanup")))

    try {
      taskInstance.commit

      verify(commitsCounter).inc()
      verify(uploadCounter).inc()
      verify(snapshotTimer).update(anyLong())
      verify(uploadTimer).update(anyLong())
      verifyZeroInteractions(commitTimer)
      verifyZeroInteractions(skippedCounter)

      // async stage exception in first commit should be caught and rethrown by the subsequent commit
      taskInstance.commit
      verifyNoMoreInteractions(commitsCounter)
    } catch {
      case e: SamzaException =>
        // exception is expected, container should fail if could not clean up old checkpoint.
        return
    }

    fail("Should have failed commit if error cleaning up previous commit")
  }

  @Test
  def testCommitFailsIfPreviousAsyncUploadFailsSynchronously() {
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)
    when(this.taskCommitManager.upload(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(
        Collections.singletonMap(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)))

    // Fail synchronously instead of returning a failed future.
    when(this.taskCommitManager.upload(any(), any()))
      .thenThrow(new RuntimeException)

    try {
      taskInstance.commit

      verify(commitsCounter).inc()
      verifyZeroInteractions(uploadCounter)
      verify(snapshotTimer).update(anyLong())
      verifyZeroInteractions(uploadTimer)
      verifyZeroInteractions(commitTimer)
      verifyZeroInteractions(skippedCounter)

      // async stage exception in first commit should be caught and rethrown by the subsequent commit
      taskInstance.commit
      verifyNoMoreInteractions(commitsCounter)
    } catch {
      case e: SamzaException =>
        // exception is expected, container should fail if could not upload previous snapshot.
        return
    }

    fail("Should have failed commit if synchronous error during upload in async stage of previous commit")
  }

  @Test
  def testCommitSucceedsIfPreviousAsyncStageSucceeds() {
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)
    when(this.taskCommitManager.upload(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(
        Collections.singletonMap(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)))
    doNothing().when(this.taskCommitManager).writeCheckpointToStoreDirectories(any())
    when(this.taskCommitManager.cleanUp(any(), any()))
      .thenReturn(CompletableFuture.completedFuture[Void](null))

    taskInstance.commit // async stage will be run by caller due to direct executor

    verify(commitsCounter).inc()
    verify(uploadCounter).inc()
    verify(snapshotTimer).update(anyLong())
    verify(uploadTimer).update(anyLong())
    verify(commitTimer).update(anyLong())
    verify(skippedCounter).set(0)

    taskInstance.commit

    // verify that all commit operations ran twice
    verify(taskCommitManager, times(2)).snapshot(any())
    verify(taskCommitManager, times(2)).upload(any(), any())
    // called 2x per commit, once for each checkpoint version
    verify(taskCommitManager, times(4)).writeCheckpointToStoreDirectories(any())
    verify(offsetManager, times(4)).writeCheckpoint(any(), any())
    verify(taskCommitManager, times(2)).cleanUp(any(), any())
    verify(commitsCounter, times(2)).inc()
  }

  @Test
  def testCommitSkipsIfPreviousAsyncCommitInProgressWithinMaxCommitDelay() {
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    val changelogSSP = new SystemStreamPartition(new SystemStream(SYSTEM_NAME, "test-changelog-stream"), new Partition(0))

    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    val stateCheckpointMarker = new KafkaStateCheckpointMarker(changelogSSP, "5").toString
    stateCheckpointMarkers.put("storeName", stateCheckpointMarker)
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)

    val snapshotSCMs = ImmutableMap.of(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)
    when(this.taskCommitManager.snapshot(any())).thenReturn(snapshotSCMs)
    val snapshotSCMFuture: CompletableFuture[util.Map[String, util.Map[String, String]]] =
      CompletableFuture.completedFuture(snapshotSCMs)

    when(this.taskCommitManager.upload(any(), Matchers.eq(snapshotSCMs))).thenReturn(snapshotSCMFuture) // kafka is no-op

    val cleanUpFuture = new CompletableFuture[Void]() // not completed until subsequent commit starts
    when(this.taskCommitManager.cleanUp(any(), any())).thenReturn(cleanUpFuture)

    // use a separate executor to perform async operations on to test caller thread blocking behavior
    setupTaskInstance(None, ForkJoinPool.commonPool())

    taskInstance.commit // async stage will not complete until cleanUpFuture is completed

    taskInstance.commit

    verify(skippedCounter).set(1)

    verify(commitsCounter, times(1)).inc() // should only have been incremented once on the initial commit
    verify(uploadCounter).inc()
    verify(snapshotTimer).update(anyLong())
    verify(uploadTimer).update(anyLong())
    verifyZeroInteractions(commitTimer)
    verifyZeroInteractions(skippedCounter)

    cleanUpFuture.complete(null) // just to unblock shared executor
  }

  @Test
  def testCommitThrowsIfPreviousAsyncCommitInProgressAfterMaxCommitDelayAndBlockTime() {
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    val changelogSSP = new SystemStreamPartition(new SystemStream(SYSTEM_NAME, "test-changelog-stream"), new Partition(0))

    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    val stateCheckpointMarker = new KafkaStateCheckpointMarker(changelogSSP, "5").toString
    stateCheckpointMarkers.put("storeName", stateCheckpointMarker)
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)

    val snapshotSCMs = ImmutableMap.of(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)
    when(this.taskCommitManager.snapshot(any())).thenReturn(snapshotSCMs)
    val snapshotSCMFuture: CompletableFuture[util.Map[String, util.Map[String, String]]] =
      CompletableFuture.completedFuture(snapshotSCMs)

    when(this.taskCommitManager.upload(any(), Matchers.eq(snapshotSCMs))).thenReturn(snapshotSCMFuture) // kafka is no-op

    val cleanUpFuture = new CompletableFuture[Void]()
    when(this.taskCommitManager.cleanUp(any(), any())).thenReturn(cleanUpFuture)

    // use a separate executor to perform async operations on to test caller thread blocking behavior
    val taskConfigsMap = new util.HashMap[String, String]()
    taskConfigsMap.put("task.commit.ms", "-1")
    // "block" immediately if previous commit async stage not complete
    taskConfigsMap.put("task.commit.max.delay.ms", "-1")
    taskConfigsMap.put("task.commit.timeout.ms", "0") // throw exception immediately if blocked
    when(this.jobContext.getConfig).thenReturn(new MapConfig(taskConfigsMap)) // override default behavior

    setupTaskInstance(None, ForkJoinPool.commonPool())

    taskInstance.commit // async stage will not complete until cleanUpFuture is completed

    try {
      taskInstance.commit // should throw exception
      fail("Should have thrown an exception if blocked for previous commit async stage.")
    } catch {
      case e: Exception =>
        verify(commitsCounter, times(1)).inc() // should only have been incremented once on the initial commit
    }

    cleanUpFuture.complete(null) // just to unblock shared executor
  }

  @Test
  def testCommitBlocksIfPreviousAsyncCommitInProgressAfterMaxCommitDelayButWithinBlockTime() {
    val commitsCounter = mock[Counter]
    when(this.metrics.commits).thenReturn(commitsCounter)
    val snapshotTimer = mock[Timer]
    when(this.metrics.snapshotNs).thenReturn(snapshotTimer)
    val commitTimer = mock[Timer]
    when(this.metrics.commitNs).thenReturn(commitTimer)
    val uploadTimer = mock[Timer]
    when(this.metrics.asyncUploadNs).thenReturn(uploadTimer)
    val uploadCounter = mock[Counter]
    when(this.metrics.asyncUploadsCompleted).thenReturn(uploadCounter)
    val skippedCounter = mock[Gauge[Int]]
    when(this.metrics.asyncCommitSkipped).thenReturn(skippedCounter)

    val inputOffsets = new util.HashMap[SystemStreamPartition, String]()
    inputOffsets.put(SYSTEM_STREAM_PARTITION,"4")
    val changelogSSP = new SystemStreamPartition(new SystemStream(SYSTEM_NAME, "test-changelog-stream"), new Partition(0))

    val stateCheckpointMarkers: util.Map[String, String] = new util.HashMap[String, String]()
    val stateCheckpointMarker = new KafkaStateCheckpointMarker(changelogSSP, "5").toString
    stateCheckpointMarkers.put("storeName", stateCheckpointMarker)
    when(this.offsetManager.getLastProcessedOffsets(TASK_NAME)).thenReturn(inputOffsets)

    val snapshotSCMs = ImmutableMap.of(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME, stateCheckpointMarkers)
    when(this.taskCommitManager.snapshot(any())).thenReturn(snapshotSCMs)
    val snapshotSCMFuture: CompletableFuture[util.Map[String, util.Map[String, String]]] =
      CompletableFuture.completedFuture(snapshotSCMs)

    when(this.taskCommitManager.upload(any(), Matchers.eq(snapshotSCMs))).thenReturn(snapshotSCMFuture) // kafka is no-op

    val cleanUpFuture = new CompletableFuture[Void]()
    when(this.taskCommitManager.cleanUp(any(), any())).thenReturn(cleanUpFuture)

    // use a separate executor to perform async operations on to test caller thread blocking behavior
    val taskConfigsMap = new util.HashMap[String, String]()
    taskConfigsMap.put("task.commit.ms", "-1")
    // "block" immediately if previous commit async stage not complete
    taskConfigsMap.put("task.commit.max.delay.ms", "-1")
    taskConfigsMap.put("task.commit.timeout.ms", "1000000") // block until previous stage is complete
    when(this.jobContext.getConfig).thenReturn(new MapConfig(taskConfigsMap)) // override default behavior

    setupTaskInstance(None, ForkJoinPool.commonPool())

    taskInstance.commit // async stage will not complete until cleanUpFuture is completed

    val executorService = Executors.newSingleThreadExecutor()
    val secondCommitFuture = CompletableFuture.runAsync(new Runnable {
      override def run(): Unit = taskInstance.commit // will block on executor
    }, executorService)

    var retries = 0 // wait no more than ~100 millis
    while (!taskInstance.commitInProgress.hasQueuedThreads && retries < 10) {
      retries += 1
      Thread.sleep(10) // wait until commit in other thread blocks on the semaphore.
    }
    if (!taskInstance.commitInProgress.hasQueuedThreads) {
      fail("Other thread should have blocked on semaphore acquisition. " +
        "May need to increase retries if transient failure.")
    }

    cleanUpFuture.complete(null) // will eventually unblock the 2nd commit in other thread.
    secondCommitFuture.join() // will complete when the sync phase of 2nd commit is complete.
    verify(commitsCounter, times(2)).inc() // should only have been incremented twice - once for each commit
    verify(uploadCounter, times(2)).inc()
    verify(snapshotTimer, times(2)).update(anyLong())
    verify(uploadTimer, times(2)).update(anyLong())
  }


  /**
    * Given that no application task context factory is provided, then no lifecycle calls should be made.
    */
  @Test
  def testNoApplicationTaskContextFactoryProvided() {
    setupTaskInstance(None)
    this.taskInstance.initTask
    this.taskInstance.shutdownTask
    verifyZeroInteractions(this.applicationTaskContext)
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

  @Test
  def testInitCaughtUpMapping() {
    val offsetManagerMock = mock[OffsetManager]
    when(offsetManagerMock.getStartingOffset(anyObject(), anyObject())).thenReturn(Option("42"))
    val cacheMock = mock[StreamMetadataCache]
    val systemStreamMetadata = mock[SystemStreamMetadata]
    when(cacheMock.getSystemStreamMetadata(anyObject(), anyBoolean()))
      .thenReturn(systemStreamMetadata)
    val sspMetadata = mock[SystemStreamMetadata.SystemStreamPartitionMetadata]
    when(sspMetadata.getUpcomingOffset).thenReturn("42")
    when(systemStreamMetadata.getSystemStreamPartitionMetadata)
      .thenReturn(Collections.singletonMap(new Partition(0), sspMetadata))

    val ssp = new SystemStreamPartition("test-system", "test-stream", new Partition(0))
    val inputStreamMetadata = collection.Map(ssp.getSystemStream -> systemStreamMetadata)

    val taskInstance = new TaskInstance(this.task,
      this.taskModel,
      this.metrics,
      this.systemAdmins,
      this.consumerMultiplexer,
      this.collector,
      offsetManager = offsetManagerMock,
      commitManager = this.taskCommitManager,
      tableManager = this.taskTableManager,
      systemStreamPartitions = ImmutableSet.of(ssp),
      exceptionHandler = this.taskInstanceExceptionHandler,
      streamMetadataCache = cacheMock,
      inputStreamMetadata = Map.empty ++ inputStreamMetadata,
      jobContext = this.jobContext,
      containerContext = this.containerContext,
      applicationContainerContextOption = Some(this.applicationContainerContext),
      applicationTaskContextFactoryOption = Some(this.applicationTaskContextFactory),
      externalContextOption = Some(this.externalContext))

    taskInstance.initCaughtUpMapping()

    assertTrue(taskInstance.ssp2CaughtupMapping(ssp))
  }

  private def setupTaskInstance(
    applicationTaskContextFactory: Option[ApplicationTaskContextFactory[ApplicationTaskContext]],
    commitThreadPool: ExecutorService = MoreExecutors.newDirectExecutorService()): Unit = {
    this.taskInstance = new TaskInstance(this.task,
      this.taskModel,
      this.metrics,
      this.systemAdmins,
      this.consumerMultiplexer,
      this.collector,
      offsetManager = this.offsetManager,
      commitManager = this.taskCommitManager,
      tableManager = this.taskTableManager,
      systemStreamPartitions = SYSTEM_STREAM_PARTITIONS,
      exceptionHandler = this.taskInstanceExceptionHandler,
      commitThreadPool = commitThreadPool,
      jobContext = this.jobContext,
      containerContext = this.containerContext,
      applicationContainerContextOption = Some(this.applicationContainerContext),
      applicationTaskContextFactoryOption = applicationTaskContextFactory,
      externalContextOption = Some(this.externalContext))
  }

  /**
    * Task type which has all task traits, which can be mocked.
    */
  trait AllTask extends AsyncStreamTask with InitableTask with ClosableTask with WindowableTask {}

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