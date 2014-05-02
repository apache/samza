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

import org.junit.Test
import org.mockito.Matchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import org.apache.samza.Partition
import org.apache.samza.system.{IncomingMessageEnvelope, SystemConsumers, SystemStreamPartition}
import org.apache.samza.task.ReadableCoordinator
import org.apache.samza.task.TaskCoordinator.RequestScope

class TestRunLoop extends AssertionsForJUnit with MockitoSugar with ShouldMatchers {
  class StopRunLoop extends RuntimeException

  val p0 = new Partition(0)
  val p1 = new Partition(1)
  val ssp0 = new SystemStreamPartition("testSystem", "testStream", p0)
  val ssp1 = new SystemStreamPartition("testSystem", "testStream", p1)
  val envelope0 = new IncomingMessageEnvelope(ssp0, "0", "key0", "value0")
  val envelope1 = new IncomingMessageEnvelope(ssp1, "1", "key1", "value1")

  @Test
  def testProcessMessageFromChooser {
    val taskInstances = Map(p0 -> mock[TaskInstance], p1 -> mock[TaskInstance])
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(taskInstances, consumers, new SamzaContainerMetrics)

    when(consumers.choose).thenReturn(envelope0).thenReturn(envelope1).thenThrow(new StopRunLoop)
    intercept[StopRunLoop] { runLoop.run }
    verify(taskInstances(p0)).process(Matchers.eq(envelope0), anyObject)
    verify(taskInstances(p1)).process(Matchers.eq(envelope1), anyObject)
    runLoop.metrics.envelopes.getCount should equal(2L)
    runLoop.metrics.nullEnvelopes.getCount should equal(0L)
  }

  @Test
  def testNullMessageFromChooser {
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(Map(p0 -> mock[TaskInstance]), consumers, new SamzaContainerMetrics)
    when(consumers.choose).thenReturn(null).thenReturn(null).thenThrow(new StopRunLoop)
    intercept[StopRunLoop] { runLoop.run }
    runLoop.metrics.envelopes.getCount should equal(0L)
    runLoop.metrics.nullEnvelopes.getCount should equal(2L)
  }

  @Test
  def testWindowAndCommitAreCalledRegularly {
    var now = 1400000000000L
    val consumers = mock[SystemConsumers]
    when(consumers.choose).thenReturn(envelope0)

    val runLoop = new RunLoop(
      taskInstances = Map(p0 -> mock[TaskInstance], p1 -> mock[TaskInstance]),
      consumerMultiplexer = consumers,
      metrics = new SamzaContainerMetrics,
      windowMs = 60000, // call window once per minute
      commitMs = 30000, // call commit twice per minute
      clock = () => {
        now += 100 // clock advances by 100 ms every time we look at it
        if (now == 1400000290000L) throw new StopRunLoop // stop after 4 minutes 50 seconds
        now
      })

    intercept[StopRunLoop] { runLoop.run }

    verify(runLoop.taskInstances(p0), times(5)).window(anyObject)
    verify(runLoop.taskInstances(p1), times(5)).window(anyObject)
    verify(runLoop.taskInstances(p0), times(10)).commit
    verify(runLoop.taskInstances(p1), times(10)).commit
  }

  @Test
  def testCommitCurrentTaskManually {
    val taskInstances = Map(p0 -> mock[TaskInstance], p1 -> mock[TaskInstance])
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(taskInstances, consumers, new SamzaContainerMetrics, windowMs = -1, commitMs = -1)

    when(consumers.choose).thenReturn(envelope0).thenReturn(envelope1).thenThrow(new StopRunLoop)
    stubProcess(taskInstances(p0), (envelope, coordinator) => coordinator.commit(RequestScope.CURRENT_TASK))

    intercept[StopRunLoop] { runLoop.run }
    verify(taskInstances(p0), times(1)).commit
    verify(taskInstances(p1), times(0)).commit
  }

  @Test
  def testCommitAllTasksManually {
    val taskInstances = Map(p0 -> mock[TaskInstance], p1 -> mock[TaskInstance])
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(taskInstances, consumers, new SamzaContainerMetrics, windowMs = -1, commitMs = -1)

    when(consumers.choose).thenReturn(envelope0).thenThrow(new StopRunLoop)
    stubProcess(taskInstances(p0), (envelope, coordinator) => coordinator.commit(RequestScope.ALL_TASKS_IN_CONTAINER))

    intercept[StopRunLoop] { runLoop.run }
    verify(taskInstances(p0), times(1)).commit
    verify(taskInstances(p1), times(1)).commit
  }

  @Test
  def testShutdownOnConsensus {
    val taskInstances = Map(p0 -> mock[TaskInstance], p1 -> mock[TaskInstance])
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(taskInstances, consumers, new SamzaContainerMetrics, windowMs = -1, commitMs = -1)

    when(consumers.choose).thenReturn(envelope0).thenReturn(envelope0).thenReturn(envelope1)
    stubProcess(taskInstances(p0), (envelope, coordinator) => coordinator.shutdown(RequestScope.CURRENT_TASK))
    stubProcess(taskInstances(p1), (envelope, coordinator) => coordinator.shutdown(RequestScope.CURRENT_TASK))

    runLoop.run
    verify(taskInstances(p0), times(2)).process(Matchers.eq(envelope0), anyObject)
    verify(taskInstances(p1), times(1)).process(Matchers.eq(envelope1), anyObject)
  }

  @Test
  def testShutdownNow {
    val taskInstances = Map(p0 -> mock[TaskInstance], p1 -> mock[TaskInstance])
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(taskInstances, consumers, new SamzaContainerMetrics, windowMs = -1, commitMs = -1)

    when(consumers.choose).thenReturn(envelope0).thenReturn(envelope1)
    stubProcess(taskInstances(p0), (envelope, coordinator) => coordinator.shutdown(RequestScope.ALL_TASKS_IN_CONTAINER))

    runLoop.run
    verify(taskInstances(p0), times(1)).process(anyObject, anyObject)
    verify(taskInstances(p1), times(0)).process(anyObject, anyObject)
  }

  def anyObject[T] = Matchers.anyObject.asInstanceOf[T]

  // Stub out TaskInstance.process. Mockito really doesn't make this easy. :(
  def stubProcess(taskInstance: TaskInstance, process: (IncomingMessageEnvelope, ReadableCoordinator) => Unit) {
    when(taskInstance.process(anyObject, anyObject)).thenAnswer(new Answer[Unit]() {
      override def answer(invocation: InvocationOnMock) {
        val envelope    = invocation.getArguments()(0).asInstanceOf[IncomingMessageEnvelope]
        val coordinator = invocation.getArguments()(1).asInstanceOf[ReadableCoordinator]
        process(envelope, coordinator)
      }
    })
  }
}
