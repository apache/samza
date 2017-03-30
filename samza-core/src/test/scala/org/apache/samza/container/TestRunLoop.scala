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


import java.util.concurrent.TimeUnit

import org.apache.samza.Partition
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.SlidingTimeWindowReservoir
import org.apache.samza.metrics.Timer
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemConsumers
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.task.TaskCoordinator.RequestScope
import org.apache.samza.task.ReadableCoordinator
import org.apache.samza.util.Clock
import org.junit.Assert._
import org.junit.Test
import org.mockito.Matchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers => ScalaTestMatchers}

class TestRunLoop extends AssertionsForJUnit with MockitoSugar with ScalaTestMatchers {
  class StopRunLoop extends RuntimeException

  val p0 = new Partition(0)
  val p1 = new Partition(1)
  val taskName0 = new TaskName(p0.toString)
  val taskName1 = new TaskName(p1.toString)
  val ssp0 = new SystemStreamPartition("testSystem", "testStream", p0)
  val ssp1 = new SystemStreamPartition("testSystem", "testStream", p1)
  val envelope0 = new IncomingMessageEnvelope(ssp0, "0", "key0", "value0")
  val envelope1 = new IncomingMessageEnvelope(ssp1, "1", "key1", "value1")

  def getMockTaskInstances: Map[TaskName, TaskInstance] = {
    val ti0 = mock[TaskInstance]
    when(ti0.systemStreamPartitions).thenReturn(Set(ssp0))
    when(ti0.taskName).thenReturn(taskName0)

    val ti1 = mock[TaskInstance]
    when(ti1.systemStreamPartitions).thenReturn(Set(ssp1))
    when(ti1.taskName).thenReturn(taskName1)

    Map(taskName0 -> ti0, taskName1 -> ti1)
  }

  @Test
  def testProcessMessageFromChooser {
    val taskInstances = getMockTaskInstances
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(taskInstances, consumers, new SamzaContainerMetrics, TimeUnit.SECONDS.toMillis(1))

    when(consumers.choose()).thenReturn(envelope0).thenReturn(envelope1).thenThrow(new StopRunLoop)
    intercept[StopRunLoop] { runLoop.run }
    verify(taskInstances(taskName0)).process(Matchers.eq(envelope0), anyObject, anyObject)
    verify(taskInstances(taskName1)).process(Matchers.eq(envelope1), anyObject, anyObject)
    runLoop.metrics.envelopes.getCount should equal(2L)
    runLoop.metrics.nullEnvelopes.getCount should equal(0L)
  }

  @Test
  def testNullMessageFromChooser {
    val consumers = mock[SystemConsumers]
    val map = getMockTaskInstances - taskName1 // This test only needs p0
    val runLoop = new RunLoop(map, consumers, new SamzaContainerMetrics, TimeUnit.SECONDS.toMillis(1))
    when(consumers.choose()).thenReturn(null).thenReturn(null).thenThrow(new StopRunLoop)
    intercept[StopRunLoop] { runLoop.run }
    runLoop.metrics.envelopes.getCount should equal(0L)
    runLoop.metrics.nullEnvelopes.getCount should equal(2L)
  }

  @Test
  def testWindowAndCommitAreCalledRegularly {
    var now = 1400000000000L
    val consumers = mock[SystemConsumers]
    when(consumers.choose()).thenReturn(envelope0)

    val runLoop = new RunLoop(
      taskInstances = getMockTaskInstances,
      consumerMultiplexer = consumers,
      metrics = new SamzaContainerMetrics,
      TimeUnit.SECONDS.toMillis(1),
      windowMs = 60000, // call window once per minute
      commitMs = 30000, // call commit twice per minute
      clock = () => {
        now += 100000000L // clock advances by 100 ms every time we look at it
        if (now == 1690000000000L) throw new StopRunLoop // stop after 4 minutes 50 seconds
        now
      })

    intercept[StopRunLoop] { runLoop.run }

    verify(runLoop.taskInstances(taskName0), times(4)).window(anyObject)
    verify(runLoop.taskInstances(taskName1), times(4)).window(anyObject)
    verify(runLoop.taskInstances(taskName0), times(9)).commit
    verify(runLoop.taskInstances(taskName1), times(9)).commit
  }

  @Test
  def testCommitCurrentTaskManually {
    val taskInstances = getMockTaskInstances
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(taskInstances, consumers, new SamzaContainerMetrics,
      TimeUnit.SECONDS.toMillis(1), windowMs = -1, commitMs = -1)

    when(consumers.choose()).thenReturn(envelope0).thenReturn(envelope1).thenThrow(new StopRunLoop)
    stubProcess(taskInstances(taskName0), (envelope, coordinator) => coordinator.commit(RequestScope.CURRENT_TASK))

    intercept[StopRunLoop] { runLoop.run }
    verify(taskInstances(taskName0), times(1)).commit
    verify(taskInstances(taskName1), times(0)).commit
  }

  @Test
  def testCommitAllTasksManually {
    val taskInstances = getMockTaskInstances
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(taskInstances, consumers, new SamzaContainerMetrics,
      TimeUnit.SECONDS.toMillis(1), windowMs = -1, commitMs = -1)

    when(consumers.choose()).thenReturn(envelope0).thenThrow(new StopRunLoop)
    stubProcess(taskInstances(taskName0), (envelope, coordinator) => coordinator.commit(RequestScope.ALL_TASKS_IN_CONTAINER))

    intercept[StopRunLoop] { runLoop.run }
    verify(taskInstances(taskName0), times(1)).commit
    verify(taskInstances(taskName1), times(1)).commit
  }

  @Test
  def testShutdownOnConsensus {
    val taskInstances = getMockTaskInstances
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(taskInstances, consumers, new SamzaContainerMetrics,
      TimeUnit.SECONDS.toMillis(1), windowMs = -1, commitMs = -1)

    when(consumers.choose()).thenReturn(envelope0).thenReturn(envelope0).thenReturn(envelope1)
    stubProcess(taskInstances(taskName0), (envelope, coordinator) => coordinator.shutdown(RequestScope.CURRENT_TASK))
    stubProcess(taskInstances(taskName1), (envelope, coordinator) => coordinator.shutdown(RequestScope.CURRENT_TASK))

    runLoop.run
    verify(taskInstances(taskName0), times(2)).process(Matchers.eq(envelope0), anyObject, anyObject)
    verify(taskInstances(taskName1), times(1)).process(Matchers.eq(envelope1), anyObject, anyObject)
  }

  @Test
  def testShutdownNow {
    val taskInstances = getMockTaskInstances
    val consumers = mock[SystemConsumers]
    val runLoop = new RunLoop(taskInstances, consumers, new SamzaContainerMetrics
      , TimeUnit.SECONDS.toMillis(1), windowMs = -1, commitMs = -1)

    when(consumers.choose()).thenReturn(envelope0).thenReturn(envelope1)
    stubProcess(taskInstances(taskName0), (envelope, coordinator) => coordinator.shutdown(RequestScope.ALL_TASKS_IN_CONTAINER))

    runLoop.run
    verify(taskInstances(taskName0), times(1)).process(anyObject, anyObject, anyObject)
    verify(taskInstances(taskName1), times(0)).process(anyObject, anyObject, anyObject)
  }

  def anyObject[T] = Matchers.anyObject.asInstanceOf[T]

  // Stub out TaskInstance.process. Mockito really doesn't make this easy. :(
  def stubProcess(taskInstance: TaskInstance, process: (IncomingMessageEnvelope, ReadableCoordinator) => Unit) {
    when(taskInstance.process(anyObject, anyObject, anyObject)).thenAnswer(new Answer[Unit]() {
      override def answer(invocation: InvocationOnMock) {
        val envelope = invocation.getArguments()(0).asInstanceOf[IncomingMessageEnvelope]
        val coordinator = invocation.getArguments()(1).asInstanceOf[ReadableCoordinator]
        process(envelope, coordinator)
      }
    })
  }

  @Test
  def testUpdateTimerCorrectly {
    var now = 0L
    val consumers = mock[SystemConsumers]
    when(consumers.choose()).thenReturn(envelope0)
    val clock = new Clock {
      var c = 0L
      def currentTimeMillis: Long = {
        c += 1L
        c
      }
    }
    val testMetrics = new SamzaContainerMetrics("test", new MetricsRegistryMap() {
      override def newTimer(group: String, name: String) = {
        newTimer(group, new Timer(name, new SlidingTimeWindowReservoir(300000, clock)))
      }
    })

    val runLoop = new RunLoop(
      taskInstances = getMockTaskInstances,
      consumerMultiplexer = consumers,
      metrics = testMetrics,
      TimeUnit.SECONDS.toMillis(1),
      windowMs = 1L,
      commitMs = 1L,
      clock = () => {
        now += 1000000L
        // clock() is called 15 times totally in RunLoop
        // stop the runLoop after one run
        if (now == 15000000L) throw new StopRunLoop
        now
      })
    intercept[StopRunLoop] { runLoop.run }

    testMetrics.chooseNs.getSnapshot.getAverage should equal(1000000L)
    testMetrics.windowNs.getSnapshot.getAverage should equal(1000000L)
    testMetrics.processNs.getSnapshot.getAverage should equal(1000000L)
    testMetrics.commitNs.getSnapshot.getAverage should equal(1000000L)

    now = 0L
    intercept[StopRunLoop] { runLoop.run }
    // after two loops
    testMetrics.chooseNs.getSnapshot.getSize should equal(3)
    testMetrics.windowNs.getSnapshot.getSize should equal(2)
    testMetrics.processNs.getSnapshot.getSize should equal(2)
    testMetrics.commitNs.getSnapshot.getSize should equal(2)
  }

  @Test
  def testCommitAndWindowNotCalledImmediatelyOnStartUp {
    var now = 0L
    val consumers = mock[SystemConsumers]
    val testMetrics = new SamzaContainerMetrics
    val runLoop = new RunLoop(
      taskInstances = getMockTaskInstances,
      consumerMultiplexer = consumers,
      metrics = testMetrics,
      TimeUnit.SECONDS.toMillis(1),
      commitMs = 1L,
      windowMs = 1L,
      clock = () => {
        now += 1000000L
        if (now == 13000000L) throw new StopRunLoop
        now
      }
    )

    intercept[StopRunLoop] {
      runLoop.run
    }
    now = 0L
    intercept[StopRunLoop] {
      runLoop.run
    }

    // after 2 run loops number of commits and windows should be 1,
    // as commit and window should not be called immediately on startup
    testMetrics.commits.getCount should equal(1L)
    testMetrics.windows.getCount should equal(1L)
  }

  @Test
  def testGetSystemStreamPartitionToTaskInstancesMapping {
    val ti0 = mock[TaskInstance]
    val ti1 = mock[TaskInstance]
    val ti2 = mock[TaskInstance]
    when(ti0.systemStreamPartitions).thenReturn(Set(ssp0))
    when(ti1.systemStreamPartitions).thenReturn(Set(ssp1))
    when(ti2.systemStreamPartitions).thenReturn(Set(ssp1))

    val mockTaskInstances = Map(taskName0 -> ti0, taskName1 -> ti1, new TaskName("2") -> ti2)
    val runLoop = new RunLoop(mockTaskInstances, null, new SamzaContainerMetrics, TimeUnit.SECONDS.toMillis(1))
    val expected = Map(ssp0 -> List(ti0), ssp1 -> List(ti1, ti2))
    assertEquals(expected, runLoop.getSystemStreamPartitionToTaskInstancesMapping)
  }
}
