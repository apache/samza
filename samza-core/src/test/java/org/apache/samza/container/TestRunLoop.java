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

package org.apache.samza.container;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.ReadableCoordinator;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCallbackFactory;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;


public class TestRunLoop {
  // Immutable objects shared by all test methods.
  private final String runId = "foo";
  private final ExecutorService executor = null;
  private final SamzaContainerMetrics containerMetrics = new SamzaContainerMetrics("container", new MetricsRegistryMap(), "");
  private final long windowMs = -1;
  private final long commitMs = -1;
  private final long callbackTimeoutMs = 0;

  private final long drainCallbackTimeoutMs = 0;
  private final long maxThrottlingDelayMs = 0;
  private final long maxIdleMs = 10;
  private final Partition p0 = new Partition(0);
  private final Partition p1 = new Partition(1);
  private final TaskName taskName0 = new TaskName(p0.toString());
  private final TaskName taskName1 = new TaskName(p1.toString());
  private final SystemStreamPartition sspA0 = new SystemStreamPartition("testSystem", "testStreamA", p0);
  private final SystemStreamPartition sspA1 = new SystemStreamPartition("testSystem", "testStreamA", p1);
  private final SystemStreamPartition sspB0 = new SystemStreamPartition("testSystem", "testStreamB", p0);
  private final SystemStreamPartition sspB1 = new SystemStreamPartition("testSystem", "testStreamB", p1);
  private final IncomingMessageEnvelope envelopeA00 = new IncomingMessageEnvelope(sspA0, "0", "key0", "value0");
  private final IncomingMessageEnvelope envelopeA11 = new IncomingMessageEnvelope(sspA1, "1", "key1", "value1");
  private final IncomingMessageEnvelope envelopeA01 = new IncomingMessageEnvelope(sspA0, "1", "key0", "value0");
  private final IncomingMessageEnvelope envelopeB00 = new IncomingMessageEnvelope(sspB0, "0", "key0", "value0");
  private final IncomingMessageEnvelope envelopeB11 = new IncomingMessageEnvelope(sspB1, "1", "key1", "value1");
  private final IncomingMessageEnvelope sspA0EndOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(sspA0);
  private final IncomingMessageEnvelope sspA1EndOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(sspA1);
  private final IncomingMessageEnvelope sspB0EndOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(sspB0);
  private final IncomingMessageEnvelope sspB1EndOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(sspB1);
  private final IncomingMessageEnvelope sspA0Drain = IncomingMessageEnvelope.buildDrainMessage(sspA0, runId);
  private final IncomingMessageEnvelope sspA1Drain = IncomingMessageEnvelope.buildDrainMessage(sspA1, runId);
  private final IncomingMessageEnvelope sspB0Drain = IncomingMessageEnvelope.buildDrainMessage(sspB0, runId);
  private final IncomingMessageEnvelope sspB1Drain = IncomingMessageEnvelope.buildDrainMessage(sspB1, runId);

  @Rule
  public Timeout maxTestDurationInSeconds = Timeout.seconds(120);

  @Test
  public void testProcessMultipleTasks() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    RunLoopTask task1 = getMockRunLoopTask(taskName1, sspA1);

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false, 1, "foo", false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(envelopeA11).thenReturn(sspA0EndOfStream).thenReturn(
        sspA1EndOfStream).thenReturn(null);
    runLoop.run();

    verify(task0).process(eq(envelopeA00), any(), any());
    verify(task1).process(eq(envelopeA11), any(), any());

    assertEquals(4L, containerMetrics.envelopes().getCount());
  }

  @Test
  public void testProcessInOrder() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(envelopeA01).thenReturn(sspA0EndOfStream).thenReturn(null);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);

    Map<TaskName, RunLoopTask> tasks = ImmutableMap.of(taskName0, task0);
    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    runLoop.run();

    InOrder inOrder = inOrder(task0);
    inOrder.verify(task0).process(eq(envelopeA00), any(), any());
    inOrder.verify(task0).process(eq(envelopeA01), any(), any());
  }

  @Test
  public void testProcessCallbacksCompletedOutOfOrder() {
    int maxMessagesInFlight = 2;
    ExecutorService taskExecutor = Executors.newFixedThreadPool(1);
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    OffsetManager offsetManager = mock(OffsetManager.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    when(task0.offsetManager()).thenReturn(offsetManager);
    CountDownLatch firstMessageBarrier = new CountDownLatch(1);
    doAnswer(invocation -> {
      ReadableCoordinator coordinator = invocation.getArgumentAt(1, ReadableCoordinator.class);
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();
      taskExecutor.submit(() -> {
        firstMessageBarrier.await();
        coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
        coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
        callback.complete();
        return null;
      });
      return null;
    }).when(task0).process(eq(envelopeA00), any(), any());

    doAnswer(invocation -> {
      assertEquals(1, task0.metrics().messagesInFlight().getValue());
      assertEquals(0, task0.metrics().asyncCallbackCompleted().getCount());

      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();
      callback.complete();
      firstMessageBarrier.countDown();
      return null;
    }).when(task0).process(eq(envelopeA01), any(), any());

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);

    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(envelopeA01).thenReturn(null);
    runLoop.run();

    InOrder inOrder = inOrder(task0);
    inOrder.verify(task0).process(eq(envelopeA00), any(), any());
    inOrder.verify(task0).process(eq(envelopeA01), any(), any());

    verify(offsetManager).update(eq(taskName0), eq(sspA0), eq(envelopeA00.getOffset()));

    assertEquals(2L, containerMetrics.processes().getCount());
    assertEquals(1L, containerMetrics.containerRunning().getValue());
  }

  @Test
  public void testProcessElasticityEnabled() {

    TaskName taskName0 = new TaskName(p0.toString() + " 0");
    SystemStreamPartition ssp = new SystemStreamPartition("testSystem", "testStreamA", p0);
    SystemStreamPartition ssp0 = new SystemStreamPartition("testSystem", "testStreamA", p0, 0);
    SystemStreamPartition ssp1 = new SystemStreamPartition("testSystem", "testStreamA", p0, 1);

    // create two IME such that one of their ssp keybucket maps to ssp0 and the other one maps to ssp1
    // task in the runloop should process only the first ime (aka the one whose ssp keybucket is ssp0)
    IncomingMessageEnvelope envelope00 = spy(new IncomingMessageEnvelope(ssp, "0", "key0", "value0"));
    IncomingMessageEnvelope envelope01 = spy(new IncomingMessageEnvelope(ssp, "1", "key0", "value0"));
    when(envelope00.getSystemStreamPartition(2)).thenReturn(ssp0);
    when(envelope01.getSystemStreamPartition(2)).thenReturn(ssp1);


    // have a single task in the run loop that processes ssp0 -> 0th keybucket of ssp
    RunLoopTask task0 = getMockRunLoopTask(taskName0, ssp0);
    doAnswer(invocation -> {
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();
      callback.complete();
      return null;
    }).when(task0).process(eq(envelope00), any(), any());

    doAnswer(invocation -> {
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();
      callback.complete();
      return null;
    }).when(task0).process(eq(envelope01), any(), any());

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.choose(false)).thenReturn(envelope00).thenReturn(envelope01).thenReturn(sspA0EndOfStream).thenReturn(null);

    Map<TaskName, RunLoopTask> tasks = ImmutableMap.of(taskName0, task0);
    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, 0, containerMetrics, () -> 0L,
        false, 2, null, false);
    runLoop.run();

    verify(task0).process(eq(envelope00), any(), any());
    verify(task0, never()).process(eq(envelope01), any(), any());
    assertEquals(2, containerMetrics.envelopes().getCount()); // envelop00 and end of stream
    assertEquals(1, containerMetrics.processes().getCount()); // only envelope00 and not envelope01 and not end of stream
  }

  @Test
  public void testDrainForTasksWithSingleSSP() {
    TaskName taskName0 = new TaskName(p0.toString() + " 0");
    TaskName taskName1 = new TaskName(p1.toString() + " 1");

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    RunLoopTask task1 = getMockRunLoopTask(taskName1, sspA1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    // insert all envelopes followed by drain messages
    when(consumerMultiplexer.choose(false))
        .thenReturn(envelopeA00).thenReturn(envelopeA11)
        .thenReturn(sspA0Drain).thenReturn(sspA1Drain);

    Map<TaskName, RunLoopTask> tasks = ImmutableMap.of(taskName0, task0, taskName1, task1);
    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, 0, containerMetrics, () -> 0L,
        false, 1, runId, false);
    runLoop.run();

    // check if process was called once for each task
    verify(task0, times(1)).process(any(), any(), any());
    verify(task1, times(1)).process(any(), any(), any());
    // check if drain was called once for each task followed by commit
    verify(task0, times(1)).drain(any());
    verify(task1, times(1)).drain(any());
    verify(task0, times(1)).commit();
    verify(task1, times(1)).commit();
  }

  @Test
  public void testDrainForTasksWithMultipleSSP() {
    TaskName taskName0 = new TaskName(p0.toString() + " 0");
    TaskName taskName1 = new TaskName(p1.toString() + " 1");

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0, sspB0);
    RunLoopTask task1 = getMockRunLoopTask(taskName1, sspA1, sspB1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    // insert all envelopes followed by drain messages
    when(consumerMultiplexer.choose(false))
        .thenReturn(envelopeA00).thenReturn(envelopeA11).thenReturn(envelopeB00).thenReturn(envelopeB11)
        .thenReturn(sspA0Drain).thenReturn(sspA1Drain).thenReturn(sspB0Drain).thenReturn(sspB1Drain);

    Map<TaskName, RunLoopTask> tasks = ImmutableMap.of(taskName0, task0, taskName1, task1);
    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, 0, containerMetrics, () -> 0L,
        false, 1, runId, false);
    runLoop.run();

    // check if process was called twice for each task
    verify(task0, times(2)).process(any(), any(), any());
    verify(task1, times(2)).process(any(), any(), any());
    // check if drain was called once for each task followed by commit
    verify(task0, times(1)).drain(any());
    verify(task1, times(1)).drain(any());
    verify(task0, times(1)).commit();
    verify(task1, times(1)).commit();
  }

  @Test
  public void testWindow() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);

    int maxMessagesInFlight = 1;
    long windowMs = 1;
    RunLoopTask task = getMockRunLoopTask(taskName0, sspA0);
    when(task.isWindowableTask()).thenReturn(true);

    final AtomicInteger windowCount = new AtomicInteger(0);
    doAnswer(x -> {
      windowCount.incrementAndGet();
      if (windowCount.get() == 4) {
        x.getArgumentAt(0, ReadableCoordinator.class).shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
      }
      return null;
    }).when(task).window(any());

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task);

    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(null);
    runLoop.run();

    verify(task, times(4)).window(any());
  }

  @Test
  public void testCommitSingleTask() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    doAnswer(invocation -> {
      ReadableCoordinator coordinator = invocation.getArgumentAt(1, ReadableCoordinator.class);
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();

      coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
      coordinator.shutdown(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);

      callback.complete();
      return null;
    }).when(task0).process(eq(envelopeA00), any(), any());

    RunLoopTask task1 = getMockRunLoopTask(taskName1, sspA1);

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(this.taskName0, task0);
    tasks.put(taskName1, task1);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    //have a null message in between to make sure task0 finishes processing and invoke the commit
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(envelopeA11).thenReturn(null);

    runLoop.run();

    verify(task0).process(any(), any(), any());
    verify(task1).process(any(), any(), any());

    verify(task0).commit();
    verify(task1, never()).commit();
  }

  @Test
  public void testCommitAllTasks() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    doAnswer(invocation -> {
      ReadableCoordinator coordinator = invocation.getArgumentAt(1, ReadableCoordinator.class);
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();

      coordinator.commit(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
      coordinator.shutdown(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);

      callback.complete();
      return null;
    }).when(task0).process(eq(envelopeA00), any(), any());

    RunLoopTask task1 = getMockRunLoopTask(taskName1, sspA1);

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(this.taskName0, task0);
    tasks.put(taskName1, task1);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    //have a null message in between to make sure task0 finishes processing and invoke the commit
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(envelopeA11).thenReturn(null);

    runLoop.run();

    verify(task0).process(any(), any(), any());
    verify(task1).process(any(), any(), any());

    verify(task0).commit();
    verify(task1).commit();
  }

  @Test
  public void testShutdownOnConsensus() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);

    int maxMessagesInFlight = 1;
    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    doAnswer(invocation -> {
      ReadableCoordinator coordinator = invocation.getArgumentAt(1, ReadableCoordinator.class);
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);

      TaskCallback callback = callbackFactory.createCallback();
      coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
      callback.complete();
      return null;
    }).when(task0).process(eq(envelopeA00), any(), any());

    RunLoopTask task1 = getMockRunLoopTask(taskName1, sspA1);
    doAnswer(invocation -> {
      ReadableCoordinator coordinator = invocation.getArgumentAt(1, ReadableCoordinator.class);
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);

      TaskCallback callback = callbackFactory.createCallback();
      coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
      callback.complete();
      return null;
    }).when(task1).process(eq(envelopeA11), any(), any());

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    // consensus is reached after envelope1 is processed.
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(envelopeA11).thenReturn(null);
    runLoop.run();

    verify(task0).process(any(), any(), any());
    verify(task1).process(any(), any(), any());

    assertEquals(2L, containerMetrics.envelopes().getCount());
    assertEquals(2L, containerMetrics.processes().getCount());
  }

  @Test
  public void testEndOfStreamWithMultipleTasks() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0, sspB0);
    RunLoopTask task1 = getMockRunLoopTask(taskName1, sspA1, sspB1);

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();

    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false))
      .thenReturn(envelopeA00)
      .thenReturn(envelopeA11)
      .thenReturn(envelopeB00)
      .thenReturn(envelopeB11)
      .thenReturn(sspA0EndOfStream)
      .thenReturn(sspB0EndOfStream)
      .thenReturn(sspB1EndOfStream)
      .thenReturn(sspA1EndOfStream)
      .thenReturn(null);

    runLoop.run();

    verify(task0).process(eq(envelopeA00), any(), any());
    verify(task0).process(eq(envelopeB00), any(), any());
    verify(task0).endOfStream(any());

    verify(task1).process(eq(envelopeA11), any(), any());
    verify(task1).process(eq(envelopeB11), any(), any());
    verify(task1).endOfStream(any());

    assertEquals(8L, containerMetrics.envelopes().getCount());
  }

  @Test
  public void testEndOfStreamWaitsForInFlightMessages() {
    int maxMessagesInFlight = 2;
    ExecutorService taskExecutor = Executors.newFixedThreadPool(1);
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    OffsetManager offsetManager = mock(OffsetManager.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    when(task0.offsetManager()).thenReturn(offsetManager);
    CountDownLatch firstMessageBarrier = new CountDownLatch(2);
    doAnswer(invocation -> {
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();
      taskExecutor.submit(() -> {
        firstMessageBarrier.await();
        callback.complete();
        return null;
      });
      return null;
    }).when(task0).process(eq(envelopeA00), any(), any());

    doAnswer(invocation -> {
      assertEquals(1, task0.metrics().messagesInFlight().getValue());

      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();
      callback.complete();
      firstMessageBarrier.countDown();
      return null;
    }).when(task0).process(eq(envelopeA01), any(), any());

    doAnswer(invocation -> {
      assertEquals(0, task0.metrics().messagesInFlight().getValue());
      assertEquals(2, task0.metrics().asyncCallbackCompleted().getCount());

      return null;
    }).when(task0).endOfStream(any());

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);

    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(envelopeA01).thenReturn(sspA0EndOfStream)
        .thenAnswer(invocation -> {
          // this ensures that the end of stream message has passed through run loop BEFORE the last remaining in flight message completes
          firstMessageBarrier.countDown();
          return null;
        });

    runLoop.run();

    verify(task0).endOfStream(any());
  }

  @Test
  public void testDrainWaitsForInFlightMessages() {
    int maxMessagesInFlight = 2;
    ExecutorService taskExecutor = Executors.newFixedThreadPool(1);
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    OffsetManager offsetManager = mock(OffsetManager.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    when(task0.offsetManager()).thenReturn(offsetManager);
    CountDownLatch firstMessageBarrier = new CountDownLatch(2);
    doAnswer(invocation -> {
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();
      taskExecutor.submit(() -> {
        firstMessageBarrier.await();
        callback.complete();
        return null;
      });
      return null;
    }).when(task0).process(eq(envelopeA00), any(), any());

    doAnswer(invocation -> {
      assertEquals(1, task0.metrics().messagesInFlight().getValue());

      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();
      callback.complete();
      firstMessageBarrier.countDown();
      return null;
    }).when(task0).process(eq(envelopeA01), any(), any());

    doAnswer(invocation -> {
      assertEquals(0, task0.metrics().messagesInFlight().getValue());
      assertEquals(2, task0.metrics().asyncCallbackCompleted().getCount());

      return null;
    }).when(task0).drain(any());

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);

    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false,
        1, runId, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(envelopeA01).thenReturn(sspA0Drain)
        .thenAnswer(invocation -> {
          // this ensures that the drain message has passed through run loop BEFORE the flight message
          // completes
          firstMessageBarrier.countDown();
          return null;
        });

    runLoop.run();

    verify(task0).drain(any());
  }

  @Test
  public void testEndOfStreamCommitBehavior() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    doAnswer(invocation -> {
      ReadableCoordinator coordinator = invocation.getArgumentAt(0, ReadableCoordinator.class);

      coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
      return null;
    }).when(task0).endOfStream(any());

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();

    tasks.put(taskName0, task0);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(sspA0EndOfStream).thenReturn(null);

    runLoop.run();

    InOrder inOrder = inOrder(task0);

    inOrder.verify(task0).endOfStream(any());
    inOrder.verify(task0).commit();
  }

  @Test
  public void testDrainCommitBehavior() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    Map<TaskName, RunLoopTask> tasks = new HashMap<>();

    tasks.put(taskName0, task0);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false,
        1, runId, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(sspA0Drain).thenReturn(null);

    runLoop.run();

    InOrder inOrder = inOrder(task0);

    inOrder.verify(task0).drain(any());
    inOrder.verify(task0).commit();
  }

  @Test
  public void testCommitWithMessageInFlightWhenAsyncCommitIsEnabled() {
    int maxMessagesInFlight = 2;
    ExecutorService taskExecutor = Executors.newFixedThreadPool(2);
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    OffsetManager offsetManager = mock(OffsetManager.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    when(task0.offsetManager()).thenReturn(offsetManager);
    CountDownLatch firstMessageBarrier = new CountDownLatch(1);
    doAnswer(invocation -> {
      ReadableCoordinator coordinator = invocation.getArgumentAt(1, ReadableCoordinator.class);
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();

      taskExecutor.submit(() -> {
        firstMessageBarrier.await();
        coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
        callback.complete();
        return null;
      });
      return null;
    }).when(task0).process(eq(envelopeA00), any(), any());

    CountDownLatch secondMessageBarrier = new CountDownLatch(1);
    doAnswer(invocation -> {
      ReadableCoordinator coordinator = invocation.getArgumentAt(1, ReadableCoordinator.class);
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      TaskCallback callback = callbackFactory.createCallback();

      taskExecutor.submit(() -> {
        // let the first message proceed to ask for a commit
        firstMessageBarrier.countDown();
        // block this message until commit is executed
        secondMessageBarrier.await();
        coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
        callback.complete();
        return null;
      });
      return null;
    }).when(task0).process(eq(envelopeA01), any(), any());

    doAnswer(invocation -> {
      assertEquals(1, task0.metrics().asyncCallbackCompleted().getCount());
      assertEquals(1, task0.metrics().messagesInFlight().getValue());

      secondMessageBarrier.countDown();
      return null;
    }).when(task0).commit();

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);

    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, true);
    when(consumerMultiplexer.choose(false)).thenReturn(envelopeA00).thenReturn(envelopeA01).thenReturn(null);
    runLoop.run();

    InOrder inOrder = inOrder(task0);
    inOrder.verify(task0).process(eq(envelopeA00), any(), any());
    inOrder.verify(task0).process(eq(envelopeA01), any(), any());
    inOrder.verify(task0).commit();
  }

  @Test(expected = SamzaException.class)
  public void testExceptionIsPropagated() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);

    RunLoopTask task0 = getMockRunLoopTask(taskName0, sspA0);
    doAnswer(invocation -> {
      TaskCallbackFactory callbackFactory = invocation.getArgumentAt(2, TaskCallbackFactory.class);
      callbackFactory.createCallback().failure(new Exception("Intentional failure"));
      return null;
    }).when(task0).process(eq(envelopeA00), any(), any());

    Map<TaskName, RunLoopTask> tasks = ImmutableMap.of(taskName0, task0);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, drainCallbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);

    when(consumerMultiplexer.choose(false))
        .thenReturn(envelopeA00)
        .thenReturn(sspA0EndOfStream)
        .thenReturn(null);

    runLoop.run();
  }

  private RunLoopTask getMockRunLoopTask(TaskName taskName, SystemStreamPartition ... ssps) {
    RunLoopTask task0 = mock(RunLoopTask.class);
    when(task0.systemStreamPartitions()).thenReturn(new HashSet<>(Arrays.asList(ssps)));
    when(task0.metrics()).thenReturn(new TaskInstanceMetrics("test", new MetricsRegistryMap(), ""));
    when(task0.taskName()).thenReturn(taskName);
    return task0;
  }
}
