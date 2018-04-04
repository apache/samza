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

package org.apache.samza.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.Partition;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.config.Config;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstance;
import org.apache.samza.container.TaskInstanceExceptionHandler;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.TestSystemConsumers;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.Timeout;
import scala.Option;
import scala.collection.JavaConverters;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestAsyncRunLoop {
  // Immutable objects shared by all test methods.
  private final ExecutorService executor = null;
  private final SamzaContainerMetrics containerMetrics = new SamzaContainerMetrics("container", new MetricsRegistryMap());
  private final long windowMs = -1;
  private final long commitMs = -1;
  private final long callbackTimeoutMs = 0;
  private final long maxThrottlingDelayMs = 0;
  private final long maxIdleMs = 10;
  private final Partition p0 = new Partition(0);
  private final Partition p1 = new Partition(1);
  private final TaskName taskName0 = new TaskName(p0.toString());
  private final TaskName taskName1 = new TaskName(p1.toString());
  private final SystemStreamPartition ssp0 = new SystemStreamPartition("testSystem", "testStream", p0);
  private final SystemStreamPartition ssp1 = new SystemStreamPartition("testSystem", "testStream", p1);
  private final IncomingMessageEnvelope envelope0 = new IncomingMessageEnvelope(ssp0, "0", "key0", "value0");
  private final IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(ssp1, "1", "key1", "value1");
  private final IncomingMessageEnvelope envelope3 = new IncomingMessageEnvelope(ssp0, "1", "key0", "value0");
  private final IncomingMessageEnvelope ssp0EndOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp0);
  private final IncomingMessageEnvelope ssp1EndOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp1);

  TaskInstance createTaskInstance(AsyncStreamTask task, TaskName taskName, SystemStreamPartition ssp, OffsetManager manager, SystemConsumers consumers) {
    TaskInstanceMetrics taskInstanceMetrics = new TaskInstanceMetrics("task", new MetricsRegistryMap());
    scala.collection.immutable.Set<SystemStreamPartition> sspSet = JavaConverters.asScalaSetConverter(Collections.singleton(ssp)).asScala().toSet();
    return new TaskInstance(task, taskName, mock(Config.class), taskInstanceMetrics,
        null, consumers, mock(TaskInstanceCollector.class), mock(SamzaContainerContext.class),
        manager, null, null, null, sspSet, new TaskInstanceExceptionHandler(taskInstanceMetrics,
        new scala.collection.immutable.HashSet<String>()), null, null, null);
  }

  interface TestCode {
    void run(TaskCallback callback);
  }

  class TestTask implements AsyncStreamTask, WindowableTask, EndOfStreamListenerTask {
    private final boolean shutdown;
    private final boolean commit;
    private final boolean success;
    private final ExecutorService callbackExecutor = Executors.newFixedThreadPool(4);

    private AtomicInteger completed = new AtomicInteger(0);
    private TestCode callbackHandler = null;
    private TestCode commitHandler = null;
    private TaskCoordinator.RequestScope commitRequest = null;
    private TaskCoordinator.RequestScope shutdownRequest = TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER;

    private CountDownLatch processedMessagesLatch = null;

    private volatile int windowCount = 0;
    private volatile int processed = 0;
    private volatile int committed = 0;

    private int maxMessagesInFlight;

    TestTask(boolean success, boolean commit, boolean shutdown, CountDownLatch processedMessagesLatch) {
      this.success = success;
      this.shutdown = shutdown;
      this.commit = commit;
      this.processedMessagesLatch = processedMessagesLatch;
    }

    TestTask(boolean success, boolean commit, boolean shutdown,
             CountDownLatch processedMessagesLatch, int maxMessagesInFlight) {
      this(success, commit, shutdown, processedMessagesLatch);
      this.maxMessagesInFlight = maxMessagesInFlight;
    }

    @Override
    public void processAsync(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator, TaskCallback callback) {

      if (maxMessagesInFlight == 1) {
        assertEquals(processed, completed.get());
      }

      processed++;

      if (commit) {
        if (commitHandler != null) {
          callbackExecutor.submit(() -> commitHandler.run(callback));
        }
        if (commitRequest != null) {
          coordinator.commit(commitRequest);
        }
        committed++;
      }

      if (shutdown) {
        coordinator.shutdown(shutdownRequest);
      }

      callbackExecutor.submit(() -> {
          if (callbackHandler != null) {
            callbackHandler.run(callback);
          }

          completed.incrementAndGet();

          if (success) {
            callback.complete();
          } else {
            callback.failure(new Exception("process failure"));
          }

          if (processedMessagesLatch != null) {
            processedMessagesLatch.countDown();
          }
        });
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
      windowCount++;

      if (shutdown && windowCount == 4) {
        coordinator.shutdown(shutdownRequest);
      }
    }

    @Override
    public void onEndOfStream(MessageCollector collector, TaskCoordinator coordinator) {
      coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    }

    void setShutdownRequest(TaskCoordinator.RequestScope shutdownRequest) {
      this.shutdownRequest = shutdownRequest;
    }

    void setCommitRequest(TaskCoordinator.RequestScope commitRequest) {
      this.commitRequest = commitRequest;
    }
  }

  @Rule
  public Timeout maxTestDurationInSeconds = Timeout.seconds(120);

  @Test
  public void testProcessMultipleTasks() throws Exception {
    CountDownLatch task0ProcessedMessages = new CountDownLatch(1);
    CountDownLatch task1ProcessedMessages = new CountDownLatch(1);
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = new TestTask(true, true, false, task0ProcessedMessages);
    TestTask task1 = new TestTask(true, false, true, task1ProcessedMessages);
    TaskInstance t0 = createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer);
    TaskInstance t1 = createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer);

    Map<TaskName, TaskInstance> tasks = new HashMap<>();
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    int maxMessagesInFlight = 1;
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics,
                                            () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope1).thenReturn(null);
    runLoop.run();

    task0ProcessedMessages.await();
    task1ProcessedMessages.await();

    assertEquals(1, task0.processed);
    assertEquals(1, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(2L, containerMetrics.envelopes().getCount());
    assertEquals(2L, containerMetrics.processes().getCount());
  }

  @Test
  public void testProcessInOrder() throws Exception {
    CountDownLatch task0ProcessedMessages = new CountDownLatch(2);
    CountDownLatch task1ProcessedMessages = new CountDownLatch(1);
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = new TestTask(true, true, false, task0ProcessedMessages);
    TestTask task1 = new TestTask(true, false, false, task1ProcessedMessages);
    TaskInstance t0 = createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer);
    TaskInstance t1 = createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer);

    Map<TaskName, TaskInstance> tasks = new HashMap<>();
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    int maxMessagesInFlight = 1;
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope3).thenReturn(envelope1).thenReturn(ssp0EndOfStream).thenReturn(ssp1EndOfStream).thenReturn(null);
    runLoop.run();

    // Wait till the tasks completes processing all the messages.
    task0ProcessedMessages.await();
    task1ProcessedMessages.await();

    assertEquals(2, task0.processed);
    assertEquals(2, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(5L, containerMetrics.envelopes().getCount());
    assertEquals(3L, containerMetrics.processes().getCount());
    assertEquals(2L, t0.metrics().asyncCallbackCompleted().getCount());
    assertEquals(1L, t1.metrics().asyncCallbackCompleted().getCount());
  }

  private TestCode buildOutofOrderCallback(final TestTask task) {
    final CountDownLatch latch = new CountDownLatch(1);
    return new TestCode() {
      @Override
      public void run(TaskCallback callback) {
        IncomingMessageEnvelope envelope = ((TaskCallbackImpl) callback).envelope;
        if (envelope.equals(envelope0)) {
          // process first message will wait till the second one is processed
          try {
            latch.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        } else {
          // second envelope complete first
          assertEquals(0, task.completed.get());
          latch.countDown();
        }
      }
    };
  }

  @Test
  public void testProcessOutOfOrder() throws Exception {
    int maxMessagesInFlight = 2;

    CountDownLatch task0ProcessedMessagesLatch = new CountDownLatch(2);
    CountDownLatch task1ProcessedMessagesLatch = new CountDownLatch(1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = new TestTask(true, true, false, task0ProcessedMessagesLatch, maxMessagesInFlight);
    TestTask task1 = new TestTask(true, false, false, task1ProcessedMessagesLatch, maxMessagesInFlight);
    TaskInstance t0 = createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer);
    TaskInstance t1 = createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer);

    Map<TaskName, TaskInstance> tasks = new HashMap<>();
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    task0.callbackHandler = buildOutofOrderCallback(task0);
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope3).thenReturn(envelope1).thenReturn(ssp0EndOfStream).thenReturn(ssp1EndOfStream).thenReturn(null);
    runLoop.run();

    task0ProcessedMessagesLatch.await();
    task1ProcessedMessagesLatch.await();

    assertEquals(2, task0.processed);
    assertEquals(2, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(5L, containerMetrics.envelopes().getCount());
    assertEquals(3L, containerMetrics.processes().getCount());
  }

  @Test
  public void testWindow() throws Exception {
    TestTask task0 = new TestTask(true, true, false, null);
    TestTask task1 = new TestTask(true, false, true, null);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TaskInstance t0 = createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer);
    TaskInstance t1 = createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer);

    Map<TaskName, TaskInstance> tasks = new HashMap<>();
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    long windowMs = 1;
    int maxMessagesInFlight = 1;
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics,
                                            () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(null);
    runLoop.run();

    assertEquals(4, task1.windowCount);
  }

  @Test
  public void testCommitSingleTask() throws Exception {
    CountDownLatch task0ProcessedMessagesLatch = new CountDownLatch(1);
    CountDownLatch task1ProcessedMessagesLatch = new CountDownLatch(1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = new TestTask(true, true, false, task0ProcessedMessagesLatch);
    task0.setCommitRequest(TaskCoordinator.RequestScope.CURRENT_TASK);
    TestTask task1 = new TestTask(true, false, true, task1ProcessedMessagesLatch);
    TaskInstance t0 = createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer);
    TaskInstance t1 = createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer);

    Map<TaskName, TaskInstance> tasks = new HashMap<>();
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    int maxMessagesInFlight = 1;
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    //have a null message in between to make sure task0 finishes processing and invoke the commit
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0)
        .thenAnswer(x -> {
            task0ProcessedMessagesLatch.await();
            return null;
          }).thenReturn(envelope1).thenReturn(null);

    runLoop.run();

    task0ProcessedMessagesLatch.await();
    task1ProcessedMessagesLatch.await();

    verify(offsetManager).buildCheckpoint(eq(taskName0));
    verify(offsetManager).writeCheckpoint(eq(taskName0), any(Checkpoint.class));
    verify(offsetManager, never()).buildCheckpoint(eq(taskName1));
    verify(offsetManager, never()).writeCheckpoint(eq(taskName1), any(Checkpoint.class));
  }

  @Test
  public void testCommitAllTasks() throws Exception {
    CountDownLatch task0ProcessedMessagesLatch = new CountDownLatch(1);
    CountDownLatch task1ProcessedMessagesLatch = new CountDownLatch(1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = new TestTask(true, true, false, task0ProcessedMessagesLatch);
    task0.setCommitRequest(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
    TestTask task1 = new TestTask(true, false, true, task1ProcessedMessagesLatch);

    TaskInstance t0 = createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer);
    TaskInstance t1 = createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer);

    Map<TaskName, TaskInstance> tasks = new HashMap<>();
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);
    int maxMessagesInFlight = 1;
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    //have a null message in between to make sure task0 finishes processing and invoke the commit
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0)
        .thenAnswer(x -> {
            task0ProcessedMessagesLatch.await();
            return null;
          }).thenReturn(envelope1).thenReturn(null);
    runLoop.run();

    task0ProcessedMessagesLatch.await();
    task1ProcessedMessagesLatch.await();

    verify(offsetManager).buildCheckpoint(eq(taskName0));
    verify(offsetManager).writeCheckpoint(eq(taskName0), any(Checkpoint.class));
    verify(offsetManager).buildCheckpoint(eq(taskName1));
    verify(offsetManager).writeCheckpoint(eq(taskName1), any(Checkpoint.class));
  }

  @Test
  public void testShutdownOnConsensus() throws Exception {
    CountDownLatch task0ProcessedMessagesLatch = new CountDownLatch(1);
    CountDownLatch task1ProcessedMessagesLatch = new CountDownLatch(1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = new TestTask(true, true, true, task0ProcessedMessagesLatch);
    task0.setShutdownRequest(TaskCoordinator.RequestScope.CURRENT_TASK);
    TestTask task1 = new TestTask(true, false, true, task1ProcessedMessagesLatch);
    task1.setShutdownRequest(TaskCoordinator.RequestScope.CURRENT_TASK);
    TaskInstance t0 = createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer);
    TaskInstance t1 = createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer);

    Map<TaskName, TaskInstance> tasks = new HashMap<>();
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    tasks.put(taskName0, createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer));
    tasks.put(taskName1, createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer));

    int maxMessagesInFlight = 1;

    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics,
                                            () -> 0L, false);
    // consensus is reached after envelope1 is processed.
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope1).thenReturn(null);
    runLoop.run();

    task0ProcessedMessagesLatch.await();
    task1ProcessedMessagesLatch.await();

    assertEquals(1, task0.processed);
    assertEquals(1, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(2L, containerMetrics.envelopes().getCount());
    assertEquals(2L, containerMetrics.processes().getCount());
  }

  @Test
  public void testEndOfStreamWithMultipleTasks() throws Exception {
    CountDownLatch task0ProcessedMessagesLatch = new CountDownLatch(1);
    CountDownLatch task1ProcessedMessagesLatch = new CountDownLatch(1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = new TestTask(true, true, false, task0ProcessedMessagesLatch);
    TestTask task1 = new TestTask(true, true, false, task1ProcessedMessagesLatch);
    TaskInstance t0 = createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer);
    TaskInstance t1 = createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer);

    Map<TaskName, TaskInstance> tasks = new HashMap<>();

    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    int maxMessagesInFlight = 1;
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics,
                                            () -> 0L, false);
    when(consumerMultiplexer.choose(false))
      .thenReturn(envelope0)
      .thenReturn(envelope1)
      .thenReturn(ssp0EndOfStream)
      .thenReturn(ssp1EndOfStream)
      .thenReturn(null);

    runLoop.run();

    task0ProcessedMessagesLatch.await();
    task1ProcessedMessagesLatch.await();

    assertEquals(1, task0.processed);
    assertEquals(1, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(4L, containerMetrics.envelopes().getCount());
    assertEquals(2L, containerMetrics.processes().getCount());
  }

  @Test
  public void testEndOfStreamWithOutOfOrderProcess() throws Exception {
    int maxMessagesInFlight = 2;

    CountDownLatch task0ProcessedMessagesLatch = new CountDownLatch(2);
    CountDownLatch task1ProcessedMessagesLatch = new CountDownLatch(1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = new TestTask(true, true, false, task0ProcessedMessagesLatch, maxMessagesInFlight);
    TestTask task1 = new TestTask(true, true, false, task1ProcessedMessagesLatch, maxMessagesInFlight);
    TaskInstance t0 = createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer);
    TaskInstance t1 = createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer);

    Map<TaskName, TaskInstance> tasks = new HashMap<>();

    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    task0.callbackHandler = buildOutofOrderCallback(task0);
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope3).thenReturn(envelope1).thenReturn(null).thenReturn(ssp0EndOfStream).thenReturn(ssp1EndOfStream).thenReturn(null);

    runLoop.run();

    task0ProcessedMessagesLatch.await();
    task1ProcessedMessagesLatch.await();

    assertEquals(2, task0.processed);
    assertEquals(2, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(5L, containerMetrics.envelopes().getCount());
    assertEquals(3L, containerMetrics.processes().getCount());
  }

  @Test
  public void testEndOfStreamCommitBehavior() throws Exception {
    CountDownLatch task0ProcessedMessagesLatch = new CountDownLatch(1);
    CountDownLatch task1ProcessedMessagesLatch = new CountDownLatch(1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    //explicitly configure to disable commits inside process or window calls and invoke commit from end of stream
    TestTask task0 = new TestTask(true, false, false, task0ProcessedMessagesLatch);
    TestTask task1 = new TestTask(true, false, false, task1ProcessedMessagesLatch);

    TaskInstance t0 = createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer);
    TaskInstance t1 = createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer);

    Map<TaskName, TaskInstance> tasks = new HashMap<>();

    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);
    int maxMessagesInFlight = 1;
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight , windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope1).thenReturn(null).thenReturn(ssp0EndOfStream).thenReturn(ssp1EndOfStream).thenReturn(null);

    runLoop.run();

    task0ProcessedMessagesLatch.await();
    task1ProcessedMessagesLatch.await();

    verify(offsetManager).buildCheckpoint(eq(taskName0));
    verify(offsetManager).writeCheckpoint(eq(taskName0), any(Checkpoint.class));
    verify(offsetManager).buildCheckpoint(eq(taskName1));
    verify(offsetManager).writeCheckpoint(eq(taskName1), any(Checkpoint.class));
  }

  @Test
  public void testEndOfStreamOffsetManagement() throws Exception {
    //explicitly configure to disable commits inside process or window calls and invoke commit from end of stream
    TestTask mockStreamTask1 = new TestTask(true, false, false, null);
    TestTask mockStreamTask2 = new TestTask(true, false, false, null);

    Partition p1 = new Partition(1);
    Partition p2 = new Partition(2);
    SystemStreamPartition ssp1 = new SystemStreamPartition("system1", "stream1", p1);
    SystemStreamPartition ssp2 = new SystemStreamPartition("system1", "stream2", p2);
    IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(ssp2, "1", "key1", "message1");
    IncomingMessageEnvelope envelope2 = new IncomingMessageEnvelope(ssp2, "2", "key1", "message1");
    IncomingMessageEnvelope envelope3 = IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp2);

    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> sspMap = new HashMap<>();
    List<IncomingMessageEnvelope> messageList = new ArrayList<>();
    messageList.add(envelope1);
    messageList.add(envelope2);
    messageList.add(envelope3);
    sspMap.put(ssp2, messageList);

    SystemConsumer mockConsumer = mock(SystemConsumer.class);
    when(mockConsumer.poll(anyObject(), anyLong())).thenReturn(sspMap);

    HashMap<String, SystemConsumer> systemConsumerMap = new HashMap<>();
    systemConsumerMap.put("system1", mockConsumer);
    SystemConsumers consumers = TestSystemConsumers.getSystemConsumers(systemConsumerMap);

    TaskName taskName1 = new TaskName("task1");
    TaskName taskName2 = new TaskName("task2");

    OffsetManager offsetManager = mock(OffsetManager.class);

    when(offsetManager.getLastProcessedOffset(taskName1, ssp1)).thenReturn(Option.apply("3"));
    when(offsetManager.getLastProcessedOffset(taskName2, ssp2)).thenReturn(Option.apply("0"));
    when(offsetManager.getStartingOffset(taskName1, ssp1)).thenReturn(Option.apply(IncomingMessageEnvelope.END_OF_STREAM_OFFSET));
    when(offsetManager.getStartingOffset(taskName2, ssp2)).thenReturn(Option.apply("1"));

    TaskInstance taskInstance1 = createTaskInstance(mockStreamTask1, taskName1, ssp1, offsetManager, consumers);
    TaskInstance taskInstance2 = createTaskInstance(mockStreamTask2, taskName2, ssp2, offsetManager, consumers);
    Map<TaskName, TaskInstance> tasks = new HashMap<>();
    tasks.put(taskName1, taskInstance1);
    tasks.put(taskName2, taskInstance2);

    taskInstance1.registerConsumers();
    taskInstance2.registerConsumers();
    consumers.start();

    int maxMessagesInFlight = 1;
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumers, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);

    runLoop.run();
  }

  //@Test
  public void testCommitBehaviourWhenAsyncCommitIsEnabled() throws InterruptedException {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    int maxMessagesInFlight = 3;
    TestTask task0 = new TestTask(true, true, false, null, maxMessagesInFlight);
    task0.setCommitRequest(TaskCoordinator.RequestScope.CURRENT_TASK);
    TestTask task1 = new TestTask(true, false, false, null, maxMessagesInFlight);

    IncomingMessageEnvelope firstMsg = new IncomingMessageEnvelope(ssp0, "0", "key0", "value0");
    IncomingMessageEnvelope secondMsg = new IncomingMessageEnvelope(ssp0, "1", "key1", "value1");
    IncomingMessageEnvelope thirdMsg = new IncomingMessageEnvelope(ssp0, "2", "key0", "value0");

    final CountDownLatch firstMsgCompletionLatch = new CountDownLatch(1);
    final CountDownLatch secondMsgCompletionLatch = new CountDownLatch(1);
    task0.callbackHandler = callback -> {
      IncomingMessageEnvelope envelope = ((TaskCallbackImpl) callback).envelope;
      try {
        if (envelope.equals(firstMsg)) {
          firstMsgCompletionLatch.await();
        } else if (envelope.equals(secondMsg)) {
          firstMsgCompletionLatch.countDown();
          secondMsgCompletionLatch.await();
        } else if (envelope.equals(thirdMsg)) {
          secondMsgCompletionLatch.countDown();
          // OffsetManager.update with firstMsg offset, task.commit has happened when second message callback has not completed.
          verify(offsetManager).update(eq(taskName0), eq(firstMsg.getSystemStreamPartition()), eq(firstMsg.getOffset()));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    };

    Map<TaskName, TaskInstance> tasks = new HashMap<>();

    tasks.put(taskName0, createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer));
    tasks.put(taskName1, createTaskInstance(task1, taskName1, ssp1, offsetManager, consumerMultiplexer));
    when(consumerMultiplexer.choose(false)).thenReturn(firstMsg).thenReturn(secondMsg).thenReturn(thirdMsg).thenReturn(envelope1).thenReturn(ssp0EndOfStream).thenReturn(ssp1EndOfStream).thenReturn(null);

    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);

    runLoop.run();

    firstMsgCompletionLatch.await();
    secondMsgCompletionLatch.await();

    verify(offsetManager, atLeastOnce()).buildCheckpoint(eq(taskName0));
    verify(offsetManager, atLeastOnce()).writeCheckpoint(eq(taskName0), any(Checkpoint.class));
    assertEquals(3, task0.processed);
    assertEquals(3, task0.committed);
    assertEquals(1, task1.processed);
    assertEquals(0, task1.committed);
  }

  @Test
  public void testProcessBehaviourWhenAsyncCommitIsEnabled() throws InterruptedException {
    int maxMessagesInFlight = 2;

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = new TestTask(true, true, false, null, maxMessagesInFlight);
    CountDownLatch commitLatch = new CountDownLatch(1);
    task0.commitHandler = callback -> {
      TaskCallbackImpl taskCallback = (TaskCallbackImpl) callback;
      if (taskCallback.envelope.equals(envelope3)) {
        try {
          commitLatch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };

    task0.callbackHandler = callback -> {
      TaskCallbackImpl taskCallback = (TaskCallbackImpl) callback;
      if (taskCallback.envelope.equals(envelope0)) {
        // Both the process call has gone through when the first commit is in progress.
        assertEquals(2, containerMetrics.processes().getCount());
        assertEquals(0, containerMetrics.commits().getCount());
        commitLatch.countDown();
      }
    };

    Map<TaskName, TaskInstance> tasks = new HashMap<>();

    tasks.put(taskName0, createTaskInstance(task0, taskName0, ssp0, offsetManager, consumerMultiplexer));
    when(consumerMultiplexer.choose(false)).thenReturn(envelope3).thenReturn(envelope0).thenReturn(ssp0EndOfStream).thenReturn(null);
    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics,
                                            () -> 0L, true);

    runLoop.run();

    commitLatch.await();
  }
}
