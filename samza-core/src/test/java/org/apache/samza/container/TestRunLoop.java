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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.scheduler.EpochTimeScheduler;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.TestSystemConsumers;
import org.apache.samza.task.ReadableCoordinator;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCallbackFactory;
import org.apache.samza.task.TaskCallbackImpl;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;


public class TestRunLoop {
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

  TestTask createTestTask(boolean success, boolean commit, boolean shutdown, CountDownLatch latch,
      int maxMessagesInFlight, TaskName taskName, SystemStreamPartition ssp, OffsetManager manager) {
    TaskModel taskModel = mock(TaskModel.class);
    when(taskModel.getTaskName()).thenReturn(taskName);
    Set<SystemStreamPartition> sspSet = Collections.singleton(ssp);
    return new TestTask(success, commit, shutdown, taskName, manager, sspSet, latch, maxMessagesInFlight);
  }

  interface TestCode {
    void run(TaskCallback callback);
  }

  class TestTask implements RunLoopTask {
    private final boolean shutdown;
    private final boolean commit;
    private final boolean success;
    private final TaskName taskName;
    private final OffsetManager offsetManager;
    private final Set<SystemStreamPartition> ssps;

    private final TaskInstanceMetrics metrics = new TaskInstanceMetrics("task", new MetricsRegistryMap());
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

    TestTask(boolean success, boolean commit, boolean shutdown, TaskName taskName, OffsetManager offsetManager,
        Set<SystemStreamPartition> ssps, CountDownLatch processedMessagesLatch, int maxMessagesInFlight) {
      this.success = success;
      this.shutdown = shutdown;
      this.commit = commit;
      this.taskName = taskName;
      this.offsetManager = offsetManager;
      this.ssps = ssps;
      this.processedMessagesLatch = processedMessagesLatch;
      this.maxMessagesInFlight = maxMessagesInFlight;
    }

    @Override
    public TaskName taskName() {
      return this.taskName;
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, ReadableCoordinator coordinator, TaskCallbackFactory callbackFactory) {
      TaskCallback callback = callbackFactory.createCallback();

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
    public void window(ReadableCoordinator coordinator) {
      windowCount++;

      if (shutdown && windowCount == 4) {
        coordinator.shutdown(shutdownRequest);
      }
    }

    @Override
    public void scheduler(ReadableCoordinator coordinator) {

    }

    @Override
    public void commit() {

    }

    @Override
    public void endOfStream(ReadableCoordinator coordinator) {
      coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    }

    @Override
    public boolean isWindowableTask() {
      return true;
    }

    @Override
    public Set<String> intermediateStreams() {
      return Collections.emptySet();
    }

    @Override
    public Set<SystemStreamPartition> systemStreamPartitions() {
      return this.ssps;
    }

    @Override
    public OffsetManager offsetManager() {
      return this.offsetManager;
    }

    @Override
    public TaskInstanceMetrics metrics() {
      return this.metrics;
    }

    @Override
    public EpochTimeScheduler epochTimeScheduler() {
      return null;
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

    TestTask task0 = createTestTask(true, true, false, task0ProcessedMessages, 0, taskName0, ssp0, offsetManager);
    TestTask task1 = createTestTask(true, false, true, task1ProcessedMessages, 0, taskName1, ssp1, offsetManager);

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
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

    TestTask task0 = createTestTask(true, true, false, task0ProcessedMessages, 0, taskName0, ssp0, offsetManager);
    TestTask task1 = createTestTask(true, false, false, task1ProcessedMessages, 0, taskName1, ssp1, offsetManager);

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
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
    assertEquals(2L, task0.metrics().asyncCallbackCompleted().getCount());
    assertEquals(1L, task1.metrics().asyncCallbackCompleted().getCount());
  }

  private TestCode buildOutofOrderCallback(final TestTask task) {
    final CountDownLatch latch = new CountDownLatch(1);
    return new TestCode() {
      @Override
      public void run(TaskCallback callback) {
        IncomingMessageEnvelope envelope = ((TaskCallbackImpl) callback).getEnvelope();
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

    TestTask task0 = createTestTask(true, true, false, task0ProcessedMessagesLatch, maxMessagesInFlight, taskName0, ssp0, offsetManager);
    TestTask task1 = createTestTask(true, false, false, task1ProcessedMessagesLatch, maxMessagesInFlight, taskName1, ssp1, offsetManager);

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    task0.callbackHandler = buildOutofOrderCallback(task0);
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
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
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = createTestTask(true, true, false, null, 0, taskName0, ssp0, offsetManager);
    TestTask task1 = createTestTask(true, false, true, null, 0, taskName1, ssp1, offsetManager);

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    long windowMs = 1;
    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
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

    TestTask task0 = spy(createTestTask(true, true, false, task0ProcessedMessagesLatch, 0, taskName0, ssp0, offsetManager));
    task0.setCommitRequest(TaskCoordinator.RequestScope.CURRENT_TASK);
    TestTask task1 = spy(createTestTask(true, false, true, task1ProcessedMessagesLatch, 0, taskName1, ssp1, offsetManager));

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    //have a null message in between to make sure task0 finishes processing and invoke the commit
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0)
        .thenAnswer(x -> {
//            task0ProcessedMessagesLatch.await();
            return null;
          }).thenReturn(envelope1).thenReturn(null);

    runLoop.run();

    task0ProcessedMessagesLatch.await();
    task1ProcessedMessagesLatch.await();

    verify(task0, times(1)).commit();
    verify(task1, never()).commit();
  }

  @Test
  public void testCommitAllTasks() throws Exception {
    CountDownLatch task0ProcessedMessagesLatch = new CountDownLatch(1);
    CountDownLatch task1ProcessedMessagesLatch = new CountDownLatch(1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = spy(createTestTask(true, true, false, task0ProcessedMessagesLatch, 0, taskName0, ssp0, offsetManager));
    task0.setCommitRequest(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
    TestTask task1 = spy(createTestTask(true, false, true, task1ProcessedMessagesLatch, 0, taskName1, ssp1, offsetManager));

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);
    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
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

    verify(task0, times(1)).commit();
    verify(task1, times(1)).commit();
  }

  @Test
  public void testShutdownOnConsensus() throws Exception {
    CountDownLatch task0ProcessedMessagesLatch = new CountDownLatch(1);
    CountDownLatch task1ProcessedMessagesLatch = new CountDownLatch(1);

    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = createTestTask(true, true, true, task0ProcessedMessagesLatch, 0, taskName0, ssp0, offsetManager);
    task0.setShutdownRequest(TaskCoordinator.RequestScope.CURRENT_TASK);
    TestTask task1 = createTestTask(true, false, true, task1ProcessedMessagesLatch, 0, taskName1, ssp1, offsetManager);
    task1.setShutdownRequest(TaskCoordinator.RequestScope.CURRENT_TASK);

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    int maxMessagesInFlight = 1;

    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
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

    TestTask task0 = spy(createTestTask(true, true, false, task0ProcessedMessagesLatch, 0, taskName0, ssp0, offsetManager));
    TestTask task1 = spy(createTestTask(true, true, false, task1ProcessedMessagesLatch, 0, taskName1, ssp1, offsetManager));

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();

    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
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

    verify(task0, times(1)).endOfStream(any());
    verify(task1, times(1)).endOfStream(any());

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

    TestTask task0 = spy(createTestTask(true, true, false, task0ProcessedMessagesLatch, maxMessagesInFlight, taskName0, ssp0, offsetManager));
    TestTask task1 = spy(createTestTask(true, true, false, task1ProcessedMessagesLatch, maxMessagesInFlight, taskName1, ssp1, offsetManager));

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();

    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);

    task0.callbackHandler = buildOutofOrderCallback(task0);
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope3).thenReturn(envelope1).thenReturn(null).thenReturn(ssp0EndOfStream).thenReturn(ssp1EndOfStream).thenReturn(null);

    runLoop.run();

    task0ProcessedMessagesLatch.await();
    task1ProcessedMessagesLatch.await();

    verify(task0, times(1)).endOfStream(any());
    verify(task1, times(1)).endOfStream(any());

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
    TestTask task0 = spy(createTestTask(true, false, false, task0ProcessedMessagesLatch, 0, taskName0, ssp0, offsetManager));
    TestTask task1 = spy(createTestTask(true, false, false, task1ProcessedMessagesLatch, 0, taskName1, ssp1, offsetManager));

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();

    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);
    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope1).thenReturn(null).thenReturn(ssp0EndOfStream).thenReturn(ssp1EndOfStream).thenReturn(null);

    runLoop.run();

    task0ProcessedMessagesLatch.await();
    task1ProcessedMessagesLatch.await();

    InOrder inOrder = inOrder(task0, task1);

    inOrder.verify(task0, times(1)).endOfStream(any());
    inOrder.verify(task0, times(1)).commit();
    inOrder.verify(task1, times(1)).endOfStream(any());
    inOrder.verify(task1, times(1)).commit();
  }

  @Test
  public void testEndOfStreamOffsetManagement() throws Exception {
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

    SystemAdmins systemAdmins = Mockito.mock(SystemAdmins.class);
    Mockito.when(systemAdmins.getSystemAdmin("system1")).thenReturn(Mockito.mock(SystemAdmin.class));
    Mockito.when(systemAdmins.getSystemAdmin("testSystem")).thenReturn(Mockito.mock(SystemAdmin.class));

    HashMap<String, SystemConsumer> systemConsumerMap = new HashMap<>();
    systemConsumerMap.put("system1", mockConsumer);

    SystemConsumers consumers = TestSystemConsumers.getSystemConsumers(systemConsumerMap, systemAdmins);

    TaskName taskName1 = new TaskName("task1");
    TaskName taskName2 = new TaskName("task2");

    OffsetManager offsetManager = mock(OffsetManager.class);

    consumers.register(ssp1, IncomingMessageEnvelope.END_OF_STREAM_OFFSET);
    consumers.register(ssp2, "1");

    //explicitly configure to disable commits inside process or window calls and invoke commit from end of stream
    TestTask task1 = spy(createTestTask(true, false, false, null, 0, taskName1, ssp1, offsetManager));
    TestTask task2 = spy(createTestTask(true, false, false, null, 0, taskName2, ssp2, offsetManager));
    Map<TaskName, RunLoopTask> tasks = new HashMap<>();
    tasks.put(taskName1, task1);
    tasks.put(taskName2, task2);

    consumers.start();

    int maxMessagesInFlight = 1;
    RunLoop runLoop = new RunLoop(tasks, executor, consumers, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics, () -> 0L, false);

    runLoop.run();

    verify(task1, never()).process(any(), any(), any());
    verify(task2, times(2)).process(any(), any(), any());
  }

  //@Test
  public void testCommitBehaviourWhenAsyncCommitIsEnabled() throws InterruptedException {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    int maxMessagesInFlight = 3;
    TestTask task0 = createTestTask(true, true, false, null, maxMessagesInFlight, taskName0, ssp0, offsetManager);
    task0.setCommitRequest(TaskCoordinator.RequestScope.CURRENT_TASK);
    TestTask task1 = createTestTask(true, false, false, null, maxMessagesInFlight, taskName1, ssp1, offsetManager);

    IncomingMessageEnvelope firstMsg = new IncomingMessageEnvelope(ssp0, "0", "key0", "value0");
    IncomingMessageEnvelope secondMsg = new IncomingMessageEnvelope(ssp0, "1", "key1", "value1");
    IncomingMessageEnvelope thirdMsg = new IncomingMessageEnvelope(ssp0, "2", "key0", "value0");

    final CountDownLatch firstMsgCompletionLatch = new CountDownLatch(1);
    final CountDownLatch secondMsgCompletionLatch = new CountDownLatch(1);
    task0.callbackHandler = callback -> {
      IncomingMessageEnvelope envelope = ((TaskCallbackImpl) callback).getEnvelope();
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

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();

    tasks.put(taskName0, task0);
    tasks.put(taskName1, task1);
    when(consumerMultiplexer.choose(false)).thenReturn(firstMsg).thenReturn(secondMsg).thenReturn(thirdMsg).thenReturn(envelope1).thenReturn(ssp0EndOfStream).thenReturn(ssp1EndOfStream).thenReturn(null);

    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
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

    TestTask task0 = createTestTask(true, true, false, null, maxMessagesInFlight, taskName0, ssp0, offsetManager);
    CountDownLatch commitLatch = new CountDownLatch(1);
    task0.commitHandler = callback -> {
      TaskCallbackImpl taskCallback = (TaskCallbackImpl) callback;
      if (taskCallback.getEnvelope().equals(envelope3)) {
        try {
          commitLatch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };

    task0.callbackHandler = callback -> {
      TaskCallbackImpl taskCallback = (TaskCallbackImpl) callback;
      if (taskCallback.getEnvelope().equals(envelope0)) {
        // Both the process call has gone through when the first commit is in progress.
        assertEquals(2, containerMetrics.processes().getCount());
        assertEquals(0, containerMetrics.commits().getCount());
        commitLatch.countDown();
      }
    };

    Map<TaskName, RunLoopTask> tasks = new HashMap<>();

    tasks.put(taskName0, task0);
    when(consumerMultiplexer.choose(false)).thenReturn(envelope3).thenReturn(envelope0).thenReturn(ssp0EndOfStream).thenReturn(null);
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics,
                                            () -> 0L, true);

    runLoop.run();

    commitLatch.await();
  }

  @Test(expected = SamzaException.class)
  public void testExceptionIsPropagated() {
    SystemConsumers consumerMultiplexer = mock(SystemConsumers.class);
    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);
    OffsetManager offsetManager = mock(OffsetManager.class);

    TestTask task0 = createTestTask(false, false, false, null, 0, taskName0, ssp0, offsetManager);

    Map<TaskName, RunLoopTask> tasks = ImmutableMap.of(taskName0, task0);

    int maxMessagesInFlight = 2;
    RunLoop runLoop = new RunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
        callbackTimeoutMs, maxThrottlingDelayMs, maxIdleMs, containerMetrics,
        () -> 0L, false);

    when(consumerMultiplexer.choose(false))
        .thenReturn(envelope0)
        .thenReturn(ssp0EndOfStream)
        .thenReturn(null);

    runLoop.run();
  }
}
