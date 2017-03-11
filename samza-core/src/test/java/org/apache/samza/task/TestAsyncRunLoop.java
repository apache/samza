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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.Partition;
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
import org.junit.Before;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConverters;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.atLeastOnce;

public class TestAsyncRunLoop {
  Map<TaskName, TaskInstance> tasks;
  ExecutorService executor;
  SystemConsumers consumerMultiplexer;
  SamzaContainerMetrics containerMetrics;
  OffsetManager offsetManager;
  long windowMs;
  long commitMs;
  long callbackTimeoutMs;
  long maxThrottlingDelayMs;
  int maxMessagesInFlight;
  TaskCoordinator.RequestScope commitRequest;
  TaskCoordinator.RequestScope shutdownRequest;

  Partition p0 = new Partition(0);
  Partition p1 = new Partition(1);
  TaskName taskName0 = new TaskName(p0.toString());
  TaskName taskName1 = new TaskName(p1.toString());
  SystemStreamPartition ssp0 = new SystemStreamPartition("testSystem", "testStream", p0);
  SystemStreamPartition ssp1 = new SystemStreamPartition("testSystem", "testStream", p1);
  IncomingMessageEnvelope envelope0 = new IncomingMessageEnvelope(ssp0, "0", "key0", "value0");
  IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(ssp1, "1", "key1", "value1");
  IncomingMessageEnvelope envelope3 = new IncomingMessageEnvelope(ssp0, "1", "key0", "value0");
  IncomingMessageEnvelope ssp0EndOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp0);
  IncomingMessageEnvelope ssp1EndOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp1);

  TestTask task0;
  TestTask task1;
  TaskInstance t0;
  TaskInstance t1;

  AsyncRunLoop createRunLoop() {
    return new AsyncRunLoop(tasks,
        executor,
        consumerMultiplexer,
        maxMessagesInFlight,
        windowMs,
        commitMs,
        callbackTimeoutMs,
        maxThrottlingDelayMs,
        containerMetrics,
        () -> 0L,
        false);
  }

  TaskInstance createTaskInstance(AsyncStreamTask task, TaskName taskName, SystemStreamPartition ssp, OffsetManager manager, SystemConsumers consumers) {
    TaskInstanceMetrics taskInstanceMetrics = new TaskInstanceMetrics("task", new MetricsRegistryMap());
    scala.collection.immutable.Set<SystemStreamPartition> sspSet = JavaConverters.asScalaSetConverter(Collections.singleton(ssp)).asScala().toSet();
    return new TaskInstance(task, taskName, mock(Config.class), taskInstanceMetrics,
        null, consumers, mock(TaskInstanceCollector.class), mock(SamzaContainerContext.class),
        manager, null, null, sspSet, new TaskInstanceExceptionHandler(taskInstanceMetrics, new scala.collection.immutable.HashSet<String>()));
  }

  TaskInstance createTaskInstance(AsyncStreamTask task, TaskName taskName, SystemStreamPartition ssp) {
    return createTaskInstance(task, taskName, ssp, offsetManager, consumerMultiplexer);
  }

  ExecutorService callbackExecutor;
  void triggerCallback(final TestTask task, final TaskCallback callback, final boolean success) {
    callbackExecutor.submit(new Runnable() {
      @Override
      public void run() {
        if (task.callbackHandler != null) {
          task.callbackHandler.run(callback);
        }

        task.completed.incrementAndGet();

        if (success) {
          callback.complete();
        } else {
          callback.failure(new Exception("process failure"));
        }
      }
    });
  }

  interface TestCode {
    void run(TaskCallback callback);
  }

  class TestTask implements AsyncStreamTask, WindowableTask, EndOfStreamListenerTask {
    boolean shutdown = false;
    boolean commit = false;
    boolean success;
    int processed = 0;
    int committed = 0;
    volatile int windowCount = 0;

    AtomicInteger completed = new AtomicInteger(0);
    TestCode callbackHandler = null;
    TestCode commitHandler = null;

    TestTask(boolean success, boolean commit, boolean shutdown) {
      this.success = success;
      this.shutdown = shutdown;
      this.commit = commit;
    }

    @Override
    public void processAsync(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator,
        TaskCallback callback) {

      if (maxMessagesInFlight == 1) {
        assertEquals(processed, completed.get());
      }

      processed++;

      if (commit) {
        if (commitHandler != null) {
          callbackExecutor.submit(() -> commitHandler.run(callback));
        }
        coordinator.commit(commitRequest);
        committed++;
      }

      if (shutdown) {
        coordinator.shutdown(shutdownRequest);
      }
      triggerCallback(this, callback, success);
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
  }

  @Before
  public void setup() {
    executor = null;
    consumerMultiplexer = mock(SystemConsumers.class);
    windowMs = -1;
    commitMs = -1;
    maxMessagesInFlight = 1;
    containerMetrics = new SamzaContainerMetrics("container", new MetricsRegistryMap());
    callbackExecutor = Executors.newFixedThreadPool(4);
    offsetManager = mock(OffsetManager.class);
    shutdownRequest = TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER;

    when(consumerMultiplexer.pollIntervalMs()).thenReturn(10);

    tasks = new HashMap<>();
    task0 = new TestTask(true, true, false);
    task1 = new TestTask(true, false, true);
    t0 = createTaskInstance(task0, taskName0, ssp0);
    t1 = createTaskInstance(task1, taskName1, ssp1);
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);
  }

  @Test
  public void testProcessMultipleTasks() throws Exception {
    AsyncRunLoop runLoop = createRunLoop();
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope1).thenReturn(null);
    runLoop.run();

    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);

    assertEquals(1, task0.processed);
    assertEquals(1, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(2L, containerMetrics.envelopes().getCount());
    assertEquals(2L, containerMetrics.processes().getCount());
  }

  @Test
  public void testProcessInOrder() throws Exception {
    AsyncRunLoop runLoop = createRunLoop();
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope3).thenReturn(envelope1).thenReturn(null);
    runLoop.run();

    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);

    assertEquals(2, task0.processed);
    assertEquals(2, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(3L, containerMetrics.envelopes().getCount());
    assertEquals(3L, containerMetrics.processes().getCount());
  }

  private TestCode buildOutofOrderCallback() {
    final CountDownLatch latch = new CountDownLatch(1);
    return new TestCode() {
      @Override
      public void run(TaskCallback callback) {
        IncomingMessageEnvelope envelope = ((TaskCallbackImpl) callback).envelope;
        if (envelope == envelope0) {
          // process first message will wait till the second one is processed
          try {
            latch.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        } else {
          // second envelope complete first
          assertEquals(0, task0.completed.get());
          latch.countDown();
        }
      }
    };
  }

  @Test
  public void testProcessOutOfOrder() throws Exception {
    maxMessagesInFlight = 2;

    task0.callbackHandler = buildOutofOrderCallback();

    AsyncRunLoop runLoop = createRunLoop();
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope3).thenReturn(envelope1).thenReturn(null);
    runLoop.run();

    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);

    assertEquals(2, task0.processed);
    assertEquals(2, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(3L, containerMetrics.envelopes().getCount());
    assertEquals(3L, containerMetrics.processes().getCount());
  }

  @Test
  public void testWindow() throws Exception {
    windowMs = 1;

    AsyncRunLoop runLoop = createRunLoop();
    when(consumerMultiplexer.choose(false)).thenReturn(null);
    runLoop.run();

    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);

    assertEquals(4, task1.windowCount);
  }

  @Test
  public void testCommitSingleTask() throws Exception {
    commitRequest = TaskCoordinator.RequestScope.CURRENT_TASK;

    AsyncRunLoop runLoop = createRunLoop();
    //have a null message in between to make sure task0 finishes processing and invoke the commit
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(null).thenReturn(envelope1).thenReturn(null);
    runLoop.run();

    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);

    verify(offsetManager).checkpoint(taskName0);
    verify(offsetManager, never()).checkpoint(taskName1);
  }

  @Test
  public void testCommitAllTasks() throws Exception {
    commitRequest = TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER;

    AsyncRunLoop runLoop = createRunLoop();
    //have a null message in between to make sure task0 finishes processing and invoke the commit
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(null).thenReturn(envelope1).thenReturn(null);
    runLoop.run();

    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);

    verify(offsetManager).checkpoint(taskName0);
    verify(offsetManager).checkpoint(taskName1);
  }

  @Test
  public void testShutdownOnConsensus() throws Exception {
    shutdownRequest = TaskCoordinator.RequestScope.CURRENT_TASK;

    tasks = new HashMap<>();
    task0 = new TestTask(true, true, true);
    task1 = new TestTask(true, false, true);
    t0 = createTaskInstance(task0, taskName0, ssp0);
    t1 = createTaskInstance(task1, taskName1, ssp1);
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    AsyncRunLoop runLoop = createRunLoop();
    // consensus is reached after envelope1 is processed.
    when(consumerMultiplexer.choose(false)).thenReturn(envelope0).thenReturn(envelope1).thenReturn(null);
    runLoop.run();

    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);

    assertEquals(1, task0.processed);
    assertEquals(1, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(2L, containerMetrics.envelopes().getCount());
    assertEquals(2L, containerMetrics.processes().getCount());
  }

  @Test
  public void testEndOfStreamWithMultipleTasks() throws Exception {
    task0 = new TestTask(true, true, false);
    task1 = new TestTask(true, true, false);
    t0 = createTaskInstance(task0, taskName0, ssp0);
    t1 = createTaskInstance(task1, taskName1, ssp1);
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    AsyncRunLoop runLoop = createRunLoop();
    when(consumerMultiplexer.choose(false))
      .thenReturn(envelope0)
      .thenReturn(envelope1)
      .thenReturn(ssp0EndOfStream)
      .thenReturn(ssp1EndOfStream)
      .thenReturn(null);

    runLoop.run();

    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);
    assertEquals(1, task0.processed);
    assertEquals(1, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(4L, containerMetrics.envelopes().getCount());
    assertEquals(2L, containerMetrics.processes().getCount());
  }

  @Test
  public void testEndOfStreamWithOutOfOrderProcess() throws Exception {
    maxMessagesInFlight = 2;
    task0 = new TestTask(true, true, false);
    task1 = new TestTask(true, true, false);
    t0 = createTaskInstance(task0, taskName0, ssp0);
    t1 = createTaskInstance(task1, taskName1, ssp1);
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);

    final CountDownLatch latch = new CountDownLatch(1);
    task0.callbackHandler = buildOutofOrderCallback();
    AsyncRunLoop runLoop = createRunLoop();
    when(consumerMultiplexer.choose(false))
        .thenReturn(envelope0)
        .thenReturn(envelope3)
        .thenReturn(envelope1)
        .thenReturn(null)
        .thenReturn(ssp0EndOfStream)
        .thenReturn(ssp1EndOfStream)
        .thenReturn(null);

    runLoop.run();

    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);
    assertEquals(2, task0.processed);
    assertEquals(2, task0.completed.get());
    assertEquals(1, task1.processed);
    assertEquals(1, task1.completed.get());
    assertEquals(5L, containerMetrics.envelopes().getCount());
    assertEquals(3L, containerMetrics.processes().getCount());
  }

  @Test
  public void testEndOfStreamCommitBehavior() throws Exception {
    //explicitly configure to disable commits inside process or window calls and invoke commit from end of stream
    task0 = new TestTask(true, false, false);
    task1 = new TestTask(true, false, false);

    t0 = createTaskInstance(task0, taskName0, ssp0);
    t1 = createTaskInstance(task1, taskName1, ssp1);
    tasks.put(taskName0, t0);
    tasks.put(taskName1, t1);
    AsyncRunLoop runLoop = createRunLoop();

    when(consumerMultiplexer.choose(false)).thenReturn(envelope0)
        .thenReturn(envelope1)
        .thenReturn(null)
        .thenReturn(ssp0EndOfStream)
        .thenReturn(ssp1EndOfStream)
        .thenReturn(null);

    runLoop.run();

    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);
    verify(offsetManager).checkpoint(taskName0);
    verify(offsetManager).checkpoint(taskName1);
  }

  @Test
  public void testEndOfStreamOffsetManagement() throws Exception {
    //explicitly configure to disable commits inside process or window calls and invoke commit from end of stream
    TestTask mockStreamTask1 = new TestTask(true, false, false);
    TestTask mockStreamTask2 = new TestTask(true, false, false);

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
    Set<TaskName> taskNames = new HashSet<>();
    taskNames.add(taskName1);
    taskNames.add(taskName2);

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

    AsyncRunLoop runLoop = new AsyncRunLoop(tasks,
        executor,
        consumers,
        maxMessagesInFlight,
        windowMs,
        commitMs,
        callbackTimeoutMs,
        maxThrottlingDelayMs,
        containerMetrics,
        () -> 0L,
        false);

    runLoop.run();
    callbackExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testCommitBehaviourWhenAsyncCommitIsEnabled() throws InterruptedException {
    commitRequest = TaskCoordinator.RequestScope.CURRENT_TASK;
    maxMessagesInFlight = 2;
    task0 = new TestTask(true, true, false);
    task1 = new TestTask(true, false, false);

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
          verify(offsetManager).update(taskName0, firstMsg.getSystemStreamPartition(), firstMsg.getOffset());
          verify(offsetManager, atLeastOnce()).checkpoint(taskName0);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    };

    tasks.put(taskName0, createTaskInstance(task0, taskName0, ssp0));
    tasks.put(taskName1, createTaskInstance(task1, taskName1, ssp0));
    when(consumerMultiplexer.choose(false)).thenReturn(firstMsg)
                                           .thenReturn(secondMsg)
                                           .thenReturn(thirdMsg)
                                           .thenReturn(ssp0EndOfStream);

    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, containerMetrics, () -> 0L, true);
    // Shutdown runLoop when all tasks are finished.
    callbackExecutor.execute(() -> {
        try {
          firstMsgCompletionLatch.await();
          secondMsgCompletionLatch.await();
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        runLoop.shutdown();
      });

    runLoop.run();
    callbackExecutor.awaitTermination(500, TimeUnit.MILLISECONDS);

    verify(offsetManager, atLeastOnce()).checkpoint(taskName0);
    assertEquals(3, task0.processed);
    assertEquals(3, task0.committed);
    assertEquals(3, task1.processed);
    assertEquals(0, task1.committed);
  }

  @Test
  public void testProcessBehaviourWhenAsyncCommitIsEnabled() throws InterruptedException {
    TestTask task0 = new TestTask(true, true, false);

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
    tasks.put(taskName0, createTaskInstance(task0, taskName0, ssp0));
    when(consumerMultiplexer.choose(false)).thenReturn(envelope3)
                                           .thenReturn(envelope0)
                                           .thenReturn(ssp0EndOfStream);

    AsyncRunLoop runLoop = new AsyncRunLoop(tasks, executor, consumerMultiplexer, maxMessagesInFlight, windowMs, commitMs,
                                            callbackTimeoutMs, maxThrottlingDelayMs, containerMetrics, () -> 0L, true);

    // Shutdown runLoop after the commit.
    callbackExecutor.execute(() -> {
        try {
          commitLatch.await();
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        runLoop.shutdown();
      });

    runLoop.run();

    callbackExecutor.awaitTermination(500, TimeUnit.MILLISECONDS);
  }
}
