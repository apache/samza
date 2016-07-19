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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAsyncRunLoop {

  Map<TaskName, TaskInstance<AsyncStreamTask>> tasks;
  ExecutorService executor;
  SystemConsumers consumerMultiplexer;
  SamzaContainerMetrics containerMetrics;
  OffsetManager offsetManager;
  long windowMs;
  long commitMs;
  long callbackTimeoutMs;
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

  TestTask task0;
  TestTask task1;
  TaskInstance<AsyncStreamTask> t0;
  TaskInstance<AsyncStreamTask> t1;

  AsyncRunLoop createRunLoop() {
    return new AsyncRunLoop(tasks,
        executor,
        consumerMultiplexer,
        maxMessagesInFlight,
        windowMs,
        commitMs,
        callbackTimeoutMs,
        containerMetrics);
  }

  TaskInstance<AsyncStreamTask> createTaskInstance(AsyncStreamTask task, TaskName taskName, SystemStreamPartition ssp) {
    TaskInstanceMetrics taskInstanceMetrics = new TaskInstanceMetrics("task", new MetricsRegistryMap());
    scala.collection.immutable.Set<SystemStreamPartition> sspSet = JavaConversions.asScalaSet(Collections.singleton(ssp)).toSet();
    return new TaskInstance<AsyncStreamTask>(task, taskName, mock(Config.class), taskInstanceMetrics,
        null, consumerMultiplexer, mock(TaskInstanceCollector.class), mock(SamzaContainerContext.class),
        offsetManager, null, null, sspSet, new TaskInstanceExceptionHandler(taskInstanceMetrics, new scala.collection.immutable.HashSet<String>()));
  }

  ExecutorService callbackExecutor;
  void triggerCallback(final TestTask task, final TaskCallback callback, final boolean success) {
    callbackExecutor.submit(new Runnable() {
      @Override
      public void run() {
        if (task.code != null) {
          task.code.run(callback);
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

  class TestTask implements AsyncStreamTask, WindowableTask {
    boolean shutdown = false;
    boolean commit = false;
    boolean success;
    int processed = 0;
    volatile int windowCount = 0;

    AtomicInteger completed = new AtomicInteger(0);
    TestCode code = null;

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
        coordinator.commit(commitRequest);
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
  }

  @Before
  public void setup() {
    executor = null;
    consumerMultiplexer = mock(SystemConsumers.class);
    windowMs = -1;
    commitMs = -1;
    maxMessagesInFlight = 1;
    containerMetrics = new SamzaContainerMetrics("container", new MetricsRegistryMap());
    callbackExecutor = Executors.newFixedThreadPool(2);
    offsetManager = mock(OffsetManager.class);
    shutdownRequest = TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER;

    when(consumerMultiplexer.pollIntervalMs()).thenReturn(1000000);

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

  @Test
  public void testProcessOutOfOrder() throws Exception {
    maxMessagesInFlight = 2;

    final CountDownLatch latch = new CountDownLatch(1);
    task0.code = new TestCode() {
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
}
