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
package org.apache.samza.processor;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.RunLoop;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainerStatus;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.processor.StreamProcessor.State;
import org.apache.samza.runtime.ProcessorLifecycleListener;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStreamProcessor {
  private ConcurrentMap<ListenerCallback, Boolean> processorListenerState;
  private enum ListenerCallback {
    BEFORE_START, AFTER_START, AFTER_STOP, AFTER_FAILURE
  }

  @Before
  public void before() {
    Mockito.reset();
    processorListenerState = new ConcurrentHashMap<ListenerCallback, Boolean>() {
      {
        put(ListenerCallback.BEFORE_START, false);
        put(ListenerCallback.AFTER_START, false);
        put(ListenerCallback.AFTER_STOP, false);
        put(ListenerCallback.AFTER_FAILURE, false);
      }
    };
  }

  @After
  public void after() {
    processorListenerState.clear();
  }

  static class TestableStreamProcessor extends StreamProcessor {
    private final CountDownLatch containerStop = new CountDownLatch(1);
    private final CountDownLatch runLoopStartForMain = new CountDownLatch(1);
    private SamzaContainer container = null;
    private final Duration runLoopShutdownDuration;

    public TestableStreamProcessor(Config config,
        Map<String, MetricsReporter> customMetricsReporters,
        StreamTaskFactory streamTaskFactory,
        ProcessorLifecycleListener processorListener,
        JobCoordinator jobCoordinator,
        SamzaContainer container) {
      this(config, customMetricsReporters, streamTaskFactory, processorListener, jobCoordinator, container,
          Duration.ZERO);
    }

    public TestableStreamProcessor(Config config,
        Map<String, MetricsReporter> customMetricsReporters,
        StreamTaskFactory streamTaskFactory,
        ProcessorLifecycleListener processorListener,
        JobCoordinator jobCoordinator,
        SamzaContainer container,
        Duration runLoopShutdownDuration) {

      super("TEST_PROCESSOR_ID", config, customMetricsReporters, streamTaskFactory, Optional.empty(), Optional.empty(), Optional.empty(), sp -> processorListener,
          jobCoordinator, Mockito.mock(MetadataStore.class));
      this.container = container;
      this.runLoopShutdownDuration = runLoopShutdownDuration;
    }

    @Override
    SamzaContainer createSamzaContainer(String processorId, JobModel jobModel) {
      if (container == null) {
        RunLoop mockRunLoop = mock(RunLoop.class);
        doAnswer(invocation ->
          {
            runLoopStartForMain.countDown();
            containerStop.await();
            Thread.sleep(this.runLoopShutdownDuration.toMillis());
            return null;
          }).when(mockRunLoop).run();

        Mockito.doAnswer(invocation ->
          {
            containerStop.countDown();
            return null;
          }).when(mockRunLoop).shutdown();
        container = StreamProcessorTestUtils.getDummyContainer(mockRunLoop, Mockito.mock(StreamTask.class));
      }
      return container;
    }

    SamzaContainerStatus getContainerStatus() {
      if (container != null)  return container.getStatus();
      return null;
    }
  }

  private JobModel getMockJobModel() {
    Map containers = mock(Map.class);
    doReturn(true).when(containers).containsKey(anyString());
    when(containers.get(anyString())).thenReturn(mock(ContainerModel.class));
    JobModel mockJobModel = mock(JobModel.class);
    when(mockJobModel.getContainers()).thenReturn(containers);
    return mockJobModel;
  }

  /**
   * Tests stop() method when Container AND JobCoordinator are running
   */
  @Test
  public void testStopByProcessor() throws InterruptedException {
    JobCoordinator mockJobCoordinator = mock(JobCoordinator.class);

    final CountDownLatch processorListenerStop = new CountDownLatch(1);
    final CountDownLatch processorListenerStart = new CountDownLatch(1);

    TestableStreamProcessor processor = new TestableStreamProcessor(
        new MapConfig(),
        new HashMap<>(),
        mock(StreamTaskFactory.class),
        new ProcessorLifecycleListener() {
          @Override
          public void afterStart() {
            processorListenerState.put(ListenerCallback.AFTER_START, true);
            processorListenerStart.countDown();
          }

          @Override
          public void afterFailure(Throwable t) {
            processorListenerState.put(ListenerCallback.AFTER_FAILURE, true);
          }

          @Override
          public void afterStop() {
            processorListenerState.put(ListenerCallback.AFTER_STOP, true);
            processorListenerStop.countDown();
          }

          @Override
          public void beforeStart() {
            processorListenerState.put(ListenerCallback.BEFORE_START, true);
          }
        },
        mockJobCoordinator,
        null);

    final CountDownLatch coordinatorStop = new CountDownLatch(1);
    final Thread jcThread = new Thread(() ->
      {
        try {
          processor.jobCoordinatorListener.onJobModelExpired();
          processor.jobCoordinatorListener.onNewJobModel("1", getMockJobModel());
          coordinatorStop.await();
          processor.jobCoordinatorListener.onCoordinatorStop();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });

    doAnswer(invocation ->
      {
        coordinatorStop.countDown();
        return null;
      }).when(mockJobCoordinator).stop();

    doAnswer(invocation ->
      {
        jcThread.start();
        return null;
      }).when(mockJobCoordinator).start();

    processor.start();
    processorListenerStart.await(10, TimeUnit.SECONDS);

    assertEquals(SamzaContainerStatus.STARTED, processor.getContainerStatus());

    // This block is required for the mockRunloop is actually start.
    // Otherwise, processor.stop gets triggered before mockRunloop begins to block
    processor.runLoopStartForMain.await();

    processor.stop();

    processorListenerStop.await();

    // Assertions on which callbacks are expected to be invoked
    assertTrue(processorListenerState.get(ListenerCallback.BEFORE_START));
    assertTrue(processorListenerState.get(ListenerCallback.AFTER_START));
    assertTrue(processorListenerState.get(ListenerCallback.AFTER_STOP));
    Assert.assertFalse(processorListenerState.get(ListenerCallback.AFTER_FAILURE));
  }

  /**
   * Given that the job model expires, but the container takes too long to stop, a TimeoutException should be propagated
   * to the processor lifecycle listener.
   */
  @Test
  public void testJobModelExpiredContainerShutdownTimeout() throws InterruptedException {
    JobCoordinator mockJobCoordinator = mock(JobCoordinator.class);
    // use this to store the exception passed to afterFailure for the processor lifecycle listener
    AtomicReference<Throwable> afterFailureException = new AtomicReference<>(null);
    TestableStreamProcessor processor = new TestableStreamProcessor(
        // set a small shutdown timeout so it triggers faster
        new MapConfig(ImmutableMap.of(TaskConfig.TASK_SHUTDOWN_MS, "1")),
        new HashMap<>(),
        mock(StreamTaskFactory.class),
        new ProcessorLifecycleListener() {
          @Override
          public void beforeStart() { }

          @Override
          public void afterStart() { }

          @Override
          public void afterFailure(Throwable t) {
            afterFailureException.set(t);
          }

          @Override
          public void afterStop() { }
        },
        mockJobCoordinator,
        null,
        // take an extra second to shut down so that task shutdown timeout gets reached
        Duration.of(1, ChronoUnit.SECONDS));

    Thread jcThread = new Thread(() -> {
        // gets processor into rebalance mode so onNewJobModel creates a new container
        processor.jobCoordinatorListener.onJobModelExpired();
        processor.jobCoordinatorListener.onNewJobModel("1", getMockJobModel());
        try {
          // wait for the run loop to be ready before triggering rebalance
          processor.runLoopStartForMain.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        processor.jobCoordinatorListener.onJobModelExpired();
      });
    doAnswer(invocation -> {
        jcThread.start();
        return null;
      }).when(mockJobCoordinator).start();

    // ensure that the coordinator stop occurred before checking the exception being thrown
    CountDownLatch coordinatorStop = new CountDownLatch(1);
    doAnswer(invocation -> {
        processor.jobCoordinatorListener.onCoordinatorStop();
        coordinatorStop.countDown();
        return null;
      }).when(mockJobCoordinator).stop();

    processor.start();

    // make sure the job model expired callback completed
    assertTrue("Job coordinator stop not called", coordinatorStop.await(10, TimeUnit.SECONDS));
    assertNotNull(afterFailureException.get());
    assertTrue(afterFailureException.get() instanceof TimeoutException);
  }

  /**
   * Tests that a failure in container correctly stops a running JobCoordinator and propagates the exception
   * through the StreamProcessor
   *
   * Assertions:
   * - JobCoordinator has been stopped from the JobCoordinatorListener callback
   * - ProcessorLifecycleListener#afterStop(Throwable) has been invoked w/ non-null Throwable
   */
  @Test
  public void testContainerFailureCorrectlyStopsProcessor() throws InterruptedException {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    Throwable expectedThrowable =  new SamzaException("Failure in Container!");
    AtomicReference<Throwable> actualThrowable = new AtomicReference<>();
    final CountDownLatch runLoopStartedLatch = new CountDownLatch(1);
    RunLoop failingRunLoop = mock(RunLoop.class);
    doAnswer(invocation ->
      {
        try {
          runLoopStartedLatch.countDown();
          throw expectedThrowable;
        } catch (InterruptedException ie) {
          ie.printStackTrace();
        }
        return null;
      }).when(failingRunLoop).run();

    SamzaContainer mockContainer = StreamProcessorTestUtils.getDummyContainer(failingRunLoop, mock(StreamTask.class));
    final CountDownLatch processorListenerFailed = new CountDownLatch(1);

    TestableStreamProcessor processor = new TestableStreamProcessor(
        new MapConfig(),
        new HashMap<>(),
        mock(StreamTaskFactory.class),
        new ProcessorLifecycleListener() {
          @Override
          public void beforeStart() {
            processorListenerState.put(ListenerCallback.BEFORE_START, true);
          }

          @Override
          public void afterStart() {
            processorListenerState.put(ListenerCallback.AFTER_START, true);
          }

          @Override
          public void afterStop() {
            processorListenerState.put(ListenerCallback.AFTER_STOP, true);
          }

          @Override
          public void afterFailure(Throwable t) {
            processorListenerState.put(ListenerCallback.AFTER_FAILURE, true);
            actualThrowable.getAndSet(t);
            processorListenerFailed.countDown();
          }
        },
        mockJobCoordinator,
        mockContainer);

    final CountDownLatch coordinatorStop = new CountDownLatch(1);
    doAnswer(invocation ->
      {
        coordinatorStop.countDown();
        return null;
      }).when(mockJobCoordinator).stop();

    doAnswer(invocation ->
      {
        new Thread(() ->
          {
            try {
              processor.jobCoordinatorListener.onJobModelExpired();
              processor.jobCoordinatorListener.onNewJobModel("1", getMockJobModel());
              coordinatorStop.await();
              processor.jobCoordinatorListener.onCoordinatorStop();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }).start();
        return null;
      }).when(mockJobCoordinator).start();

    processor.start();

    // This block is required for the mockRunloop is actually started.
    // Otherwise, processor.stop gets triggered before mockRunloop begins to block
    runLoopStartedLatch.await();
    assertTrue(
        "Container failed and processor listener failed was not invoked within timeout!",
        processorListenerFailed.await(30, TimeUnit.SECONDS));
    assertEquals(expectedThrowable, actualThrowable.get());

    assertTrue(processorListenerState.get(ListenerCallback.BEFORE_START));
    assertTrue(processorListenerState.get(ListenerCallback.AFTER_START));
    Assert.assertFalse(processorListenerState.get(ListenerCallback.AFTER_STOP));
    assertTrue(processorListenerState.get(ListenerCallback.AFTER_FAILURE));
  }

  @Test
  public void testStartOperationShouldBeIdempotent() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    Mockito.doNothing().when(mockJobCoordinator).start();
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    StreamProcessor streamProcessor = Mockito.spy(new StreamProcessor("TestProcessorId", new MapConfig(),
        new HashMap<>(), null, Optional.empty(), Optional.empty(), Optional.empty(), sp -> lifecycleListener,
        mockJobCoordinator, Mockito.mock(MetadataStore.class)));
    assertEquals(State.NEW, streamProcessor.getState());
    streamProcessor.start();

    assertEquals(State.STARTED, streamProcessor.getState());

    streamProcessor.start();

    assertEquals(State.STARTED, streamProcessor.getState());

    Mockito.verify(mockJobCoordinator, Mockito.times(1)).start();
  }

  @Test
  public void testOnJobModelExpiredShouldMakeCorrectStateTransitions() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    SamzaContainer mockSamzaContainer = Mockito.mock(SamzaContainer.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = new StreamProcessor("TestProcessorId", config, new HashMap<>(), null,
        Optional.empty(), Optional.empty(), Optional.empty(), sp -> lifecycleListener, mockJobCoordinator, Mockito.mock(MetadataStore.class));

    /**
     * Without a SamzaContainer running in StreamProcessor and current StreamProcessor state is STARTED,
     * onJobModelExpired should move the state to IN_REBALANCE.
     */

    streamProcessor.start();

    assertEquals(State.STARTED, streamProcessor.getState());

    streamProcessor.jobCoordinatorListener.onJobModelExpired();

    assertEquals(State.IN_REBALANCE, streamProcessor.getState());

    /**
     * When there's initialized SamzaContainer in StreamProcessor and the container shutdown
     * fails in onJobModelExpired. onJobModelExpired should move StreamProcessor to STOPPING
     * state and should shutdown JobCoordinator.
     */
    Mockito.doNothing().when(mockJobCoordinator).start();
    Mockito.doNothing().when(mockJobCoordinator).stop();
    Mockito.doNothing().when(mockSamzaContainer).shutdown();
    Mockito.when(mockSamzaContainer.hasStopped()).thenReturn(false);
    Mockito.when(mockSamzaContainer.getStatus())
            .thenReturn(SamzaContainerStatus.STARTED)
            .thenReturn(SamzaContainerStatus.STOPPED);
    streamProcessor.container = mockSamzaContainer;
    streamProcessor.state = State.STARTED;

    streamProcessor.jobCoordinatorListener.onJobModelExpired();

    assertEquals(State.STOPPING, streamProcessor.getState());
    Mockito.verify(mockSamzaContainer, Mockito.times(1)).shutdown();
    Mockito.verify(mockJobCoordinator, Mockito.times(1)).stop();
  }

  @Test
  public void testJobModelExpiredDuringAnExistingRebalance() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    ExecutorService mockExecutorService = Mockito.mock(ExecutorService.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = new StreamProcessor("TestProcessorId", config, new HashMap<>(), null,
        Optional.empty(), Optional.empty(), Optional.empty(), sp -> lifecycleListener, mockJobCoordinator, Mockito.mock(MetadataStore.class));

    runJobModelExpireDuringRebalance(streamProcessor, mockExecutorService, false);

    assertEquals(State.IN_REBALANCE, streamProcessor.state);
    assertNotEquals(mockExecutorService, streamProcessor.containerExecutorService);
    Mockito.verify(mockExecutorService, Mockito.times(1)).shutdownNow();
  }

  @Test
  public void testJobModelExpiredDuringAnExistingRebalanceWithContainerInterruptFailed() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    ExecutorService mockExecutorService = Mockito.mock(ExecutorService.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = new StreamProcessor("TestProcessorId", config, new HashMap<>(), null,
        Optional.empty(), Optional.empty(), Optional.empty(), sp -> lifecycleListener, mockJobCoordinator, Mockito.mock(MetadataStore.class));

    runJobModelExpireDuringRebalance(streamProcessor, mockExecutorService, true);

    assertEquals(State.STOPPING, streamProcessor.state);
    assertEquals(mockExecutorService, streamProcessor.containerExecutorService);
    Mockito.verify(mockExecutorService, Mockito.times(1)).shutdownNow();
    Mockito.verify(mockJobCoordinator, Mockito.times(1)).stop();
  }

  private void runJobModelExpireDuringRebalance(StreamProcessor streamProcessor, ExecutorService executorService,
      boolean failContainerInterrupt) {
    SamzaContainer mockSamzaContainer = Mockito.mock(SamzaContainer.class);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    /*
     * When there is an initialized container that hasn't started and the stream processor is still in re-balance phase,
     * subsequent job model expire request should attempt to interrupt the existing container to safely shut it down
     * before proceeding to join the barrier. As part of safe shutdown sequence,  we want to ensure shutdownNow is invoked
     * on the existing executorService to signal interrupt and make sure new executor service is created.
     */

    Mockito.when(executorService.shutdownNow()).thenAnswer(ctx -> {
        if (!failContainerInterrupt) {
          shutdownLatch.countDown();
        }
        return null;
      });
    Mockito.when(executorService.isShutdown()).thenReturn(true);

    streamProcessor.state = State.IN_REBALANCE;
    streamProcessor.container = mockSamzaContainer;
    streamProcessor.containerExecutorService = executorService;
    streamProcessor.containerShutdownLatch = shutdownLatch;

    streamProcessor.jobCoordinatorListener.onJobModelExpired();
  }

  @Test
  public void testOnNewJobModelShouldResultInValidStateTransitions() throws Exception {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    SamzaContainer mockSamzaContainer = Mockito.mock(SamzaContainer.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = new TestableStreamProcessor(config, new HashMap<>(), null,
        lifecycleListener, mockJobCoordinator, mockSamzaContainer);

    streamProcessor.state = State.IN_REBALANCE;
    Mockito.doNothing().when(mockSamzaContainer).run();

    streamProcessor.jobCoordinatorListener.onNewJobModel("TestProcessorId", new JobModel(new MapConfig(), new HashMap<>()));

    Mockito.verify(mockSamzaContainer, Mockito.times(1)).setContainerListener(any());
    Mockito.verify(mockSamzaContainer, Mockito.atMost(1)).run();
  }

  @Test
  public void testStopShouldBeIdempotent() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    SamzaContainer mockSamzaContainer = Mockito.mock(SamzaContainer.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = PowerMockito.spy(new StreamProcessor("TestProcessorId", config, new HashMap<>(),
        null, Optional.empty(), Optional.empty(), Optional.empty(), sp -> lifecycleListener, mockJobCoordinator,
        Mockito.mock(MetadataStore.class)));

    Mockito.doNothing().when(mockJobCoordinator).stop();
    Mockito.doNothing().when(mockSamzaContainer).shutdown();
    Mockito.when(mockSamzaContainer.hasStopped()).thenReturn(false);
    Mockito.when(mockSamzaContainer.getStatus())
           .thenReturn(SamzaContainerStatus.STARTED)
           .thenReturn(SamzaContainerStatus.STOPPED);

    streamProcessor.state = State.RUNNING;

    streamProcessor.stop();

    assertEquals(State.STOPPING, streamProcessor.state);
  }

  @Test
  public void testCoordinatorFailureShouldStopTheStreamProcessor() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    SamzaContainer mockSamzaContainer = Mockito.mock(SamzaContainer.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = new StreamProcessor("TestProcessorId", config, new HashMap<>(), null,
        Optional.empty(), Optional.empty(), Optional.empty(), sp -> lifecycleListener, mockJobCoordinator, Mockito.mock(MetadataStore.class));

    Exception failureException = new Exception("dummy exception");

    streamProcessor.container = mockSamzaContainer;
    streamProcessor.state = State.RUNNING;
    streamProcessor.jobCoordinatorListener.onCoordinatorFailure(failureException);
    Mockito.doNothing().when(mockSamzaContainer).shutdown();
    Mockito.when(mockSamzaContainer.hasStopped()).thenReturn(false);


    assertEquals(State.STOPPED, streamProcessor.state);
    Mockito.verify(lifecycleListener).afterFailure(failureException);
    Mockito.verify(mockSamzaContainer).shutdown();
  }

  @Test
  public void testCoordinatorStopShouldStopTheStreamProcessor() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = new StreamProcessor("TestProcessorId", config, new HashMap<>(), null,
        Optional.empty(), Optional.empty(), Optional.empty(), sp -> lifecycleListener, mockJobCoordinator, Mockito.mock(MetadataStore.class));

    streamProcessor.state = State.RUNNING;
    streamProcessor.jobCoordinatorListener.onCoordinatorStop();

    assertEquals(State.STOPPED, streamProcessor.state);
    Mockito.verify(lifecycleListener).afterStop();
  }

  @Test
  public void testStreamProcessorWithStreamProcessorListenerFactory() {
    AtomicReference<MockStreamProcessorLifecycleListener> mockListener = new AtomicReference<>();
    StreamProcessor streamProcessor =
        new StreamProcessor("TestProcessorId", mock(Config.class), new HashMap<>(), mock(TaskFactory.class),
            Optional.empty(), Optional.empty(), Optional.empty(),
            sp -> mockListener.updateAndGet(old -> new MockStreamProcessorLifecycleListener(sp)),
            mock(JobCoordinator.class), Mockito.mock(MetadataStore.class));
    assertEquals(streamProcessor, mockListener.get().processor);
  }

  class MockStreamProcessorLifecycleListener implements ProcessorLifecycleListener {
    final StreamProcessor processor;

    MockStreamProcessorLifecycleListener(StreamProcessor processor) {
      this.processor = processor;
    }
  }
}
