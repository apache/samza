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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.SamzaContainerStatus;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.RunLoop;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.processor.StreamProcessor.State;
import org.apache.samza.runtime.ProcessorLifecycleListener;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TestStreamProcessor {
  private ConcurrentMap<ListenerCallback, Boolean> processorListenerState;
  private enum ListenerCallback {
    BEFORE_START, AFTER_START, BEFORE_STOP, AFTER_STOP, AFTER_STOP_WITH_FAILURE
  }

  @Before
  public void before() {
    Mockito.reset();
    processorListenerState = new ConcurrentHashMap<ListenerCallback, Boolean>() {
      {
        put(ListenerCallback.BEFORE_START, false);
        put(ListenerCallback.AFTER_START, false);
        put(ListenerCallback.BEFORE_STOP, false);
        put(ListenerCallback.AFTER_STOP, false);
        put(ListenerCallback.AFTER_STOP_WITH_FAILURE, false);
      }
    };
  }

  @After
  public void after() {
    processorListenerState.clear();
  }

  class TestableStreamProcessor extends StreamProcessor {
    private final CountDownLatch containerStop = new CountDownLatch(1);
    private final CountDownLatch runLoopStartForMain = new CountDownLatch(1);
    public SamzaContainer container = null;

    public TestableStreamProcessor(
        Config config,
        Map<String, MetricsReporter> customMetricsReporters,
        StreamTaskFactory streamTaskFactory,
        ProcessorLifecycleListener processorListener,
        JobCoordinator jobCoordinator,
        SamzaContainer container) {
      super(config, customMetricsReporters, streamTaskFactory, processorListener, jobCoordinator);
      this.container = container;
    }

    @Override
    SamzaContainer createSamzaContainer(String processorId, JobModel jobModel) {
      if (container == null) {
        RunLoop mockRunLoop = mock(RunLoop.class);
        doAnswer(invocation ->
          {
            try {
              runLoopStartForMain.countDown();
              containerStop.await();
            } catch (InterruptedException e) {
              System.out.println("In exception" + e);
              e.printStackTrace();
            }
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
          public void afterStop(Throwable t) {
            if (t != null) {
              processorListenerState.put(ListenerCallback.AFTER_STOP_WITH_FAILURE, true);
            } else {
              processorListenerState.put(ListenerCallback.AFTER_STOP, true);
              processorListenerStop.countDown();
            }
          }

          @Override
          public void beforeStop() {
            processorListenerState.put(ListenerCallback.BEFORE_STOP, true);
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
    processorListenerStart.await();

    Assert.assertEquals(SamzaContainerStatus.STARTED, processor.getContainerStatus());

    // This block is required for the mockRunloop is actually start.
    // Otherwise, processor.stop gets triggered before mockRunloop begins to block
    processor.runLoopStartForMain.await();

    processor.stop();

    processorListenerStop.await();

    // Assertions on which callbacks are expected to be invoked
    Assert.assertTrue(processorListenerState.get(ListenerCallback.BEFORE_START));
    Assert.assertTrue(processorListenerState.get(ListenerCallback.AFTER_START));
    Assert.assertTrue(processorListenerState.get(ListenerCallback.BEFORE_STOP));
    Assert.assertTrue(processorListenerState.get(ListenerCallback.AFTER_STOP));
    Assert.assertFalse(processorListenerState.get(ListenerCallback.AFTER_STOP_WITH_FAILURE));
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
          public void beforeStop() {
            processorListenerState.put(ListenerCallback.BEFORE_STOP, true);
          }

          @Override
          public void afterStop(Throwable t) {
            if (t == null) {
              // successful stop
              processorListenerState.put(ListenerCallback.AFTER_STOP, true);
            } else {
              processorListenerState.put(ListenerCallback.AFTER_STOP_WITH_FAILURE, true);
              actualThrowable.getAndSet(t);
              processorListenerFailed.countDown();
            }
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
    Assert.assertTrue(
        "Container failed and processor listener failed was not invoked within timeout!",
        processorListenerFailed.await(30, TimeUnit.SECONDS));
    Assert.assertEquals(expectedThrowable, actualThrowable.get());

    Assert.assertTrue(processorListenerState.get(ListenerCallback.BEFORE_START));
    Assert.assertTrue(processorListenerState.get(ListenerCallback.AFTER_START));
    Assert.assertFalse(processorListenerState.get(ListenerCallback.BEFORE_STOP));
    Assert.assertFalse(processorListenerState.get(ListenerCallback.AFTER_STOP));
    Assert.assertTrue(processorListenerState.get(ListenerCallback.AFTER_STOP_WITH_FAILURE));
  }

  @Test
  public void testStartOperationShouldBeIdempotent() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    Mockito.doNothing().when(mockJobCoordinator).start();
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    StreamProcessor streamProcessor = new StreamProcessor(new MapConfig(), new HashMap<>(), null, lifecycleListener, mockJobCoordinator);
    Assert.assertEquals(State.NEW, streamProcessor.getState());
    streamProcessor.start();

    Assert.assertEquals(State.STARTED, streamProcessor.getState());

    streamProcessor.start();

    Assert.assertEquals(State.STARTED, streamProcessor.getState());

    Mockito.verify(mockJobCoordinator, Mockito.times(1)).start();
  }

  @Test
  public void testOnJobModelExpiredShouldMakeCorrectStateTransitions() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    SamzaContainer mockSamzaContainer = Mockito.mock(SamzaContainer.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = new StreamProcessor(config, new HashMap<>(), null, lifecycleListener, mockJobCoordinator);

    /**
     * Without a SamzaContainer running in StreamProcessor and current StreamProcessor state is STARTED,
     * onJobModelExpired should move the state to IN_REBALANCE.
     */

    streamProcessor.start();

    Assert.assertEquals(State.STARTED, streamProcessor.getState());

    streamProcessor.jobCoordinatorListener.onJobModelExpired();

    Assert.assertEquals(State.IN_REBALANCE, streamProcessor.getState());

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

    Assert.assertEquals(State.STOPPING, streamProcessor.getState());
    Mockito.verify(mockSamzaContainer, Mockito.times(1)).shutdown();
    Mockito.verify(mockJobCoordinator, Mockito.times(1)).stop();

    // If StreamProcessor is in IN_REBALANCE state, onJobModelExpired should be a NO_OP.
    streamProcessor.state = State.IN_REBALANCE;

    streamProcessor.jobCoordinatorListener.onJobModelExpired();

    Assert.assertEquals(State.IN_REBALANCE, streamProcessor.state);
  }

  @Test
  public void testOnNewJobModelShouldResultInValidStateTransitions() throws Exception {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    SamzaContainer mockSamzaContainer = Mockito.mock(SamzaContainer.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = PowerMockito.spy(new StreamProcessor(config, new HashMap<>(), null, lifecycleListener, mockJobCoordinator));

    streamProcessor.container = mockSamzaContainer;
    streamProcessor.state = State.IN_REBALANCE;
    Mockito.doNothing().when(mockSamzaContainer).run();

    streamProcessor.jobCoordinatorListener.onNewJobModel("TestProcessorId", new JobModel(new MapConfig(), new HashMap<>()));

    Mockito.verify(mockSamzaContainer, Mockito.atMost(1)).run();
  }

  @Test
  public void testStopShouldBeIdempotent() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    SamzaContainer mockSamzaContainer = Mockito.mock(SamzaContainer.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = PowerMockito.spy(new StreamProcessor(config, new HashMap<>(), null, lifecycleListener, mockJobCoordinator));

    Mockito.doNothing().when(mockJobCoordinator).stop();
    Mockito.doNothing().when(mockSamzaContainer).shutdown();
    Mockito.when(mockSamzaContainer.hasStopped()).thenReturn(false);
    Mockito.when(mockSamzaContainer.getStatus())
           .thenReturn(SamzaContainerStatus.STARTED)
           .thenReturn(SamzaContainerStatus.STOPPED);

    streamProcessor.state = State.RUNNING;

    streamProcessor.stop();

    Assert.assertEquals(State.STOPPING, streamProcessor.state);
  }

  @Test
  public void testCoordinatorFailureShouldStopTheStreamProcessor() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    SamzaContainer mockSamzaContainer = Mockito.mock(SamzaContainer.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = new StreamProcessor(config, new HashMap<>(), null, lifecycleListener, mockJobCoordinator);

    Exception failureException = new Exception("dummy exception");

    streamProcessor.container = mockSamzaContainer;
    streamProcessor.state = State.RUNNING;
    streamProcessor.jobCoordinatorListener.onCoordinatorFailure(failureException);
    Mockito.doNothing().when(mockSamzaContainer).shutdown();
    Mockito.when(mockSamzaContainer.hasStopped()).thenReturn(false);


    Assert.assertEquals(State.STOPPED, streamProcessor.state);
    Mockito.verify(lifecycleListener).afterStop(failureException);
    Mockito.verify(mockSamzaContainer).shutdown();
  }

  @Test
  public void testCoordinatorStopShouldStopTheStreamProcessor() {
    JobCoordinator mockJobCoordinator = Mockito.mock(JobCoordinator.class);
    ProcessorLifecycleListener lifecycleListener = Mockito.mock(ProcessorLifecycleListener.class);
    MapConfig config = new MapConfig(ImmutableMap.of("task.shutdown.ms", "0"));
    StreamProcessor streamProcessor = new StreamProcessor(config, new HashMap<>(), null, lifecycleListener, mockJobCoordinator);

    streamProcessor.state = State.RUNNING;
    streamProcessor.jobCoordinatorListener.onCoordinatorStop();

    Assert.assertEquals(State.STOPPED, streamProcessor.state);
    Mockito.verify(lifecycleListener).afterStop(null);
  }
}
