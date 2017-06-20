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

import org.apache.samza.SamzaContainerStatus;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.RunLoop;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStreamProcessor {

  class TestableStreamProcessor extends StreamProcessor {
    private final CountDownLatch containerStop = new CountDownLatch(1);
    private final CountDownLatch runLoopStartForMain = new CountDownLatch(1);
    public SamzaContainer container = null;

    public TestableStreamProcessor(
        Config config,
        Map<String, MetricsReporter> customMetricsReporters,
        StreamTaskFactory streamTaskFactory,
        StreamProcessorLifecycleListener processorListener,
        JobCoordinator jobCoordinator,
        SamzaContainer container) {
      super(config, customMetricsReporters, streamTaskFactory, processorListener, jobCoordinator);
      this.container = container;
    }

    @Override
    SamzaContainer createSamzaContainer(
        ContainerModel containerModel,
        int maxChangelogStreamPartitions) {
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

        doAnswer(invocation ->
        {
          containerStop.countDown();
          return null;
        }).when(mockRunLoop).shutdown();
        container = StreamProcessorTestUtils.getDummyContainer(mockRunLoop, mock(StreamTask.class));
      }
      return container;
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
  public void testStopByProcessor() {
    JobCoordinator mockJobCoordinator = mock(JobCoordinator.class);

    final CountDownLatch processorListenerStop = new CountDownLatch(1);
    final CountDownLatch processorListenerStart = new CountDownLatch(1);

    TestableStreamProcessor processor = new TestableStreamProcessor(
        new MapConfig(),
        new HashMap<>(),
        mock(StreamTaskFactory.class),
        new StreamProcessorLifecycleListener() {
          @Override
          public void onStart() {
            processorListenerStart.countDown();
          }

          @Override
          public void onShutdown() {
            processorListenerStop.countDown();
          }

          @Override
          public void onFailure(Throwable t) {

          }
        },
        mockJobCoordinator,
        null);

    final CountDownLatch coordinatorStop = new CountDownLatch(1);
    final Thread jcThread = new Thread(() ->
      {
        try {
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

    try {
      processor.start();
      processorListenerStart.await();

      Assert.assertEquals(SamzaContainerStatus.STARTED, processor.container.getStatus());

      // This block is required for the mockRunloop is actually start.
      // Otherwise, processor.stop gets triggered before mockRunloop begins to block
      processor.runLoopStartForMain.await();

      processor.stop();

      processorListenerStop.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Tests that a failure in container correctly stops a running JobCoordinator and propagates the exception
   * through the StreamProcessor
   *
   * Assertions:
   * - JobCoordinator has been stopped can be asserted from the JobCoordinatorListener callback
   * - StreamProcessorLifecycleListener#onFailure(Throwable) has been invoked
   */
  @Test
  public void testContainerFailureCorrectlyStopsProcessor() {
    JobCoordinator mockJobCoordinator = mock(JobCoordinator.class);
    Throwable expectedThrowable =  new SamzaException("Failure in Container!");
    AtomicReference<Throwable> actualThrowable = new AtomicReference<>();
    final CountDownLatch runLoopStartedLatch = new CountDownLatch(1);
    RunLoop failingRunLoop = mock(RunLoop.class);
    doAnswer(invocation -> {
      try {
        Thread.sleep(10);
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
        new StreamProcessorLifecycleListener() {
          @Override
          public void onStart() { }

          @Override
          public void onShutdown() { }

          @Override
          public void onFailure(Throwable t) {
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
          processor.jobCoordinatorListener.onNewJobModel("1", getMockJobModel());
          coordinatorStop.await();
          processor.jobCoordinatorListener.onCoordinatorStop();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }).start();
      return null;
    }).when(mockJobCoordinator).start();

    try {
      processor.start();

      // This block is required for the mockRunloop is actually start.
      // Otherwise, processor.stop gets triggered before mockRunloop begins to block
      runLoopStartedLatch.await();
      Assert.assertTrue(
          "Container failed and processor listener failed was not invoked within timeout!",
          processorListenerFailed.await(30, TimeUnit.SECONDS));
      Assert.assertEquals(expectedThrowable, actualThrowable.get());

    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  // TODO:
  // Test multiple start / stop and its ordering
  // test onNewJobModel
  // test onJobModelExpiry
  // test Coordinator failure - correctly shutsdown the streamprocessor
}
