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
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.RunLoop;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStreamProcessor {

  class TestableStreamProcessor extends StreamProcessor {
    private final CountDownLatch containerStop = new CountDownLatch(1);
    private final CountDownLatch runLoopStartForMain = new CountDownLatch(1);
    private SamzaContainer containerReference = null;

    public TestableStreamProcessor(
        Config config,
        Map<String, MetricsReporter> customMetricsReporters,
        StreamTaskFactory streamTaskFactory,
        StreamProcessorLifecycleListener processorListener,
        JobCoordinator jobCoordinator) {
      super(config, customMetricsReporters, streamTaskFactory, processorListener, jobCoordinator);
    }

    @Override
    SamzaContainer createSamzaContainer(
        ContainerModel containerModel,
        int maxChangelogStreamPartitions,
        JmxServer jmxServer) {
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
      containerReference = StreamProcessorTestUtils.getDummyContainer(mockRunLoop, null, mock(StreamTask.class));
      return containerReference;
    }
  }

  /**
   * Tests stop() method when container and JobCoordinator are running
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
        mockJobCoordinator);

    Map containers = mock(Map.class);
    doReturn(true).when(containers).containsKey(anyString());
    when(containers.get(anyString())).thenReturn(mock(ContainerModel.class));
    JobModel mockJobModel = mock(JobModel.class);
    when(mockJobModel.getContainers()).thenReturn(containers);

    final CountDownLatch coordinatorStop = new CountDownLatch(1);
    final Thread jcThread = new Thread(() ->
      {
        try {
          processor.jobCoordinatorListener.onNewJobModel("1", mockJobModel);
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

      Assert.assertEquals(SamzaContainerStatus.STARTED, processor.containerReference.getStatus());

      // This block is required for the mockRunloop is actually start.
      // Otherwise, processor.stop gets triggered before mockRunloop begins to block
      processor.runLoopStartForMain.await();

      processor.stop();

      processorListenerStop.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  // TODO:
  // Test multiple start / stop and its ordering
  // test onNewJobModel
  // test onJobModelExpiry
  // test Coordinator failure - correctly shutsdown the streamprocessor
  // test Container failure
}
