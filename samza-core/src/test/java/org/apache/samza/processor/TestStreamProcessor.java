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

import org.apache.samza.config.MapConfig;
import org.apache.samza.container.RunLoop;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTask;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TestStreamProcessor {

  // Test multiple start / stop and its ordering
  // test onNewJobModel
  // test onJobModelExpiry
  // test Coordinator failure - correctly shutsdown the streamprocessor
  // test Container failure
  // test sp.stop

  @Test
  public void testStop() throws NoSuchFieldException, IllegalAccessException {
//    SamzaContainer mockContainer = mock(SamzaContainer.class);
    JobCoordinator mockJobCoordinator = mock(JobCoordinator.class);

    final AtomicBoolean processorListenerStopCalled = new AtomicBoolean(false);
    final CountDownLatch processorListenerStop = new CountDownLatch(1);
    StreamProcessor processor = new StreamProcessor(
        "1",
        new MapConfig(),
        new HashMap<String, MetricsReporter>(),
        mock(AsyncStreamTaskFactory.class),
        new StreamProcessorLifecycleListener() {
          @Override
          public void onStart() {

          }

          @Override
          public void onShutdown() {
            processorListenerStopCalled.getAndSet(true);
            processorListenerStop.countDown();
          }

          @Override
          public void onFailure(Throwable t) {

          }
        },
        mockJobCoordinator);

    SamzaContainerListener containerListener = new SamzaContainerListener() {
      @Override
      public void onContainerStart() {
        System.out.println("Container Thread: onContainerStart()");
      }

      @Override
      public void onContainerStop(boolean pausedOrNot) {
        if (!pausedOrNot) {
          System.out.println("Container Thread: onContainerStop()");
          try {
            Field containerField = processor.getClass().getDeclaredField("container");
            containerField.setAccessible(true);
            containerField.set(processor, null);
          } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
          }
          processor.stop();
        }
      }

      @Override
      public void onContainerFailed(Throwable t) {

      }
    };

    final CountDownLatch containerStop = new CountDownLatch(1);

    RunLoop mockRunLoop = mock(RunLoop.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        try {
          System.out.println("Starting Runloop");
          containerStop.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return null;
      }
    }).when(mockRunLoop).run();

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        containerStop.countDown();
        return null;
      }
    }).when(mockRunLoop).shutdown();


    SamzaContainer container = StreamProcessorTestUtils.getDummyContainer(mockRunLoop, containerListener, mock(StreamTask.class));

    Field containerField = processor.getClass().getDeclaredField("container");
    containerField.setAccessible(true);
    containerField.set(processor, container);

    final CountDownLatch coordinatorStop = new CountDownLatch(1);
    final Thread jcThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          System.out.println("Starting JC Thread");
          coordinatorStop.await();
          processor.jobCoordinatorListener.onCoordinatorStop();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }, "JOBCOORDINATOR-THREAD");

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        System.out.println("coordinatorStop latch countdown");
        coordinatorStop.countDown();
        return null;
      }
    }).when(mockJobCoordinator).stop();


    new Thread(new Runnable() {
      @Override
      public void run() {
        System.out.println("Starting Container thread - run loop");
        container.run();
      }
    }, "CONTAINER-THREAD").start();

    jcThread.start();

    try {
      Thread.sleep(1000);
      processor.stop();
      processorListenerStop.await();
      jcThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
