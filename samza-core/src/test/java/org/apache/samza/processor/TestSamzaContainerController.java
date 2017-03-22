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

import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.RunLoop;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.StreamTaskFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SamzaContainerController.class, SamzaContainerWrapper.class, RunLoop.class})
@PowerMockIgnore({"javax.management.*", "javax.naming.*"})
public class TestSamzaContainerController {

  class MyCallback implements ProcessorLifecycleCallback {
    public volatile CountDownLatch hold = new CountDownLatch(1);
    @Override
    public void onError(Throwable error) {
      hold.countDown();
    }
  }

  private ContainerModel getContainerModel(int containerId, String taskName) {
    TaskName tn = new TaskName(taskName);
    TaskModel tm = new TaskModel(tn, Collections.<SystemStreamPartition>emptySet(), new Partition(0));
    return new ContainerModel(containerId, new HashMap<TaskName, TaskModel>() {
      {
        put(tn, tm);
      }
    });
  }

  // Tests that any error during the {@link RunLoop} execution invokes the lifecycleCallback being passed in
  @Test
  public void testContainerErrorInvokesProcessorCallback() {
    ContainerModel cm = getContainerModel(1, "taskName");

    // Configure a mock SamzaContainer
    SamzaContainer mockContainer = PowerMockito.mock(SamzaContainer.class);
    PowerMockito.doThrow(new SamzaException("Failure in container initialization")).when(mockContainer).run();

    // Mock for static method in SamzaContainerWrapper
    PowerMockito.mockStatic(SamzaContainerWrapper.class);
    PowerMockito
        .when(SamzaContainerWrapper.createInstance(eq(1), any(ContainerModel.class), any(Config.class), eq(1), eq(null),
            any(JmxServer.class), any(scala.collection.immutable.Map.class), any(StreamTaskFactory.class)))
        .thenReturn(mockContainer);

    final ProcessorLifecycleCallback callback = new MyCallback();
    SamzaContainerController controller = new SamzaContainerController(
        PowerMockito.mock(StreamTaskFactory.class), TaskConfigJava.DEFAULT_TASK_SHUTDOWN_MS, callback, new HashMap<String, MetricsReporter>());

    controller.startContainer(cm, new MapConfig(), 1);
    try {
      ((MyCallback) callback).hold.await(2000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      Assert.fail("Failed waiting for callback to be invoked. Exception: " + e.getMessage());
    }
    controller.stopContainer();
  }

  @Test
  public void testIsContainerRunning() throws Exception {
    ContainerModel cm = getContainerModel(1, "taskName");

    // Configure a mock SamzaContainer
    SamzaContainer mockContainer = PowerMockito.mock(SamzaContainer.class);
    final CountDownLatch latch = new CountDownLatch(1);

    PowerMockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        latch.await(3000, TimeUnit.MILLISECONDS);
        return null;
      }
    }).when(mockContainer).run();

    PowerMockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        latch.countDown();
        return null;
      }
    }).when(mockContainer).shutdown();

    // Mock for static method in SamzaContainerWrapper
    PowerMockito.mockStatic(SamzaContainerWrapper.class);
    PowerMockito
        .when(SamzaContainerWrapper.createInstance(eq(1), any(ContainerModel.class), any(Config.class), eq(1), eq(null),
            any(JmxServer.class), any(scala.collection.immutable.Map.class), any(StreamTaskFactory.class)))
        .thenReturn(mockContainer);

    SamzaContainerController controller = new SamzaContainerController(
        PowerMockito.mock(StreamTaskFactory.class), TaskConfigJava.DEFAULT_TASK_SHUTDOWN_MS, null, new HashMap<String, MetricsReporter>());
    Assert.assertFalse(controller.isContainerRunning());

    controller.startContainer(cm, new MapConfig(), 1);
    controller.awaitStart(1000);
    Assert.assertTrue(controller.isContainerRunning());

    controller.stopContainer();
    Assert.assertFalse(controller.isContainerRunning());
  }

  @Test
  public void testMultipleStartStop() throws Exception {
    ContainerModel cm = getContainerModel(1, "taskName");

    SamzaContainer mockContainer = PowerMockito.mock(SamzaContainer.class);
    final CountDownLatch latch = new CountDownLatch(1);

    PowerMockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        latch.await(3000, TimeUnit.MILLISECONDS);
        return null;
      }
    }).when(mockContainer).run();

    PowerMockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        latch.countDown();
        return null;
      }
    }).when(mockContainer).shutdown();

    PowerMockito.mockStatic(SamzaContainerWrapper.class);
    PowerMockito
        .when(SamzaContainerWrapper.createInstance(eq(1), any(ContainerModel.class), any(Config.class), eq(1), eq(null),
            any(JmxServer.class), any(scala.collection.immutable.Map.class), any(StreamTaskFactory.class)))
        .thenReturn(mockContainer);

    SamzaContainerController controller = new SamzaContainerController(
        PowerMockito.mock(StreamTaskFactory.class), TaskConfigJava.DEFAULT_TASK_SHUTDOWN_MS, null, new HashMap<String, MetricsReporter>());

    try {
      controller.stopContainer();
      controller.startContainer(cm, new MapConfig(), 1);
      controller.startContainer(cm, new MapConfig(), 1);
      controller.startContainer(cm, new MapConfig(), 1);
    } catch (Exception e) {
      Assert.fail("Exception thrown when startContainer was called multiple times!\n" + e);
    }

    controller.stopContainer();
    controller.stopContainer();
  }
}
