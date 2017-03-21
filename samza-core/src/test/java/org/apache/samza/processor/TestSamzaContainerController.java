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

  // Test error in Container invokes the  configured callback
  @Test
  public void testContainerErrorInvokesProcessorCallback() throws Exception {
    final ProcessorLifecycleCallback callback = new MyCallback();

    SamzaContainerController controller = new SamzaContainerController(
        PowerMockito.mock(StreamTaskFactory.class), callback, new HashMap<String, MetricsReporter>());
    Config config = new MapConfig();

    JmxServer mockJmxServer = PowerMockito.mock(JmxServer.class);
    PowerMockito.whenNew(JmxServer.class).withAnyArguments().thenReturn(mockJmxServer);


    TaskName tn = new TaskName("taskName");
    TaskModel tm = new TaskModel(tn, Collections.<SystemStreamPartition>emptySet(), new Partition(0));
    ContainerModel cm = new ContainerModel(1, new HashMap<TaskName, TaskModel>() {
      {
        put(tn, tm);
      }
    });

    RunLoop mockRunloop = PowerMockito.mock(RunLoop.class);
    PowerMockito.doThrow(new SamzaException("Something has to fail!")).when(mockRunloop).run();

    PowerMockito.mockStatic(SamzaContainerWrapper.class);
    SamzaContainer mockContainer = PowerMockito.mock(SamzaContainer.class);
    PowerMockito.doThrow(new SamzaException("Failure in container initialization")).when(mockContainer).run();

    PowerMockito
        .when(SamzaContainerWrapper.createInstance(eq(1), any(ContainerModel.class), eq(config), eq(1), eq(null),
            eq(mockJmxServer), any(scala.collection.immutable.Map.class), any(StreamTaskFactory.class)))
        .thenReturn(mockContainer);


    controller.startContainer(cm, config, 1);

    try {
      ((MyCallback) callback).hold.await(2000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      Assert.fail("Failed waiting for callback to be invoked. Exception: " + e.getMessage());
    }
    controller.stopContainer();

    try {
      controller.awaitStop(2000);
    } catch (InterruptedException e) { }
  }

  // Test multiple start / stop is idempotent
  @Test
  public void testMultipleStart() throws Exception {
    // returns false, if startContainer was never called or containerFuture is not done yet
    SamzaContainerController controller = new SamzaContainerController(
        PowerMockito.mock(StreamTaskFactory.class), null, new HashMap<String, MetricsReporter>());
    Config config = new MapConfig();

    JmxServer mockJmxServer = PowerMockito.mock(JmxServer.class);
    PowerMockito.whenNew(JmxServer.class).withAnyArguments().thenReturn(mockJmxServer);


    TaskName tn = new TaskName("taskName");
    TaskModel tm = new TaskModel(tn, Collections.<SystemStreamPartition>emptySet(), new Partition(0));
    ContainerModel cm = new ContainerModel(1, new HashMap<TaskName, TaskModel>() {
      {
        put(tn, tm);
      }
    });

    PowerMockito.mockStatic(SamzaContainerWrapper.class);
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

    PowerMockito
        .when(SamzaContainerWrapper.createInstance(eq(1), any(ContainerModel.class), eq(config), eq(1), eq(null),
            eq(mockJmxServer), any(scala.collection.immutable.Map.class), any(StreamTaskFactory.class)))
        .thenReturn(mockContainer);

    try {
      controller.stopContainer();
      controller.awaitStop(1000);
      controller.startContainer(cm, config, 1);
      controller.startContainer(cm, config, 1);
      controller.startContainer(cm, config, 1);
    } catch (Exception e) {
      Assert.fail("Exception thrown when startContainer was called multiple times!\n" + e);
    }

    controller.stopContainer();
    controller.stopContainer();
  }


  // Test startContainer the container run loop

  // Test stopContainer stops the container thread

  @Test
  public void testIsContainerRunning() throws Exception {
    // returns false, if startContainer was never called or containerFuture is not done yet
    SamzaContainerController controller = new SamzaContainerController(
        PowerMockito.mock(StreamTaskFactory.class), null, new HashMap<String, MetricsReporter>());
    Config config = new MapConfig();

    JmxServer mockJmxServer = PowerMockito.mock(JmxServer.class);
    PowerMockito.whenNew(JmxServer.class).withAnyArguments().thenReturn(mockJmxServer);


    TaskName tn = new TaskName("taskName");
    TaskModel tm = new TaskModel(tn, Collections.<SystemStreamPartition>emptySet(), new Partition(0));
    ContainerModel cm = new ContainerModel(1, new HashMap<TaskName, TaskModel>() {
      {
        put(tn, tm);
      }
    });

    PowerMockito.mockStatic(SamzaContainerWrapper.class);
    SamzaContainer mockContainer = PowerMockito.mock(SamzaContainer.class);
    final CountDownLatch latch = new CountDownLatch(1);

    PowerMockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        latch.await(3000, TimeUnit.MILLISECONDS);
        return null;
      }
    }).when(mockContainer).run();

    PowerMockito
        .when(SamzaContainerWrapper.createInstance(eq(1), any(ContainerModel.class), eq(config), eq(1), eq(null),
            eq(mockJmxServer), any(scala.collection.immutable.Map.class), any(StreamTaskFactory.class)))
        .thenReturn(mockContainer);

    controller.startContainer(cm, new MapConfig(), 1);
    controller.awaitStart(1000);
    Assert.assertTrue(controller.isContainerRunning());
    latch.countDown();
    controller.stopContainer();
    controller.awaitStop(1000);
    Assert.assertFalse(controller.isContainerRunning());
  }
}
