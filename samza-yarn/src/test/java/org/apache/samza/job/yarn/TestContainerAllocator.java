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

package org.apache.samza.job.yarn;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.YarnConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.job.yarn.util.MockContainerListener;
import org.apache.samza.job.yarn.util.MockContainerRequestState;
import org.apache.samza.job.yarn.util.MockHttpServer;
import org.apache.samza.job.yarn.util.TestAMRMClientImpl;
import org.apache.samza.job.yarn.util.TestUtil;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestContainerAllocator {
  private static final String ANY_HOST = ContainerRequestState.ANY_HOST;
  private final HttpServer server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));

  private AMRMClientAsyncImpl amRmClientAsync;
  private TestAMRMClientImpl testAMRMClient;
  private MockContainerRequestState requestState;
  private ContainerAllocator containerAllocator;
  private Thread allocatorThread;

  private Config config = new MapConfig(new HashMap<String, String>() {
    {
      put("yarn.container.count", "1");
      put("systems.test-system.samza.factory", "org.apache.samza.job.yarn.MockSystemFactory");
      put("yarn.container.memory.mb", "512");
      put("yarn.package.path", "/foo");
      put("task.inputs", "test-system.test-stream");
      put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
      put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
      put("yarn.container.retry.count", "1");
      put("yarn.container.retry.window.ms", "1999999999");
      put("yarn.allocator.sleep.ms", "10");
    }
  });

  private SamzaAppState state = new SamzaAppState(getCoordinator(1), -1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2);

  private JobCoordinator getCoordinator(int containerCount) {
    Map<Integer, ContainerModel> containers = new java.util.HashMap<>();
    for (int i = 0; i < containerCount; i++) {
      ContainerModel container = new ContainerModel(i, new HashMap<TaskName, TaskModel>());
      containers.put(i, container);
    }
    JobModel jobModel = new JobModel(config, containers);
    return new JobCoordinator(jobModel, server);
  }

  @Before
  public void setup() throws Exception {
    // Create AMRMClient
    testAMRMClient = new TestAMRMClientImpl(
        TestUtil.getAppMasterResponse(
            false,
            new ArrayList<Container>(),
            new ArrayList<ContainerStatus>()
        ));
    amRmClientAsync = TestUtil.getAMClient(testAMRMClient);

    // Initialize certain state variables (mostly to avoid NPE)
    state.coordinatorUrl = new URL("http://localhost:7778/");

    requestState = new MockContainerRequestState(amRmClientAsync, false);
    containerAllocator = new ContainerAllocator(
        amRmClientAsync,
        TestUtil.getContainerUtil(config, state),
        new YarnConfig(config)
    );
    Field requestStateField = containerAllocator.getClass().getSuperclass().getDeclaredField("containerRequestState");
    requestStateField.setAccessible(true);
    requestStateField.set(containerAllocator, requestState);

    allocatorThread = new Thread(containerAllocator);
  }

  @After
  public void teardown() throws Exception {
    containerAllocator.setIsRunning(false);
    allocatorThread.join();
  }

  /**
   * Adds all containers returned to ANY_HOST only
   */
  @Test
  public void testAddContainer() throws Exception {
    assertNull(requestState.getContainersOnAHost("abc"));
    assertNull(requestState.getContainersOnAHost(ANY_HOST));

    containerAllocator.addContainer(TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "abc", 123));
    containerAllocator.addContainer(TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000003"), "xyz", 123));

    assertNull(requestState.getContainersOnAHost("abc"));
    assertNotNull(requestState.getContainersOnAHost(ANY_HOST));
    assertTrue(requestState.getContainersOnAHost(ANY_HOST).size() == 2);
  }

  /**
   * Test requestContainers
   */
  @Test
  public void testRequestContainers() throws Exception {
    Map<Integer, String> containersToHostMapping = new HashMap<Integer, String>() {
      {
        put(0, "abc");
        put(1, "def");
        put(2, null);
        put(3, "abc");
      }
    };

    allocatorThread.start();

    containerAllocator.requestContainers(containersToHostMapping);

    assertNotNull(testAMRMClient.requests);
    assertEquals(4, testAMRMClient.requests.size());

    assertNotNull(requestState);

    assertNotNull(requestState.getRequestsQueue());
    assertTrue(requestState.getRequestsQueue().size() == 4);

    // If host-affinty is not enabled, it doesn't update the requestMap
    assertNotNull(requestState.getRequestsToCountMap());
    assertTrue(requestState.getRequestsToCountMap().keySet().size() == 0);
  }

  /**
   * Test request containers with no containerToHostMapping makes the right number of requests
   */
  @Test
  public void testRequestContainersWithNoMapping() throws Exception {
    int containerCount = 4;
    Map<Integer, String> containersToHostMapping = new HashMap<Integer, String>();
    for (int i = 0; i < containerCount; i++) {
      containersToHostMapping.put(i, null);
    }
    allocatorThread.start();

    containerAllocator.requestContainers(containersToHostMapping);

    assertNotNull(requestState);

    assertNotNull(requestState.getRequestsQueue());
    assertTrue(requestState.getRequestsQueue().size() == 4);

    // If host-affinty is not enabled, it doesn't update the requestMap
    assertNotNull(requestState.getRequestsToCountMap());
    assertTrue(requestState.getRequestsToCountMap().keySet().size() == 0);
  }

  /**
   * Extra allocated containers that are returned by the RM and unused by the AM should be released.
   * Containers are considered "extra" only when there are no more pending requests to fulfill
   * @throws Exception
   */
  @Test
  public void testAllocatorReleasesExtraContainers() throws Exception {
    final Container container = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "abc", 123);
    final Container container1 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "abc", 123);
    final Container container2 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000003"), "def", 123);

    // Set up our final asserts before starting the allocator thread
    MockContainerListener listener = new MockContainerListener(3, 2, null, new Runnable() {
      @Override
      public void run() {
        assertNotNull(testAMRMClient.getRelease());
        assertEquals(2, testAMRMClient.getRelease().size());
        assertTrue(testAMRMClient.getRelease().contains(container1.getId()));
        assertTrue(testAMRMClient.getRelease().contains(container2.getId()));

        // Test that state is cleaned up
        assertEquals(0, requestState.getRequestsQueue().size());
        assertEquals(0, requestState.getRequestsToCountMap().size());
        assertNull(requestState.getContainersOnAHost("abc"));
        assertNull(requestState.getContainersOnAHost("def"));
      }
    });
    requestState.registerContainerListener(listener);

    allocatorThread.start();

    containerAllocator.requestContainer(0, "abc");

    containerAllocator.addContainer(container);
    containerAllocator.addContainer(container1);
    containerAllocator.addContainer(container2);

    listener.verify();
  }

}
