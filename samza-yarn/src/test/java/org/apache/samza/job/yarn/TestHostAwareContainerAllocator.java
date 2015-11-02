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
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.samza.job.yarn.util.MockContainerUtil;
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

public class TestHostAwareContainerAllocator {
  private static final String ANY_HOST = ContainerRequestState.ANY_HOST;

  private final HttpServer server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));

  private AMRMClientAsyncImpl amRmClientAsync;
  private TestAMRMClientImpl testAMRMClient;
  private MockContainerRequestState requestState;
  private HostAwareContainerAllocator containerAllocator;
  private Thread allocatorThread;
  private ContainerUtil containerUtil;

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
      put("yarn.samza.host-affinity.enabled", "true");
      put("yarn.container.request.timeout.ms", "3");
      put("yarn.allocator.sleep.ms", "1");
    }
  });

  private Config getConfig() {
    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    return new MapConfig(map);
  }

  private SamzaAppState state = new SamzaAppState(getCoordinator(1), -1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2);

  private JobCoordinator getCoordinator(int containerCount) {
    Map<Integer, ContainerModel> containers = new java.util.HashMap<>();
    for (int i = 0; i < containerCount; i++) {
      ContainerModel container = new ContainerModel(i, new HashMap<TaskName, TaskModel>());
      containers.put(i, container);
    }
    JobModel jobModel = new JobModel(getConfig(), containers);
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

    // Initialize certain state variables
    state.coordinatorUrl = new URL("http://localhost:7778/");

    containerUtil = TestUtil.getContainerUtil(getConfig(), state);

    requestState = new MockContainerRequestState(amRmClientAsync, true);
    containerAllocator = new HostAwareContainerAllocator(
        amRmClientAsync,
        containerUtil,
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
    assertEquals(4, requestState.getRequestsQueue().size());

    assertNotNull(requestState.getRequestsToCountMap());
    assertEquals(1, requestState.getRequestsToCountMap().keySet().size());
    assertTrue(requestState.getRequestsToCountMap().keySet().contains(ANY_HOST));
  }

  /**
   * Add containers to the correct host in the request state
   */
  @Test
  public void testAddContainerWithHostAffinity() throws Exception {
    containerAllocator.requestContainers(new HashMap<Integer, String>() {
      {
        put(0, "abc");
        put(1, "xyz");
      }
    });

    assertNotNull(requestState.getContainersOnAHost("abc"));
    assertEquals(0, requestState.getContainersOnAHost("abc").size());

    assertNotNull(requestState.getContainersOnAHost("xyz"));
    assertEquals(0, requestState.getContainersOnAHost("xyz").size());

    assertNull(requestState.getContainersOnAHost(ANY_HOST));

    containerAllocator.addContainer(TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "abc", 123));
    containerAllocator.addContainer(TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000004"), "def", 123));
    containerAllocator.addContainer(TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000003"), "xyz", 123));

    assertNotNull(requestState.getContainersOnAHost("abc"));
    assertEquals(1, requestState.getContainersOnAHost("abc").size());

    assertNotNull(requestState.getContainersOnAHost("xyz"));
    assertEquals(1, requestState.getContainersOnAHost("xyz").size());

    assertNotNull(requestState.getContainersOnAHost(ANY_HOST));
    assertTrue(requestState.getContainersOnAHost(ANY_HOST).size() == 1);
    assertEquals(ConverterUtils.toContainerId("container_1350670447861_0003_01_000004"),
        requestState.getContainersOnAHost(ANY_HOST).get(0).getId());
  }


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

    assertNotNull(requestState.getRequestsToCountMap());
    Map<String, AtomicInteger> requestsMap = requestState.getRequestsToCountMap();

    assertNotNull(requestsMap.get("abc"));
    assertEquals(2, requestsMap.get("abc").get());

    assertNotNull(requestsMap.get("def"));
    assertEquals(1, requestsMap.get("def").get());

    assertNotNull(requestsMap.get(ANY_HOST));
    assertEquals(1, requestsMap.get(ANY_HOST).get());
  }

  /**
   * Handles expired requests correctly and assigns ANY_HOST
   */
  @Test
  public void testExpiredRequestHandling() throws Exception {
    Map<Integer, String> containersToHostMapping = new HashMap<Integer, String>() {
      {
        put(0, "abc");
        put(1, "def");
      }
    };
    containerAllocator.requestContainers(containersToHostMapping);

    assertNotNull(requestState.getRequestsQueue());
    assertTrue(requestState.getRequestsQueue().size() == 2);

    assertNotNull(requestState.getRequestsToCountMap());
    assertNotNull(requestState.getRequestsToCountMap().get("abc"));
    assertTrue(requestState.getRequestsToCountMap().get("abc").get() == 1);

    assertNotNull(requestState.getRequestsToCountMap().get("def"));
    assertTrue(requestState.getRequestsToCountMap().get("def").get() == 1);

    // Set up our final asserts before starting the allocator thread
    MockContainerListener listener = new MockContainerListener(2, 0, new Runnable() {
      @Override
      public void run() {
        assertNull(requestState.getContainersOnAHost("xyz"));
        assertNull(requestState.getContainersOnAHost("zzz"));
        assertNotNull(requestState.getContainersOnAHost(ANY_HOST));
        assertTrue(requestState.getContainersOnAHost(ANY_HOST).size() == 2);
      }
    }, null);
    requestState.registerContainerListener(listener);

    allocatorThread.start();

    Container container0 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "xyz", 123);
    Container container1 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000003"), "zzz", 123);
    containerAllocator.addContainer(container0);
    containerAllocator.addContainer(container1);

    listener.verify();

    Thread.sleep(1000);

    MockContainerUtil mockContainerUtil = (MockContainerUtil) containerUtil;

    assertNotNull(mockContainerUtil.runningContainerList.get("xyz"));
    assertTrue(mockContainerUtil.runningContainerList.get("xyz").contains(container0));

    assertNotNull(mockContainerUtil.runningContainerList.get("zzz"));
    assertTrue(mockContainerUtil.runningContainerList.get("zzz").contains(container1));

    assertNull(requestState.getContainersOnAHost(ANY_HOST));

    assertNotNull(requestState.getRequestsQueue());
    assertTrue(requestState.getRequestsQueue().size() == 0);

    assertNotNull(requestState.getRequestsToCountMap());
    assertNull(requestState.getRequestsToCountMap().get("abc"));

    assertNull(requestState.getRequestsToCountMap().get("def"));
  }
}
