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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.yarn.util.MockContainerListener;
import org.apache.samza.job.yarn.util.MockContainerRequestState;
import org.apache.samza.job.yarn.util.MockContainerUtil;
import org.apache.samza.job.yarn.util.TestUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestHostAwareContainerAllocator extends TestContainerAllocatorCommon {

  private final Config config = new MapConfig(new HashMap<String, String>() {
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

  @Override
  protected Config getConfig() {
    return config;
  }

  @Override
  protected MockContainerRequestState createContainerRequestState(AMRMClientAsync<AMRMClient.ContainerRequest> amClient) {
    return new MockContainerRequestState(amClient, true);
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
        put(0, "host1");
        put(1, "host3");
      }
    });

    assertNotNull(requestState.getContainersOnAHost("host1"));
    assertEquals(0, requestState.getContainersOnAHost("host1").size());

    assertNotNull(requestState.getContainersOnAHost("host3"));
    assertEquals(0, requestState.getContainersOnAHost("host3").size());

    assertNull(requestState.getContainersOnAHost(ANY_HOST));

    containerAllocator.addContainer(TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "host1", 123));
    containerAllocator.addContainer(TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000004"), "host2", 123));
    containerAllocator.addContainer(TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000003"), "host3", 123));

    assertNotNull(requestState.getContainersOnAHost("host1"));
    assertEquals(1, requestState.getContainersOnAHost("host1").size());

    assertNotNull(requestState.getContainersOnAHost("host3"));
    assertEquals(1, requestState.getContainersOnAHost("host3").size());

    assertNotNull(requestState.getContainersOnAHost(ANY_HOST));
    assertTrue(requestState.getContainersOnAHost(ANY_HOST).size() == 1);
    assertEquals(ConverterUtils.toContainerId("container_1350670447861_0003_01_000004"),
        requestState.getContainersOnAHost(ANY_HOST).get(0).getId());
  }


  @Test
  public void testRequestContainers() throws Exception {
    Map<Integer, String> containersToHostMapping = new HashMap<Integer, String>() {
      {
        put(0, "host1");
        put(1, "host2");
        put(2, null);
        put(3, "host1");
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

    assertNotNull(requestsMap.get("host1"));
    assertEquals(2, requestsMap.get("host1").get());

    assertNotNull(requestsMap.get("host2"));
    assertEquals(1, requestsMap.get("host2").get());

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
        put(0, "requestedHost1");
        put(1, "requestedHost2");
      }
    };
    containerAllocator.requestContainers(containersToHostMapping);

    assertNotNull(requestState.getRequestsQueue());
    assertTrue(requestState.getRequestsQueue().size() == 2);

    assertNotNull(requestState.getRequestsToCountMap());
    assertNotNull(requestState.getRequestsToCountMap().get("requestedHost1"));
    assertTrue(requestState.getRequestsToCountMap().get("requestedHost1").get() == 1);

    assertNotNull(requestState.getRequestsToCountMap().get("requestedHost2"));
    assertTrue(requestState.getRequestsToCountMap().get("requestedHost2").get() == 1);

    final Container container0 = TestUtil
        .getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "availableHost1", 123);
    final Container container1 = TestUtil
        .getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000003"), "availableHost2", 123);

    Runnable addedContainerAssertions = new Runnable() {
      @Override
      public void run() {
        assertNotNull(requestState.getRequestsToCountMap());
        assertNull(requestState.getContainersOnAHost("availableHost1"));
        assertNull(requestState.getContainersOnAHost("availableHost2"));
        assertNotNull(requestState.getContainersOnAHost(ANY_HOST));
        assertTrue(requestState.getContainersOnAHost(ANY_HOST).size() == 2);
      }
    };

    Runnable assignedContainerAssertions = new Runnable() {
      @Override
      public void run() {
        List<Container> anyHostContainers = requestState.getContainersOnAHost(ANY_HOST);
        assertTrue(anyHostContainers == null || anyHostContainers.isEmpty());

        assertNotNull(requestState.getRequestsQueue());
        assertTrue(requestState.getRequestsQueue().size() == 0);

        assertNotNull(requestState.getRequestsToCountMap());
        assertNotNull(requestState.getRequestsToCountMap().get("requestedHost1"));
        assertNotNull(requestState.getRequestsToCountMap().get("requestedHost2"));
      }
    };

    Runnable runningContainerAssertions = new Runnable() {
      @Override
      public void run() {
        MockContainerUtil mockContainerUtil = (MockContainerUtil) containerUtil;

        assertNotNull(mockContainerUtil.runningContainerList.get("availableHost1"));
        assertTrue(mockContainerUtil.runningContainerList.get("availableHost1").contains(container0));

        assertNotNull(mockContainerUtil.runningContainerList.get("availableHost2"));
        assertTrue(mockContainerUtil.runningContainerList.get("availableHost2").contains(container1));
      }
    };
    // Set up our final asserts before starting the allocator thread
    MockContainerListener listener = new MockContainerListener(
        2, 0, 2, 2,
        addedContainerAssertions,
        null,
        assignedContainerAssertions,
        runningContainerAssertions);
    requestState.registerContainerListener(listener);
    ((MockContainerUtil) containerUtil).registerContainerListener(listener);

    containerAllocator.addContainer(container0);
    containerAllocator.addContainer(container1);

    // Start after adding containers to avoid a race condition between the allocator thread
    // using the containers and the assertions after the containers are added.
    allocatorThread.start();

    listener.verify();
  }
}
