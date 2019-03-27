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

package org.apache.samza.clustermanager;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManagerTestUtil;
import org.apache.samza.testUtils.MockHttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestContainerAllocator {
  private final MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
  private final Config config = getConfig();
  private final JobModelManager jobModelManager = JobModelManagerTestUtil.getJobModelManager(config, 1,
      new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class)));

  private final SamzaApplicationState state = new SamzaApplicationState(jobModelManager);
  private final MockClusterResourceManager manager = new MockClusterResourceManager(callback, state);

  private ContainerAllocator containerAllocator;
  private MockContainerRequestState requestState;
  private Thread allocatorThread;

  @Before
  public void setup() throws Exception {
    containerAllocator = new ContainerAllocator(manager, config, state);
    requestState = new MockContainerRequestState(manager, false);
    Field requestStateField = containerAllocator.getClass().getSuperclass().getDeclaredField("resourceRequestState");
    requestStateField.setAccessible(true);
    requestStateField.set(containerAllocator, requestState);
    allocatorThread = new Thread(containerAllocator);
  }



  @After
  public void teardown() throws Exception {
    jobModelManager.stop();
    containerAllocator.stop();
  }


  private static Config getConfig() {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        put("cluster-manager.container.count", "1");
        put("cluster-manager.container.retry.count", "1");
        put("cluster-manager.container.retry.window.ms", "1999999999");
        put("cluster-manager.allocator.sleep.ms", "10");
        put("cluster-manager.container.memory.mb", "512");
        put("yarn.package.path", "/foo");
        put("task.inputs", "test-system.test-stream");
        put("systems.test-system.samza.factory", "org.apache.samza.system.MockSystemFactory");
        put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
        put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
      }
    });

    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    return new MapConfig(map);
  }



  /**
   * Adds all containers returned to ANY_HOST only
   */
  @Test
  public void testAddContainer() throws Exception {
    assertNull(requestState.getResourcesOnAHost("abc"));
    assertNull(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST));

    containerAllocator.addResource(new SamzaResource(1, 1000, "abc", "id1"));
    containerAllocator.addResource(new SamzaResource(1, 1000, "xyz", "id1"));


    assertNull(requestState.getResourcesOnAHost("abc"));
    assertNotNull(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST));
    assertTrue(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST).size() == 2);
  }

  /**
   * Test requestContainers
   */
  @Test
  public void testRequestContainers() throws Exception {
    Map<String, String> containersToHostMapping = new HashMap<String, String>() {
      {
        put("0", null);
        put("1", null);
        put("2", null);
        put("3", null);
      }
    };

    allocatorThread.start();

    containerAllocator.requestResources(containersToHostMapping);

    assertEquals(4, manager.resourceRequests.size());

    assertNotNull(requestState);

    assertEquals(requestState.numPendingRequests(), 4);

    // If host-affinty is not enabled, it doesn't update the requestMap
    assertNotNull(requestState.getRequestsToCountMap());
    assertEquals(requestState.getRequestsToCountMap().keySet().size(), 0);
  }

  /**
   * Test requestContainers with containerToHostMapping with host.affinity disabled
   */
  @Test
  public void testRequestContainersWithExistingHosts() throws Exception {
    Map<String, String> containersToHostMapping = new HashMap<String, String>() {
      {
        put("0", "prev_host");
        put("1", "prev_host");
        put("2", "prev_host");
        put("3", "prev_host");
      }
    };

    allocatorThread.start();

    containerAllocator.requestResources(containersToHostMapping);

    assertEquals(4, manager.resourceRequests.size());

    assertNotNull(requestState);

    assertEquals(requestState.numPendingRequests(), 4);

    // If host-affinty is not enabled, it doesn't update the requestMap
    assertNotNull(requestState.getRequestsToCountMap());
    assertEquals(requestState.getRequestsToCountMap().keySet().size(), 0);

    assertNotNull(state);
    assertEquals(state.anyHostRequests.get(), 4);
    assertEquals(state.preferredHostRequests.get(), 0);
  }

  /**
   * Test request containers with no containerToHostMapping makes the right number of requests
   */
  @Test
  public void testRequestContainersWithNoMapping() throws Exception {
    int containerCount = 4;
    Map<String, String> containersToHostMapping = new HashMap<String, String>();
    for (int i = 0; i < containerCount; i++) {
      containersToHostMapping.put(String.valueOf(i), null);
    }
    allocatorThread.start();

    containerAllocator.requestResources(containersToHostMapping);

    assertNotNull(requestState);

    assertTrue(requestState.numPendingRequests() == 4);

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
    final SamzaResource resource = new SamzaResource(1, 1000, "abc", "id1");
    final SamzaResource resource1 = new SamzaResource(1, 1000, "abc", "id2");
    final SamzaResource resource2 = new SamzaResource(1, 1000, "def", "id3");


    // Set up our final asserts before starting the allocator thread
    MockContainerListener listener = new MockContainerListener(3, 2, 0, 0, null, new Runnable() {
      @Override
      public void run() {

        assertTrue(manager.releasedResources.contains(resource1));
        assertTrue(manager.releasedResources.contains(resource2));

        // Test that state is cleaned up
        assertEquals(0, requestState.numPendingRequests());
        assertEquals(0, requestState.getRequestsToCountMap().size());
        assertNull(requestState.getResourcesOnAHost("abc"));
        assertNull(requestState.getResourcesOnAHost("def"));
      }
    }, null, null);
    requestState.registerContainerListener(listener);

    allocatorThread.start();

    containerAllocator.requestResource("0", "abc");

    containerAllocator.addResource(resource);
    containerAllocator.addResource(resource1);
    containerAllocator.addResource(resource2);

    listener.verify();
  }

}
