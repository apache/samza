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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManagerTestUtil;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.testUtils.MockHttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHostAwareContainerAllocator {

  private final Config config = getConfig();
  private final JobModelManager jobModelManager = initializeJobModelManager(config, 1);
  private final MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
  private final SamzaApplicationState state = new SamzaApplicationState(jobModelManager);

  private final MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);

  private JobModelManager initializeJobModelManager(Config config, int containerCount) {
    Map<String, Map<String, String>> localityMap = new HashMap<>();
    localityMap.put("0", new HashMap<String, String>() { {
        put(SetContainerHostMapping.HOST_KEY, "abc");
      } });
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    when(mockLocalityManager.readContainerLocality()).thenReturn(localityMap);

    return JobModelManagerTestUtil.getJobModelManagerWithLocalityManager(getConfig(), containerCount, mockLocalityManager,
        new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class)));
  }

  private HostAwareContainerAllocator containerAllocator;
  private final int timeoutMillis = 1000;
  private MockContainerRequestState requestState;
  private Thread allocatorThread;

  @Before
  public void setup() throws Exception {
    containerAllocator = new HostAwareContainerAllocator(clusterResourceManager, timeoutMillis, config, Optional.empty(), state);
    requestState = new MockContainerRequestState(clusterResourceManager, true);
    Field requestStateField = containerAllocator.getClass().getSuperclass().getDeclaredField("resourceRequestState");
    requestStateField.setAccessible(true);
    requestStateField.set(containerAllocator, requestState);
    allocatorThread = new Thread(containerAllocator);
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

    assertEquals(4, requestState.numPendingRequests());

    assertNotNull(requestState.getRequestsToCountMap());
    assertEquals(1, requestState.getRequestsToCountMap().keySet().size());
    assertTrue(requestState.getRequestsToCountMap().keySet().contains(ResourceRequestState.ANY_HOST));
  }

  /**
   * Add containers to the correct host in the request state
   */
  @Test
  public void testAddContainerWithHostAffinity() throws Exception {
    containerAllocator.requestResources(new HashMap<String, String>() {
      {
        put("0", "abc");
        put("1", "xyz");
      }
    });

    assertNotNull(requestState.getResourcesOnAHost("abc"));
    assertEquals(0, requestState.getResourcesOnAHost("abc").size());

    assertNotNull(requestState.getResourcesOnAHost("xyz"));
    assertEquals(0, requestState.getResourcesOnAHost("xyz").size());

    assertNull(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST));

    containerAllocator.addResource(new SamzaResource(1, 10, "abc", "ID1"));
    containerAllocator.addResource(new SamzaResource(1, 10, "def", "ID2"));
    containerAllocator.addResource(new SamzaResource(1, 10, "xyz", "ID3"));


    assertNotNull(requestState.getResourcesOnAHost("abc"));
    assertEquals(1, requestState.getResourcesOnAHost("abc").size());

    assertNotNull(requestState.getResourcesOnAHost("xyz"));
    assertEquals(1, requestState.getResourcesOnAHost("xyz").size());

    assertNotNull(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST));
    assertTrue(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST).size() == 1);
    assertEquals("ID2", requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST).get(0).getResourceID());
  }

  /**
   * Test that extra resources are buffered under ANY_HOST
   */
  @Test
  public void testSurplusResourcesAreBufferedUnderAnyHost() throws Exception {
    containerAllocator.requestResources(new HashMap<String, String>() {
      {
        put("0", "abc");
        put("1", "xyz");
      }
    });

    assertNotNull(requestState.getResourcesOnAHost("abc"));
    assertEquals(0, requestState.getResourcesOnAHost("abc").size());

    assertNotNull(requestState.getResourcesOnAHost("xyz"));
    assertEquals(0, requestState.getResourcesOnAHost("xyz").size());

    assertNull(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST));

    containerAllocator.addResource(new SamzaResource(1, 10, "abc", "ID1"));
    containerAllocator.addResource(new SamzaResource(1, 10, "xyz", "ID2"));
    // surplus resources for host - "abc"
    containerAllocator.addResource(new SamzaResource(1, 10, "abc", "ID3"));
    containerAllocator.addResource(new SamzaResource(1, 10, "abc", "ID4"));
    containerAllocator.addResource(new SamzaResource(1, 10, "abc", "ID5"));
    containerAllocator.addResource(new SamzaResource(1, 10, "abc", "ID6"));

    assertNotNull(requestState.getResourcesOnAHost("abc"));
    assertEquals(1, requestState.getResourcesOnAHost("abc").size());

    assertNotNull(requestState.getResourcesOnAHost("xyz"));
    assertEquals(1, requestState.getResourcesOnAHost("xyz").size());

    assertNotNull(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST));
    // assert that the surplus resources goto the ANY_HOST buffer
    assertTrue(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST).size() == 4);
    assertEquals("ID3", requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST).get(0).getResourceID());
  }

  @Test
  public void testAllocatorReleasesExtraContainers() throws Exception {
    final SamzaResource resource0 = new SamzaResource(1, 1024, "abc", "id1");
    final SamzaResource resource1 = new SamzaResource(1, 1024, "abc", "id2");
    final SamzaResource resource2 = new SamzaResource(1, 1024, "def", "id3");

    Runnable releasedAssertions = new Runnable() {
      @Override
      public void run() {
        assertEquals(2, clusterResourceManager.releasedResources.size());
        assertTrue(clusterResourceManager.releasedResources.contains(resource1));
        assertTrue(clusterResourceManager.releasedResources.contains(resource2));

        // Test that state is cleaned up
        assertEquals(0, requestState.numPendingRequests());
        assertEquals(0, requestState.getRequestsToCountMap().size());
        assertNull(requestState.getResourcesOnAHost("abc"));
        assertNull(requestState.getResourcesOnAHost("def"));
      }
    };
    // Set up our final asserts before starting the allocator thread
    MockContainerListener listener = new MockContainerListener(3, 2, 0, 0, null, releasedAssertions,
        null, null);
    requestState.registerContainerListener(listener);

    allocatorThread.start();

    containerAllocator.requestResource("0", "abc");

    containerAllocator.addResource(resource0);
    containerAllocator.addResource(resource1);
    containerAllocator.addResource(resource2);

    listener.verify();
  }

  @Test
  public void testRequestContainers() throws Exception {
    Map<String, String> containersToHostMapping = new HashMap<String, String>() {
      {
        put("0", "abc");
        put("1", "def");
        put("2", null);
        put("3", "abc");
      }
    };

    containerAllocator.requestResources(containersToHostMapping);

    assertNotNull(clusterResourceManager.resourceRequests);
    assertEquals(clusterResourceManager.resourceRequests.size(), 4);
    assertEquals(requestState.numPendingRequests(), 4);

    Map<String, AtomicInteger> requestsMap = requestState.getRequestsToCountMap();
    assertNotNull(requestsMap.get("abc"));
    assertEquals(2, requestsMap.get("abc").get());

    assertNotNull(requestsMap.get("def"));
    assertEquals(1, requestsMap.get("def").get());

    assertNotNull(requestsMap.get(ResourceRequestState.ANY_HOST));
    assertEquals(1, requestsMap.get(ResourceRequestState.ANY_HOST).get());
  }

  /**
   * Handles expired requests correctly and assigns ANY_HOST
   */

  @Test
  public void testExpiredRequestAreAssignedToAnyHost() throws Exception {
    final SamzaResource resource0 = new SamzaResource(1, 1000, "xyz", "id1");
    final SamzaResource resource1 = new SamzaResource(1, 1000, "zzz", "id2");

    Map<String, String> containersToHostMapping = new HashMap<String, String>() {
      {
        put("0", "abc");
        put("1", "def");
      }
    };
    containerAllocator.requestResources(containersToHostMapping);
    assertEquals(requestState.numPendingRequests(), 2);
    assertNotNull(requestState.getRequestsToCountMap());
    assertNotNull(requestState.getRequestsToCountMap().get("abc"));
    assertTrue(requestState.getRequestsToCountMap().get("abc").get() == 1);

    assertNotNull(requestState.getRequestsToCountMap().get("def"));
    assertTrue(requestState.getRequestsToCountMap().get("def").get() == 1);

    Runnable addContainerAssertions = new Runnable() {
      @Override
      public void run() {
        assertNull(requestState.getResourcesOnAHost("xyz"));
        assertNull(requestState.getResourcesOnAHost("zzz"));
        assertNotNull(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST));
        assertTrue(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST).size() == 2);
      }
    };

    Runnable assignContainerAssertions = new Runnable() {
      @Override
      public void run() {
        assertEquals(requestState.numPendingRequests(), 0);
        assertNotNull(requestState.getRequestsToCountMap());
        assertNotNull(requestState.getRequestsToCountMap().get("abc"));
        assertNotNull(requestState.getRequestsToCountMap().get("def"));
      }
    };

    Runnable runningContainerAssertions = new Runnable() {
      @Override
      public void run() {
        assertTrue(clusterResourceManager.launchedResources.contains(resource0));
        assertTrue(clusterResourceManager.launchedResources.contains(resource1));
      }
    };
    MockContainerListener listener = new MockContainerListener(2, 0, 2, 2, addContainerAssertions, null, assignContainerAssertions, runningContainerAssertions);
    requestState.registerContainerListener(listener);
    ((MockClusterResourceManager) clusterResourceManager).registerContainerListener(listener);
    containerAllocator.addResource(resource0);
    containerAllocator.addResource(resource1);
    allocatorThread.start();

    listener.verify();
  }

  @Test
  public void testExpiredRequestsAreCancelled() throws Exception {
    // request one container each on host-1 and host-2
    containerAllocator.requestResources(ImmutableMap.of("0", "host-1", "1", "host-2"));
    // assert that the requests made it to YARN
    Assert.assertEquals(clusterResourceManager.resourceRequests.size(), 2);
    // allocate one resource from YARN on a different host (host-3)
    SamzaResource resource0 = new SamzaResource(1, 1000, "host-3", "id1");
    containerAllocator.addResource(resource0);
    // let the matching begin
    allocatorThread.start();

    // verify that a container is launched on host-3 after the request expires
    if (!clusterResourceManager.awaitContainerLaunch(1, 20, TimeUnit.SECONDS)) {
      Assert.fail("Timed out waiting container launch");
    }
    Assert.assertEquals(1, clusterResourceManager.launchedResources.size());
    Assert.assertEquals(clusterResourceManager.launchedResources.get(0).getHost(), "host-3");
    Assert.assertEquals(clusterResourceManager.launchedResources.get(0).getResourceID(), "id1");

    // Now, there are no more resources left to run the 2nd container. Verify that we eventually issue another request
    if (!clusterResourceManager.awaitResourceRequests(4, 20, TimeUnit.SECONDS)) {
      Assert.fail("Timed out waiting for resource requests");
    }
    // verify that we have cancelled previous requests and there's one outstanding request
    Assert.assertEquals(clusterResourceManager.cancelledRequests.size(), 3);
  }

  //@Test
  public void testExpiryWithNonResponsiveClusterManager() throws Exception {

    final SamzaResource resource0 = new SamzaResource(1, 1000, "host-3", "id1");
    final SamzaResource resource1 = new SamzaResource(1, 1000, "host-4", "id2");

    Map<String, String> containersToHostMapping = ImmutableMap.of("0", "host-1", "1", "host-2");

    Runnable addContainerAssertions = new Runnable() {
      @Override
      public void run() {
        // verify that resources are buffered in the right queue. ie, only those resources on previously requested hosts
        // in the preferred-host queue while other resources end up in the ANY_HOST queue
        assertNull(requestState.getResourcesOnAHost("host-3"));
        assertNull(requestState.getResourcesOnAHost("host-4"));
        assertNotNull(requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST));
        assertEquals(1, requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST).size());
      }
    };

    Runnable assignContainerAssertions = new Runnable() {
      // verify that we processed all requests
      @Override
      public void run() {
        assertEquals(requestState.numPendingRequests(), 0);
      }
    };

    Runnable runningContainerAssertions = new Runnable() {
      // verify that the two containers were actually launched
      @Override
      public void run() {
        assertTrue(clusterResourceManager.launchedResources.contains(resource0));
        assertTrue(clusterResourceManager.launchedResources.contains(resource1));
      }
    };
    MockContainerListener listener = new MockContainerListener(2, 0, 2, 2, addContainerAssertions, null, assignContainerAssertions, runningContainerAssertions);
    requestState.registerContainerListener(listener);
    clusterResourceManager.registerContainerListener(listener);

    // request for resources - one each on host-1 and host-2
    containerAllocator.requestResources(containersToHostMapping);
    assertEquals(requestState.numPendingRequests(), 2);
    assertNotNull(requestState.getRequestsToCountMap());
    assertNotNull(requestState.getRequestsToCountMap().get("host-1"));
    assertTrue(requestState.getRequestsToCountMap().get("host-1").get() == 1);
    assertNotNull(requestState.getRequestsToCountMap().get("host-2"));
    assertTrue(requestState.getRequestsToCountMap().get("host-2").get() == 1);

    // verify that no containers have been launched yet (since, YARN has not provided any resources)
    assertEquals(0, clusterResourceManager.launchedResources.size());

    // provide a resource on host-3
    containerAllocator.addResource(resource0);
    allocatorThread.start();
    // verify that the first preferred host request should expire and container-0 should launch on host-3
    if (!clusterResourceManager.awaitContainerLaunch(1, 20, TimeUnit.SECONDS)) {
      Assert.fail("Timed out waiting for container-0 to launch");
    }
    // verify that the second preferred host request should expire and should trigger ANY_HOST requests
    // wait for 4 requests to be made (2 preferred-host requests - one each on host-1 & host-2;  2 any-host requests)
    if (!clusterResourceManager.awaitResourceRequests(4, 20, TimeUnit.SECONDS)) {
      Assert.fail("Timed out waiting for resource requests");
    }
    // verify 2 preferred host requests should have been made for host-1 and host-2
    Assert.assertEquals(2, state.preferredHostRequests.get());
    // verify both of them should have expired.
    Assert.assertEquals(2, state.expiredPreferredHostRequests.get());
    // verify there were at-least 2 any-host requests
    Assert.assertTrue(state.anyHostRequests.get() >= 2);
    Assert.assertTrue(state.expiredAnyHostRequests.get() <= state.anyHostRequests.get());
    // finally, provide a container from YARN after multiple requests
    containerAllocator.addResource(resource1);
    // verify all the test assertions
    listener.verify();
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
        put("cluster-manager.container.request.timeout.ms", "3");
        put("cluster-manager.allocator.sleep.ms", "1");
        put("cluster-manager.container.memory.mb", "512");
        put("yarn.package.path", "/foo");
        put("task.inputs", "test-system.test-stream");
        put("systems.test-system.samza.factory", "org.apache.samza.system.MockSystemFactory");
        put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
        put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
        put("job.host-affinity.enabled", "true");
      }
    });

    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    return new MapConfig(map);
  }

}
