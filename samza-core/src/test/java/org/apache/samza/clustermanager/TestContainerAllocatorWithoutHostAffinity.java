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
import java.util.List;
import java.util.Map;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementMetadataStore;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManagerTestUtil;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.testUtils.MockHttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class TestContainerAllocatorWithoutHostAffinity {
  private final MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
  private final Config config = getConfig();
  private final JobModelManager jobModelManager = JobModelManagerTestUtil.getJobModelManager(config, 1,
      new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class)));

  private final SamzaApplicationState state = new SamzaApplicationState(jobModelManager);
  private final MockClusterResourceManager manager = new MockClusterResourceManager(callback, state);

  private CoordinatorStreamStore coordinatorStreamStore;
  private ContainerPlacementMetadataStore containerPlacementMetadataStore;

  private ContainerAllocator containerAllocator;
  private MockContainerRequestState requestState;
  private Thread allocatorThread;

  private Thread spyThread;
  private ContainerAllocator spyAllocator;

  @Before
  public void setup() throws Exception {
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(config);
    CoordinatorStreamStore coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    coordinatorStreamStore.init();
    containerPlacementMetadataStore = new ContainerPlacementMetadataStore(coordinatorStreamStore);
    containerPlacementMetadataStore.start();
    containerAllocator = new ContainerAllocator(manager, config, state, false,
        new ContainerManager(containerPlacementMetadataStore, state, manager, false, false));
    requestState = new MockContainerRequestState(manager, false);
    Field requestStateField = containerAllocator.getClass().getDeclaredField("resourceRequestState");
    requestStateField.setAccessible(true);
    requestStateField.set(containerAllocator, requestState);
    allocatorThread = new Thread(containerAllocator);
  }

  @After
  public void teardown() throws Exception {
    jobModelManager.stop();
    validateMockitoUsage();
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
        put("job.name", "test-job");
        put("job.coordinator.system", "test-kafka");
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
    assertNotNull(requestState.getHostRequestCounts());
    assertEquals(requestState.getHostRequestCounts().keySet().size(), 0);
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

    assertEquals(4, requestState.numPendingRequests());
    assertEquals(0, requestState.numDelayedRequests());

    // If host-affinty is not enabled, it doesn't update the requestMap
    assertNotNull(requestState.getHostRequestCounts());
    assertEquals(0, requestState.getHostRequestCounts().keySet().size());

    assertNotNull(state);
    assertEquals(4, state.anyHostRequests.get());
    assertEquals(0, state.preferredHostRequests.get());
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
    assertEquals(0, requestState.numDelayedRequests());

    // If host-affinty is not enabled, it doesn't update the requestMap
    assertNotNull(requestState.getHostRequestCounts());
    assertEquals(0, requestState.getHostRequestCounts().keySet().size());
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
        assertEquals(0, requestState.numDelayedRequests());
        assertEquals(0, requestState.getHostRequestCounts().size());
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

  /**
   * Test the complete flow from container request creation to allocation when host affinity is disabled
   */
  @Test
  public void testRequestAllocationWithRunStreamProcessor() throws Exception {
    Map<String, String> containersToHostMapping = new HashMap<String, String>() {
      {
        put("0", "prev_host");
        put("1", "prev_host");
        put("2", "prev_host");
        put("3", "prev_host");
      }
    };

    ClusterResourceManager.Callback mockCPM = mock(ClusterResourceManager.Callback.class);
    ClusterResourceManager mockManager = new MockClusterResourceManager(mockCPM, state);
    ContainerManager spyContainerManager =
        spy(new ContainerManager(containerPlacementMetadataStore, state, mockManager, false, false));
    spyAllocator = Mockito.spy(
        new ContainerAllocator(mockManager, config, state, false, spyContainerManager));
    // Mock the callback from ClusterManager to add resources to the allocator
    doAnswer((InvocationOnMock invocation) -> {
      SamzaResource resource = (SamzaResource) invocation.getArgumentAt(0, List.class).get(0);
      spyAllocator.addResource(resource);
      return null;
    }).when(mockCPM).onResourcesAvailable(anyList());
    // Request Resources
    spyAllocator.requestResources(containersToHostMapping);
    spyThread = new Thread(spyAllocator, "Container Allocator Thread");
    // Start the container allocator thread periodic assignment
    spyThread.start();
    Thread.sleep(1000);
    // Verify that all the request that were created were "ANY_HOST" requests
    ArgumentCaptor<SamzaResourceRequest> resourceRequestCaptor = ArgumentCaptor.forClass(SamzaResourceRequest.class);
    verify(spyAllocator, times(4)).runStreamProcessor(resourceRequestCaptor.capture(), anyString());
    resourceRequestCaptor.getAllValues()
        .forEach(resourceRequest -> assertEquals(resourceRequest.getPreferredHost(), ResourceRequestState.ANY_HOST));
    assertTrue(state.anyHostRequests.get() == containersToHostMapping.size());
    // Expiry currently should not be invoked for host affinity enabled cases only
    verify(spyContainerManager, never()).handleExpiredRequest(anyString(), anyString(),
        any(SamzaResourceRequest.class), any(ContainerAllocator.class), any(ResourceRequestState.class));
    // Only updated when host affinity is enabled
    assertTrue(state.matchedResourceRequests.get() == 0);
    assertTrue(state.preferredHostRequests.get() == 0);
    spyAllocator.stop();
  }
}
