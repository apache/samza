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

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementMetadataStore;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManagerTestUtil;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.LocalityModel;
import org.apache.samza.testUtils.MockHttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class TestContainerAllocatorWithHostAffinity {

  private final Config config = getConfig();
  private final JobModelManager jobModelManager = initializeJobModelManager(getConfig(), 1);
  private final MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
  private final SamzaApplicationState state = new SamzaApplicationState(jobModelManager);

  private final MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
  private final FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
  private ContainerPlacementMetadataStore containerPlacementMetadataStore;
  private ContainerManager containerManager;

  private JobModelManager initializeJobModelManager(Config config, int containerCount) {
    return JobModelManagerTestUtil.getJobModelManager(config, containerCount,
        new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class)));
  }

  private ContainerAllocator containerAllocator;
  private ContainerAllocator spyAllocator;
  private final int timeoutMillis = 1000;
  private MockContainerRequestState requestState;
  private Thread allocatorThread;
  private Thread spyAllocatorThread;

  @Before
  public void setup() throws Exception {
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    when(mockLocalityManager.readLocality())
        .thenReturn(new LocalityModel(ImmutableMap.of("0", new ProcessorLocality("0", "abc"))));
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(config);
    CoordinatorStreamStore coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    coordinatorStreamStore.init();
    containerPlacementMetadataStore = new ContainerPlacementMetadataStore(coordinatorStreamStore);
    containerPlacementMetadataStore.start();
    containerManager = new ContainerManager(containerPlacementMetadataStore, state, clusterResourceManager, true, false, mockLocalityManager, faultDomainManager, config);
    containerAllocator =
        new ContainerAllocator(clusterResourceManager, config, state, true, containerManager);
    requestState = new MockContainerRequestState(clusterResourceManager, true);
    Field requestStateField = containerAllocator.getClass().getDeclaredField("resourceRequestState");
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

    assertNotNull(requestState.getHostRequestCounts());
    assertEquals(1, requestState.getHostRequestCounts().keySet().size());
    assertTrue(requestState.getHostRequestCounts().keySet().contains(ResourceRequestState.ANY_HOST));
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
    assertEquals("ID2", requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST).get(0).getContainerId());
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
    assertEquals("ID3", requestState.getResourcesOnAHost(ResourceRequestState.ANY_HOST).get(0).getContainerId());
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
        assertEquals(0, requestState.getHostRequestCounts().size());
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

    Map<String, AtomicInteger> requestsMap = requestState.getHostRequestCounts();
    assertNotNull(requestsMap.get("abc"));
    assertEquals(2, requestsMap.get("abc").get());

    assertNotNull(requestsMap.get("def"));
    assertEquals(1, requestsMap.get("def").get());

    assertNotNull(requestsMap.get(ResourceRequestState.ANY_HOST));
    assertEquals(1, requestsMap.get(ResourceRequestState.ANY_HOST).get());
  }

  @Test
  public void testDelayedRequestedContainers() {
    containerAllocator.requestResource("0", "abc");
    containerAllocator.requestResourceWithDelay("0", "efg", Duration.ofHours(2));
    containerAllocator.requestResourceWithDelay("0", "hij", Duration.ofHours(3));
    containerAllocator.requestResourceWithDelay("0", "klm", Duration.ofHours(4));

    assertNotNull(clusterResourceManager.resourceRequests);
    assertEquals(clusterResourceManager.resourceRequests.size(), 1);
    assertEquals(requestState.numPendingRequests(), 1);
    assertEquals(requestState.numDelayedRequests(), 3);
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
    assertNotNull(requestState.getHostRequestCounts());
    assertNotNull(requestState.getHostRequestCounts().get("abc"));
    assertTrue(requestState.getHostRequestCounts().get("abc").get() == 1);

    assertNotNull(requestState.getHostRequestCounts().get("def"));
    assertTrue(requestState.getHostRequestCounts().get("def").get() == 1);

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
        assertNotNull(requestState.getHostRequestCounts());
        assertNotNull(requestState.getHostRequestCounts().get("abc"));
        assertNotNull(requestState.getHostRequestCounts().get("def"));
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
    Assert.assertEquals(clusterResourceManager.launchedResources.get(0).getContainerId(), "id1");

    // Now, there are no more resources left to run the 2nd container. Verify that we eventually issue another request
    if (!clusterResourceManager.awaitResourceRequests(4, 20, TimeUnit.SECONDS)) {
      Assert.fail("Timed out waiting for resource requests");
    }
    // verify that we have cancelled previous requests and there's one outstanding request
    Assert.assertEquals(clusterResourceManager.cancelledRequests.size(), 3);
  }

  @Test
  public void testRequestAllocationOnPreferredHostWithRunStreamProcessor() throws Exception {
    ClusterResourceManager.Callback mockCPM = mock(MockClusterResourceManagerCallback.class);
    ClusterResourceManager mockClusterResourceManager = new MockClusterResourceManager(mockCPM, state);
    ContainerManager containerManager =
        new ContainerManager(containerPlacementMetadataStore, state, mockClusterResourceManager, true, false, mock(LocalityManager.class), faultDomainManager, config);
    // Mock the callback from ClusterManager to add resources to the allocator
    doAnswer((InvocationOnMock invocation) -> {
      SamzaResource resource = (SamzaResource) invocation.getArgumentAt(0, List.class).get(0);
      spyAllocator.addResource(resource);
      return null;
    }).when(mockCPM).onResourcesAvailable(anyList());

    spyAllocator = Mockito.spy(
        new ContainerAllocator(mockClusterResourceManager, config, state, true, containerManager));

    // Request Resources
    spyAllocator.requestResources(new HashMap<String, String>() {
      {
        put("0", "abc");
        put("1", "xyz");
      }
    });

    spyAllocatorThread = new Thread(spyAllocator);

    // Start the container allocator thread periodic assignment
    spyAllocatorThread.start();
    // Let Allocator thread periodically fulfill requests
    Thread.sleep(100);

    // Verify that all the request that were created were preferred host requests
    ArgumentCaptor<SamzaResourceRequest> resourceRequestCaptor = ArgumentCaptor.forClass(SamzaResourceRequest.class);
    verify(spyAllocator, times(2)).runStreamProcessor(resourceRequestCaptor.capture(), anyString());
    resourceRequestCaptor.getAllValues()
        .forEach(resourceRequest -> assertNotEquals(resourceRequest.getPreferredHost(), ResourceRequestState.ANY_HOST));
    Set<String> hostNames = resourceRequestCaptor.getAllValues().stream().map(request -> request.getPreferredHost()).collect(
        Collectors.toSet());
    assertTrue(hostNames.contains("abc"));
    assertTrue(hostNames.contains("xyz"));
    // No any host requests should be made if preferred host is satisfied
    assertTrue(state.anyHostRequests.get() == 0);
    // State check when host affinity is enabled
    assertTrue(state.matchedResourceRequests.get() == 2);
    assertTrue(state.preferredHostRequests.get() == 2);
    spyAllocator.stop();
  }

  @Test
  public void testExpiredRequestAllocationOnAnyHost() throws Exception {
    MockClusterResourceManager spyManager = spy(new MockClusterResourceManager(callback, state));
    ContainerManager spyContainerManager =
        spy(new ContainerManager(containerPlacementMetadataStore, state, spyManager, true, false, mock(LocalityManager.class), faultDomainManager, config));
    spyAllocator = Mockito.spy(
        new ContainerAllocator(spyManager, config, state, true, spyContainerManager));
    // Request Preferred Resources
    spyAllocator.requestResources(new HashMap<String, String>() {
      {
        put("0", "hostname-0");
        put("1", "hostname-1");
      }
    });

    spyAllocatorThread = new Thread(spyAllocator);
    // Start the container allocator thread periodic assignment
    spyAllocatorThread.start();

    // Let the preferred host requests and the follow-up ANY_HOST request expire, expiration timeout is 500 ms
    Thread.sleep(1500);

    // Verify that all the request that were created as preferred host requests expired
    assertTrue(state.preferredHostRequests.get() == 2);
    assertTrue(state.expiredPreferredHostRequests.get() == 2);
    // expirations for initial preferred host requests
    verify(spyContainerManager).handleExpiredRequest(eq("0"), eq("hostname-0"),
        any(SamzaResourceRequest.class), any(ContainerAllocator.class), any(ResourceRequestState.class));
    verify(spyContainerManager).handleExpiredRequest(eq("1"), eq("hostname-1"),
        any(SamzaResourceRequest.class), any(ContainerAllocator.class), any(ResourceRequestState.class));
    // expirations for follow-up ANY_HOST requests
    // allocator keeps running in a loop so it might expire more than once during the wait time
    verify(spyContainerManager, atLeast(1)).handleExpiredRequest(eq("0"), eq(ResourceRequestState.ANY_HOST),
        any(SamzaResourceRequest.class), any(ContainerAllocator.class), any(ResourceRequestState.class));
    verify(spyContainerManager, atLeast(1)).handleExpiredRequest(eq("1"), eq(ResourceRequestState.ANY_HOST),
        any(SamzaResourceRequest.class), any(ContainerAllocator.class), any(ResourceRequestState.class));

    // Verify that preferred host request were cancelled and since no surplus resources were available
    // requestResource was invoked with ANY_HOST requests
    ArgumentCaptor<SamzaResourceRequest> cancelledRequestCaptor = ArgumentCaptor.forClass(SamzaResourceRequest.class);
    // preferred host requests and ANY_HOST requests got cancelled
    verify(spyManager, atLeast(4)).cancelResourceRequest(cancelledRequestCaptor.capture());
    assertEquals(ImmutableSet.of("hostname-0", "hostname-1", ResourceRequestState.ANY_HOST),
        cancelledRequestCaptor.getAllValues()
            .stream()
            .map(SamzaResourceRequest::getPreferredHost)
            .collect(Collectors.toSet()));
    assertTrue(state.matchedResourceRequests.get() == 0);
    // at least 2 for expired preferred host requests, 2 for expired follow-up ANY_HOST requests
    assertTrue(state.anyHostRequests.get() >= 4);
    spyAllocator.stop();
  }

  @Test
  public void testExpiredRequestAllocationOnSurplusAnyHostWithRunStreamProcessor() throws Exception {
    // Add Extra Resources
    MockClusterResourceManager spyClusterResourceManager = spy(new MockClusterResourceManager(callback, state));
    ContainerManager spyContainerManager =
        spy(new ContainerManager(containerPlacementMetadataStore, state, spyClusterResourceManager, true, false, mock(LocalityManager.class), faultDomainManager, config));

    spyAllocator = Mockito.spy(
        new ContainerAllocator(spyClusterResourceManager, config, state, true, spyContainerManager));
    spyAllocator.addResource(new SamzaResource(1, 1000, "xyz", "id1"));
    spyAllocator.addResource(new SamzaResource(1, 1000, "zzz", "id2"));

    // Request Preferred Resources
    spyAllocator.requestResources(new HashMap<String, String>() {
      {
        put("0", "hostname-0");
        put("1", "hostname-1");
      }
    });

    spyAllocatorThread = new Thread(spyAllocator);
    // Start the container allocator thread periodic assignment
    spyAllocatorThread.start();

    // Let the request expire, expiration timeout is 500 ms
    Thread.sleep(1000);

    // Verify that all the request that were created as preferred host requests expired
    assertEquals(state.expiredPreferredHostRequests.get(), 2);
    verify(spyContainerManager, times(1)).handleExpiredRequest(eq("0"), eq("hostname-0"),
        any(SamzaResourceRequest.class), any(ContainerAllocator.class), any(ResourceRequestState.class));
    verify(spyContainerManager, times(1)).handleExpiredRequest(eq("1"), eq("hostname-1"),
        any(SamzaResourceRequest.class), any(ContainerAllocator.class), any(ResourceRequestState.class));

    // Verify that runStreamProcessor was invoked with already available ANY_HOST requests
    ArgumentCaptor<SamzaResourceRequest> resourceRequestCaptor = ArgumentCaptor.forClass(SamzaResourceRequest.class);
    ArgumentCaptor<String> hostCaptor = ArgumentCaptor.forClass(String.class);
    verify(spyAllocator, times(2)).runStreamProcessor(resourceRequestCaptor.capture(), hostCaptor.capture());
    // Resource request were preferred host requests
    resourceRequestCaptor.getAllValues()
        .forEach(resourceRequest -> assertNotEquals(resourceRequest.getPreferredHost(), ResourceRequestState.ANY_HOST));

    // Since requests expired, allocator ran the requests on surplus available ANY_HOST
    hostCaptor.getAllValues()
        .forEach(host -> assertEquals(host, ResourceRequestState.ANY_HOST));

    // State Update check
    assertTrue(state.matchedResourceRequests.get() == 0);
    assertTrue(state.preferredHostRequests.get() == 2);
    assertTrue(state.anyHostRequests.get() == 0);
    spyAllocator.stop();
  }

  @Test(timeout = 5000)
  public void testExpiredAllocatedResourcesAreReleased() throws Exception {
    ClusterResourceManager.Callback mockCPM = mock(MockClusterResourceManagerCallback.class);
    MockClusterResourceManager mockClusterResourceManager = new MockClusterResourceManager(mockCPM, state);
    ContainerManager spyContainerManager =
        spy(new ContainerManager(containerPlacementMetadataStore, state, mockClusterResourceManager, true, false, mock(LocalityManager.class), faultDomainManager, config));

    SamzaResource expiredAllocatedResource = new SamzaResource(1, 1000, "host-0", "id0",
        System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());
    spyAllocator = Mockito.spy(
        new ContainerAllocator(mockClusterResourceManager, config, state, true, spyContainerManager));
    spyAllocator.addResource(expiredAllocatedResource);
    spyAllocator.addResource(new SamzaResource(1, 1000, "host-1", "1d1"));

    // Request Preferred Resources
    spyAllocator.requestResources(new HashMap<String, String>() {
      {
        put("0", "host-0");
        put("1", "host-1");
      }
    });

    spyAllocatorThread = new Thread(spyAllocator);
    // Start the container allocator thread periodic assignment
    spyAllocatorThread.start();

    // Wait until allocated resource is expired
    while (state.preferredHostRequests.get() != 3) {
      Thread.sleep(100);
    }

    // Verify that handleExpiredResource was invoked once for expired allocated resource
    ArgumentCaptor<SamzaResourceRequest> resourceRequestCaptor = ArgumentCaptor.forClass(SamzaResourceRequest.class);
    ArgumentCaptor<SamzaResource> resourceArgumentCaptor = ArgumentCaptor.forClass(SamzaResource.class);
    verify(spyContainerManager, times(1)).handleExpiredResource(resourceRequestCaptor.capture(),
        resourceArgumentCaptor.capture(), eq("host-0"), any(), any());
    resourceRequestCaptor.getAllValues()
        .forEach(resourceRequest -> assertEquals(resourceRequest.getProcessorId(), "0"));
    resourceArgumentCaptor.getAllValues()
        .forEach(resource -> assertEquals(resource.getHost(), "host-0"));
    // Verify resources were released
    assertTrue(mockClusterResourceManager.containsReleasedResource(expiredAllocatedResource));
    spyAllocator.stop();
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
    assertNotNull(requestState.getHostRequestCounts());
    assertNotNull(requestState.getHostRequestCounts().get("host-1"));
    assertTrue(requestState.getHostRequestCounts().get("host-1").get() == 1);
    assertNotNull(requestState.getHostRequestCounts().get("host-2"));
    assertTrue(requestState.getHostRequestCounts().get("host-2").get() == 1);

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
        put("cluster-manager.container.request.timeout.ms", "500");
        put("cluster-manager.allocator.sleep.ms", "1");
        put("cluster-manager.container.memory.mb", "512");
        put("yarn.package.path", "/foo");
        put("task.inputs", "test-system.test-stream");
        put("systems.test-system.samza.factory", "org.apache.samza.system.MockSystemFactory");
        put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
        put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
        put("job.host-affinity.enabled", "true");
        put("job.name", "test-job");
        put("job.coordinator.system", "test-kafka");
      }
    });

    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    return new MapConfig(map);
  }

}
