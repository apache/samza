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

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


public class TestContainerRequestState {

  private final MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
  private final MockClusterResourceManager manager = new MockClusterResourceManager(callback, new SamzaApplicationState(null));

  private static final String ANY_HOST = ResourceRequestState.ANY_HOST;

  /**
   * Test state after a request is submitted
   */
  @Test
  public void testUpdateRequestState() {
    // Host-affinity is enabled
    ResourceRequestState state = new ResourceRequestState(true, manager);
    SamzaResourceRequest request = new SamzaResourceRequest(1, 1024, "abc", "0");
    state.addResourceRequest(request);

    assertNotNull(manager.resourceRequests);
    assertEquals(1, manager.resourceRequests.size());

    assertNotNull(state.numPendingRequests() == 1);

    assertNotNull(state.getHostRequestCounts());
    assertNotNull(state.getHostRequestCounts().get("abc"));
    assertEquals(1, state.getHostRequestCounts().get("abc").get());

    assertNotNull(state.getResourcesOnAHost("abc"));
    assertEquals(0, state.getResourcesOnAHost("abc").size());

    // Host-affinity is not enabled
    ResourceRequestState state1 = new ResourceRequestState(false, manager);
    SamzaResourceRequest request1 = new SamzaResourceRequest(1, 1024, null, "1");
    state1.addResourceRequest(request1);

    assertNotNull(manager.resourceRequests);
    assertEquals(2, manager.resourceRequests.size());


    assertTrue(state1.numPendingRequests() == 1);

    assertNotNull(state1.getHostRequestCounts());
    assertNull(state1.getHostRequestCounts().get(ANY_HOST));

  }

  /**
   * Test addContainer() updates the state correctly
   */
  @Test
  public void testAddContainer() {
    // Add container to ANY_LIST when host-affinity is not enabled
    ResourceRequestState state = new ResourceRequestState(false, manager);
    SamzaResource resource = new SamzaResource(1, 1024, "abc", "id1");

    state.addResource(resource);

    assertNotNull(state.getHostRequestCounts());
    assertNotNull(state.getResourcesOnAHost(ANY_HOST));

    assertEquals(1, state.getResourcesOnAHost(ANY_HOST).size());
    assertEquals(resource, state.getResourcesOnAHost(ANY_HOST).get(0));

    // Container Allocated when there is no request in queue
    ResourceRequestState state1 = spy(new ResourceRequestState(true, manager));
    SamzaResource container1 = new SamzaResource(1, 1024, "zzz", "id2");
    state1.addResource(container1);

    assertEquals(0, state1.numPendingRequests());

    assertNull(state1.getResourcesOnAHost("zzz"));
    assertNotNull(state1.getResourcesOnAHost(ANY_HOST));
    assertEquals(1, state1.getResourcesOnAHost(ANY_HOST).size());
    assertEquals(container1, state1.getResourcesOnAHost(ANY_HOST).get(0));

    // Container Allocated on a Requested Host
    state1.addResourceRequest(new SamzaResourceRequest(1, 1024, "abc", "0"));

    // Delayed request
    state1.addResourceRequest(new SamzaResourceRequest(1, 1024, "def", "1",
        Instant.now().plus(Duration.ofHours(1))));
    state1.addResourceRequest(new SamzaResourceRequest(1, 1024, "ghi", "2",
        Instant.now().plus(Duration.ofHours(2))));

    assertEquals(1, state1.numPendingRequests());
    assertEquals(2, state1.numDelayedRequests());

    // Verify request sent only once for the non-delayed request
    verify(state1).sendResourceRequest(any(SamzaResourceRequest.class));

    assertNotNull(state1.getHostRequestCounts());
    assertNotNull(state1.getHostRequestCounts().get("abc"));
    assertEquals(1, state1.getHostRequestCounts().get("abc").get());

    state1.addResource(resource);

    assertNotNull(state1.getResourcesOnAHost("abc"));
    assertEquals(1, state1.getResourcesOnAHost("abc").size());
    assertEquals(resource, state1.getResourcesOnAHost("abc").get(0));

    // Container Allocated on host that was not requested
    SamzaResource container2 = new SamzaResource(1, 1024, "xyz", "id2");

    state1.addResource(container2);

    assertNull(state1.getResourcesOnAHost("xyz"));
    assertNotNull(state1.getResourcesOnAHost(ANY_HOST));
    assertEquals(2, state1.getResourcesOnAHost(ANY_HOST).size());
    assertEquals(container2, state1.getResourcesOnAHost(ANY_HOST).get(1));

    // Extra containers were allocated on a host that was requested
    SamzaResource container3 = new SamzaResource(1, 1024, "abc", "id3");
    state1.addResource(container3);

    assertEquals(3, state1.getResourcesOnAHost(ANY_HOST).size());
    assertEquals(container3, state1.getResourcesOnAHost(ANY_HOST).get(2));
  }

  /**
   * Test request state after container is assigned to a host
   * * Assigned on requested host
   * * Assigned on any host
   */
  @Test
  public void testContainerAssignment() throws Exception {
    // Host-affinity enabled
    ResourceRequestState state = new ResourceRequestState(true, manager);
    SamzaResourceRequest request = new SamzaResourceRequest(1, 1024, "abc", "0");

    SamzaResourceRequest request1 = new SamzaResourceRequest(1, 1024, "def", "0");

    state.addResourceRequest(request);
    state.addResourceRequest(request1);

    SamzaResource container = new SamzaResource(1, 1024, "abc", "id0");

    SamzaResource container1 = new SamzaResource(1, 1024, "zzz", "id1");
    state.addResource(container);
    state.addResource(container1);

    assertEquals(2, state.numPendingRequests());
    assertEquals(2, state.getHostRequestCounts().size());

    assertNotNull(state.getResourcesOnAHost("abc"));
    assertEquals(1, state.getResourcesOnAHost("abc").size());
    assertEquals(container, state.getResourcesOnAHost("abc").get(0));

    assertNotNull(state.getResourcesOnAHost("def"));
    assertEquals(0, state.getResourcesOnAHost("def").size());

    assertNotNull(state.getResourcesOnAHost(ANY_HOST));
    assertEquals(1, state.getResourcesOnAHost(ANY_HOST).size());
    assertEquals(container1, state.getResourcesOnAHost(ANY_HOST).get(0));

    // Container assigned on the requested host
    state.updateStateAfterAssignment(request, "abc", container);

    assertEquals(request1, state.peekPendingRequest());

    assertNotNull(state.getHostRequestCounts().get("abc"));
    assertEquals(0, state.getHostRequestCounts().get("abc").get());

    assertNotNull(state.getResourcesOnAHost("abc"));
    assertEquals(0, state.getResourcesOnAHost("abc").size());

    // Container assigned on any host
    state.updateStateAfterAssignment(request1, ANY_HOST, container1);

    assertEquals(0, state.numPendingRequests());

    assertNotNull(state.getHostRequestCounts().get("def"));
    assertEquals(0, state.getHostRequestCounts().get("def").get());

    assertNotNull(state.getResourcesOnAHost(ANY_HOST));
    assertEquals(0, state.getResourcesOnAHost(ANY_HOST).size());

  }

  @Test
  public void testReleaseResource() {
    // Host-affinity is enabled
    ResourceRequestState state = new ResourceRequestState(true, manager);

    SamzaResourceRequest request = new SamzaResourceRequest(1, 1024, "abc", "0");
    SamzaResourceRequest request1 = new SamzaResourceRequest(1, 1024, "def", "0");
    state.addResourceRequest(request);
    state.addResourceRequest(request1);

    SamzaResource container = new SamzaResource(1, 1024, "abc", "id0");
    SamzaResource container1 = new SamzaResource(1, 1024, ANY_HOST, "id1");
    state.addResource(container);
    state.addResource(container1);

    state.releaseResource("id0");
    assertEquals(0, state.getResourcesOnAHost("abc").size());
    assertEquals(1, state.getResourcesOnAHost(ANY_HOST).size());

    state.releaseResource("id1");
    assertEquals(0, state.getResourcesOnAHost("abc").size());
    assertEquals(0, state.getResourcesOnAHost(ANY_HOST).size());
  }

  @Test
  public void testPriorityQueueOrdering() {
    PriorityQueue<SamzaResourceRequest> pq = new PriorityQueue<>();
    Instant now = Instant.now();

    ImmutableList<SamzaResourceRequest> expectedOrder = ImmutableList.of(
        createRequestForActive(now.minusSeconds(120)),
        createRequestForActive(now),
        createRequestForActive(now.plusSeconds(120)),
        createRequestForActive(now.plusSeconds(240)),
        createRequestForStandby(now.minusSeconds(120)),
        createRequestForStandby(now),
        createRequestForStandby(now.plusSeconds(120)),
        createRequestForStandby(now.plusSeconds(240)));

    SamzaResourceRequest[] copyExpectedOrder = new SamzaResourceRequest[expectedOrder.size()];
    copyExpectedOrder = expectedOrder.toArray(copyExpectedOrder);
    List<SamzaResourceRequest> shuffled = Arrays.asList(copyExpectedOrder);

    Collections.shuffle(shuffled, new Random(Instant.now().toEpochMilli()));
    pq.addAll(shuffled);

    ArrayList priorityQueueOrder = new ArrayList();
    for (int i = 0; i < expectedOrder.size(); ++i) {
      priorityQueueOrder.add(pq.poll());
    }
    assertEquals(expectedOrder, priorityQueueOrder);
  }

  @Test
  public void testDelayedQueueOrdering() {
    ResourceRequestState.DelayedRequestQueue delayedRequestQueue = new ResourceRequestState.DelayedRequestQueue();
    Instant now = Instant.now();

    // Expected priority by request timestamp only, regardless of active or standby
    ImmutableList<SamzaResourceRequest> expectedOrder = ImmutableList.of(
        createRequestForActive(now),
        createRequestForStandby(now.plusSeconds(60)),
        createRequestForActive(now.plusSeconds(120)),
        createRequestForStandby(now.plusSeconds(121)),
        createRequestForActive(now.plusSeconds(240)),
        createRequestForStandby(now.plusSeconds(241)));

    SamzaResourceRequest[] copyExpectedOrder = new SamzaResourceRequest[expectedOrder.size()];
    copyExpectedOrder = expectedOrder.toArray(copyExpectedOrder);
    List<SamzaResourceRequest> shuffled = Arrays.asList(copyExpectedOrder);

    Collections.shuffle(shuffled, new Random(Instant.now().toEpochMilli()));
    delayedRequestQueue.addAll(shuffled);

    ArrayList priorityQueueOrder = new ArrayList();
    for (int i = 0; i < expectedOrder.size(); ++i) {
      priorityQueueOrder.add(delayedRequestQueue.poll());
    }
    assertEquals(expectedOrder, priorityQueueOrder);
  }

  SamzaResourceRequest createRequestForActive(Instant requestTime) {
    String randomHost = RandomStringUtils.randomAlphanumeric(4);
    String randomId = RandomStringUtils.randomAlphanumeric(8);
    return new SamzaResourceRequest(1, 1, randomHost, randomId, requestTime);
  }

  SamzaResourceRequest createRequestForStandby(Instant requestTime) {
    String randomHost = RandomStringUtils.randomAlphanumeric(4);
    String randomId = RandomStringUtils.randomAlphanumeric(8) + "-standby"; // hyphen in ID denotes a standby processor
    return new SamzaResourceRequest(1, 1, randomHost, randomId, requestTime);
  }
}
