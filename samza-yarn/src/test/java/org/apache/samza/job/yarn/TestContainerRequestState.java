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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.samza.job.yarn.util.TestAMRMClientImpl;
import org.apache.samza.job.yarn.util.TestUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class TestContainerRequestState {
  private AMRMClientAsyncImpl amRmClientAsync;
  private TestAMRMClientImpl testAMRMClient;
  private static final String ANY_HOST = ContainerRequestState.ANY_HOST;

  @Before
  public void setup() {
    // Create AMRMClient
    testAMRMClient = new TestAMRMClientImpl(
        TestUtil.getAppMasterResponse(
            false,
            new ArrayList<Container>(),
            new ArrayList<ContainerStatus>()
        ));
    amRmClientAsync = TestUtil.getAMClient(testAMRMClient);
  }

  /**
   * Test state after a request is submitted
   */
  @Test
  public void testUpdateRequestState() {
    // Host-affinity is enabled
    ContainerRequestState state = new ContainerRequestState(amRmClientAsync, true);
    SamzaContainerRequest request = new SamzaContainerRequest(0, "abc");
    state.updateRequestState(request);

    assertNotNull(testAMRMClient.requests);
    assertEquals(1, testAMRMClient.requests.size());
    assertEquals(request.getIssuedRequest(), testAMRMClient.requests.get(0));

    assertNotNull(state.getRequestsQueue());
    assertTrue(state.getRequestsQueue().size() == 1);

    assertNotNull(state.getRequestsToCountMap());
    assertNotNull(state.getRequestsToCountMap().get("abc"));
    assertEquals(1, state.getRequestsToCountMap().get("abc").get());

    assertNotNull(state.getContainersOnAHost("abc"));
    assertEquals(0, state.getContainersOnAHost("abc").size());

    // Host-affinity is not enabled
    ContainerRequestState state1 = new ContainerRequestState(amRmClientAsync, false);
    SamzaContainerRequest request1 = new SamzaContainerRequest(1, null);
    state1.updateRequestState(request1);

    assertNotNull(testAMRMClient.requests);
    assertEquals(2, testAMRMClient.requests.size());

    AMRMClient.ContainerRequest expectedContainerRequest = request1.getIssuedRequest();
    AMRMClient.ContainerRequest actualContainerRequest = testAMRMClient.requests.get(1);

    assertEquals(expectedContainerRequest.getCapability(), actualContainerRequest.getCapability());
    assertEquals(expectedContainerRequest.getPriority(), actualContainerRequest.getPriority());
    assertNull(actualContainerRequest.getNodes());

    assertNotNull(state1.getRequestsQueue());
    assertTrue(state1.getRequestsQueue().size() == 1);

    assertNotNull(state1.getRequestsToCountMap());
    assertNull(state1.getRequestsToCountMap().get(ANY_HOST));

  }

  /**
   * Test addContainer() updates the state correctly
   */
  @Test
  public void testAddContainer() {
    // Add container to ANY_LIST when host-affinity is not enabled
    ContainerRequestState state = new ContainerRequestState(amRmClientAsync, false);
    Container container = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "abc", 123);
    state.addContainer(container);

    assertNotNull(state.getRequestsQueue());
    assertNotNull(state.getRequestsToCountMap());
    assertNotNull(state.getContainersOnAHost(ANY_HOST));

    assertEquals(1, state.getContainersOnAHost(ANY_HOST).size());
    assertEquals(container, state.getContainersOnAHost(ANY_HOST).get(0));

    // Container Allocated when there is no request in queue
    ContainerRequestState state1 = new ContainerRequestState(amRmClientAsync, true);
    Container container1 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000003"), "zzz", 123);
    state1.addContainer(container1);

    assertNotNull(state1.getRequestsQueue());
    assertEquals(0, state1.getRequestsQueue().size());

    assertNull(state1.getContainersOnAHost("zzz"));
    assertNotNull(state1.getContainersOnAHost(ANY_HOST));
    assertEquals(1, state1.getContainersOnAHost(ANY_HOST).size());
    assertEquals(container1, state1.getContainersOnAHost(ANY_HOST).get(0));

    // Container Allocated on a Requested Host
    state1.updateRequestState(new SamzaContainerRequest(0, "abc"));

    assertNotNull(state1.getRequestsQueue());
    assertEquals(1, state1.getRequestsQueue().size());

    assertNotNull(state1.getRequestsToCountMap());
    assertNotNull(state1.getRequestsToCountMap().get("abc"));
    assertEquals(1, state1.getRequestsToCountMap().get("abc").get());

    state1.addContainer(container);

    assertNotNull(state1.getContainersOnAHost("abc"));
    assertEquals(1, state1.getContainersOnAHost("abc").size());
    assertEquals(container, state1.getContainersOnAHost("abc").get(0));

    // Container Allocated on host that was not requested
    Container container2 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000004"), "xyz", 123);
    state1.addContainer(container2);

    assertNull(state1.getContainersOnAHost("xyz"));
    assertNotNull(state1.getContainersOnAHost(ANY_HOST));
    assertEquals(2, state1.getContainersOnAHost(ANY_HOST).size());
    assertEquals(container2, state1.getContainersOnAHost(ANY_HOST).get(1));

    // Extra containers were allocated on a host that was requested
    Container container3 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000005"), "abc", 123);
    state1.addContainer(container3);

    assertEquals(3, state1.getContainersOnAHost(ANY_HOST).size());
    assertEquals(container3, state1.getContainersOnAHost(ANY_HOST).get(2));
  }

  /**
   * Test request state after container is assigned to a host
   * * Assigned on requested host
   * * Assigned on any host
   */
  @Test
  public void testContainerAssignment() throws Exception {
    // Host-affinity enabled
    ContainerRequestState state = new ContainerRequestState(amRmClientAsync, true);
    SamzaContainerRequest request = new SamzaContainerRequest(0, "abc");
    SamzaContainerRequest request1 = new SamzaContainerRequest(0, "def");

    state.updateRequestState(request);
    state.updateRequestState(request1);

    Container container = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "abc", 123);
    Container container1 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "zzz", 123);
    state.addContainer(container);
    state.addContainer(container1);

    assertEquals(2, state.getRequestsQueue().size());
    assertEquals(2, state.getRequestsToCountMap().size());

    assertNotNull(state.getContainersOnAHost("abc"));
    assertEquals(1, state.getContainersOnAHost("abc").size());
    assertEquals(container, state.getContainersOnAHost("abc").get(0));

    assertNotNull(state.getContainersOnAHost("def"));
    assertEquals(0, state.getContainersOnAHost("def").size());

    assertNotNull(state.getContainersOnAHost(ANY_HOST));
    assertEquals(1, state.getContainersOnAHost(ANY_HOST).size());
    assertEquals(container1, state.getContainersOnAHost(ANY_HOST).get(0));

    // Container assigned on the requested host
    state.updateStateAfterAssignment(request, "abc", container);

    assertEquals(1, state.getRequestsQueue().size());
    assertEquals(request1, state.getRequestsQueue().peek());

    assertNotNull(state.getRequestsToCountMap().get("abc"));
    assertEquals(0, state.getRequestsToCountMap().get("abc").get());

    assertNotNull(state.getContainersOnAHost("abc"));
    assertEquals(0, state.getContainersOnAHost("abc").size());

    // Container assigned on any host
    state.updateStateAfterAssignment(request1, ANY_HOST, container1);

    assertEquals(0, state.getRequestsQueue().size());

    assertNotNull(state.getRequestsToCountMap().get("def"));
    assertEquals(0, state.getRequestsToCountMap().get("def").get());

    assertNotNull(state.getContainersOnAHost(ANY_HOST));
    assertEquals(0, state.getContainersOnAHost(ANY_HOST).size());

  }

}
