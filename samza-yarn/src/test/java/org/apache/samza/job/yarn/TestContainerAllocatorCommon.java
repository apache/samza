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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.samza.config.Config;
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


/**
 * Handles all common fields/tests for ContainerAllocators.
 */
public abstract class TestContainerAllocatorCommon {
  protected static final String ANY_HOST = ContainerRequestState.ANY_HOST;

  protected final HttpServer server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));

  protected AMRMClientAsyncImpl amRmClientAsync;
  protected TestAMRMClientImpl testAMRMClient;
  protected MockContainerRequestState requestState;
  protected AbstractContainerAllocator containerAllocator;
  protected Thread allocatorThread;
  protected ContainerUtil containerUtil;

  protected SamzaAppState state = new SamzaAppState(getCoordinator(1), -1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2);

  protected abstract Config getConfig();
  protected abstract MockContainerRequestState createContainerRequestState(AMRMClientAsync<AMRMClient.ContainerRequest> amClient);

  private JobCoordinator getCoordinator(int containerCount) {
    Map<Integer, ContainerModel> containers = new java.util.HashMap<>();
    for (int i = 0; i < containerCount; i++) {
      ContainerModel container = new ContainerModel(i, new HashMap<TaskName, TaskModel>());
      containers.put(i, container);
    }
    JobModel jobModel = new JobModel(getConfig(), containers);
    return new JobCoordinator(jobModel, server, null);
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

    requestState = createContainerRequestState(amRmClientAsync);
    containerAllocator = new HostAwareContainerAllocator(
        amRmClientAsync,
        containerUtil,
        new YarnConfig(getConfig())
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
   * If the container fails to start e.g because it fails to connect to a NM on a host that
   * is down, the allocator should request a new container on a different host.
   */
  @Test
  public void testRerequestOnAnyHostIfContainerStartFails() throws Exception {
    final Container container = TestUtil
        .getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "host2", 123);
    final Container container1 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "host1", 123);

    ((MockContainerUtil) containerUtil).containerStartException = new IOException("You shall not... connect to the NM!");

    Runnable releasedContainerAssertions = new Runnable() {
      @Override
      public void run() {
        // The failed container should be released. The successful one should not.
        assertNotNull(testAMRMClient.getRelease());
        assertEquals(1, testAMRMClient.getRelease().size());
        assertTrue(testAMRMClient.getRelease().contains(container.getId()));
      }
    };

    Runnable assignedContainerAssertions = new Runnable() {
      @Override
      public void run() {
        // Test that the first request assignment had a preferred host and the retry didn't
        assertEquals(2, requestState.assignedRequests.size());

        SamzaContainerRequest request = requestState.assignedRequests.remove();
        assertEquals(0, request.expectedContainerId);
        assertEquals("host2", request.getPreferredHost());

        request = requestState.assignedRequests.remove();
        assertEquals(0, request.expectedContainerId);
        assertEquals("ANY_HOST", request.getPreferredHost());

        // This routine should be called after the retry is assigned, but before it's started.
        // So there should still be 1 container needed because neededContainers should not be decremented for a failed start.
        assertEquals(1, state.neededContainers.get());
      }
    };

    // Set up our final asserts before starting the allocator thread
    MockContainerListener listener = new MockContainerListener(2, 1, 2, 0, null, releasedContainerAssertions, assignedContainerAssertions, null);
    requestState.registerContainerListener(listener);
    state.neededContainers.set(1); // Normally this would be done in the SamzaTaskManager

    // Only request 1 container and we should see 2 assignments in the assertions above (because of the retry)
    containerAllocator.requestContainer(0, "host2");
    containerAllocator.addContainer(container);
    containerAllocator.addContainer(container1);

    allocatorThread.start();

    listener.verify();
  }


  /**
   * Extra allocated containers that are returned by the RM and unused by the AM should be released.
   * Containers are considered "extra" only when there are no more pending requests to fulfill
   * @throws Exception
   */
  @Test
  public void testAllocatorReleasesExtraContainers() throws Exception {
    final Container container = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "host1", 123);
    final Container container1 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "host1", 123);
    final Container container2 = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000003"), "host2", 123);

    Runnable releasedContainerAssertions = new Runnable() {
      @Override
      public void run() {
        assertNotNull(testAMRMClient.getRelease());
        assertEquals(2, testAMRMClient.getRelease().size());
        assertTrue(testAMRMClient.getRelease().contains(container1.getId()));
        assertTrue(testAMRMClient.getRelease().contains(container2.getId()));

        // Test that state is cleaned up
        assertEquals(0, requestState.getRequestsQueue().size());
        assertEquals(0, requestState.getRequestsToCountMap().size());
        assertNull(requestState.getContainersOnAHost("host1"));
        assertNull(requestState.getContainersOnAHost("host2"));
      }
    };

    // Set up our final asserts before starting the allocator thread
    MockContainerListener listener = new MockContainerListener(3, 2, 0, 0, null, releasedContainerAssertions, null, null);
    requestState.registerContainerListener(listener);

    containerAllocator.requestContainer(0, "host1");

    containerAllocator.addContainer(container);
    containerAllocator.addContainer(container1);
    containerAllocator.addContainer(container2);

    allocatorThread.start();

    listener.verify();
  }
}
