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
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestContainerAllocator {
  private final MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
  private final MockClusterResourceManager manager = new MockClusterResourceManager(callback);
  private final Config config = getConfig();
  private final JobModelManager reader = getJobModelReader(1);
  private final SamzaApplicationState state = new SamzaApplicationState(reader);
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
    reader.stop();
    containerAllocator.stop();
  }


  private static Config getConfig() {
    Config config = new MapConfig(new HashMap<String, String>() {
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

    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    return new MapConfig(map);
  }

  private static JobModelManager getJobModelReader(int containerCount) {
    //Ideally, the JobModelReader should be constructed independent of HttpServer.
    //That way it becomes easier to mock objects. Save it for later.

    HttpServer server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));
    Map<String, ContainerModel> containers = new java.util.HashMap<>();
    for (int i = 0; i < containerCount; i++) {
      ContainerModel container = new ContainerModel(String.valueOf(i), i, new HashMap<TaskName, TaskModel>());
      containers.put(String.valueOf(i), container);
    }
    JobModel jobModel = new JobModel(getConfig(), containers);
    return new JobModelManager(jobModel, server, null);
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
        put("0", "abc");
        put("1", "def");
        put("2", null);
        put("3", "abc");
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


  /**
   * If the container fails to start e.g because it fails to connect to a NM on a host that
   * is down, the allocator should request a new container on a different host.
   */
  @Test
  public void testRerequestOnAnyHostIfContainerStartFails() throws Exception {
    final SamzaResource container = new SamzaResource(1, 1024, "2", "id0");
    final SamzaResource container1 = new SamzaResource(1, 1024, "2", "id0");
    manager.nextException = new IOException("Cant connect to RM");

    // Set up our final asserts before starting the allocator thread
    MockContainerListener listener = new MockContainerListener(2, 1, 2, 0, null, new Runnable() {
      @Override
      public void run() {
        // The failed container should be released. The successful one should not.
        assertNotNull(manager.releasedResources);
        assertEquals(1, manager.releasedResources.size());
        assertTrue(manager.releasedResources.contains(container));
      }
    },
        new Runnable() {
          @Override
          public void run() {
            // Test that the first request assignment had a preferred host and the retry didn't
            assertEquals(2, requestState.assignedRequests.size());

            SamzaResourceRequest request = requestState.assignedRequests.remove();
            assertEquals("0", request.getContainerID());
            assertEquals("2", request.getPreferredHost());

            request = requestState.assignedRequests.remove();
            assertEquals("0", request.getContainerID());
            assertEquals("ANY_HOST", request.getPreferredHost());

            // This routine should be called after the retry is assigned, but before it's started.
            // So there should still be 1 container needed.
            assertEquals(1, state.neededContainers.get());
          }
        }, null
    );
    state.neededContainers.set(1);
    requestState.registerContainerListener(listener);

    containerAllocator.requestResource("0", "2");
    containerAllocator.addResource(container);
    containerAllocator.addResource(container1);
    allocatorThread.start();

    listener.verify();

  }


}
