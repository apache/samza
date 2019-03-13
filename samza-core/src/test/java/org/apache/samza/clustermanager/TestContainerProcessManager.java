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
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManagerTestUtil;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.testUtils.MockHttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestContainerProcessManager {
  private final MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
  private final MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback);

  private static volatile boolean isRunning = false;

  private Map<String, String> configVals = new HashMap<String, String>() {
    {
      put("cluster-manager.container.count", "1");
      put("cluster-manager.container.retry.count", "1");
      put("cluster-manager.container.retry.window.ms", "1999999999");
      put("cluster-manager.allocator.sleep.ms", "1");
      put("cluster-manager.container.request.timeout.ms", "2");
      put("cluster-manager.container.memory.mb", "512");
      put("yarn.package.path", "/foo");
      put("task.inputs", "test-system.test-stream");
      put("systems.test-system.samza.factory", "org.apache.samza.system.MockSystemFactory");
      put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
      put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
    }
  };
  private Config config = new MapConfig(configVals);

  private Config getConfig() {
    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    return new MapConfig(map);
  }

  private Config getConfigWithHostAffinity() {
    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    map.put("job.host-affinity.enabled", "true");
    return new MapConfig(map);
  }

  private HttpServer server = null;

  private SamzaApplicationState state = null;

  private JobModelManager getJobModelManagerWithHostAffinity(Map<String, String> containerIdToHost) {
    Map<String, Map<String, String>> localityMap = new HashMap<>();
    containerIdToHost.forEach((containerId, host) -> {
        localityMap.put(containerId, ImmutableMap.of(SetContainerHostMapping.HOST_KEY, containerIdToHost.get(containerId)));
      });
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    when(mockLocalityManager.readContainerLocality()).thenReturn(localityMap);

    return JobModelManagerTestUtil.getJobModelManagerWithLocalityManager(getConfig(),
        containerIdToHost.size(), mockLocalityManager, this.server);
  }

  private JobModelManager getJobModelManagerWithoutHostAffinity(int containerCount) {
    return JobModelManagerTestUtil.getJobModelManager(getConfig(), containerCount, this.server);
  }

  @Before
  public void setup() throws Exception {
    server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));
  }

  private Field getPrivateFieldFromTaskManager(String fieldName, ContainerProcessManager object) throws Exception {
    Field field = object.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }


  @Test
  public void testContainerProcessManager() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.putAll(getConfig());
    conf.put("cluster-manager.container.memory.mb", "500");
    conf.put("cluster-manager.container.cpu.cores", "5");

    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1));
    ContainerProcessManager taskManager = new ContainerProcessManager(
        new MapConfig(conf),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );

    AbstractContainerAllocator allocator =
        (AbstractContainerAllocator) getPrivateFieldFromTaskManager("containerAllocator", taskManager).get(taskManager);
    assertEquals(ContainerAllocator.class, allocator.getClass());
    // Asserts that samza exposed container configs is honored by allocator thread
    assertEquals(500, allocator.containerMemoryMb);
    assertEquals(5, allocator.containerNumCpuCores);

    conf.clear();
    conf.putAll(getConfigWithHostAffinity());
    conf.put("cluster-manager.container.memory.mb", "500");
    conf.put("cluster-manager.container.cpu.cores", "5");

    state = new SamzaApplicationState(getJobModelManagerWithHostAffinity(ImmutableMap.of("0", "host1")));
    taskManager = new ContainerProcessManager(
        new MapConfig(conf),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );

    allocator =
        (AbstractContainerAllocator) getPrivateFieldFromTaskManager("containerAllocator", taskManager).get(taskManager);
    assertEquals(HostAwareContainerAllocator.class, allocator.getClass());
    // Asserts that samza exposed container configs is honored by allocator thread
    assertEquals(500, allocator.containerMemoryMb);
    assertEquals(5, allocator.containerNumCpuCores);
  }

  @Test
  public void testOnInit() throws Exception {
    Config conf = getConfig();
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1));

    ContainerProcessManager taskManager = new ContainerProcessManager(
        new MapConfig(conf),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );

    MockContainerAllocator allocator = new MockContainerAllocator(
        clusterResourceManager,
        conf,
        state);

    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);
    CountDownLatch latch = new CountDownLatch(1);
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, new Thread() {
      public void run() {
        isRunning = true;
        latch.countDown();
      }
    });

    taskManager.start();

    if (!latch.await(2, TimeUnit.SECONDS)) {
      Assert.fail("timed out waiting for the latch to expire");
    }

    // Verify Allocator thread has started running
    assertTrue(isRunning);

    // Verify the remaining state
    assertEquals(1, state.neededContainers.get());
    assertEquals(1, allocator.requestedContainers);

    taskManager.stop();
  }

  @Test
  public void testOnShutdown() throws Exception {
    Config conf = getConfig();
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1));

    ContainerProcessManager taskManager = new ContainerProcessManager(
        new MapConfig(conf),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );
    taskManager.start();

    Thread allocatorThread = (Thread) getPrivateFieldFromTaskManager("allocatorThread", taskManager).get(taskManager);
    assertTrue(allocatorThread.isAlive());

    taskManager.stop();

    assertFalse(allocatorThread.isAlive());
  }

  /**
   * Test Task Manager should stop when all containers finish
   */
  @Test
  public void testTaskManagerShouldStopWhenContainersFinish() throws Exception {
    Config conf = getConfig();
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1));

    ContainerProcessManager taskManager = new ContainerProcessManager(
        new MapConfig(conf),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );

    MockContainerAllocator allocator = new MockContainerAllocator(
        clusterResourceManager,
        conf,
        state);

    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);

    Thread thread = new Thread(allocator);
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread);

    // start triggers a request
    taskManager.start();

    assertFalse(taskManager.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());

    SamzaResource container = new SamzaResource(1, 1024, "host1", "id0");
    taskManager.onResourceAllocated(container);

    // Allow container to run and update state

    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    taskManager.onStreamProcessorLaunchSuccess(container);
    assertFalse(taskManager.shouldShutdown());

    taskManager.onResourceCompleted(new SamzaResourceStatus("id0", "diagnostics", SamzaResourceStatus.SUCCESS));
    assertTrue(taskManager.shouldShutdown());
  }


  /**
   * Test Task Manager should request a new container when a task fails with unknown exit code
   * When host-affinity is not enabled, it will always request for ANY_HOST
   */
  @Test
  public void testNewContainerRequestedOnFailureWithUnknownCode() throws Exception {
    Config conf = getConfig();
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1));

    ContainerProcessManager taskManager = new ContainerProcessManager(
        new MapConfig(conf),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );

    MockContainerAllocator allocator = new MockContainerAllocator(
        clusterResourceManager,
        conf,
        state);

    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);

    Thread thread = new Thread(allocator);
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread);

    // start triggers a request
    taskManager.start();

    assertFalse(taskManager.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());


    SamzaResource container = new SamzaResource(1, 1024, "host1", "id0");
    taskManager.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    taskManager.onStreamProcessorLaunchSuccess(container);
    // Create first container failure
    taskManager.onResourceCompleted(new SamzaResourceStatus(container.getResourceID(), "diagnostics", 1));

    // The above failure should trigger a container request
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());


    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(2, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());

    taskManager.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    taskManager.onStreamProcessorLaunchSuccess(container);
    assertTrue(state.jobHealthy.get());

    // Create a second failure
    taskManager.onResourceCompleted(new SamzaResourceStatus(container.getResourceID(), "diagnostics", 1));


    // The above failure should trigger a job shutdown because our retry count is set to 1
    assertEquals(0, allocator.getContainerRequestState().numPendingRequests());
    assertEquals(2, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());
    assertFalse(state.jobHealthy.get());
    assertTrue(taskManager.shouldShutdown());
    assertEquals(SamzaApplicationState.SamzaAppStatus.FAILED, state.status);

    taskManager.stop();
  }

  @Test
  public void testInvalidNotificationsAreIgnored() throws Exception {
    Config conf = getConfig();

    Map<String, String> config = new HashMap<>();
    config.putAll(getConfig());
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1));

    ContainerProcessManager taskManager = new ContainerProcessManager(
        new MapConfig(conf),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );

    MockContainerAllocator allocator = new MockContainerAllocator(
        clusterResourceManager,
        conf,
        state);
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);

    Thread thread = new Thread(allocator);
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread);

    // Start the task clusterResourceManager
    taskManager.start();

    SamzaResource container = new SamzaResource(1, 1000, "host1", "id1");
    taskManager.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    // Create container failure - with ContainerExitStatus.DISKS_FAILED
    taskManager.onResourceCompleted(new SamzaResourceStatus("invalidContainerID", "Disk failure", SamzaResourceStatus.DISK_FAIL));

    // The above failure should not trigger any container requests, since it is for an invalid container ID
    assertEquals(0, allocator.getContainerRequestState().numPendingRequests());
    assertFalse(taskManager.shouldShutdown());
    assertTrue(state.jobHealthy.get());
    assertEquals(state.redundantNotifications.get(), 1);
  }

  @Test
  public void testRerequestOnAnyHostIfContainerStartFails() throws Exception {
    state = new SamzaApplicationState(getJobModelManagerWithHostAffinity(ImmutableMap.of("1", "host1")));
    Map<String, String> configMap = new HashMap<>();
    configMap.putAll(getConfig());

    MockContainerAllocator allocator = new MockContainerAllocator(
        clusterResourceManager,
        new MapConfig(config),
        state);

    ContainerProcessManager manager = new ContainerProcessManager(config, state, new MetricsRegistryMap(), allocator,
        clusterResourceManager);

    manager.start();
    SamzaResource resource = new SamzaResource(1, 1024, "host1", "resource-1");
    state.pendingContainers.put("1", resource);
    Assert.assertEquals(clusterResourceManager.resourceRequests.size(), 1);
    manager.onStreamProcessorLaunchFailure(resource, new Exception("cannot launch container!"));
    Assert.assertEquals(clusterResourceManager.resourceRequests.size(), 2);
    Assert.assertEquals(clusterResourceManager.resourceRequests.get(1).getHost(), ResourceRequestState.ANY_HOST);
  }

  @Test
  public void testAllBufferedResourcesAreUtilized() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.putAll(getConfigWithHostAffinity());
    config.put("job.container.count", "2");
    Config cfg = new MapConfig(config);
    // 1. Request two containers on hosts - host1 and host2
    state = new SamzaApplicationState(getJobModelManagerWithHostAffinity(ImmutableMap.of("0", "host1",
        "1", "host2")));

    ContainerProcessManager taskManager = new ContainerProcessManager(
        cfg,
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );

    MockHostAwareContainerAllocator allocator = new MockHostAwareContainerAllocator(
        clusterResourceManager,
        cfg,
        state);
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);

    Thread thread = new Thread(allocator);
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread);

    taskManager.start();
    assertFalse(taskManager.shouldShutdown());
    // 2. When the task manager starts, there should have been a pending request on host1 and host2
    assertEquals(2, allocator.getContainerRequestState().numPendingRequests());

    // 3. Allocate an extra resource on host1 and no resource on host2 yet.
    SamzaResource resource1 = new SamzaResource(1, 1000, "host1", "id1");
    SamzaResource resource2 = new SamzaResource(1, 1000, "host1", "id2");
    taskManager.onResourceAllocated(resource1);
    taskManager.onResourceAllocated(resource2);

    // 4. Wait for the container to start on host1 and immediately fail
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    taskManager.onStreamProcessorLaunchSuccess(resource1);
    assertEquals("host2", allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());

    taskManager.onResourceCompleted(new SamzaResourceStatus(resource1.getResourceID(), "App Error", 1));
    assertEquals(2, allocator.getContainerRequestState().numPendingRequests());

    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(3, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());

    // 5. Do not allocate any further resource on host1, and verify that the re-run of the container on host1 uses the
    // previously allocated extra resource
    SamzaResource resource3 = new SamzaResource(1, 1000, "host2", "id3");
    taskManager.onResourceAllocated(resource3);

    if (!allocator.awaitContainersStart(2, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    taskManager.onStreamProcessorLaunchSuccess(resource2);
    taskManager.onStreamProcessorLaunchSuccess(resource3);

    assertTrue(state.jobHealthy.get());
  }

  @Test
  public void testDuplicateNotificationsDoNotAffectJobHealth() throws Exception {
    Config conf = getConfig();

    Map<String, String> config = new HashMap<>();
    config.putAll(getConfig());
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1));

    ContainerProcessManager taskManager = new ContainerProcessManager(
        new MapConfig(conf),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );

    MockContainerAllocator allocator = new MockContainerAllocator(
        clusterResourceManager,
        conf,
        state);
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);

    Thread thread = new Thread(allocator);
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread);

    // Start the task manager
    taskManager.start();
    assertFalse(taskManager.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());

    SamzaResource container1 = new SamzaResource(1, 1000, "host1", "id1");
    taskManager.onResourceAllocated(container1);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    taskManager.onStreamProcessorLaunchSuccess(container1);
    assertEquals(0, allocator.getContainerRequestState().numPendingRequests());

    // Create container failure - with ContainerExitStatus.DISKS_FAILED
    taskManager.onResourceCompleted(new SamzaResourceStatus(container1.getResourceID(), "Disk failure", SamzaResourceStatus.DISK_FAIL));

    // The above failure should trigger a container request
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(2, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());

    SamzaResource container2 = new SamzaResource(1, 1000, "host1", "id2");
    taskManager.onResourceAllocated(container2);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    taskManager.onStreamProcessorLaunchSuccess(container2);

    assertTrue(state.jobHealthy.get());

    // Simulate a duplicate notification for container 1 with a different exit code
    taskManager.onResourceCompleted(new SamzaResourceStatus(container1.getResourceID(), "Disk failure", SamzaResourceStatus.PREEMPTED));
    // assert that a duplicate notification does not change metrics (including job health)
    assertEquals(state.redundantNotifications.get(), 1);
    assertEquals(2, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());
    assertTrue(state.jobHealthy.get());
  }

  /**
   * Test AM requests a new container when a task fails
   * Error codes with same behavior - Disk failure, preemption and aborted
   */
  @Test
  public void testNewContainerRequestedOnFailureWithKnownCode() throws Exception {
    Config conf = getConfig();

    Map<String, String> config = new HashMap<>();
    config.putAll(getConfig());
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1));

    ContainerProcessManager taskManager = new ContainerProcessManager(
        new MapConfig(conf),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );

    MockContainerAllocator allocator = new MockContainerAllocator(
        clusterResourceManager,
        conf,
        state);
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);

    Thread thread = new Thread(allocator);
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread);

    // Start the task clusterResourceManager
    taskManager.start();
    assertFalse(taskManager.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());

    SamzaResource container1 = new SamzaResource(1, 1000, "host1", "id1");
    taskManager.onResourceAllocated(container1);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    assertEquals(0, allocator.getContainerRequestState().numPendingRequests());
    taskManager.onStreamProcessorLaunchSuccess(container1);
    // Create container failure - with ContainerExitStatus.DISKS_FAILED
    taskManager.onResourceCompleted(new SamzaResourceStatus(container1.getResourceID(), "App error", 1));

    // The above failure should trigger a container request
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(2, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());

    SamzaResource container2 = new SamzaResource(1, 1000, "host1", "id2");
    taskManager.onResourceAllocated(container2);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    taskManager.onStreamProcessorLaunchSuccess(container2);

    // Create container failure - with ContainerExitStatus.PREEMPTED
    taskManager.onResourceCompleted(new SamzaResourceStatus(container2.getResourceID(), "Preemption", SamzaResourceStatus.PREEMPTED));
    assertEquals(3, clusterResourceManager.resourceRequests.size());

    // The above failure should trigger a container request
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());
    SamzaResource container3 = new SamzaResource(1, 1000, "host1", "id3");
    taskManager.onResourceAllocated(container3);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    taskManager.onStreamProcessorLaunchSuccess(container3);

    // Create container failure - with ContainerExitStatus.ABORTED
    taskManager.onResourceCompleted(new SamzaResourceStatus(container3.getResourceID(), "Aborted", SamzaResourceStatus.ABORTED));

    // The above failure should trigger a container request
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertEquals(4, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());
    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());

    taskManager.stop();
  }

  @Test
  public void testAppMasterWithFwk() {
    Config conf = getConfig();
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1));

    ContainerProcessManager taskManager = new ContainerProcessManager(
        new MapConfig(conf),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );
    taskManager.start();
    SamzaResource container2 = new SamzaResource(1, 1024, "", "id0");
    assertFalse(taskManager.shouldShutdown());
    taskManager.onResourceAllocated(container2);

    configVals.put(JobConfig.SAMZA_FWK_PATH(), "/export/content/whatever");
    Config config1 = new MapConfig(configVals);

    ContainerProcessManager taskManager1 = new ContainerProcessManager(
        new MapConfig(config),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager
    );
    taskManager1.start();
    taskManager1.onResourceAllocated(container2);
  }

  @After
  public void teardown() {
    server.stop();
  }

}
