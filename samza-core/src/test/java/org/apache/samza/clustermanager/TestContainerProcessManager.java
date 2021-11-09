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
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementMetadataStore;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManagerTestUtil;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.LocalityModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.testUtils.MockHttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestContainerProcessManager {

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
      put("job.name", "test-job");
      put("job.coordinator.system", "test-kafka");
    }
  };
  private Config config = new MapConfig(configVals);
  private ContainerPlacementMetadataStore containerPlacementMetadataStore;

  private Config getConfig() {
    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    return new MapConfig(map);
  }

  private Config getConfigWithHostAffinity() {
    return getConfigWithHostAffinityAndRetries(true, 1, true);
  }

  private Config getConfigWithHostAffinityAndRetries(boolean withHostAffinity, int maxRetries, boolean failAfterRetries) {
    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    map.put("job.host-affinity.enabled", String.valueOf(withHostAffinity));
    map.put(ClusterManagerConfig.CLUSTER_MANAGER_CONTAINER_RETRY_COUNT, String.valueOf(maxRetries));
    map.put(ClusterManagerConfig.CLUSTER_MANAGER_CONTAINER_FAIL_JOB_AFTER_RETRIES, String.valueOf(failAfterRetries));
    return new MapConfig(map);
  }

  private HttpServer server = null;

  private JobModelManager getJobModelManager(int containerCount) {
    return JobModelManagerTestUtil.getJobModelManager(getConfig(), containerCount, this.server);
  }

  @Before
  public void setup() throws Exception {
    server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(config);
    CoordinatorStreamStore coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    coordinatorStreamStore.init();
    containerPlacementMetadataStore = new ContainerPlacementMetadataStore(coordinatorStreamStore);
    containerPlacementMetadataStore.start();
  }

  private Field getPrivateFieldFromCpm(String fieldName, ContainerProcessManager object) throws Exception {
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

    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    when(mockLocalityManager.readLocality())
        .thenReturn(new LocalityModel(ImmutableMap.of("0", new ProcessorLocality("0", "host1"))));
    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager, true, false, mockLocalityManager, faultDomainManager);
    ContainerProcessManager cpm =
        buildContainerProcessManager(new ClusterManagerConfig(new MapConfig(conf)), state, clusterResourceManager, Optional.empty());

    ContainerAllocator allocator =
        (ContainerAllocator) getPrivateFieldFromCpm("containerAllocator", cpm).get(cpm);
    assertEquals(ContainerAllocator.class, allocator.getClass());
    // Asserts that samza exposed container configs is honored by allocator thread
    assertEquals(500, allocator.containerMemoryMb);
    assertEquals(5, allocator.containerNumCpuCores);

    conf.clear();
    conf.putAll(getConfigWithHostAffinity());
    conf.put("cluster-manager.container.memory.mb", "500");
    conf.put("cluster-manager.container.cpu.cores", "5");

    state = new SamzaApplicationState(getJobModelManager(1));
    callback = new MockClusterResourceManagerCallback();
    clusterResourceManager = new MockClusterResourceManager(callback, state);
    cpm = new ContainerProcessManager(
        new ClusterManagerConfig(new MapConfig(conf)),
        state,
        new MetricsRegistryMap(),
        clusterResourceManager,
        Optional.empty(),
        containerManager,
        mockLocalityManager,
        false
    );

    allocator =
        (ContainerAllocator) getPrivateFieldFromCpm("containerAllocator", cpm).get(cpm);
    assertEquals(ContainerAllocator.class, allocator.getClass());
    // Asserts that samza exposed container configs is honored by allocator thread
    assertEquals(500, allocator.containerMemoryMb);
    assertEquals(5, allocator.containerNumCpuCores);
  }

  @Test
  public void testOnInit() throws Exception {
    Config conf = getConfig();
    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    ClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ClusterManagerConfig clusterManagerConfig = spy(new ClusterManagerConfig(conf));
    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            clusterManagerConfig.getHostAffinityEnabled(), false);

    ContainerProcessManager cpm =
        buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.empty());

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        conf,
        state,
        containerManager);

    getPrivateFieldFromCpm("containerAllocator", cpm).set(cpm, allocator);
    CountDownLatch latch = new CountDownLatch(1);
    getPrivateFieldFromCpm("allocatorThread", cpm).set(cpm, new Thread() {
      public void run() {
        isRunning = true;
        latch.countDown();
      }
    });

    cpm.start();

    if (!latch.await(2, TimeUnit.SECONDS)) {
      Assert.fail("timed out waiting for the latch to expire");
    }

    // Verify Allocator thread has started running
    assertTrue(isRunning);

    // Verify the remaining state
    assertEquals(1, state.neededProcessors.get());
    assertEquals(1, allocator.requestedContainers);

    cpm.stop();
  }

  @Test
  public void testOnInitAMHighAvailability() throws Exception {
    Map<String, String> configMap = new HashMap<>(configVals);
    configMap.put(JobConfig.YARN_AM_HIGH_AVAILABILITY_ENABLED, "true");
    Config conf = new MapConfig(configMap);

    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(2));
    state.runningProcessors.put("0", new SamzaResource(1, 1024, "host", "0"));

    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    ClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ClusterManagerConfig clusterManagerConfig = spy(new ClusterManagerConfig(conf));
    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            clusterManagerConfig.getHostAffinityEnabled(), false);

    ContainerProcessManager cpm =
        buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.empty());

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        conf,
        state,
        containerManager);

    getPrivateFieldFromCpm("containerAllocator", cpm).set(cpm, allocator);
    CountDownLatch latch = new CountDownLatch(1);
    getPrivateFieldFromCpm("allocatorThread", cpm).set(cpm, new Thread() {
      public void run() {
        isRunning = true;
        latch.countDown();
      }
    });

    cpm.start();

    if (!latch.await(2, TimeUnit.SECONDS)) {
      Assert.fail("timed out waiting for the latch to expire");
    }

    // Verify Allocator thread has started running
    assertTrue(isRunning);

    // Verify only 1 was requested with allocator
    assertEquals(1, allocator.requestedContainers);
    assertTrue("Ensure no processors were forcefully restarted", callback.resourceStatuses.isEmpty());

    cpm.stop();
  }

  @Test
  public void testOnInitToForceRestartAMHighAvailability() throws Exception {
    Map<String, String> configMap = new HashMap<>(configVals);
    configMap.put(JobConfig.YARN_AM_HIGH_AVAILABILITY_ENABLED, "true");
    Config conf = new MapConfig(configMap);
    SamzaResource samzaResource = new SamzaResource(1, 1024, "host", "0");

    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(2));
    state.runningProcessors.put("0", samzaResource);

    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    ClusterResourceManager clusterResourceManager = spy(new MockClusterResourceManager(callback, state));
    ClusterManagerConfig clusterManagerConfig = spy(new ClusterManagerConfig(conf));
    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            clusterManagerConfig.getHostAffinityEnabled(), false);

    ContainerProcessManager cpm =
        buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.empty(), true);

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        conf,
        state,
        containerManager);

    getPrivateFieldFromCpm("containerAllocator", cpm).set(cpm, allocator);
    CountDownLatch latch = new CountDownLatch(1);
    getPrivateFieldFromCpm("allocatorThread", cpm).set(cpm, new Thread() {
      public void run() {
        isRunning = true;
        latch.countDown();
      }
    });

    cpm.start();

    if (!latch.await(2, TimeUnit.SECONDS)) {
      Assert.fail("timed out waiting for the latch to expire");
    }

    verify(clusterResourceManager, times(1)).stopStreamProcessor(samzaResource);
    assertEquals("CPM should stop the running container", 1, callback.resourceStatuses.size());

    SamzaResourceStatus actualResourceStatus = callback.resourceStatuses.get(0);
    assertEquals("Container 0 should be stopped", "0", actualResourceStatus.getContainerId());
    assertEquals("Container 0 should have exited with preempted status", SamzaResourceStatus.PREEMPTED,
        actualResourceStatus.getExitCode());
    cpm.stop();
  }

  @Test
  public void testOnShutdown() throws Exception {
    Config conf = getConfig();
    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ClusterManagerConfig clusterManagerConfig = spy(new ClusterManagerConfig(conf));

    ContainerProcessManager cpm =
        buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.empty());
    cpm.start();

    Thread allocatorThread = (Thread) getPrivateFieldFromCpm("allocatorThread", cpm).get(cpm);
    assertTrue(allocatorThread.isAlive());

    cpm.stop();

    assertFalse(allocatorThread.isAlive());
  }

  /**
   * Test Container Process Manager should stop when all containers finish
   */
  @Test
  public void testCpmShouldStopWhenContainersFinish() throws Exception {
    Config conf = getConfig();
    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ClusterManagerConfig clusterManagerConfig = spy(new ClusterManagerConfig(conf));
    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            clusterManagerConfig.getHostAffinityEnabled(), false);

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        conf,
        state,
        containerManager);

    ContainerProcessManager cpm =
        spy(buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.of(allocator)));

    // start triggers a request
    cpm.start();

    assertFalse(cpm.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertEquals(0, allocator.getContainerRequestState().numDelayedRequests());

    SamzaResource container = new SamzaResource(1, 1024, "host1", "id0");
    cpm.onResourceAllocated(container);

    // Allow container to run and update state

    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container);
    assertFalse(cpm.shouldShutdown());

    cpm.onResourceCompleted(new SamzaResourceStatus("id0", "diagnostics", SamzaResourceStatus.SUCCESS));
    verify(cpm, never()).onResourceCompletedWithUnknownStatus(any(SamzaResourceStatus.class), anyString(), anyString(), anyInt());
    assertTrue(cpm.shouldShutdown());
    cpm.stop();
  }


  /**
   * Test Container Process Manager should request a new container when a task fails with unknown exit code
   * When host-affinity is not enabled, it will always request for ANY_HOST
   */
  @Test
  public void testNewContainerRequestedOnFailureWithUnknownCode() throws Exception {
    Config conf = getConfig();
    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ClusterManagerConfig clusterManagerConfig = spy(new ClusterManagerConfig(conf));
    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            clusterManagerConfig.getHostAffinityEnabled(), false);

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        conf,
        state,
        containerManager);

    ContainerProcessManager cpm = spy(
        buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.of(allocator)));

    // start triggers a request
    cpm.start();

    verify(clusterManagerConfig, never()).getContainerPreferredHostLastRetryDelayMs();
    verify(cpm, never()).onResourceCompletedWithUnknownStatus(any(), anyString(), anyString(), anyInt());
    assertFalse(cpm.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());


    SamzaResource container = new SamzaResource(1, 1024, "host1", "id0");
    cpm.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container);
    // Create first container failure
    SamzaResourceStatus samzaResourceStatus = new SamzaResourceStatus(container.getContainerId(), "diagnostics", 1);
    cpm.onResourceCompleted(samzaResourceStatus);


    // The above failure should trigger a container request
    verify(cpm).onResourceCompletedWithUnknownStatus(eq(samzaResourceStatus), eq(container.getContainerId()), eq("0"), eq(1));
    verify(clusterManagerConfig, never()).getContainerPreferredHostLastRetryDelayMs();
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());


    assertFalse(cpm.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(2, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());

    cpm.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container);
    assertTrue(state.jobHealthy.get());

    // Create a second failure
    cpm.onResourceCompleted(samzaResourceStatus);

    // The above failure should trigger a job shutdown because our retry count is set to 1
    verify(cpm, times(2)).onResourceCompletedWithUnknownStatus(eq(samzaResourceStatus), eq(container.getContainerId()), eq("0"), eq(1));
    verify(clusterManagerConfig, never()).getContainerPreferredHostLastRetryDelayMs();
    assertEquals(0, allocator.getContainerRequestState().numPendingRequests());
    assertEquals(2, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());
    assertFalse(state.jobHealthy.get());
    assertTrue(cpm.shouldShutdown());
    assertEquals(SamzaApplicationState.SamzaAppStatus.FAILED, state.status);

    cpm.stop();
  }

  /**
   * Test scenario where a container fails multiple times but failures are more than retryWindow apart without host affinity
   * @throws Exception
   */
  @Test
  public void testContainerRequestedRetriesExceedingWindowOnFailureWithUnknownCodeWithNoHostAffinity() throws Exception {
    testContainerRequestedRetriesExceedingWindowOnFailureWithUnknownCode(false, true);
    testContainerRequestedRetriesExceedingWindowOnFailureWithUnknownCode(false, false);
  }

  /**
   * Test scenario where a container fails multiple times but failures are more than retryWindow apart with host affinity
   * @throws Exception
   */
  @Test
  public void testContainerRequestedRetriesExceedingWindowOnFailureWithUnknownCodeWithHostAffinity() throws Exception {
    testContainerRequestedRetriesExceedingWindowOnFailureWithUnknownCode(true, true);
    testContainerRequestedRetriesExceedingWindowOnFailureWithUnknownCode(true, false);
  }

  private void testContainerRequestedRetriesExceedingWindowOnFailureWithUnknownCode(boolean withHostAffinity, boolean failAfterRetries) throws Exception {
    int maxRetries = 3;
    String processorId = "0";
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(getConfigWithHostAffinityAndRetries(withHostAffinity, maxRetries, failAfterRetries));
    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            clusterManagerConfig.getHostAffinityEnabled(), false);

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        clusterManagerConfig,
        state,
        containerManager);

    ContainerProcessManager cpm =
        buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.of(allocator));

    // start triggers a request
    cpm.start();

    assertFalse(cpm.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());

    SamzaResource container = new SamzaResource(1, 1024, "host1", "id0");
    cpm.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container);

    // Mock 2nd failure exceeding retry window.
    int longWindow = clusterManagerConfig.getContainerRetryWindowMs() + 10;
    cpm.getProcessorFailures().put(processorId, new ProcessorFailure(1, Instant.now().minusMillis(longWindow), Duration.ZERO));
    assertEquals(1, cpm.getProcessorFailures().get(processorId).getCount());
    cpm.onResourceCompleted(new SamzaResourceStatus(container.getContainerId(), "diagnostics", 1));
    assertEquals(false, cpm.getJobFailureCriteriaMet());
    assertEquals(1, cpm.getProcessorFailures().get(processorId).getCount());

    cpm.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container);

    // Mock 3rd failure exceeding retry window.
    cpm.getProcessorFailures().put(processorId, new ProcessorFailure(2, Instant.now().minusMillis(longWindow), Duration.ZERO));
    cpm.onResourceCompleted(new SamzaResourceStatus(container.getContainerId(), "diagnostics", 1));
    assertEquals(false, cpm.getJobFailureCriteriaMet());
    assertEquals(1, cpm.getProcessorFailures().get(processorId).getCount());

    cpm.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container);

    // Mock 4th failure exceeding retry window.
    cpm.getProcessorFailures().put(processorId, new ProcessorFailure(3, Instant.now().minusMillis(longWindow), Duration.ZERO));
    cpm.onResourceCompleted(new SamzaResourceStatus(container.getContainerId(), "diagnostics", 1));
    assertEquals(false, cpm.getJobFailureCriteriaMet());
    assertEquals(1, cpm.getProcessorFailures().get(processorId).getCount());

    cpm.stop();
  }

  @Test
  public void testContainerRequestedRetriesNotExceedingWindowOnFailureWithUnknownCodeWithNoHostAffinity() throws Exception {
    testContainerRequestedRetriesNotExceedingWindowOnFailureWithUnknownCode(false, true);
    testContainerRequestedRetriesNotExceedingWindowOnFailureWithUnknownCode(false, false);
  }

  @Test
  public void testContainerRequestedRetriesNotExceedingWindowOnFailureWithUnknownCodeWithHostAffinity() throws Exception {
    testContainerRequestedRetriesNotExceedingWindowOnFailureWithUnknownCode(true, true);
    testContainerRequestedRetriesNotExceedingWindowOnFailureWithUnknownCode(true, false);
  }

  private void testContainerRequestedRetriesNotExceedingWindowOnFailureWithUnknownCode(boolean withHostAffinity, boolean failAfterRetries) throws Exception {
    int maxRetries = 3;
    String processorId = "0";
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(getConfigWithHostAffinityAndRetries(withHostAffinity, maxRetries, failAfterRetries));
    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    LocalityManager mockLocalityManager = mock(LocalityManager.class);

    if (withHostAffinity) {
      when(mockLocalityManager.readLocality())
          .thenReturn(new LocalityModel(ImmutableMap.of("0", new ProcessorLocality("0", "host1"))));
    } else {
      when(mockLocalityManager.readLocality())
          .thenReturn(new LocalityModel(new HashMap<>()));
    }

    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            clusterManagerConfig.getHostAffinityEnabled(), false, mockLocalityManager, faultDomainManager);

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        clusterManagerConfig,
        state,
        containerManager);

    ContainerProcessManager cpm =
        buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.of(allocator),
            mockLocalityManager, false, faultDomainManager);

    // start triggers a request
    cpm.start();

    assertFalse(cpm.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertEquals(0, allocator.getContainerRequestState().numDelayedRequests());

    SamzaResource container = new SamzaResource(1, 1024, "host1", "id0");
    cpm.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container);

    // Mock 2nd failure not exceeding retry window.
    cpm.getProcessorFailures().put(processorId, new ProcessorFailure(1, Instant.now(), Duration.ZERO));
    cpm.onResourceCompleted(new SamzaResourceStatus(container.getContainerId(), "diagnostics", 1));
    assertEquals(false, cpm.getJobFailureCriteriaMet());
    assertEquals(2, cpm.getProcessorFailures().get(processorId).getCount());
    assertFalse(cpm.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertEquals(0, allocator.getContainerRequestState().numDelayedRequests());

    cpm.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container);

    // Mock 3rd failure not exceeding retry window.
    cpm.getProcessorFailures().put(processorId, new ProcessorFailure(2, Instant.now(), Duration.ZERO));
    cpm.onResourceCompleted(new SamzaResourceStatus(container.getContainerId(), "diagnostics", 1));
    assertEquals(false, cpm.getJobFailureCriteriaMet());
    assertEquals(3, cpm.getProcessorFailures().get(processorId).getCount());
    assertFalse(cpm.shouldShutdown());

    if (withHostAffinity) {
      assertEquals(0, allocator.getContainerRequestState().numPendingRequests());
      assertEquals(1, allocator.getContainerRequestState().numDelayedRequests());
    } else {
      assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
      assertEquals(0, allocator.getContainerRequestState().numDelayedRequests());
    }

    cpm.onResourceAllocated(container);

    if (withHostAffinity) {
      if (allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
        // No delayed retry requests for there host affinity is disabled. Call back should return immediately.
        fail("Expecting a delayed request so allocator callback should have timed out waiting for a response.");
      }

      // For the sake of testing the mocked 4th failure below, send delayed requests now.
      SamzaResourceRequest request = allocator.getContainerRequestState().getDelayedRequestsQueue().poll();
      SamzaResourceRequest fastForwardRequest =
          new SamzaResourceRequest(request.getNumCores(), request.getMemoryMB(), request.getPreferredHost(), request.getProcessorId(), Instant.now().minusSeconds(1));
      allocator.getContainerRequestState().getDelayedRequestsQueue().add(fastForwardRequest);
      int numSent = allocator.getContainerRequestState().sendPendingDelayedResourceRequests();
      assertEquals(1, numSent);
      cpm.onResourceAllocated(container);
    }

    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      // No delayed retry requests for there host affinity is disabled. Call back should return immediately.
      fail("Timed out waiting for the containers to start");
    }

    cpm.onStreamProcessorLaunchSuccess(container);

    // Mock 4th failure not exceeding retry window.
    cpm.getProcessorFailures().put(processorId, new ProcessorFailure(3, Instant.now(), Duration.ZERO));
    cpm.onResourceCompleted(new SamzaResourceStatus(container.getContainerId(), "diagnostics", 1));
    assertEquals(failAfterRetries, cpm.getJobFailureCriteriaMet()); // expecting failed container
    assertEquals(3, cpm.getProcessorFailures().get(processorId).getCount()); // count won't update on failure
    if (failAfterRetries) {
      assertTrue(cpm.shouldShutdown());
    } else {
      assertFalse(cpm.shouldShutdown());
    }
    assertEquals(0, allocator.getContainerRequestState().numPendingRequests());
    assertEquals(0, allocator.getContainerRequestState().numDelayedRequests());

    cpm.stop();
  }

  @Test
  public void testInvalidNotificationsAreIgnored() throws Exception {
    Config conf = getConfig();

    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ClusterManagerConfig clusterManagerConfig = spy(new ClusterManagerConfig(conf));
    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            clusterManagerConfig.getHostAffinityEnabled(), false);

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        conf,
        state,
        containerManager);

    ContainerProcessManager cpm =
        spy(buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.of(allocator)));

    // Start the task clusterResourceManager
    cpm.start();

    SamzaResource container = new SamzaResource(1, 1000, "host1", "id1");
    cpm.onResourceAllocated(container);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    // Create container failure - with ContainerExitStatus.DISKS_FAILED
    cpm.onResourceCompleted(new SamzaResourceStatus("invalidContainerID", "Disk failure", SamzaResourceStatus.DISK_FAIL));
    verify(cpm, never()).onResourceCompletedWithUnknownStatus(any(SamzaResourceStatus.class), anyString(), anyString(), anyInt());

    // The above failure should not trigger any container requests, since it is for an invalid container ID
    assertEquals(0, allocator.getContainerRequestState().numPendingRequests());
    assertFalse(cpm.shouldShutdown());
    assertTrue(state.jobHealthy.get());
    assertEquals(state.redundantNotifications.get(), 1);
    cpm.stop();
  }

  @Test
  public void testRerequestOnAnyHostIfContainerStartFails() throws Exception {
    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    Map<String, String> configMap = new HashMap<>();
    configMap.putAll(getConfig());
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    when(mockLocalityManager.readLocality())
        .thenReturn(new LocalityModel(ImmutableMap.of("0", new ProcessorLocality("1", "host1"))));
    ContainerManager containerManager = buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            Boolean.valueOf(config.get(ClusterManagerConfig.HOST_AFFINITY_ENABLED)), false, mockLocalityManager, faultDomainManager);

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        new MapConfig(config),
        state,
        containerManager);

    ContainerProcessManager manager =
        new ContainerProcessManager(new ClusterManagerConfig(config), state, new MetricsRegistryMap(), clusterResourceManager,
            Optional.of(allocator), containerManager, mockLocalityManager, false);

    manager.start();
    SamzaResource resource = new SamzaResource(1, 1024, "host1", "resource-1");
    state.pendingProcessors.put("1", resource);
    Assert.assertEquals(clusterResourceManager.resourceRequests.size(), 1);
    manager.onStreamProcessorLaunchFailure(resource, new Exception("cannot launch container!"));
    Assert.assertEquals(clusterResourceManager.resourceRequests.size(), 2);
    Assert.assertEquals(clusterResourceManager.resourceRequests.get(1).getHost(), ResourceRequestState.ANY_HOST);
    manager.stop();
  }

  @Test
  public void testAllBufferedResourcesAreUtilized() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.putAll(getConfigWithHostAffinity());
    config.put("job.container.count", "2");
    config.put("cluster-manager.container.retry.count", "2");
    config.put("cluster-manager.container.request.timeout.ms", "10000");
    Config cfg = new MapConfig(config);
    // 1. Request two containers on hosts - host1 and host2
    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(2));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    when(mockLocalityManager.readLocality())
        .thenReturn(new LocalityModel(ImmutableMap.of("0", new ProcessorLocality("0", "host1"), "1", new ProcessorLocality("1", "host2"))));
    ContainerManager containerManager = buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            Boolean.parseBoolean(config.get(ClusterManagerConfig.HOST_AFFINITY_ENABLED)), false, mockLocalityManager, faultDomainManager);

    MockContainerAllocatorWithHostAffinity allocator = new MockContainerAllocatorWithHostAffinity(
        clusterResourceManager,
        cfg,
        state,
        containerManager);

    ContainerProcessManager cpm =
        spy(buildContainerProcessManager(new ClusterManagerConfig(cfg), state, clusterResourceManager,
            Optional.of(allocator), mockLocalityManager, false, faultDomainManager));

    cpm.start();
    assertFalse(cpm.shouldShutdown());
    // 2. When the task manager starts, there should have been a pending request on host1 and host2
    assertEquals(2, allocator.getContainerRequestState().numPendingRequests());

    // 3. Allocate an extra resource on host1 and no resource on host2 yet.
    SamzaResource resource1 = new SamzaResource(1, 1000, "host1", "id1");
    SamzaResource resource2 = new SamzaResource(1, 1000, "host1", "id2");
    cpm.onResourceAllocated(resource1);
    cpm.onResourceAllocated(resource2);

    // 4. Wait for the container to start on host1 and immediately fail
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(resource1);
    assertEquals("host2", allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());

    cpm.onResourceCompleted(new SamzaResourceStatus(resource1.getContainerId(), "App Error", 1));
    verify(cpm).onResourceCompletedWithUnknownStatus(any(SamzaResourceStatus.class), anyString(), anyString(), anyInt());
    assertEquals(2, allocator.getContainerRequestState().numPendingRequests());

    assertFalse(cpm.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(3, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());

    // 5. Do not allocate any further resource on host1, and verify that the re-run of the container on host1 uses the
    // previously allocated extra resource
    SamzaResource resource3 = new SamzaResource(1, 1000, "host2", "id3");
    cpm.onResourceAllocated(resource3);

    if (!allocator.awaitContainersStart(2, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(resource2);
    cpm.onStreamProcessorLaunchSuccess(resource3);

    assertTrue(state.jobHealthy.get());
    cpm.stop();
  }

  @Test
  public void testDuplicateNotificationsDoNotAffectJobHealth() throws Exception {
    Config conf = getConfig();

    Map<String, String> config = new HashMap<>();
    config.putAll(getConfig());
    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ClusterManagerConfig clusterManagerConfig = spy(new ClusterManagerConfig(new MapConfig(conf)));
    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            clusterManagerConfig.getHostAffinityEnabled(), false);

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        conf,
        state,
        containerManager);

    ContainerProcessManager cpm =
        spy(buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.of(allocator)));

    // Start the task manager
    cpm.start();
    assertFalse(cpm.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());

    SamzaResource container1 = new SamzaResource(1, 1000, "host1", "id1");
    cpm.onResourceAllocated(container1);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container1);
    assertEquals(0, allocator.getContainerRequestState().numPendingRequests());

    // Create container failure - with ContainerExitStatus.DISKS_FAILED
    cpm.onResourceCompleted(new SamzaResourceStatus(container1.getContainerId(), "Disk failure", SamzaResourceStatus.DISK_FAIL));
    verify(cpm, never()).onResourceCompletedWithUnknownStatus(any(SamzaResourceStatus.class), anyString(), anyString(), anyInt());

    // The above failure should trigger a container request
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertFalse(cpm.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(2, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());

    SamzaResource container2 = new SamzaResource(1, 1000, "host1", "id2");
    cpm.onResourceAllocated(container2);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container2);

    assertTrue(state.jobHealthy.get());

    // Simulate a duplicate notification for container 1 with a different exit code
    cpm.onResourceCompleted(new SamzaResourceStatus(container1.getContainerId(), "Disk failure", SamzaResourceStatus.PREEMPTED));
    verify(cpm, never()).onResourceCompletedWithUnknownStatus(any(SamzaResourceStatus.class), anyString(), anyString(), anyInt());
    // assert that a duplicate notification does not change metrics (including job health)
    assertEquals(state.redundantNotifications.get(), 1);
    assertEquals(2, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());
    assertTrue(state.jobHealthy.get());
    cpm.stop();
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
    SamzaApplicationState state = new SamzaApplicationState(getJobModelManager(1));
    MockClusterResourceManagerCallback callback = new MockClusterResourceManagerCallback();
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ClusterManagerConfig clusterManagerConfig = spy(new ClusterManagerConfig(new MapConfig(config)));
    ContainerManager containerManager =
        buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
            clusterManagerConfig.getHostAffinityEnabled(), false);

    MockContainerAllocatorWithoutHostAffinity allocator = new MockContainerAllocatorWithoutHostAffinity(
        clusterResourceManager,
        conf,
        state,
        containerManager);

    ContainerProcessManager cpm =
        spy(buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, Optional.of(allocator)));

    // Start the task clusterResourceManager
    cpm.start();
    assertFalse(cpm.shouldShutdown());
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());

    SamzaResource container1 = new SamzaResource(1, 1000, "host1", "id1");
    cpm.onResourceAllocated(container1);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    assertEquals(0, allocator.getContainerRequestState().numPendingRequests());
    cpm.onStreamProcessorLaunchSuccess(container1);
    // Create container failure - with ContainerExitStatus.DISKS_FAILED
    SamzaResourceStatus resourceStatusOnAppError = new SamzaResourceStatus(container1.getContainerId(), "App error", 1);
    cpm.onResourceCompleted(resourceStatusOnAppError);
    verify(cpm).onResourceCompletedWithUnknownStatus(eq(resourceStatusOnAppError), anyString(), anyString(), anyInt());

    // The above failure should trigger a container request
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertFalse(cpm.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(2, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());

    SamzaResource container2 = new SamzaResource(1, 1000, "host1", "id2");
    cpm.onResourceAllocated(container2);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container2);

    // Create container failure - with ContainerExitStatus.PREEMPTED
    SamzaResourceStatus resourceStatusOnPreemption =
        new SamzaResourceStatus(container2.getContainerId(), "Preemption", SamzaResourceStatus.PREEMPTED);
    cpm.onResourceCompleted(resourceStatusOnPreemption);
    verify(cpm, never()).onResourceCompletedWithUnknownStatus(eq(resourceStatusOnPreemption), anyString(), anyString(), anyInt());
    assertEquals(3, clusterResourceManager.resourceRequests.size());

    // The above failure should trigger a container request
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertFalse(cpm.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());
    SamzaResource container3 = new SamzaResource(1, 1000, "host1", "id3");
    cpm.onResourceAllocated(container3);

    // Allow container to run and update state
    if (!allocator.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    cpm.onStreamProcessorLaunchSuccess(container3);

    // Create container failure - with ContainerExitStatus.ABORTED
    SamzaResourceStatus resourceStatusOnAborted =
        new SamzaResourceStatus(container3.getContainerId(), "Aborted", SamzaResourceStatus.ABORTED);
    cpm.onResourceCompleted(resourceStatusOnAborted);
    verify(cpm, never()).onResourceCompletedWithUnknownStatus(eq(resourceStatusOnAborted), anyString(), anyString(), anyInt());

    // The above failure should trigger a container request
    assertEquals(1, allocator.getContainerRequestState().numPendingRequests());
    assertEquals(4, clusterResourceManager.resourceRequests.size());
    assertEquals(0, clusterResourceManager.releasedResources.size());
    assertFalse(cpm.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState().peekPendingRequest().getPreferredHost());

    cpm.stop();
  }

  @After
  public void teardown() {
    server.stop();
  }

  private ContainerManager buildContainerManager(ContainerPlacementMetadataStore containerPlacementMetadataStore,
      SamzaApplicationState samzaApplicationState, ClusterResourceManager clusterResourceManager,
      boolean hostAffinityEnabled, boolean standByEnabled) {
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    when(mockLocalityManager.readLocality()).thenReturn(new LocalityModel(new HashMap<>()));
    return buildContainerManager(containerPlacementMetadataStore, samzaApplicationState, clusterResourceManager,
        hostAffinityEnabled, standByEnabled, mockLocalityManager, faultDomainManager);
  }

  private ContainerManager buildContainerManager(ContainerPlacementMetadataStore containerPlacementMetadataStore,
      SamzaApplicationState samzaApplicationState, ClusterResourceManager clusterResourceManager, boolean hostAffinityEnabled,
      boolean standByEnabled, LocalityManager localityManager, FaultDomainManager faultDomainManager) {
    return new ContainerManager(containerPlacementMetadataStore, samzaApplicationState, clusterResourceManager, hostAffinityEnabled, standByEnabled, localityManager, faultDomainManager, config);
  }
  private ContainerProcessManager buildContainerProcessManager(ClusterManagerConfig clusterManagerConfig, SamzaApplicationState state,
      ClusterResourceManager clusterResourceManager, Optional<ContainerAllocator> allocator) {
    return buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, allocator, false);
  }

  private ContainerProcessManager buildContainerProcessManager(ClusterManagerConfig clusterManagerConfig, SamzaApplicationState state,
      ClusterResourceManager clusterResourceManager, Optional<ContainerAllocator> allocator, boolean restartContainer) {
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    when(mockLocalityManager.readLocality()).thenReturn(new LocalityModel(new HashMap<>()));
    return buildContainerProcessManager(clusterManagerConfig, state, clusterResourceManager, allocator,
        mockLocalityManager, restartContainer, faultDomainManager);
  }

  private ContainerProcessManager buildContainerProcessManager(ClusterManagerConfig clusterManagerConfig, SamzaApplicationState state,
      ClusterResourceManager clusterResourceManager, Optional<ContainerAllocator> allocator, LocalityManager localityManager,
      boolean restartContainers, FaultDomainManager faultDomainManager) {
    return new ContainerProcessManager(clusterManagerConfig, state, new MetricsRegistryMap(), clusterResourceManager,
        allocator, buildContainerManager(containerPlacementMetadataStore, state, clusterResourceManager,
        clusterManagerConfig.getHostAffinityEnabled(), false, localityManager, faultDomainManager), localityManager, restartContainers);
  }
}
