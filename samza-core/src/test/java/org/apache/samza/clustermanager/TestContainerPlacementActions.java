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
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementMetadataStore;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementRequestAllocator;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementMetadata;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.placement.ContainerPlacementMessage;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.apache.samza.container.placement.ContainerPlacementResponseMessage;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Set of Integration tests for container placement actions
 *
 * Please note that semaphores are used wherever possible, there are some Thread.sleep used for the main thread to check
 * on state changes to atomic variables or synchroized metadata objects because of difficulty of plugging semaphores to
 * those pieces of logic
 */
@RunWith(MockitoJUnitRunner.class)
public class TestContainerPlacementActions {

  private HttpServer server = null;

  private Map<String, String> configVals = new HashMap<String, String>() {
    {
      put("cluster-manager.container.count", "1");
      put("cluster-manager.container.retry.count", "1");
      put("cluster-manager.container.retry.window.ms", "1999999999");
      put("cluster-manager.allocator.sleep.ms", "10");
      put("cluster-manager.container.request.timeout.ms", "2000");
      put("cluster-manager.container.memory.mb", "512");
      put("yarn.package.path", "/foo");
      put("task.inputs", "test-system.test-stream");
      put("systems.test-system.samza.factory", "org.apache.samza.system.MockSystemFactory");
      put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
      put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
      put("job.name", "test-job");
      put("job.coordinator.system", "test-kafka");
      put("app.run.id", "appAttempt-001");
      put("job.standbytasks.replication.factor", "2");
    }
  };

  private Config config = new MapConfig(configVals);

  private CoordinatorStreamStore coordinatorStreamStore;
  private ContainerPlacementMetadataStore containerPlacementMetadataStore;

  volatile private SamzaApplicationState state;
  private ContainerManager containerManager;
  private MockContainerAllocatorWithHostAffinity allocatorWithHostAffinity;
  private ContainerProcessManager cpm;
  private LocalityManager localityManager;
  private ClusterResourceManager.Callback callback;

  private Config getConfig() {
    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    return new MapConfig(map);
  }

  private Config getConfigWithHostAffinityAndRetries(boolean withHostAffinity, int maxRetries,
      boolean failAfterRetries) {
    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    map.put("job.host-affinity.enabled", String.valueOf(withHostAffinity));
    map.put(ClusterManagerConfig.CLUSTER_MANAGER_CONTAINER_RETRY_COUNT, String.valueOf(maxRetries));
    map.put(ClusterManagerConfig.CLUSTER_MANAGER_CONTAINER_FAIL_JOB_AFTER_RETRIES, String.valueOf(failAfterRetries));
    map.put(ClusterManagerConfig.CLUSTER_MANAGER_CONTAINER_PREFERRED_HOST_LAST_RETRY_DELAY_MS, "100");
    return new MapConfig(map);
  }

  private JobModelManager getJobModelManagerWithStandby() {
    return new JobModelManager(TestStandbyAllocator.getJobModelWithStandby(2, 2, 2), server);
  }

  @Before
  public void setup() throws Exception {
    server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));
    // Utils Related to Container Placement Metadata store
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(config);
    coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    coordinatorStreamStore.init();
    containerPlacementMetadataStore = new ContainerPlacementMetadataStore(coordinatorStreamStore);
    containerPlacementMetadataStore.start();
    // Utils Related to Cluster manager:
    config = new MapConfig(configVals, getConfigWithHostAffinityAndRetries(true, 1, true));
    state = new SamzaApplicationState(JobModelManagerTestUtil.getJobModelManager(getConfig(), 2, server));
    callback = mock(ClusterResourceManager.Callback.class);
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    localityManager = mock(LocalityManager.class);
    when(localityManager.readLocality())
        .thenReturn(new LocalityModel(ImmutableMap.of(
            "0", new ProcessorLocality("0", "host-1"),
            "1", new ProcessorLocality("1", "host-2"))));
    containerManager = spy(new ContainerManager(containerPlacementMetadataStore, state, clusterResourceManager, true, false, localityManager, faultDomainManager, config));
    allocatorWithHostAffinity = new MockContainerAllocatorWithHostAffinity(clusterResourceManager, config, state, containerManager);
    cpm = new ContainerProcessManager(clusterManagerConfig, state, new MetricsRegistryMap(),
            clusterResourceManager, Optional.of(allocatorWithHostAffinity), containerManager, localityManager, false);
  }

  @After
  public void teardown() {
    containerPlacementMetadataStore.stop();
    cpm.stop();
    coordinatorStreamStore.close();
  }

  public void setupStandby() throws Exception {
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    when(mockLocalityManager.readLocality())
        .thenReturn(new LocalityModel(ImmutableMap.of(
            "0", new ProcessorLocality("0", "host-1"),
            "1", new ProcessorLocality("1", "host-2"),
            "0-0", new ProcessorLocality("0", "host-2"),
            "1-0", new ProcessorLocality("0", "host-1"))));
    state = new SamzaApplicationState(getJobModelManagerWithStandby());
    callback = mock(ClusterResourceManager.Callback.class);
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    // Enable standby
    containerManager = spy(new ContainerManager(containerPlacementMetadataStore, state, clusterResourceManager, true, true, mockLocalityManager, faultDomainManager, config));
    allocatorWithHostAffinity = new MockContainerAllocatorWithHostAffinity(clusterResourceManager, config, state, containerManager);
    cpm = new ContainerProcessManager(clusterManagerConfig, state, new MetricsRegistryMap(),
        clusterResourceManager, Optional.of(allocatorWithHostAffinity), containerManager, mockLocalityManager, false);
  }

  @Test(timeout = 10000)
  public void testContainerSuccessfulMoveActionWithoutStandby() throws Exception {
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onResourcesAvailable((List<SamzaResource>) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesAvailable(anyList());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onStreamProcessorLaunchSuccess((SamzaResource) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onStreamProcessorLaunchSuccess(any());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onResourcesCompleted((List<SamzaResourceStatus>) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesCompleted(anyList());

    cpm.start();

    if (!allocatorWithHostAffinity.awaitContainersStart(2, 5, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    while (state.runningProcessors.size() != 2) {
      Thread.sleep(100);
    }

    // App is in running state with two containers running
    assertEquals(state.runningProcessors.size(), 2);
    assertEquals(state.runningProcessors.get("0").getHost(), "host-1");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.preferredHostRequests.get(), 2);
    assertEquals(state.anyHostRequests.get(), 0);

    // Initiate container placement action to move a container with container id 0
    ContainerPlacementRequestMessage requestMessage =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "appAttempt-001", "0", "host-3",
            System.currentTimeMillis());

    ContainerPlacementMetadata metadata =
        containerManager.registerContainerPlacementActionForTest(requestMessage, allocatorWithHostAffinity);

    // Wait for the ControlAction to complete
    if (!allocatorWithHostAffinity.awaitContainersStart(1, 3, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    Optional<ContainerPlacementResponseMessage> responseMessage =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage.getUuid());

    // Wait for the placement action to be complete & get written to the underlying metastore
    while (true) {
      if (metadata.getActionStatus() == ContainerPlacementMessage.StatusCode.SUCCEEDED && responseMessage.isPresent()
          && responseMessage.get().getStatusCode() == ContainerPlacementMessage.StatusCode.SUCCEEDED) {
        break;
      }
      Thread.sleep(100);
      responseMessage = containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage.getUuid());
    }

    assertEquals(state.preferredHostRequests.get(), 3);
    assertEquals(state.runningProcessors.size(), 2);
    assertEquals(state.runningProcessors.get("0").getHost(), "host-3");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.anyHostRequests.get(), 0);
    assertEquals(metadata.getActionStatus(), ContainerPlacementMessage.StatusCode.SUCCEEDED);

    assertTrue(responseMessage.isPresent());
    assertEquals(responseMessage.get().getStatusCode(), ContainerPlacementMessage.StatusCode.SUCCEEDED);
    assertResponseMessage(responseMessage.get(), requestMessage);
  }

  @Test(timeout = 30000)
  public void testActionQueuingForConsecutivePlacementActions() throws Exception {
    // Spawn a Request Allocator Thread
    ContainerPlacementRequestAllocator requestAllocator =
        new ContainerPlacementRequestAllocator(containerPlacementMetadataStore, cpm, new ApplicationConfig(config), 100);
    Thread requestAllocatorThread = new Thread(requestAllocator, "ContainerPlacement Request Allocator Thread");

    requestAllocatorThread.start();

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onResourcesAvailable((List<SamzaResource>) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesAvailable(anyList());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onStreamProcessorLaunchSuccess((SamzaResource) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onStreamProcessorLaunchSuccess(any());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onResourcesCompleted((List<SamzaResourceStatus>) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesCompleted(anyList());

    cpm.start();

    if (!allocatorWithHostAffinity.awaitContainersStart(2, 5, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    while (state.runningProcessors.size() != 2) {
      Thread.sleep(100);
    }

    // App is in running state with two containers running
    assertEquals(state.runningProcessors.size(), 2);
    assertEquals(state.runningProcessors.get("0").getHost(), "host-1");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.preferredHostRequests.get(), 2);
    assertEquals(state.anyHostRequests.get(), 0);

    // Initiate container placement action to move a container with container id 0

    UUID requestUUIDMove1 = containerPlacementMetadataStore.writeContainerPlacementRequestMessage("appAttempt-001", "0", "host-3",
        null, System.currentTimeMillis());

    UUID requestUUIDMoveBad = containerPlacementMetadataStore.writeContainerPlacementRequestMessage("appAttempt-002", "0", "host-4",
        null, System.currentTimeMillis());

    UUID requestUUIDMove2 = containerPlacementMetadataStore.writeContainerPlacementRequestMessage("appAttempt-001", "0", "host-4",
        null, System.currentTimeMillis());

    // Wait for the ControlAction to complete
    while (true) {
      if (containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestUUIDMove2).isPresent() &&
          containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestUUIDMove2).get().getStatusCode()
              == ContainerPlacementMessage.StatusCode.SUCCEEDED) {
        break;
      }
      Thread.sleep(100);
    }

    assertEquals(state.preferredHostRequests.get(), 4);
    assertEquals(state.runningProcessors.size(), 2);
    assertEquals(state.runningProcessors.get("0").getHost(), "host-4");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.anyHostRequests.get(), 0);

    Optional<ContainerPlacementResponseMessage> responseMessageMove1 =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestUUIDMove1);

    Optional<ContainerPlacementResponseMessage> responseMessageMove2 =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestUUIDMove2);

    assertTrue(responseMessageMove1.isPresent());
    assertEquals(responseMessageMove1.get().getStatusCode(), ContainerPlacementMessage.StatusCode.SUCCEEDED);

    assertTrue(responseMessageMove2.isPresent());
    assertEquals(responseMessageMove2.get().getStatusCode(), ContainerPlacementMessage.StatusCode.SUCCEEDED);

    // Request should be deleted as soon as ita accepted / being acted upon
    assertFalse(containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestUUIDMove1).isPresent());
    assertFalse(containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestUUIDMove2).isPresent());

    // Requests from Previous deploy must be cleaned
    assertFalse(containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestUUIDMoveBad).isPresent());
    assertFalse(containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestUUIDMoveBad).isPresent());

    // Cleanup Request Allocator Thread
    cleanUpRequestAllocatorThread(requestAllocator, requestAllocatorThread);
  }

  @Test(timeout = 10000)
  public void testContainerMoveActionExpiredRequestNotAffectRunningContainers() throws Exception {

    // Mimic the behavior of Expired request
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          List<SamzaResource> resources = (List<SamzaResource>) args[0];
          if (resources.get(0).getHost().equals("host-1") || resources.get(0).getHost().equals("host-2")) {
            cpm.onResourcesAvailable((List<SamzaResource>) args[0]);
          }
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesAvailable(anyList());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onStreamProcessorLaunchSuccess((SamzaResource) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onStreamProcessorLaunchSuccess(any());

    cpm.start();

    if (!allocatorWithHostAffinity.awaitContainersStart(2, 3, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    while (state.runningProcessors.size() != 2) {
      Thread.sleep(100);
    }

    // App is in running state with two containers running
    assertEquals(state.runningProcessors.size(), 2);
    assertEquals(state.runningProcessors.get("0").getHost(), "host-1");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.preferredHostRequests.get(), 2);
    assertEquals(state.anyHostRequests.get(), 0);

    // Initiate container placement action to move a container with container id 0
    ContainerPlacementRequestMessage requestMessage =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "appAttempt-001", "0", "host-3", Duration.ofMillis(10),
            System.currentTimeMillis());
    ContainerPlacementMetadata metadata =
        containerManager.registerContainerPlacementActionForTest(requestMessage, allocatorWithHostAffinity);


    Optional<ContainerPlacementResponseMessage> responseMessage =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage.getUuid());

    // Wait for the placement action to be complete & get written to the underlying metastore
    while (true) {
      if (metadata.getActionStatus() == ContainerPlacementMessage.StatusCode.FAILED
          && responseMessage.isPresent()
          && responseMessage.get().getStatusCode() == ContainerPlacementMessage.StatusCode.FAILED) {
        break;
      }
      Thread.sleep(100);
      responseMessage = containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage.getUuid());
    }

    assertEquals(state.preferredHostRequests.get(), 3);
    assertEquals(state.runningProcessors.size(), 2);
    // Container should not be stooped
    assertEquals(state.runningProcessors.get("0").getHost(), "host-1");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.anyHostRequests.get(), 0);

    assertTrue(responseMessage.isPresent());
    assertEquals(responseMessage.get().getStatusCode(), ContainerPlacementMessage.StatusCode.FAILED);
    assertResponseMessage(responseMessage.get(), requestMessage);
    // Request shall be deleted as soon as it is acted upon
    assertFalse(containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestMessage.getUuid()).isPresent());
  }

  @Test(timeout = 10000)
  public void testActiveContainerLaunchFailureOnControlActionShouldFallbackToSourceHost() throws Exception {
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onResourcesAvailable((List<SamzaResource>) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesAvailable(anyList());

    // Mimic stream processor launch failure only on host-3
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          SamzaResource host3Resource = (SamzaResource) args[0];
          if (host3Resource.getHost().equals("host-3")) {
            cpm.onStreamProcessorLaunchFailure(host3Resource, new Throwable("Custom Exception for Host-3"));
          } else {
            cpm.onStreamProcessorLaunchSuccess((SamzaResource) args[0]);
          }
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onStreamProcessorLaunchSuccess(any());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onResourcesCompleted((List<SamzaResourceStatus>) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesCompleted(anyList());

    cpm.start();

    if (!allocatorWithHostAffinity.awaitContainersStart(2, 5, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    while (state.runningProcessors.size() != 2) {
      Thread.sleep(100);
    }

    // App is in running state with two containers running
    assertEquals(state.runningProcessors.size(), 2);
    assertEquals(state.runningProcessors.get("0").getHost(), "host-1");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.preferredHostRequests.get(), 2);
    assertEquals(state.anyHostRequests.get(), 0);

    // Take a container placement action to move a container with container id 0
    ContainerPlacementRequestMessage requestMessage =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "0", "host-3",
            System.currentTimeMillis());
    ContainerPlacementMetadata metadata =
        containerManager.registerContainerPlacementActionForTest(requestMessage, allocatorWithHostAffinity);

    // Wait for the ControlAction to complete
    if (!allocatorWithHostAffinity.awaitContainersStart(1, 3, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    while (state.runningProcessors.size() != 2) {
      Thread.sleep(100);
    }

    assertEquals(state.preferredHostRequests.get(), 4);
    assertEquals(state.runningProcessors.size(), 2);
    // Container 0 should fallback to source host
    assertEquals(state.runningProcessors.get("0").getHost(), "host-1");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.anyHostRequests.get(), 0);
    // Control Action should be failed in this case
    assertEquals(metadata.getActionStatus(), ContainerPlacementMessage.StatusCode.FAILED);

    Optional<ContainerPlacementResponseMessage> responseMessage =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage.getUuid());

    assertTrue(responseMessage.isPresent());
    assertEquals(responseMessage.get().getStatusCode(), ContainerPlacementMessage.StatusCode.FAILED);
    assertResponseMessage(responseMessage.get(), requestMessage);

    // Request shall be deleted as soon as it is acted upon
    assertFalse(containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestMessage.getUuid()).isPresent());
  }


  @Test(timeout = 20000)
  public void testContainerPlacementsForJobRunningInDegradedState() throws Exception {
    // Set failure after retries to false to enable job running in degraded state
    config = new MapConfig(configVals, getConfigWithHostAffinityAndRetries(true, 1, false));
    state = new SamzaApplicationState(JobModelManagerTestUtil.getJobModelManager(getConfig(), 2, this.server));
    callback = mock(ClusterResourceManager.Callback.class);
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    containerManager = spy(new ContainerManager(containerPlacementMetadataStore, state, clusterResourceManager, true, false, localityManager, faultDomainManager, config));
    allocatorWithHostAffinity = new MockContainerAllocatorWithHostAffinity(clusterResourceManager, config, state, containerManager);
    cpm = new ContainerProcessManager(clusterManagerConfig, state, new MetricsRegistryMap(),
        clusterResourceManager, Optional.of(allocatorWithHostAffinity), containerManager, localityManager, false);

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
          new Thread(() -> {
            Object[] args = invocation.getArguments();
            cpm.onResourcesAvailable((List<SamzaResource>) args[0]);
          }, "AMRMClientAsync").start();
          return null;
        }
      }).when(callback).onResourcesAvailable(anyList());

    // Mimic stream processor launch failure only on host-2,
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
          new Thread(() -> {
            Object[] args = invocation.getArguments();
            cpm.onStreamProcessorLaunchSuccess((SamzaResource) args[0]);
          }, "AMRMClientAsync").start();
          return null;
        }
      }).when(callback).onStreamProcessorLaunchSuccess(any());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
          new Thread(() -> {
            Object[] args = invocation.getArguments();
            cpm.onResourcesCompleted((List<SamzaResourceStatus>) args[0]);
          }, "AMRMClientAsync").start();
          return null;
        }
      }).when(callback).onResourcesCompleted(anyList());

    cpm.start();


    if (!allocatorWithHostAffinity.awaitContainersStart(2, 5, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    while (state.runningProcessors.size() != 2) {
      Thread.sleep(100);
    }

    // App is in running state with two containers running
    assertEquals(state.runningProcessors.size(), 2);
    assertEquals(state.runningProcessors.get("0").getHost(), "host-1");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.preferredHostRequests.get(), 2);
    assertEquals(state.anyHostRequests.get(), 0);


    // Trigger a container failure
    clusterResourceManager.stopStreamProcessor(state.runningProcessors.get("1"), -103);
    // Wait for container to start
    if (!allocatorWithHostAffinity.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }
    while (state.runningProcessors.size() != 2) {
      Thread.sleep(100);
    }
    // Trigger a container failure again
    clusterResourceManager.stopStreamProcessor(state.runningProcessors.get("1"), -103);
    // Ensure that this container has exhausted all retires
    while (state.failedProcessors.size() != 1 && state.runningProcessors.size() != 1) {
      Thread.sleep(100);
    }

    // At this point the application should only have one container running
    assertEquals(state.runningProcessors.get("0").getHost(), "host-1");
    assertEquals(state.runningProcessors.size(), 1);
    assertEquals(state.pendingProcessors.size(), 0);
    assertTrue(state.failedProcessors.containsKey("1"));

    ContainerPlacementRequestMessage requestMessage =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "1", "host-3",
            System.currentTimeMillis());

    ContainerPlacementMetadata metadata =
        containerManager.registerContainerPlacementActionForTest(requestMessage, allocatorWithHostAffinity);

    // Wait for the ControlAction to complete
    if (!allocatorWithHostAffinity.awaitContainersStart(1, 2, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    // Wait for both the containers to be in running state & control action metadata to succeed
    while (state.runningProcessors.size() != 2
        && metadata.getActionStatus() != ContainerPlacementMessage.StatusCode.SUCCEEDED) {
      Thread.sleep(100);
    }

    assertEquals(state.preferredHostRequests.get(), 4);
    assertEquals(state.runningProcessors.size(), 2);
    // Container 1 should not go to host-3
    assertEquals(state.runningProcessors.get("0").getHost(), "host-1");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-3");
    assertEquals(state.anyHostRequests.get(), 0);
    // Failed processors must be empty
    assertEquals(state.failedProcessors.size(), 0);
  }

  @Test(timeout = 10000)
  public void testAlwaysMoveToAnyHostForHostAffinityDisabled() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.putAll(getConfigWithHostAffinityAndRetries(false, 1, true));
    SamzaApplicationState state =
        new SamzaApplicationState(JobModelManagerTestUtil.getJobModelManager(getConfig(), 2, this.server));
    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    ContainerManager containerManager =
        new ContainerManager(containerPlacementMetadataStore, state, clusterResourceManager, false, false, localityManager, faultDomainManager, config);
    MockContainerAllocatorWithoutHostAffinity allocatorWithoutHostAffinity =
        new MockContainerAllocatorWithoutHostAffinity(clusterResourceManager, new MapConfig(conf), state,
            containerManager);

    ContainerProcessManager cpm = new ContainerProcessManager(
        new ClusterManagerConfig(new MapConfig(getConfig(), getConfigWithHostAffinityAndRetries(false, 1, true))), state,
        new MetricsRegistryMap(), clusterResourceManager, Optional.of(allocatorWithoutHostAffinity), containerManager, localityManager, false);

    // Mimic Cluster Manager returning any request
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          List<SamzaResource> resources = (List<SamzaResource>) args[0];
          SamzaResource preferredResource = resources.get(0);
          SamzaResource anyResource =
              new SamzaResource(preferredResource.getNumCores(), preferredResource.getMemoryMb(),
                  "host-" + RandomStringUtils.randomAlphanumeric(5), preferredResource.getContainerId());
          cpm.onResourcesAvailable(ImmutableList.of(anyResource));
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesAvailable(anyList());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
          new Thread(() -> {
            Object[] args = invocation.getArguments();
            cpm.onStreamProcessorLaunchSuccess((SamzaResource) args[0]);
          }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onStreamProcessorLaunchSuccess(any());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
          new Thread(() -> {
            Object[] args = invocation.getArguments();
            cpm.onResourcesCompleted((List<SamzaResourceStatus>) args[0]);
          }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesCompleted(anyList());

    cpm.start();

    // This spawns async start request and waits for async requests to complete
    if (!allocatorWithoutHostAffinity.awaitContainersStart(2, 3, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    while (state.runningProcessors.size() != 2) {
      Thread.sleep(100);
    }

    // App is in running state with two containers running
    assertEquals(state.runningProcessors.size(), 2);
    assertEquals(state.preferredHostRequests.get(), 0);
    assertEquals(state.anyHostRequests.get(), 2);

    String previousHostOfContainer1 = state.runningProcessors.get("0").getHost();
    String previousHostOfContainer2 = state.runningProcessors.get("1").getHost();

    // Initiate container placement action to move a container with container id 0
    ContainerPlacementRequestMessage requestMessage =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "0", "host-3",
            System.currentTimeMillis());
    ContainerPlacementMetadata metadata =
        containerManager.registerContainerPlacementActionForTest(requestMessage, allocatorWithoutHostAffinity);

    // Wait for the ControlAction to complete and spawn an async request
    if (!allocatorWithoutHostAffinity.awaitContainersStart(1, 3, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    Optional<ContainerPlacementResponseMessage> responseMessage =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage.getUuid());

    while (true) {
      if (metadata.getActionStatus() == ContainerPlacementMessage.StatusCode.SUCCEEDED && responseMessage.isPresent()
          && responseMessage.get().getStatusCode() == ContainerPlacementMessage.StatusCode.SUCCEEDED) {
        break;
      }
      Thread.sleep(100);
      responseMessage = containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage.getUuid());
    }

    // We should have no preferred host request
    assertEquals(0, state.preferredHostRequests.get());
    // We should have one more ANY_HOST request
    assertEquals(3, state.anyHostRequests.get());
    assertEquals(2, state.runningProcessors.size());
    assertNotEquals(previousHostOfContainer1, state.runningProcessors.get("0").getHost());
    // Container 2 should not be affected
    assertEquals(previousHostOfContainer2, state.runningProcessors.get("1").getHost());
    assertEquals(3, state.anyHostRequests.get());
    // Action should success
    assertEquals(ContainerPlacementMessage.StatusCode.SUCCEEDED, metadata.getActionStatus());

    assertTrue(responseMessage.isPresent());
    assertEquals(responseMessage.get().getStatusCode(), ContainerPlacementMessage.StatusCode.SUCCEEDED);
    assertResponseMessage(responseMessage.get(), requestMessage);

    /**
     * Inject a duplicate request and check it is not accepted
     */
    ContainerPlacementRequestMessage duplicateRequestToBeIgnored =
        new ContainerPlacementRequestMessage(requestMessage.getUuid(), "app-attempt-001", "1",
            "host-3", System.currentTimeMillis());

    // Request with a dup uuid should not be accepted
    metadata = containerManager.registerContainerPlacementActionForTest(duplicateRequestToBeIgnored,
        allocatorWithoutHostAffinity);
    // metadata should be from the previous completed action
    assertTrue(metadata == null || metadata.getUuid() != duplicateRequestToBeIgnored.getUuid());

    responseMessage =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage.getUuid());

    assertTrue(responseMessage.isPresent());
    assertEquals(responseMessage.get().getStatusCode(), ContainerPlacementMessage.StatusCode.BAD_REQUEST);
    assertResponseMessage(responseMessage.get(), duplicateRequestToBeIgnored);

    // Request shall be deleted as soon as it is acted upon
    assertFalse(containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestMessage.getUuid()).isPresent());
  }

  @Test(expected = NullPointerException.class)
  public void testBadControlRequestRejected() throws Exception {
    SamzaApplicationState state =
        new SamzaApplicationState(JobModelManagerTestUtil.getJobModelManager(getConfig(), 2, this.server));
    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    FaultDomainManager faultDomainManager = mock(FaultDomainManager.class);
    ContainerManager containerManager =
        spy(new ContainerManager(containerPlacementMetadataStore, state, clusterResourceManager, true, false, localityManager, faultDomainManager, config));
    MockContainerAllocatorWithHostAffinity allocatorWithHostAffinity =
        new MockContainerAllocatorWithHostAffinity(clusterResourceManager, config, state, containerManager);
    ContainerProcessManager cpm = new ContainerProcessManager(
        new ClusterManagerConfig(new MapConfig(getConfig(), getConfigWithHostAffinityAndRetries(true, 1, true))), state,
        new MetricsRegistryMap(), clusterResourceManager, Optional.of(allocatorWithHostAffinity), containerManager, localityManager, false);

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          cpm.onResourcesAvailable((List<SamzaResource>) args[0]);
          return null;
        }
    }).when(callback).onResourcesAvailable(anyList());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          cpm.onStreamProcessorLaunchSuccess((SamzaResource) args[0]);
          return null;
        }
    }).when(callback).onStreamProcessorLaunchSuccess(any());

    cpm.start();

    if (!allocatorWithHostAffinity.awaitContainersStart(2, 3, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    assertBadRequests(null, "host2", containerManager, allocatorWithHostAffinity);
    assertBadRequests("0", null, containerManager, allocatorWithHostAffinity);
    assertBadRequests("2", "host8", containerManager, allocatorWithHostAffinity);
  }


  @Test(timeout = 30000)
  public void testContainerSuccessfulMoveActionWithStandbyEnabled() throws Exception {
    // Setup standby for job
    setupStandby();

    // Spawn a Request Allocator Thread
    ContainerPlacementRequestAllocator requestAllocator =
        new ContainerPlacementRequestAllocator(containerPlacementMetadataStore, cpm, new ApplicationConfig(config), 100);
    Thread requestAllocatorThread = new Thread(requestAllocator, "ContainerPlacement Request Allocator Thread");

    requestAllocatorThread.start();

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onResourcesAvailable((List<SamzaResource>) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesAvailable(anyList());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onStreamProcessorLaunchSuccess((SamzaResource) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onStreamProcessorLaunchSuccess(any());

    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        new Thread(() -> {
          Object[] args = invocation.getArguments();
          cpm.onResourcesCompleted((List<SamzaResourceStatus>) args[0]);
        }, "AMRMClientAsync").start();
        return null;
      }
    }).when(callback).onResourcesCompleted(anyList());

    cpm.start();

    if (!allocatorWithHostAffinity.awaitContainersStart(4, 4, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    while (state.runningProcessors.size() != 4) {
      Thread.sleep(100);
    }

    // First running state of the app
    Consumer<SamzaApplicationState> stateCheck = (SamzaApplicationState state) -> {
      assertEquals(4, state.runningProcessors.size());
      assertEquals("host-1", state.runningProcessors.get("0").getHost());
      assertEquals("host-2", state.runningProcessors.get("1").getHost());
      assertEquals("host-2", state.runningProcessors.get("0-0").getHost());
      assertEquals("host-1", state.runningProcessors.get("1-0").getHost());
      assertEquals(4, state.preferredHostRequests.get());
      assertEquals(0, state.failedStandbyAllocations.get());
      assertEquals(0, state.anyHostRequests.get());
    };
    // Invoke a state check
    stateCheck.accept(state);

    // Initiate a bad container placement action to move a standby to its active host and vice versa
    // which should fail because this violates standby constraints
    UUID badRequest1 = containerPlacementMetadataStore.writeContainerPlacementRequestMessage("appAttempt-001", "0-0", "host-1",
        null, System.currentTimeMillis());

    UUID badRequest2 = containerPlacementMetadataStore.writeContainerPlacementRequestMessage("appAttempt-001", "0", "host-2",
        null, System.currentTimeMillis() + 100);

    // Wait for the ControlActions to complete
    while (true) {
      if (containerPlacementMetadataStore.readContainerPlacementResponseMessage(badRequest2).isPresent() &&
          containerPlacementMetadataStore.readContainerPlacementResponseMessage(badRequest2).get().getStatusCode()
              == ContainerPlacementMessage.StatusCode.BAD_REQUEST) {
        break;
      }
      Thread.sleep(100);
    }

    // App running state should remain the same
    stateCheck.accept(state);

    Optional<ContainerPlacementResponseMessage> responseMessageMove1 =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(badRequest1);
    Optional<ContainerPlacementResponseMessage> responseMessageMove2 =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(badRequest2);

    // Assert that both the requests were bad
    assertTrue(responseMessageMove1.isPresent());
    assertEquals(responseMessageMove1.get().getStatusCode(), ContainerPlacementMessage.StatusCode.BAD_REQUEST);
    assertTrue(responseMessageMove2.isPresent());
    assertEquals(responseMessageMove2.get().getStatusCode(), ContainerPlacementMessage.StatusCode.BAD_REQUEST);


    // Initiate a standby failover which is supposed to be done in two steps
    // Step 1. Move the standby container to any other host: move 0-0 to say host-3
    // Step 2. Move the active container to the standby's host: move 0 to host-1

    // Action will get executed first
    UUID standbyMoveRequest =
        containerPlacementMetadataStore.writeContainerPlacementRequestMessage("appAttempt-001", "0-0", "host-3",
            null, System.currentTimeMillis());
    // Action will get executed when standbyMoveRequest move request is complete
    UUID activeMoveRequest = containerPlacementMetadataStore.writeContainerPlacementRequestMessage("appAttempt-001", "0", "host-2", null,
            System.currentTimeMillis() + 100);

    // Wait for the ControlActions to complete
    while (true) {
      if (containerPlacementMetadataStore.readContainerPlacementResponseMessage(activeMoveRequest).isPresent() &&
          containerPlacementMetadataStore.readContainerPlacementResponseMessage(activeMoveRequest).get().getStatusCode()
              == ContainerPlacementMessage.StatusCode.SUCCEEDED) {
        break;
      }
      Thread.sleep(100);
    }

    assertEquals(4, state.runningProcessors.size());
    assertEquals("host-2", state.runningProcessors.get("0").getHost());
    assertEquals("host-2", state.runningProcessors.get("1").getHost());
    assertEquals("host-3", state.runningProcessors.get("0-0").getHost());
    assertEquals("host-1", state.runningProcessors.get("1-0").getHost());
    assertEquals(6, state.preferredHostRequests.get());
    assertEquals(0, state.failedStandbyAllocations.get());
    assertEquals(0, state.anyHostRequests.get());


    Optional<ContainerPlacementResponseMessage> responseStandbyMove =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(standbyMoveRequest);

    Optional<ContainerPlacementResponseMessage> responseActiveMove =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(activeMoveRequest);

    assertTrue(responseStandbyMove.isPresent());
    assertEquals(responseStandbyMove.get().getStatusCode(), ContainerPlacementMessage.StatusCode.SUCCEEDED);

    assertTrue(responseActiveMove.isPresent());
    assertEquals(responseActiveMove.get().getStatusCode(), ContainerPlacementMessage.StatusCode.SUCCEEDED);

    // Request should be deleted as soon as ita accepted / being acted upon
    assertFalse(containerPlacementMetadataStore.readContainerPlacementRequestMessage(standbyMoveRequest).isPresent());
    assertFalse(containerPlacementMetadataStore.readContainerPlacementRequestMessage(activeMoveRequest).isPresent());

    // Cleanup Request Allocator Thread
    cleanUpRequestAllocatorThread(requestAllocator, requestAllocatorThread);
  }

  private void assertResponseMessage(ContainerPlacementResponseMessage responseMessage,
      ContainerPlacementRequestMessage requestMessage) {
    assertEquals(responseMessage.getProcessorId(), requestMessage.getProcessorId());
    assertEquals(responseMessage.getDeploymentId(), requestMessage.getDeploymentId());
    assertEquals(responseMessage.getDestinationHost(), requestMessage.getDestinationHost());
  }

  private void assertBadRequests(String processorId, String destinationHost, ContainerManager containerManager,
      ContainerAllocator allocator) throws InterruptedException {
    ContainerPlacementRequestMessage requestMessage =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-Attemp-001", processorId, destinationHost,
            System.currentTimeMillis());
    ContainerPlacementMetadata metadata =
        containerManager.registerContainerPlacementActionForTest(requestMessage, allocator);
    assertNull(metadata);

    Optional<ContainerPlacementResponseMessage> responseMessage =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage.getUuid());

    while (true) {
      if (responseMessage.isPresent()
          && responseMessage.get().getStatusCode() == ContainerPlacementMessage.StatusCode.BAD_REQUEST) {
        break;
      }
      Thread.sleep(100);
      responseMessage = containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage.getUuid());
    }

    assertEquals(responseMessage.get().getStatusCode(), ContainerPlacementMessage.StatusCode.BAD_REQUEST);
    assertResponseMessage(responseMessage.get(), requestMessage);
    // Request shall be deleted as soon as it is acted upon
    assertFalse(containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestMessage.getUuid()).isPresent());
  }

  private void cleanUpRequestAllocatorThread(ContainerPlacementRequestAllocator requestAllocator, Thread containerPlacementRequestAllocatorThread) {
    requestAllocator.stop();
    try {
      containerPlacementRequestAllocatorThread.join();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
