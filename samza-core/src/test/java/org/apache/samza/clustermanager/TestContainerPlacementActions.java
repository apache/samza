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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.samza.clustermanager.container.placements.ContainerPlacementMetadata;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.placement.ContainerPlacementMessage;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManagerTestUtil;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.testUtils.MockHttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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
    }
  };

  private Config config = new MapConfig(configVals);

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
    return new MapConfig(map);
  }

  private JobModelManager getJobModelManagerWithHostAffinity(Map<String, String> containerIdToHost) {
    Map<String, Map<String, String>> localityMap = new HashMap<>();
    containerIdToHost.forEach((containerId, host) -> {
        localityMap.put(containerId,
            ImmutableMap.of(SetContainerHostMapping.HOST_KEY, containerIdToHost.get(containerId)));
      });
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    when(mockLocalityManager.readContainerLocality()).thenReturn(localityMap);

    return JobModelManagerTestUtil.getJobModelManagerWithLocalityManager(getConfig(), containerIdToHost.size(),
        mockLocalityManager, this.server);
  }

  @Before
  public void setup() throws Exception {
    server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));
  }

  @Test(timeout = 10000)
  public void testContainerSuccessfulMoveAction() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.putAll(getConfigWithHostAffinityAndRetries(true, 1, true));

    SamzaApplicationState state =
        new SamzaApplicationState(getJobModelManagerWithHostAffinity(ImmutableMap.of("0", "host-1", "1", "host-2")));

    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    ContainerManager containerManager = spy(new ContainerManager(state, clusterResourceManager, true, false));
    MockContainerAllocatorWithHostAffinity allocatorWithHostAffinity =
        new MockContainerAllocatorWithHostAffinity(clusterResourceManager, config, state, containerManager);
    ContainerProcessManager cpm =
        new ContainerProcessManager(clusterManagerConfig, state, new MetricsRegistryMap(),
            clusterResourceManager, Optional.of(allocatorWithHostAffinity), containerManager);

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

    while (metadata.getActionStatus() != ContainerPlacementMessage.StatusCode.SUCCEEDED) {
      Thread.sleep(100);
    }

    assertEquals(state.preferredHostRequests.get(), 3);
    assertEquals(state.runningProcessors.size(), 2);
    assertEquals(state.runningProcessors.get("0").getHost(), "host-3");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.anyHostRequests.get(), 0);
    assertEquals(metadata.getActionStatus(), ContainerPlacementMessage.StatusCode.SUCCEEDED);
  }

  @Test(timeout = 10000)
  public void testContainerMoveActionExpiredRequestNotAffectRunningContainers() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.putAll(getConfigWithHostAffinityAndRetries(true, 1, true));

    SamzaApplicationState state =
        new SamzaApplicationState(getJobModelManagerWithHostAffinity(ImmutableMap.of("0", "host-1", "1", "host-2")));

    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ContainerManager containerManager = spy(new ContainerManager(state, clusterResourceManager, true, false));
    MockContainerAllocatorWithHostAffinity allocatorWithHostAffinity =
        new MockContainerAllocatorWithHostAffinity(clusterResourceManager, config, state, containerManager);
    ContainerProcessManager cpm =
        new ContainerProcessManager(new ClusterManagerConfig(new MapConfig(conf)), state, new MetricsRegistryMap(),
            clusterResourceManager, Optional.of(allocatorWithHostAffinity), containerManager);

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

    assertEquals(metadata.getActionStatus(), ContainerPlacementMessage.StatusCode.IN_PROGRESS);

    while (metadata.getActionStatus() != ContainerPlacementMessage.StatusCode.FAILED) {
      Thread.sleep(100);
    }

    assertEquals(state.preferredHostRequests.get(), 3);
    assertEquals(state.runningProcessors.size(), 2);
    // Container should not be stooped
    assertEquals(state.runningProcessors.get("0").getHost(), "host-1");
    assertEquals(state.runningProcessors.get("1").getHost(), "host-2");
    assertEquals(state.anyHostRequests.get(), 0);
  }

  @Test(timeout = 10000)
  public void testActiveContainerLaunchFailureOnControlActionShouldFallbackToSourceHost() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.putAll(getConfigWithHostAffinityAndRetries(true, 1, true));

    SamzaApplicationState state =
        new SamzaApplicationState(getJobModelManagerWithHostAffinity(ImmutableMap.of("0", "host-1", "1", "host-2")));

    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ContainerManager containerManager = spy(new ContainerManager(state, clusterResourceManager, true, false));
    MockContainerAllocatorWithHostAffinity allocatorWithHostAffinity =
        new MockContainerAllocatorWithHostAffinity(clusterResourceManager, config, state, containerManager);
    ContainerProcessManager cpm =
        new ContainerProcessManager(new ClusterManagerConfig(new MapConfig(conf)), state, new MetricsRegistryMap(),
            clusterResourceManager, Optional.of(allocatorWithHostAffinity), containerManager);

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

    // Control action should be in progress
    assertEquals(metadata.getActionStatus(), ContainerPlacementMessage.StatusCode.IN_PROGRESS);

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
  }

  @Test(timeout = 10000)
  public void testAlwaysMoveToAnyHostForHostAffinityDisabled() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.putAll(getConfigWithHostAffinityAndRetries(false, 1, true));
    SamzaApplicationState state =
        new SamzaApplicationState(getJobModelManagerWithHostAffinity(ImmutableMap.of("0", "host-1", "1", "host-2")));
    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ContainerManager containerManager = new ContainerManager(state, clusterResourceManager, false, false);
    MockContainerAllocatorWithoutHostAffinity allocatorWithoutHostAffinity =
        new MockContainerAllocatorWithoutHostAffinity(clusterResourceManager, new MapConfig(conf), state,
            containerManager);

    ContainerProcessManager cpm = new ContainerProcessManager(
        new ClusterManagerConfig(new MapConfig(getConfig(), getConfigWithHostAffinityAndRetries(true, 1, true))), state,
        new MetricsRegistryMap(), clusterResourceManager, Optional.of(allocatorWithoutHostAffinity), containerManager);

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

    // Initiated Control action should be in progress
    assertEquals(metadata.getActionStatus(), ContainerPlacementMessage.StatusCode.IN_PROGRESS);

    // Wait for the ControlAction to complete and spawn an async request
    if (!allocatorWithoutHostAffinity.awaitContainersStart(1, 3, TimeUnit.SECONDS)) {
      fail("timed out waiting for the containers to start");
    }

    while (metadata.getActionStatus() != ContainerPlacementMessage.StatusCode.SUCCEEDED) {
      Thread.sleep(100);
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
  }

  @Test(expected = NullPointerException.class)
  public void testBadControlRequestRejected() throws Exception {
    SamzaApplicationState state =
        new SamzaApplicationState(getJobModelManagerWithHostAffinity(ImmutableMap.of("0", "host-1", "1", "host-2")));
    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);
    MockClusterResourceManager clusterResourceManager = new MockClusterResourceManager(callback, state);
    ContainerManager containerManager = spy(new ContainerManager(state, clusterResourceManager, true, false));
    MockContainerAllocatorWithHostAffinity allocatorWithHostAffinity =
        new MockContainerAllocatorWithHostAffinity(clusterResourceManager, config, state, containerManager);
    ContainerProcessManager cpm = new ContainerProcessManager(
        new ClusterManagerConfig(new MapConfig(getConfig(), getConfigWithHostAffinityAndRetries(true, 1, true))), state,
        new MetricsRegistryMap(), clusterResourceManager, Optional.of(allocatorWithHostAffinity), containerManager);

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

  private void assertBadRequests(String processorId, String destinationHost, ContainerManager containerManager,
      ContainerAllocator allocator) {
    ContainerPlacementRequestMessage requestMessage =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-Attemp-001", processorId, destinationHost,
            System.currentTimeMillis());
    ContainerPlacementMetadata metadata =
        containerManager.registerContainerPlacementActionForTest(requestMessage, allocator);
    assertNull(metadata);
  }
}
