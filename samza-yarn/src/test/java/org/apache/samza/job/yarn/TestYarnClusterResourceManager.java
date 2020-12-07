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
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.samza.clustermanager.ClusterResourceManager;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.clustermanager.SamzaResource;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;


public class TestYarnClusterResourceManager {

  @Test
  public void testErrorInStartContainerShouldUpdateState() {
    // create mocks
    final int samzaContainerId = 1;
    YarnConfiguration yarnConfiguration = mock(YarnConfiguration.class);
    SamzaAppMasterMetrics metrics = mock(SamzaAppMasterMetrics.class);
    Config config = mock(Config.class);
    AMRMClientAsync asyncClient = mock(AMRMClientAsync.class);
    YarnAppState yarnAppState = new YarnAppState(0, mock(ContainerId.class), "host", 8080, 8081);
    SamzaYarnAppMasterLifecycle lifecycle = mock(SamzaYarnAppMasterLifecycle.class);
    SamzaYarnAppMasterService service = mock(SamzaYarnAppMasterService.class);
    NMClientAsync asyncNMClient = mock(NMClientAsync.class);
    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);

    // start the cluster manager
    YarnClusterResourceManager yarnClusterResourceManager =
        new YarnClusterResourceManager(asyncClient, asyncNMClient, callback, yarnAppState, lifecycle, service, metrics,
            yarnConfiguration, config);

    yarnAppState.pendingProcessors.put(String.valueOf(samzaContainerId), new YarnContainer(Container.newInstance(
        ContainerId.newContainerId(ApplicationAttemptId.newInstance(ApplicationId.newInstance(10000L, 1), 1), 1),
        NodeId.newInstance("host1", 8088), "http://host1", Resource.newInstance(1024, 1), Priority.newInstance(1),
        Token.newInstance("id".getBytes(), "read", "password".getBytes(), "service"))));

    yarnClusterResourceManager.start();
    assertEquals(1, yarnAppState.pendingProcessors.size());

    yarnClusterResourceManager.onStartContainerError(ContainerId.newContainerId(ApplicationAttemptId.newInstance(ApplicationId.newInstance(10000L, 1), 1), 1),
        new Exception());

    assertEquals(0, yarnAppState.pendingProcessors.size());
    verify(callback, times(1)).onStreamProcessorLaunchFailure(anyObject(), any(Exception.class));
  }

  @Test
  public void testAllocatedResourceExpiryForYarn() {
    YarnConfiguration yarnConfiguration = mock(YarnConfiguration.class);
    SamzaAppMasterMetrics metrics = mock(SamzaAppMasterMetrics.class);
    Config config = mock(Config.class);
    AMRMClientAsync asyncClient = mock(AMRMClientAsync.class);
    YarnAppState yarnAppState = new YarnAppState(0, mock(ContainerId.class), "host", 8080, 8081);
    SamzaYarnAppMasterLifecycle lifecycle = mock(SamzaYarnAppMasterLifecycle.class);
    SamzaYarnAppMasterService service = mock(SamzaYarnAppMasterService.class);
    NMClientAsync asyncNMClient = mock(NMClientAsync.class);
    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);

    // start the cluster manager
    YarnClusterResourceManager yarnClusterResourceManager =
        new YarnClusterResourceManager(asyncClient, asyncNMClient, callback, yarnAppState, lifecycle, service, metrics,
            yarnConfiguration, config);

    SamzaResource allocatedResource = mock(SamzaResource.class);
    when(allocatedResource.getTimestamp()).thenReturn(System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());

    Assert.assertTrue(yarnClusterResourceManager.isResourceExpired(allocatedResource));
  }

  @Test
  public void testAMShutdownOnRMCallback() throws IOException, YarnException {
    // create mocks
    YarnConfiguration yarnConfiguration = mock(YarnConfiguration.class);
    SamzaAppMasterMetrics metrics = mock(SamzaAppMasterMetrics.class);
    Config config = mock(Config.class);
    AMRMClientAsync asyncClient = mock(AMRMClientAsync.class);
    YarnAppState yarnAppState = new YarnAppState(0, mock(ContainerId.class), "host", 8080, 8081);
    SamzaYarnAppMasterLifecycle lifecycle = Mockito.spy(new SamzaYarnAppMasterLifecycle(512, 2, mock(SamzaApplicationState.class), yarnAppState, asyncClient, false));
    SamzaYarnAppMasterService service = mock(SamzaYarnAppMasterService.class);
    NMClientAsync asyncNMClient = mock(NMClientAsync.class);
    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);

    // start the cluster manager
    YarnClusterResourceManager yarnClusterResourceManager =
        new YarnClusterResourceManager(asyncClient, asyncNMClient, callback, yarnAppState, lifecycle, service, metrics,
            yarnConfiguration, config);

    yarnClusterResourceManager.onShutdownRequest();

    verify(lifecycle, times(1)).onShutdown(SamzaApplicationState.SamzaAppStatus.FAILED);
    verify(asyncClient, times(1)).unregisterApplicationMaster(FinalApplicationStatus.FAILED, null, null);
    verify(asyncClient, times(1)).stop();
    verify(asyncNMClient, times(1)).stop();
    verify(service, times(1)).onShutdown();
    verify(metrics, times(1)).stop();
  }

  @Test
  public void testAMShutdownThrowingExceptionOnRMCallback() throws IOException, YarnException {
    // create mocks
    YarnConfiguration yarnConfiguration = mock(YarnConfiguration.class);
    SamzaAppMasterMetrics metrics = mock(SamzaAppMasterMetrics.class);
    Config config = mock(Config.class);
    AMRMClientAsync asyncClient = mock(AMRMClientAsync.class);
    YarnAppState yarnAppState = new YarnAppState(0, mock(ContainerId.class), "host", 8080, 8081);
    SamzaYarnAppMasterLifecycle lifecycle = Mockito.spy(new SamzaYarnAppMasterLifecycle(512, 2, mock(SamzaApplicationState.class), yarnAppState, asyncClient, false));
    SamzaYarnAppMasterService service = mock(SamzaYarnAppMasterService.class);
    NMClientAsync asyncNMClient = mock(NMClientAsync.class);
    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);

    doThrow(InvalidApplicationMasterRequestException.class).when(asyncClient).unregisterApplicationMaster(FinalApplicationStatus.FAILED, null, null);

    // start the cluster manager
    YarnClusterResourceManager yarnClusterResourceManager =
        new YarnClusterResourceManager(asyncClient, asyncNMClient, callback, yarnAppState, lifecycle, service, metrics,
            yarnConfiguration, config);

    yarnClusterResourceManager.onShutdownRequest();

    verify(lifecycle, times(1)).onShutdown(SamzaApplicationState.SamzaAppStatus.FAILED);
    verify(asyncClient, times(1)).unregisterApplicationMaster(FinalApplicationStatus.FAILED, null, null);
    verify(asyncClient, times(1)).stop();
    verify(asyncNMClient, times(1)).stop();
    verify(service, times(1)).onShutdown();
    verify(metrics, times(1)).stop();
  }

  @Test
  public void testAMHACallbackInvokedForPreviousAttemptContainers() throws IOException, YarnException {
    String previousAttemptContainerId = "0";
    String previousAttemptYarnContainerId = "container_1607304997422_0008_02_000002";
    // create mocks
    YarnConfiguration yarnConfiguration = mock(YarnConfiguration.class);
    SamzaAppMasterMetrics metrics = mock(SamzaAppMasterMetrics.class);
    AMRMClientAsync asyncClient = mock(AMRMClientAsync.class);
    YarnAppState yarnAppState = Mockito.spy(new YarnAppState(0, mock(ContainerId.class), "host", 8080, 8081));
    SamzaYarnAppMasterLifecycle lifecycle = mock(SamzaYarnAppMasterLifecycle.class);
    SamzaYarnAppMasterService service = mock(SamzaYarnAppMasterService.class);
    NMClientAsync asyncNMClient = mock(NMClientAsync.class);
    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);

    ContainerId containerId = mock(ContainerId.class);
    when(containerId.toString()).thenReturn(previousAttemptYarnContainerId);

    YarnContainer yarnContainer = mock(YarnContainer.class);
    Resource resource = mock(Resource.class);
    when(resource.getMemory()).thenReturn(1024);
    Mockito.when(resource.getVirtualCores()).thenReturn(1);
    Mockito.when(yarnContainer.resource()).thenReturn(resource);
    Mockito.when(yarnContainer.id()).thenReturn(containerId);
    NodeId nodeId = mock(NodeId.class);
    when(nodeId.getHost()).thenReturn("host");
    when(yarnContainer.nodeId()).thenReturn(nodeId);

    yarnAppState.pendingProcessors.put(previousAttemptContainerId, yarnContainer);

    Set<ContainerId> previousAttemptContainers = new HashSet<>();
    previousAttemptContainers.add(containerId);
    when(lifecycle.onInit()).thenReturn(previousAttemptContainers);

    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.YARN_AM_HIGH_AVAILABILITY_ENABLED, "true");
    Config config = new MapConfig(configMap);

    // start the cluster manager
    YarnClusterResourceManager yarnClusterResourceManager =
        new YarnClusterResourceManager(asyncClient, asyncNMClient, callback, yarnAppState, lifecycle, service, metrics,
            yarnConfiguration, config);

    yarnClusterResourceManager.start();
    verify(lifecycle).onInit();
    ArgumentCaptor<SamzaResource> samzaResourceArgumentCaptor = ArgumentCaptor.forClass(SamzaResource.class);
    verify(callback).onStreamProcessorLaunchSuccess(samzaResourceArgumentCaptor.capture());
    SamzaResource samzaResource = samzaResourceArgumentCaptor.getValue();
    assertEquals(previousAttemptYarnContainerId, samzaResource.getContainerId());
  }
}