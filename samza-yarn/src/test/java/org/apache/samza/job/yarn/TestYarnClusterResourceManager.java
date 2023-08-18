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

import com.google.common.collect.ImmutableList;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;


public class TestYarnClusterResourceManager {

  private YarnConfiguration yarnConfiguration;
  private Config config;
  private SamzaAppMasterMetrics metrics;
  private AMRMClientAsync asyncClient;
  private SamzaYarnAppMasterLifecycle lifecycle;
  private SamzaYarnAppMasterService service;
  private NMClientAsync asyncNMClient;
  private ClusterResourceManager.Callback callback;
  private YarnAppState yarnAppState;

  @Before
  public void setup() {
    yarnConfiguration = mock(YarnConfiguration.class);
    config = mock(Config.class);
    metrics = mock(SamzaAppMasterMetrics.class);
    asyncClient = mock(AMRMClientAsync.class);
    lifecycle = mock(SamzaYarnAppMasterLifecycle.class);
    service = mock(SamzaYarnAppMasterService.class);
    asyncNMClient = mock(NMClientAsync.class);
    callback = mock(ClusterResourceManager.Callback.class);
    yarnAppState = new YarnAppState(0, mock(ContainerId.class), "host", 8080, 8081);
  }

  @Test
  public void testErrorInStartContainerShouldUpdateState() {
    // create mocks
    final int samzaContainerId = 1;

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
    SamzaYarnAppMasterLifecycle lifecycle = Mockito.spy(new SamzaYarnAppMasterLifecycle(512, 2, mock(SamzaApplicationState.class), yarnAppState, asyncClient, false));

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
    SamzaYarnAppMasterLifecycle lifecycle = Mockito.spy(new SamzaYarnAppMasterLifecycle(512, 2, mock(SamzaApplicationState.class), yarnAppState, asyncClient, false));

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
  public void testAMHACallbackInvokedForPreviousAttemptContainers() {
    String previousAttemptContainerId = "0";
    String previousAttemptYarnContainerId = "container_1607304997422_0008_02_000002";
    // create mocks
    YarnAppState yarnAppState = Mockito.spy(new YarnAppState(0, mock(ContainerId.class), "host", 8080, 8081));

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
    ArgumentCaptor<Integer> containerFromPreviousAttemptCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(metrics).setContainersFromPreviousAttempts(containerFromPreviousAttemptCaptor.capture());
    SamzaResource samzaResource = samzaResourceArgumentCaptor.getValue();
    assertEquals(previousAttemptYarnContainerId, samzaResource.getContainerId());
    assertEquals(1, containerFromPreviousAttemptCaptor.getValue().intValue());
  }

  @Test
  public void testStopStreamProcessorForContainerFromPreviousAttempt() {
    String containerId = "Yarn_Container_id_0";
    String processorId = "Container_id_0";
    YarnContainer runningYarnContainer = mock(YarnContainer.class);
    ContainerId previousRunningContainerId = mock(ContainerId.class);
    YarnAppState yarnAppState = Mockito.spy(new YarnAppState(0, mock(ContainerId.class), "host", 8080, 8081));

    yarnAppState.runningProcessors.put(processorId, runningYarnContainer);
    when(runningYarnContainer.id()).thenReturn(previousRunningContainerId);
    when(previousRunningContainerId.toString()).thenReturn(containerId);

    YarnClusterResourceManager yarnClusterResourceManager =
        new YarnClusterResourceManager(asyncClient, asyncNMClient, callback, yarnAppState, lifecycle, service, metrics,
            yarnConfiguration, config);

    SamzaResource containerResourceFromPreviousRun = mock(SamzaResource.class);
    when(containerResourceFromPreviousRun.getContainerId()).thenReturn(containerId);

    yarnClusterResourceManager.stopStreamProcessor(containerResourceFromPreviousRun);
    verify(asyncClient, times(1)).releaseAssignedContainer(previousRunningContainerId);
  }

  @Test
  public void testStopStreamProcessorForContainerStartedInCurrentLifecycle() {
    YarnClusterResourceManager yarnClusterResourceManager =
        new YarnClusterResourceManager(asyncClient, asyncNMClient, callback, yarnAppState, lifecycle, service, metrics,
            yarnConfiguration, config);

    SamzaResource allocatedContainerResource = mock(SamzaResource.class);
    Container runningContainer = mock(Container.class);
    ContainerId runningContainerId = mock(ContainerId.class);
    NodeId runningNodeId = mock(NodeId.class);

    when(runningContainer.getId()).thenReturn(runningContainerId);
    when(runningContainer.getNodeId()).thenReturn(runningNodeId);

    yarnClusterResourceManager.getAllocatedResources().put(allocatedContainerResource, runningContainer);
    yarnClusterResourceManager.stopStreamProcessor(allocatedContainerResource);

    verify(asyncNMClient, times(1)).stopContainerAsync(runningContainerId, runningNodeId);
  }

  @Test
  public void testIncrementAllocatedContainersInBuffer() {
    YarnClusterResourceManager yarnClusterResourceManager =
        new YarnClusterResourceManager(asyncClient, asyncNMClient, callback, yarnAppState, lifecycle, service, metrics,
            yarnConfiguration, config);
    yarnClusterResourceManager.start();
    Container allocatedContainer = mock(Container.class);
    ContainerId allocatedContainerId = mock(ContainerId.class);
    NodeId allocatedNodeId = mock(NodeId.class);
    Resource resource = mock(Resource.class);

    when(allocatedNodeId.getHost()).thenReturn("fake_host");
    when(resource.getVirtualCores()).thenReturn(1);
    when(resource.getMemory()).thenReturn(1024);
    when(allocatedContainer.getId()).thenReturn(allocatedContainerId);
    when(allocatedContainer.getNodeId()).thenReturn(allocatedNodeId);
    when(allocatedContainer.getResource()).thenReturn(resource);
    yarnClusterResourceManager.onContainersAllocated(ImmutableList.of(allocatedContainer));
    verify(metrics).incrementAllocatedContainersInBuffer();
  }
  @Test
  public void testDecrementAllocatedContainersInBuffer() {
    YarnClusterResourceManager yarnClusterResourceManager =
        new YarnClusterResourceManager(asyncClient, asyncNMClient, callback, yarnAppState, lifecycle, service, metrics,
            yarnConfiguration, config);
    yarnClusterResourceManager.start();
    SamzaResource allocatedContainerResource = mock(SamzaResource.class);
    Container runningContainer = mock(Container.class);
    ContainerId runningContainerId = mock(ContainerId.class);
    NodeId runningNodeId = mock(NodeId.class);

    when(runningContainer.getId()).thenReturn(runningContainerId);
    when(runningContainer.getNodeId()).thenReturn(runningNodeId);

    yarnClusterResourceManager.getAllocatedResources().put(allocatedContainerResource, runningContainer);
    yarnClusterResourceManager.releaseResources(allocatedContainerResource);
    verify(metrics).decrementAllocatedContainersInBuffer();
  }

}