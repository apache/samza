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
package org.apache.samza.zk;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.MetadataResourceUtil;
import org.apache.samza.coordinator.StreamPartitionCountMonitor;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.samza.zk.ZkJobCoordinator.ZkSessionStateChangedListener;
import org.apache.zookeeper.Watcher;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TestZkJobCoordinator {
  private static final String TEST_BARRIER_ROOT = "/testBarrierRoot";
  private static final String TEST_JOB_MODEL_VERSION = "1";

  private final Config config;
  private final JobModel jobModel;
  private final MetadataStore zkMetadataStore;
  private final CoordinatorStreamStore coordinatorStreamStore;

  public TestZkJobCoordinator() {
    Map<String, String> configMap = ImmutableMap.of(
        "job.coordinator.system", "kafka",
        "job.name", "test-job",
        "systems.kafka.samza.factory", "org.apache.samza.system.MockSystemFactory");
    config = new MapConfig(configMap);

    Set<SystemStreamPartition> ssps = ImmutableSet.of(
        new SystemStreamPartition("system1", "stream1_1", new Partition(0)),
        new SystemStreamPartition("system1", "stream1_2", new Partition(0)));
    Map<TaskName, TaskModel> tasksForContainer = ImmutableMap.of(
        new TaskName("t1"), new TaskModel(new TaskName("t1"), ssps, new Partition(0)));
    ContainerModel containerModel = new ContainerModel("0", tasksForContainer);
    jobModel = new JobModel(config, ImmutableMap.of("0", containerModel));
    zkMetadataStore = Mockito.mock(MetadataStore.class);
    coordinatorStreamStore = Mockito.mock(CoordinatorStreamStore.class);
  }

  @Test
  public void testFollowerShouldStopWhenNotPartOfGeneratedJobModel() throws Exception {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    CountDownLatch jcShutdownLatch = new CountDownLatch(1);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    ZkUtils zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator("TEST_PROCESSOR_ID", new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));
    doReturn(new JobModel(new MapConfig(), new HashMap<>())).when(zkJobCoordinator).readJobModelFromMetadataStore(TEST_JOB_MODEL_VERSION);
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        jcShutdownLatch.countDown();
        return null;
      }
    }).when(zkJobCoordinator).stop();

    final ZkJobCoordinator.ZkJobModelVersionChangeHandler zkJobModelVersionChangeHandler = zkJobCoordinator.new ZkJobModelVersionChangeHandler(zkUtils);
    zkJobModelVersionChangeHandler.doHandleDataChange("path", TEST_JOB_MODEL_VERSION);
    verify(zkJobCoordinator, Mockito.atMost(1)).stop();
    assertTrue("Timed out waiting for JobCoordinator to stop", jcShutdownLatch.await(1, TimeUnit.MINUTES));
  }

  @Test
  public void testShouldRemoveBufferedEventsInDebounceQueueOnSessionExpiration() {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    ZkUtils zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator("TEST_PROCESSOR_ID", new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));
    zkJobCoordinator.debounceTimer = mockDebounceTimer;
    zkJobCoordinator.zkSessionMetrics = new ZkSessionMetrics(new MetricsRegistryMap());
    final ZkSessionStateChangedListener zkSessionStateChangedListener = zkJobCoordinator.new ZkSessionStateChangedListener();

    zkSessionStateChangedListener.handleStateChanged(Watcher.Event.KeeperState.Expired);

    verify(zkUtils).incGeneration();
    verify(mockDebounceTimer).cancelAllScheduledActions();
    verify(mockDebounceTimer).scheduleAfterDebounceTime(eq("ZK_SESSION_EXPIRED"), eq(0L), Mockito.any(Runnable.class));
    Assert.assertEquals(1, zkJobCoordinator.zkSessionMetrics.zkSessionExpirations.getCount());
  }

  @Test
  public void testZookeeperSessionMetricsAreUpdatedCoorrectly() {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    ZkUtils zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator("TEST_PROCESSOR_ID", new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));
    zkJobCoordinator.debounceTimer = mockDebounceTimer;
    zkJobCoordinator.zkSessionMetrics = new ZkSessionMetrics(new MetricsRegistryMap());
    final ZkSessionStateChangedListener zkSessionStateChangedListener = zkJobCoordinator.new ZkSessionStateChangedListener();

    zkSessionStateChangedListener.handleStateChanged(Watcher.Event.KeeperState.Disconnected);
    zkSessionStateChangedListener.handleStateChanged(Watcher.Event.KeeperState.SyncConnected);
    zkSessionStateChangedListener.handleStateChanged(Watcher.Event.KeeperState.AuthFailed);

    Assert.assertEquals(1, zkJobCoordinator.zkSessionMetrics.zkSessionErrors.getCount());

    zkSessionStateChangedListener.handleSessionEstablishmentError(new SamzaException("Test exception"));

    Assert.assertEquals(1, zkJobCoordinator.zkSessionMetrics.zkSessionDisconnects.getCount());
    Assert.assertEquals(1, zkJobCoordinator.zkSessionMetrics.zkSyncConnected.getCount());
    Assert.assertEquals(2, zkJobCoordinator.zkSessionMetrics.zkSessionErrors.getCount());
  }

  @Test
  public void testShouldStopPartitionCountMonitorOnSessionExpiration() {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    ZkUtils zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator("TEST_PROCESSOR_ID", new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));
    StreamPartitionCountMonitor monitor = Mockito.mock(StreamPartitionCountMonitor.class);
    zkJobCoordinator.debounceTimer = mockDebounceTimer;
    zkJobCoordinator.streamPartitionCountMonitor = monitor;

    ZkSessionStateChangedListener zkSessionStateChangedListener = zkJobCoordinator.new ZkSessionStateChangedListener();
    zkSessionStateChangedListener.handleStateChanged(Watcher.Event.KeeperState.Expired);
    Mockito.verify(monitor).stop();
  }

  @Test
  public void testShouldStartPartitionCountMonitorOnBecomingLeader() {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    ZkUtils zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator("TEST_PROCESSOR_ID", new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));

    StreamPartitionCountMonitor monitor = Mockito.mock(StreamPartitionCountMonitor.class);
    zkJobCoordinator.debounceTimer = mockDebounceTimer;
    zkJobCoordinator.streamPartitionCountMonitor = monitor;
    when(zkJobCoordinator.getPartitionCountMonitor()).thenReturn(monitor);

    ZkJobCoordinator.LeaderElectorListenerImpl listener = zkJobCoordinator.new LeaderElectorListenerImpl();

    listener.onBecomingLeader();

    Mockito.verify(monitor).start();
  }

  @Test
  public void testShouldStopPartitionCountMonitorWhenStoppingTheJobCoordinator() {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    ZkUtils zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator("TEST_PROCESSOR_ID", new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));

    StreamPartitionCountMonitor monitor = Mockito.mock(StreamPartitionCountMonitor.class);
    zkJobCoordinator.debounceTimer = mockDebounceTimer;
    zkJobCoordinator.streamPartitionCountMonitor = monitor;

    zkJobCoordinator.stop();

    Mockito.verify(monitor).stop();
  }

  @Test
  public void testLoadMetadataResources() throws IOException {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    ZkUtils zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(jobModel);

    StartpointManager mockStartpointManager = Mockito.mock(StartpointManager.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator("TEST_PROCESSOR_ID", config, new NoOpMetricsRegistry(), zkUtils,
        zkMetadataStore, coordinatorStreamStore));
    doReturn(mockStartpointManager).when(zkJobCoordinator).createStartpointManager();

    MetadataResourceUtil mockMetadataResourceUtil = mock(MetadataResourceUtil.class);
    doReturn(mockMetadataResourceUtil).when(zkJobCoordinator)
        .createMetadataResourceUtil(any(), eq(getClass().getClassLoader()));

    verifyZeroInteractions(mockStartpointManager);

    zkJobCoordinator.loadMetadataResources(jobModel);

    verify(mockMetadataResourceUtil).createResources();
    verify(mockStartpointManager).start();
    verify(mockStartpointManager).fanOut(any());
    verify(mockStartpointManager).stop();
  }

  @Test
  public void testDoOnProcessorChange() {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    ZkUtils zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(jobModel);

    StartpointManager mockStartpointManager = Mockito.mock(StartpointManager.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator("TEST_PROCESSOR_ID", config,
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));
    doReturn(mockStartpointManager).when(zkJobCoordinator).createStartpointManager();

    doReturn(jobModel).when(zkJobCoordinator).generateNewJobModel(any());
    doNothing().when(zkJobCoordinator).loadMetadataResources(jobModel);

    zkJobCoordinator.doOnProcessorChange();

    verify(zkUtils).publishJobModelVersion(anyString(), anyString());
    verify(zkJobCoordinator).loadMetadataResources(eq(jobModel));
  }
}
