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
import java.util.function.BiConsumer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinatorListener;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TestZkJobCoordinator {
  private static final String LATEST_JOB_MODEL_VERSION = "2";
  private static final String PROCESSOR_ID = "testProcessor";
  private static final String TEST_BARRIER_ROOT = "/testBarrierRoot";
  private static final String TEST_JOB_MODEL_VERSION = "1";

  private final Config config;
  private final JobModel jobModel;
  private final MetadataStore zkMetadataStore;
  private final CoordinatorStreamStore coordinatorStreamStore;

  private ZkUtils zkUtils;

  @Before
  public void setup() {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);
  }

  public TestZkJobCoordinator() {
    Map<String, String> configMap = ImmutableMap.of(
        "job.coordinator.system", "kafka",
        "job.name", "test-job",
        "systems.kafka.samza.factory", "org.apache.samza.system.MockSystemFactory",
        ZkConfig.STARTUP_WITH_ACTIVE_JOB_MODEL, "true");
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
  public void testCheckAndExpireWithMultipleRebalances() {
    final TaskName taskName = new TaskName("task1");
    final ContainerModel mockContainerModel = mock(ContainerModel.class);
    final JobCoordinatorListener mockListener = mock(JobCoordinatorListener.class);
    final JobModel jobModelVersion1 = mock(JobModel.class);
    final JobModel jobModelVersion2 = mock(JobModel.class);
    final JobModel jobModelVersion3 = jobModelVersion1;

    when(mockContainerModel.getTasks()).thenReturn(ImmutableMap.of(taskName, mock(TaskModel.class)));
    when(jobModelVersion3.getContainers()).thenReturn(ImmutableMap.of(PROCESSOR_ID, mockContainerModel));

    ZkJobCoordinator zkJobCoordinator = new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(), new NoOpMetricsRegistry(),
        zkUtils, zkMetadataStore, coordinatorStreamStore);
    zkJobCoordinator.setListener(mockListener);
    zkJobCoordinator.setActiveJobModel(jobModelVersion1);

    /*
     * The following mimics the scenario where new work assignment(V2) is proposed by the leader and the work assignment
     * differs from the active work assignment(V1) and hence results in job model expiration
     */
    zkJobCoordinator.checkAndExpireJobModel(jobModelVersion2);

    verify(mockListener, times(1)).onJobModelExpired();
    assertTrue("JobModelExpired should be true for work assignment changes", zkJobCoordinator.getJobModelExpired());
    assertEquals("Active job model shouldn't be updated", jobModelVersion1, zkJobCoordinator.getActiveJobModel());

    /*
     * The following mimics the scenario where leader kicked off another rebalance where the new work assignment(V3)
     * is same as the old work assignment(V1) and doesn't trigger job model expiration. We check the interactions w/
     * the listener to ensure job model expiration isn't invoked. However, the previous rebalance should have already
     * triggered job model expiration and set the job model expired flag to true
     */
    zkJobCoordinator.checkAndExpireJobModel(jobModelVersion1);
    verifyNoMoreInteractions(mockListener);
    assertTrue("JobModelExpired should remain unchanged", zkJobCoordinator.getJobModelExpired());
    assertEquals("Active job model shouldn't be updated", jobModelVersion1, zkJobCoordinator.getActiveJobModel());


    /*
     * The following mimics the scenario where the new work assignment(V3) proposed by the leader is accepted and
     * on new job model is invoked. Even though the work assignment remains the same w/ the active job model version,
     * onNewJobModel is invoked on the listener as an intermediate rebalance expired the old work assignment(V1)
     */
    zkJobCoordinator.onNewJobModel(jobModelVersion3);
    verify(mockListener, times(1)).onNewJobModel(PROCESSOR_ID, jobModelVersion3);
    verify(zkUtils, times(1)).writeTaskLocality(any(), any());

    assertEquals("Active job model should be updated to new job model", zkJobCoordinator.getActiveJobModel(), jobModelVersion3);
    assertFalse("JobModelExpired should be set to false after onNewJobModel", zkJobCoordinator.getJobModelExpired());
  }

  @Test
  public void testCheckAndExpireWithNoChangeInWorkAssignment() {
    BiConsumer<ZkUtils, JobCoordinatorListener> verificationMethod =
      (ignored, coordinatorListener) -> verifyZeroInteractions(coordinatorListener);

    testNoChangesInWorkAssignmentHelper(ZkJobCoordinator::checkAndExpireJobModel, verificationMethod);
  }

  @Test
  public void testCheckAndExpireWithChangeInWorkAssignment() {
    final String processorId = "testProcessor";
    JobCoordinatorListener mockListener = mock(JobCoordinatorListener.class);

    ZkJobCoordinator zkJobCoordinator = new ZkJobCoordinator(processorId, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore);

    zkJobCoordinator.setListener(mockListener);
    zkJobCoordinator.checkAndExpireJobModel(mock(JobModel.class));
    verify(mockListener, times(1)).onJobModelExpired();
  }

  @Test(expected = NullPointerException.class)
  public void testCheckAndExpireJobModelWithNullJobModel() {
    final String processorId = "testProcessor";

    ZkJobCoordinator zkJobCoordinator = new ZkJobCoordinator(processorId, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore);
    zkJobCoordinator.checkAndExpireJobModel(null);
  }

  @Test
  public void testOnNewJobModelWithChangeInWorkAssignment() {
    final TaskName taskName = new TaskName("task1");
    final ContainerModel mockContainerModel = mock(ContainerModel.class);
    final JobCoordinatorListener mockListener = mock(JobCoordinatorListener.class);
    final JobModel mockJobModel = mock(JobModel.class);

    when(mockContainerModel.getTasks()).thenReturn(ImmutableMap.of(taskName, mock(TaskModel.class)));
    when(mockJobModel.getContainers()).thenReturn(ImmutableMap.of(PROCESSOR_ID, mockContainerModel));

    ZkJobCoordinator zkJobCoordinator = new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore);
    zkJobCoordinator.setListener(mockListener);
    zkJobCoordinator.setJobModelExpired(true);
    zkJobCoordinator.onNewJobModel(mockJobModel);

    verify(zkUtils, times(1)).writeTaskLocality(eq(taskName), any());
    verify(mockListener, times(1)).onNewJobModel(PROCESSOR_ID, mockJobModel);
    assertEquals("Active job model should be updated with the new job model", mockJobModel,
        zkJobCoordinator.getActiveJobModel());
  }

  @Test
  public void testOnNewJobModelWithNoChangesInWorkAssignment() {
    BiConsumer<ZkUtils, JobCoordinatorListener> verificationMethod = (zkUtils, coordinatorListener) -> {
      verify(zkUtils, times(0)).writeTaskLocality(any(), any());
      verifyZeroInteractions(coordinatorListener);
    };

    testNoChangesInWorkAssignmentHelper(ZkJobCoordinator::onNewJobModel, verificationMethod);
  }

  @Test(expected = NullPointerException.class)
  public void testOnNewJobModelWithNullJobModel() {
    ZkJobCoordinator zkJobCoordinator = new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore);
    zkJobCoordinator.onNewJobModel(null);
  }

  /**
   * Test job model version changed changes to work assignment. In this scenario, existing work should
   * be stopped a.k.a processor should stop the container through the listener. The processor then proceeds to join
   * the barrier to notify its acceptance on the proposed job model.
   */
  @Test
  public void testJobModelVersionChangeWithChangeInWorkAssignment() throws Exception {
    BiConsumer<ZkBarrierForVersionUpgrade, JobCoordinatorListener> verificationMethod =
      (barrier, listener) -> {
        verify(listener, times(1)).onJobModelExpired();
        verify(barrier, times(1)).join(TEST_JOB_MODEL_VERSION, PROCESSOR_ID);
      };
    testJobModelVersionChangeHelper(null, mock(JobModel.class), verificationMethod);
  }

  /**
   * Test job model version changed without any changes to work assignment. In this scenario, existing work should
   * not be stopped a.k.a processor shouldn't stop the container. However, the processor proceeds to join the barrier
   * to notify its acceptance on the proposed job model.
   */
  @Test
  public void testJobModelVersionChangeWithNoChangeInWorkAssignment() throws Exception {
    final JobModel jobModel = mock(JobModel.class);
    BiConsumer<ZkBarrierForVersionUpgrade, JobCoordinatorListener> verificationMethod =
      (barrier, listener) -> {
        verifyZeroInteractions(listener);
        verify(barrier, times(1)).join(TEST_JOB_MODEL_VERSION, PROCESSOR_ID);
      };
    testJobModelVersionChangeHelper(jobModel, jobModel, verificationMethod);
  }

  @Test
  public void testShouldRemoveBufferedEventsInDebounceQueueOnSessionExpiration() {
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
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
  public void testZookeeperSessionMetricsAreUpdatedCorrectly() {
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
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
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
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
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));

    StreamPartitionCountMonitor monitor = Mockito.mock(StreamPartitionCountMonitor.class);
    zkJobCoordinator.debounceTimer = mockDebounceTimer;
    zkJobCoordinator.streamPartitionCountMonitor = monitor;
    doReturn(monitor).when(zkJobCoordinator).getPartitionCountMonitor();

    ZkJobCoordinator.LeaderElectorListenerImpl listener = zkJobCoordinator.new LeaderElectorListenerImpl();

    listener.onBecomingLeader();

    Mockito.verify(monitor).start();
  }

  @Test
  public void testShouldStopPartitionCountMonitorWhenStoppingTheJobCoordinator() {
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));

    StreamPartitionCountMonitor monitor = Mockito.mock(StreamPartitionCountMonitor.class);
    zkJobCoordinator.debounceTimer = mockDebounceTimer;
    zkJobCoordinator.streamPartitionCountMonitor = monitor;

    zkJobCoordinator.stop();

    Mockito.verify(monitor).stop();
  }

  @Test
  public void testStartWithActiveJobModelDisabled() {
    final ScheduleAfterDebounceTime mockDebounceTimer = mock(ScheduleAfterDebounceTime.class);
    ZkJobCoordinator zkJobCoordinator = new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore);
    zkJobCoordinator.setLeaderElector(mock(ZkLeaderElector.class));
    zkJobCoordinator.setDebounceTimer(mockDebounceTimer);

    zkJobCoordinator.start();

    verifyZeroInteractions(mockDebounceTimer);
  }

  @Test
  public void testStartWithActiveJobModelEnabled() {
    final ScheduleAfterDebounceTime mockDebounceTimer = mock(ScheduleAfterDebounceTime.class);
    ZkJobCoordinator zkJobCoordinator = new ZkJobCoordinator(PROCESSOR_ID, config, new NoOpMetricsRegistry(), zkUtils,
        zkMetadataStore, coordinatorStreamStore);
    zkJobCoordinator.setLeaderElector(mock(ZkLeaderElector.class));
    zkJobCoordinator.setDebounceTimer(mockDebounceTimer);

    zkJobCoordinator.start();

    verify(mockDebounceTimer, times(1)).scheduleAfterDebounceTime(
        eq(ZkJobCoordinator.START_WORK_WITH_LAST_ACTIVE_JOB_MODEL),
        anyLong(),
        any());
  }

  @Test
  public void testStartWorkWithLastActiveJobModel() {
    final TaskName taskName = new TaskName("task1");
    final ContainerModel mockContainerModel = mock(ContainerModel.class);
    final JobCoordinatorListener mockListener = mock(JobCoordinatorListener.class);
    final JobModel mockJobModel = mock(JobModel.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));

    when(mockContainerModel.getTasks()).thenReturn(ImmutableMap.of(taskName, mock(TaskModel.class)));
    when(mockJobModel.getContainers()).thenReturn(ImmutableMap.of(PROCESSOR_ID, mockContainerModel));
    when(zkUtils.getLastActiveJobModelVersion()).thenReturn(TEST_JOB_MODEL_VERSION);
    when(zkUtils.getJobModelVersion()).thenReturn(TEST_JOB_MODEL_VERSION);
    doReturn(mockJobModel).when(zkJobCoordinator).readJobModelFromMetadataStore(TEST_JOB_MODEL_VERSION);

    zkJobCoordinator.setListener(mockListener);
    zkJobCoordinator.startWorkWithLastActiveJobModel();
    verify(mockListener, times(1)).onJobModelExpired();
    verify(zkUtils, times(1)).writeTaskLocality(eq(taskName), any());
    verify(mockListener, times(1)).onNewJobModel(PROCESSOR_ID, mockJobModel);
    assertEquals("Active job model should be updated with the new job model", mockJobModel,
        zkJobCoordinator.getActiveJobModel());
  }

  @Test
  public void testStartWorkWithLastActiveJobModelShouldNotStartContainer() {
    final JobCoordinatorListener mockListener = mock(JobCoordinatorListener.class);
    ZkJobCoordinator zkJobCoordinator = new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore);

    zkJobCoordinator.setListener(mockListener);

    when(zkUtils.getLastActiveJobModelVersion()).thenReturn(TEST_JOB_MODEL_VERSION);
    when(zkUtils.getJobModelVersion()).thenReturn(LATEST_JOB_MODEL_VERSION);

    zkJobCoordinator.startWorkWithLastActiveJobModel();

    verifyZeroInteractions(mockListener);
    assertNull("Expected active job model to be null", zkJobCoordinator.getActiveJobModel());
  }

  @Test
  public void testStartWorkWithLastActiveJobModelWithNullActiveJobModelVersion() {
    final JobCoordinatorListener mockListener = mock(JobCoordinatorListener.class);
    ZkJobCoordinator zkJobCoordinator = new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore);

    zkJobCoordinator.setListener(mockListener);

    zkJobCoordinator.startWorkWithLastActiveJobModel();

    verifyZeroInteractions(mockListener);
    assertNull("Expected active job model to be null", zkJobCoordinator.getActiveJobModel());
  }

  @Test
  public void testLoadMetadataResources() throws IOException {
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(jobModel);

    StartpointManager mockStartpointManager = Mockito.mock(StartpointManager.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(PROCESSOR_ID, config, new NoOpMetricsRegistry(), zkUtils,
        zkMetadataStore, coordinatorStreamStore));
    doReturn(mockStartpointManager).when(zkJobCoordinator).createStartpointManager();

    MetadataResourceUtil mockMetadataResourceUtil = mock(MetadataResourceUtil.class);
    doReturn(mockMetadataResourceUtil).when(zkJobCoordinator).createMetadataResourceUtil(any(), any(Config.class));

    verifyZeroInteractions(mockStartpointManager);

    zkJobCoordinator.loadMetadataResources(jobModel);

    verify(mockMetadataResourceUtil).createResources();
    verify(mockStartpointManager).start();
    verify(mockStartpointManager).fanOut(any());
    verify(mockStartpointManager).stop();
  }

  @Test
  public void testDoOnProcessorChange() {
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(jobModel);

    StartpointManager mockStartpointManager = Mockito.mock(StartpointManager.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(PROCESSOR_ID, config,
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));
    doReturn(mockStartpointManager).when(zkJobCoordinator).createStartpointManager();

    doReturn(jobModel).when(zkJobCoordinator).generateNewJobModel(any());
    doNothing().when(zkJobCoordinator).loadMetadataResources(jobModel);

    zkJobCoordinator.doOnProcessorChange();

    verify(zkUtils).publishJobModelVersion(anyString(), anyString());
    verify(zkJobCoordinator).loadMetadataResources(eq(jobModel));
  }

  @Test
  public void testDoOnProcessorChangeWithNoChangesToWorkAssignment() {
    ZkBarrierForVersionUpgrade mockBarrier = mock(ZkBarrierForVersionUpgrade.class);
    ScheduleAfterDebounceTime mockDebounceTimer = mock(ScheduleAfterDebounceTime.class);
    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(PROCESSOR_ID, config,
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));
    zkJobCoordinator.setActiveJobModel(jobModel);
    zkJobCoordinator.setDebounceTimer(mockDebounceTimer);
    zkJobCoordinator.setZkBarrierUpgradeForVersion(mockBarrier);

    doReturn(jobModel).when(zkJobCoordinator).generateNewJobModel(any());
    zkJobCoordinator.doOnProcessorChange();

    verify(zkUtils, times(0)).publishJobModelVersion(anyString(), anyString());
    verifyZeroInteractions(mockBarrier);
    verifyZeroInteractions(mockDebounceTimer);
    verify(zkJobCoordinator, times(0)).loadMetadataResources(any());
  }

  private void testNoChangesInWorkAssignmentHelper(BiConsumer<ZkJobCoordinator, JobModel> testMethod,
      BiConsumer<ZkUtils, JobCoordinatorListener> verificationMethod) {
    final JobCoordinatorListener mockListener = mock(JobCoordinatorListener.class);
    final JobModel mockJobModel = mock(JobModel.class);

    ZkJobCoordinator zkJobCoordinator = new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore);
    zkJobCoordinator.setListener(mockListener);
    zkJobCoordinator.setActiveJobModel(mockJobModel);

    testMethod.accept(zkJobCoordinator, mockJobModel);
    verificationMethod.accept(zkUtils, mockListener);
  }

  private void testJobModelVersionChangeHelper(JobModel activeJobModel, JobModel newJobModel,
      BiConsumer<ZkBarrierForVersionUpgrade, JobCoordinatorListener> verificationMethod) throws InterruptedException {
    final CountDownLatch completionLatch = new CountDownLatch(1);
    final JobCoordinatorListener mockListener = mock(JobCoordinatorListener.class);
    final ScheduleAfterDebounceTime mockDebounceTimer = mock(ScheduleAfterDebounceTime.class);
    final ZkBarrierForVersionUpgrade mockBarrier = mock(ZkBarrierForVersionUpgrade.class);

    doAnswer(ctx -> {
      Object[] args = ctx.getArguments();
      ((Runnable) args[2]).run();
      completionLatch.countDown();
      return null;
    }).when(mockDebounceTimer).scheduleAfterDebounceTime(anyString(), anyLong(), anyObject());


    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(PROCESSOR_ID, new MapConfig(),
        new NoOpMetricsRegistry(), zkUtils, zkMetadataStore, coordinatorStreamStore));
    zkJobCoordinator.setListener(mockListener);
    zkJobCoordinator.setActiveJobModel(activeJobModel);
    zkJobCoordinator.setZkBarrierUpgradeForVersion(mockBarrier);
    zkJobCoordinator.debounceTimer = mockDebounceTimer;
    doReturn(newJobModel).when(zkJobCoordinator).readJobModelFromMetadataStore(TEST_JOB_MODEL_VERSION);

    final ZkJobCoordinator.ZkJobModelVersionChangeHandler zkJobModelVersionChangeHandler =
        zkJobCoordinator.new ZkJobModelVersionChangeHandler(zkUtils);
    zkJobModelVersionChangeHandler.doHandleDataChange("path", TEST_JOB_MODEL_VERSION);
    completionLatch.await(1, TimeUnit.SECONDS);

    verificationMethod.accept(mockBarrier, mockListener);
  }
}
