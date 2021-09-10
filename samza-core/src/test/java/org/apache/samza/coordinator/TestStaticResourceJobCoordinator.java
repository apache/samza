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
package org.apache.samza.coordinator;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.communication.CoordinatorCommunication;
import org.apache.samza.coordinator.communication.JobModelServingContext;
import org.apache.samza.coordinator.lifecycle.JobRestartSignal;
import org.apache.samza.job.JobCoordinatorMetadata;
import org.apache.samza.job.JobMetadataChange;
import org.apache.samza.job.metadata.JobCoordinatorMetadataManager;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * In general, these tests start the {@link StaticResourceJobCoordinator} in a separate thread and then execute certain
 * actions (trigger callbacks which will change the job model, trigger shutdown) to check the coordination flow.
 */
public class TestStaticResourceJobCoordinator {
  private static final SystemStream SYSTEM_STREAM = new SystemStream("system", "stream");
  private static final TaskName TASK_NAME = new TaskName("Partition " + 0);
  private static final Map<String, ContainerModel> CONTAINERS = ImmutableMap.of("0", new ContainerModel("0",
      ImmutableMap.of(TASK_NAME,
          new TaskModel(TASK_NAME, ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM, new Partition(0))),
              new Partition(0)))));
  private static final Map<TaskName, Set<SystemStreamPartition>> SINGLE_SSP_FANOUT =
      ImmutableMap.of(TASK_NAME, ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM, new Partition(0))));

  @Mock
  private JobModelHelper jobModelHelper;
  @Mock
  private JobModelServingContext jobModelServingContext;
  @Mock
  private CoordinatorCommunication coordinatorCommunication;
  @Mock
  private JobCoordinatorMetadataManager jobCoordinatorMetadataManager;
  @Mock
  private StreamPartitionCountMonitorFactory streamPartitionCountMonitorFactory;
  @Mock
  private StreamRegexMonitorFactory streamRegexMonitorFactory;
  @Mock
  private StartpointManager startpointManager;
  @Mock
  private ChangelogStreamManager changelogStreamManager;
  @Mock
  private JobRestartSignal jobRestartSignal;
  @Mock
  private Map<TaskName, Integer> changelogPartitionMapping;
  @Mock
  private MetricsRegistryMap metrics;
  @Mock
  private SystemAdmins systemAdmins;
  @Mock
  private Config config;

  private StaticResourceJobCoordinator staticResourceJobCoordinator;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(this.changelogStreamManager.readPartitionMapping()).thenReturn(this.changelogPartitionMapping);
    this.staticResourceJobCoordinator =
        spy(new StaticResourceJobCoordinator(this.jobModelHelper, this.jobModelServingContext,
            this.coordinatorCommunication, this.jobCoordinatorMetadataManager, this.streamPartitionCountMonitorFactory,
            this.streamRegexMonitorFactory, this.startpointManager, this.changelogStreamManager, this.jobRestartSignal,
            this.metrics, this.systemAdmins, this.config));
  }

  @Test
  public void testNoExistingJobModel() throws InterruptedException, IOException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = setupStreamPartitionCountMonitor(jobModelConfig);
    StreamRegexMonitor streamRegexMonitor = setupStreamRegexMonitor(jobModel, jobModelConfig);
    JobCoordinatorMetadata newMetadata = setupJobCoordinatorMetadata(jobModel, jobModelConfig,
        ImmutableSet.copyOf(Arrays.asList(JobMetadataChange.values())), false);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);
    Thread jobCoordinatorThread = startCoordinatorAndWaitForMonitor(streamRegexMonitor);
    verifyStartLifecycle();
    verifyPrepareWorkerExecutionAndMonitor(jobModel, metadataResourceUtil, streamPartitionCountMonitor,
        streamRegexMonitor, newMetadata, SINGLE_SSP_FANOUT);
    shutdownJobCoordinator(jobCoordinatorThread);
    verifyStopLifecycle();
  }

  @Test
  public void testSameJobModelAsPrevious() throws IOException, InterruptedException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = setupStreamPartitionCountMonitor(jobModelConfig);
    StreamRegexMonitor streamRegexMonitor = setupStreamRegexMonitor(jobModel, jobModelConfig);
    setupJobCoordinatorMetadata(jobModel, jobModelConfig, ImmutableSet.of(), true);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);
    Thread jobCoordinatorThread = startCoordinatorAndWaitForMonitor(streamRegexMonitor);
    verifyStartLifecycle();
    verifyPrepareWorkerExecutionAndMonitor(jobModel, metadataResourceUtil, streamPartitionCountMonitor,
        streamRegexMonitor, null, null);
    shutdownJobCoordinator(jobCoordinatorThread);
    verifyStopLifecycle();
  }

  @Test
  public void testSameDeploymentWithNewJobModel() throws InterruptedException, IOException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = setupStreamPartitionCountMonitor(jobModelConfig);
    StreamRegexMonitor streamRegexMonitor = setupStreamRegexMonitor(jobModel, jobModelConfig);
    setupJobCoordinatorMetadata(jobModel, jobModelConfig, ImmutableSet.of(JobMetadataChange.JOB_MODEL), true);
    Thread jobCoordinatorThread = startCoordinatorAndWaitForRestart();
    verifyStartLifecycle();
    verify(this.jobRestartSignal).restartJob();
    shutdownJobCoordinator(jobCoordinatorThread);
    verifyStopLifecycle();
    verifyNoSideEffects(streamPartitionCountMonitor, streamRegexMonitor);
  }

  @Test
  public void testNewDeploymentNewJobModel() throws InterruptedException, IOException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = setupStreamPartitionCountMonitor(jobModelConfig);
    StreamRegexMonitor streamRegexMonitor = setupStreamRegexMonitor(jobModel, jobModelConfig);
    JobCoordinatorMetadata newMetadata = setupJobCoordinatorMetadata(jobModel, jobModelConfig,
        ImmutableSet.of(JobMetadataChange.NEW_DEPLOYMENT, JobMetadataChange.JOB_MODEL), true);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);
    Thread jobCoordinatorThread = startCoordinatorAndWaitForMonitor(streamRegexMonitor);
    verifyStartLifecycle();
    verifyPrepareWorkerExecutionAndMonitor(jobModel, metadataResourceUtil, streamPartitionCountMonitor,
        streamRegexMonitor, newMetadata, SINGLE_SSP_FANOUT);
    shutdownJobCoordinator(jobCoordinatorThread);
    verifyStopLifecycle();
  }

  @Test
  public void testPartitionCountChange() throws IOException, InterruptedException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = mock(StreamPartitionCountMonitor.class);
    ArgumentCaptor<StreamPartitionCountMonitor.Callback> callbackArgumentCaptor =
        ArgumentCaptor.forClass(StreamPartitionCountMonitor.Callback.class);
    when(
        this.streamPartitionCountMonitorFactory.build(eq(jobModelConfig), callbackArgumentCaptor.capture())).thenReturn(
        streamPartitionCountMonitor);
    StreamRegexMonitor streamRegexMonitor = setupStreamRegexMonitor(jobModel, jobModelConfig);
    JobCoordinatorMetadata newMetadata = setupJobCoordinatorMetadata(jobModel, jobModelConfig,
        ImmutableSet.of(JobMetadataChange.NEW_DEPLOYMENT, JobMetadataChange.JOB_MODEL), true);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);
    Thread jobCoordinatorThread = startCoordinatorAndWaitForMonitor(streamRegexMonitor);
    verifyStartLifecycle();
    verifyPrepareWorkerExecutionAndMonitor(jobModel, metadataResourceUtil, streamPartitionCountMonitor,
        streamRegexMonitor, newMetadata, SINGLE_SSP_FANOUT);
    // call the callback from the monitor
    callbackArgumentCaptor.getValue().onSystemStreamPartitionChange(ImmutableSet.of(SYSTEM_STREAM));
    verify(this.jobRestartSignal).restartJob();
    shutdownJobCoordinator(jobCoordinatorThread);
    verifyStopLifecycle();
  }

  @Test
  public void testStreamRegexChange() throws IOException, InterruptedException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = setupStreamPartitionCountMonitor(jobModelConfig);
    StreamRegexMonitor streamRegexMonitor = mock(StreamRegexMonitor.class);
    ArgumentCaptor<StreamRegexMonitor.Callback> callbackArgumentCaptor =
        ArgumentCaptor.forClass(StreamRegexMonitor.Callback.class);
    when(this.streamRegexMonitorFactory.build(eq(jobModel), eq(jobModelConfig),
        callbackArgumentCaptor.capture())).thenReturn(Optional.of(streamRegexMonitor));
    JobCoordinatorMetadata newMetadata = setupJobCoordinatorMetadata(jobModel, jobModelConfig,
        ImmutableSet.of(JobMetadataChange.NEW_DEPLOYMENT, JobMetadataChange.JOB_MODEL), true);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);
    Thread jobCoordinatorThread = startCoordinatorAndWaitForMonitor(streamRegexMonitor);
    verifyStartLifecycle();
    verifyPrepareWorkerExecutionAndMonitor(jobModel, metadataResourceUtil, streamPartitionCountMonitor,
        streamRegexMonitor, newMetadata, SINGLE_SSP_FANOUT);
    // call the callback from the monitor
    callbackArgumentCaptor.getValue()
        .onInputStreamsChanged(ImmutableSet.of(SYSTEM_STREAM),
            ImmutableSet.of(SYSTEM_STREAM, new SystemStream("system", "stream1")),
            ImmutableMap.of("system", Pattern.compile("stream.*")));
    verify(this.jobRestartSignal).restartJob();
    shutdownJobCoordinator(jobCoordinatorThread);
    verifyStopLifecycle();
  }

  @Test
  public void testInterruptedAfterTriggeringRestart() throws InterruptedException, IOException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = setupStreamPartitionCountMonitor(jobModelConfig);
    StreamRegexMonitor streamRegexMonitor = setupStreamRegexMonitor(jobModel, jobModelConfig);
    setupJobCoordinatorMetadata(jobModel, jobModelConfig, ImmutableSet.of(JobMetadataChange.JOB_MODEL), true);
    Thread jobCoordinatorThread = startCoordinatorAndWaitForRestart();
    verifyStartLifecycle();
    verify(this.jobRestartSignal).restartJob();
    jobCoordinatorThread.interrupt();
    // coordinator thread should exit on its own
    assertThreadExited(jobCoordinatorThread);
    verifyStopLifecycle();
    verifyNoSideEffects(streamPartitionCountMonitor, streamRegexMonitor);
  }

  @Test
  public void testInterruptedWhileMonitoring() throws InterruptedException, IOException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = setupStreamPartitionCountMonitor(jobModelConfig);
    StreamRegexMonitor streamRegexMonitor = setupStreamRegexMonitor(jobModel, jobModelConfig);
    JobCoordinatorMetadata newMetadata = setupJobCoordinatorMetadata(jobModel, jobModelConfig,
        ImmutableSet.of(JobMetadataChange.NEW_DEPLOYMENT, JobMetadataChange.JOB_MODEL), true);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);
    Thread jobCoordinatorThread = startCoordinatorAndWaitForMonitor(streamRegexMonitor);
    verifyStartLifecycle();
    verifyPrepareWorkerExecutionAndMonitor(jobModel, metadataResourceUtil, streamPartitionCountMonitor,
        streamRegexMonitor, newMetadata, SINGLE_SSP_FANOUT);
    jobCoordinatorThread.interrupt();
    // coordinator thread should exit on its own
    assertThreadExited(jobCoordinatorThread);
    verifyStopLifecycle();
  }

  @Test
  public void testShutdownBeforeTriggerRestartComplete() throws InterruptedException, IOException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = setupStreamPartitionCountMonitor(jobModelConfig);
    StreamRegexMonitor streamRegexMonitor = setupStreamRegexMonitor(jobModel, jobModelConfig);
    setupJobCoordinatorMetadata(jobModel, jobModelConfig, ImmutableSet.of(JobMetadataChange.CONFIG), true);
    CountDownLatch restartSignalCompleteLatch = new CountDownLatch(1);
    // block restart signal completion so that test can trigger a shutdown first
    doAnswer(invocation -> {
      restartSignalCompleteLatch.await();
      return null;
    }).when(jobRestartSignal).restartJob();
    Thread jobCoordinatorThread = new Thread(() -> this.staticResourceJobCoordinator.run());
    jobCoordinatorThread.start();
    // signal shutdown before restart signal completes
    this.staticResourceJobCoordinator.signalShutdown();
    // allow restart signal to complete
    restartSignalCompleteLatch.countDown();
    // job coordinator should exit on its own
    assertThreadExited(jobCoordinatorThread);
    verifyStopLifecycle();
    verifyNoSideEffects(streamPartitionCountMonitor, streamRegexMonitor);
  }

  @Test
  public void testShutdownBeforeStartMonitoring() throws InterruptedException, IOException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = setupStreamPartitionCountMonitor(jobModelConfig);
    StreamRegexMonitor streamRegexMonitor = setupStreamRegexMonitor(jobModel, jobModelConfig);
    JobCoordinatorMetadata newMetadata = setupJobCoordinatorMetadata(jobModel, jobModelConfig,
        ImmutableSet.of(JobMetadataChange.NEW_DEPLOYMENT, JobMetadataChange.JOB_MODEL), true);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);
    CountDownLatch streamRegexMonitorStartLatch = new CountDownLatch(1);
    // block restart signal completion so that test can trigger a shutdown first
    doAnswer(invocation -> {
      streamRegexMonitorStartLatch.await();
      return null;
    }).when(streamRegexMonitor).start();
    Thread jobCoordinatorThread = new Thread(() -> this.staticResourceJobCoordinator.run());
    jobCoordinatorThread.start();
    // signal shutdown before monitor start completes
    this.staticResourceJobCoordinator.signalShutdown();
    // allow restart signal to complete
    streamRegexMonitorStartLatch.countDown();
    // job coordinator should exit on its own
    assertThreadExited(jobCoordinatorThread);
    verifyPrepareWorkerExecutionAndMonitor(jobModel, metadataResourceUtil, streamPartitionCountMonitor,
        streamRegexMonitor, newMetadata, SINGLE_SSP_FANOUT);
    verifyStopLifecycle();
  }

  private static void assertThreadExited(Thread thread) throws InterruptedException {
    thread.join(Duration.ofSeconds(10).toMillis());
    assertFalse(thread.isAlive());
  }

  /**
   * Set up {@link StreamPartitionCountMonitorFactory} to return a mock {@link StreamPartitionCountMonitor}.
   */
  private StreamPartitionCountMonitor setupStreamPartitionCountMonitor(Config config) {
    StreamPartitionCountMonitor streamPartitionCountMonitor = mock(StreamPartitionCountMonitor.class);
    when(this.streamPartitionCountMonitorFactory.build(eq(config), any())).thenReturn(streamPartitionCountMonitor);
    return streamPartitionCountMonitor;
  }

  /**
   * Set up {@link StreamRegexMonitorFactory} to return a mock {@link StreamRegexMonitor}.
   */
  private StreamRegexMonitor setupStreamRegexMonitor(JobModel jobModel, Config jobModelConfig) {
    StreamRegexMonitor streamRegexMonitor = mock(StreamRegexMonitor.class);
    when(this.streamRegexMonitorFactory.build(eq(jobModel), eq(jobModelConfig), any())).thenReturn(
        Optional.of(streamRegexMonitor));
    return streamRegexMonitor;
  }

  /**
   * Set up {@link JobModel} and {@link JobModelHelper} to return job model.
   */
  private JobModel setupJobModel(Config config) {
    JobModel jobModel = mock(JobModel.class);
    when(jobModel.getContainers()).thenReturn(CONTAINERS);
    when(jobModel.getConfig()).thenReturn(config);
    when(this.jobModelHelper.newJobModel(this.config, this.changelogPartitionMapping)).thenReturn(jobModel);
    return jobModel;
  }

  /**
   * Set up mocks for {@link JobCoordinatorMetadataManager}.
   * {@code jobMetadataChanges} defines which changes should be detected by the {@link JobCoordinatorMetadataManager}
   */
  private JobCoordinatorMetadata setupJobCoordinatorMetadata(JobModel jobModel, Config jobModelConfig,
      Set<JobMetadataChange> jobMetadataChanges, boolean hasPreviousMetadata) {
    JobCoordinatorMetadata previousMetadata = hasPreviousMetadata ? mock(JobCoordinatorMetadata.class) : null;
    JobCoordinatorMetadata newMetadata = mock(JobCoordinatorMetadata.class);
    when(this.jobCoordinatorMetadataManager.generateJobCoordinatorMetadata(jobModel, jobModelConfig)).thenReturn(
        newMetadata);
    when(this.jobCoordinatorMetadataManager.readJobCoordinatorMetadata()).thenReturn(previousMetadata);
    if (hasPreviousMetadata) {
      when(this.jobCoordinatorMetadataManager.checkForMetadataChanges(newMetadata, previousMetadata)).thenReturn(
          jobMetadataChanges);
    } else {
      when(this.jobCoordinatorMetadataManager.checkForMetadataChanges(eq(newMetadata),
          isNull(JobCoordinatorMetadata.class))).thenReturn(jobMetadataChanges);
    }
    return newMetadata;
  }

  private MetadataResourceUtil metadataResourceUtil(JobModel jobModel) {
    MetadataResourceUtil metadataResourceUtil = mock(MetadataResourceUtil.class);
    doReturn(metadataResourceUtil).when(this.staticResourceJobCoordinator).metadataResourceUtil(jobModel);
    return metadataResourceUtil;
  }

  /**
   * Attaches a {@link CountDownLatch} to {@link StreamRegexMonitor} so that the tests know when the job coordinator is
   * has started monitoring. {@link StreamRegexMonitor} is used because it is one of the last things that gets called
   * before the job coordinator transitions to waiting for shutdown.
   */
  private CountDownLatch waitingForShutdownLatch(StreamRegexMonitor streamRegexMonitor) {
    CountDownLatch waitingForShutdown = new CountDownLatch(1);
    doAnswer(invocation -> {
      waitingForShutdown.countDown();
      return null;
    }).when(streamRegexMonitor).start();
    return waitingForShutdown;
  }

  /**
   * Attaches a {@link CountDownLatch} to {@link JobRestartSignal} so that the tests know when the job coordinator is
   * has started monitoring. {@link JobRestartSignal} is used because it is one of the last things that gets called
   * before the job coordinator transitions to waiting for shutdown.
   */
  private CountDownLatch waitingForShutdownLatch(JobRestartSignal jobRestartSignal) {
    CountDownLatch waitingForShutdown = new CountDownLatch(1);
    doAnswer(invocation -> {
      waitingForShutdown.countDown();
      return null;
    }).when(jobRestartSignal).restartJob();
    return waitingForShutdown;
  }

  private void verifyStartLifecycle() {
    verify(this.systemAdmins).start();
    verify(this.startpointManager).start();
  }

  private void verifyStopLifecycle() {
    verify(this.coordinatorCommunication).stop();
    verify(this.startpointManager).stop();
    verify(this.systemAdmins).stop();
  }

  /**
   * Signal a shutdown to the job coordinator and then wait for the thread to exit.
   */
  private void shutdownJobCoordinator(Thread jobCoordinatorThread) throws InterruptedException {
    this.staticResourceJobCoordinator.signalShutdown();
    jobCoordinatorThread.join(Duration.ofSeconds(10).toMillis());
    assertFalse(jobCoordinatorThread.isAlive());
  }

  /**
   * Common steps to verify when preparing workers for processing.
   * @param jobModel job model to be served for workers
   * @param metadataResourceUtil expected to be used for creating resources
   * @param streamPartitionCountMonitor expected to be started
   * @param streamRegexMonitor expected to be started
   * @param newMetadata if not null, expected to be written to {@link JobCoordinatorMetadataManager}
   * @param expectedFanOut if not null, expected to be passed to {@link StartpointManager} for fan out
   */
  private void verifyPrepareWorkerExecutionAndMonitor(JobModel jobModel, MetadataResourceUtil metadataResourceUtil,
      StreamPartitionCountMonitor streamPartitionCountMonitor, StreamRegexMonitor streamRegexMonitor,
      JobCoordinatorMetadata newMetadata, Map<TaskName, Set<SystemStreamPartition>> expectedFanOut) throws IOException {
    InOrder inOrder = inOrder(this.jobCoordinatorMetadataManager, this.jobModelServingContext, metadataResourceUtil,
        this.startpointManager, this.coordinatorCommunication, streamPartitionCountMonitor, streamRegexMonitor);
    if (newMetadata != null) {
      inOrder.verify(this.jobCoordinatorMetadataManager).writeJobCoordinatorMetadata(newMetadata);
    } else {
      verify(this.jobCoordinatorMetadataManager, never()).writeJobCoordinatorMetadata(any());
    }
    inOrder.verify(this.jobModelServingContext).setJobModel(jobModel);
    inOrder.verify(metadataResourceUtil).createResources();
    if (expectedFanOut != null) {
      inOrder.verify(this.startpointManager).fanOut(expectedFanOut);
    } else {
      verify(this.startpointManager, never()).fanOut(any());
    }
    inOrder.verify(this.coordinatorCommunication).start();
    inOrder.verify(streamPartitionCountMonitor).start();
    inOrder.verify(streamRegexMonitor).start();
  }

  /**
   * Start the job coordinator in a separate thread and wait until the job coordinator starts its monitors.
   * Also verifies that coordinator thread stays alive for some time after monitors are started.
   * Returns the thread running the job coordinator.
   */
  private Thread startCoordinatorAndWaitForMonitor(StreamRegexMonitor streamRegexMonitor) throws InterruptedException {
    return startCoordinatorAndWaitForLatch(waitingForShutdownLatch(streamRegexMonitor));
  }

  /**
   * Start the job coordinator in a separate thread and wait until the job coordinator signals a restart.
   * Also verifies that coordinator thread stays alive for some time after restart is signalled.
   * Returns the thread running the job coordinator.
   */
  private Thread startCoordinatorAndWaitForRestart() throws InterruptedException {
    return startCoordinatorAndWaitForLatch(waitingForShutdownLatch(this.jobRestartSignal));
  }

  private Thread startCoordinatorAndWaitForLatch(CountDownLatch countDownLatch) throws InterruptedException {
    Thread jobCoordinatorThread = new Thread(() -> this.staticResourceJobCoordinator.run());
    jobCoordinatorThread.start();
    assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
    // sleep for a little time and then make sure thread is still alive, since coordinator should be waiting
    Thread.sleep(Duration.ofMillis(500).toMillis());
    assertTrue(jobCoordinatorThread.isAlive());
    return jobCoordinatorThread;
  }

  private void verifyNoSideEffects(StreamPartitionCountMonitor streamPartitionCountMonitor,
      StreamRegexMonitor streamRegexMonitor) throws IOException {
    verify(this.jobCoordinatorMetadataManager, never()).writeJobCoordinatorMetadata(any());
    verify(this.jobModelServingContext, never()).setJobModel(any());
    verify(this.staticResourceJobCoordinator, never()).metadataResourceUtil(any());
    verify(this.startpointManager, never()).fanOut(any());
    verify(this.coordinatorCommunication, never()).start();
    verify(streamPartitionCountMonitor, never()).start();
    verify(streamRegexMonitor, never()).start();
  }
}