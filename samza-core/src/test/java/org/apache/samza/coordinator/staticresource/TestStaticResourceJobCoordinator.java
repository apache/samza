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
package org.apache.samza.coordinator.staticresource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelHelper;
import org.apache.samza.coordinator.MetadataResourceUtil;
import org.apache.samza.coordinator.communication.CoordinatorCommunication;
import org.apache.samza.coordinator.communication.JobInfoServingContext;
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
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
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
  private static final String PROCESSOR_ID = "samza-job-coordinator";
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
  private JobInfoServingContext jobModelServingContext;
  @Mock
  private CoordinatorCommunication coordinatorCommunication;
  @Mock
  private JobCoordinatorMetadataManager jobCoordinatorMetadataManager;
  @Mock
  private StartpointManager startpointManager;
  @Mock
  private ChangelogStreamManager changelogStreamManager;
  @Mock
  private Map<TaskName, Integer> changelogPartitionMapping;
  @Mock
  private MetricsRegistryMap metrics;
  @Mock
  private SystemAdmins systemAdmins;
  @Mock
  private Config config;
  @Mock
  private JobCoordinatorListener jobCoordinatorListener;

  private StaticResourceJobCoordinator staticResourceJobCoordinator;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(this.changelogStreamManager.readPartitionMapping()).thenReturn(this.changelogPartitionMapping);
    this.staticResourceJobCoordinator =
        spy(new StaticResourceJobCoordinator(PROCESSOR_ID, this.jobModelHelper, this.jobModelServingContext,
            this.coordinatorCommunication, this.jobCoordinatorMetadataManager, this.startpointManager,
            this.changelogStreamManager, this.metrics, this.systemAdmins, this.config));
    this.staticResourceJobCoordinator.setListener(this.jobCoordinatorListener);
  }

  @Test
  public void testNoExistingJobModel() throws IOException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    JobCoordinatorMetadata newMetadata = setupJobCoordinatorMetadata(jobModel, jobModelConfig,
        ImmutableSet.copyOf(Arrays.asList(JobMetadataChange.values())), false);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);

    this.staticResourceJobCoordinator.start();
    assertEquals(jobModel, this.staticResourceJobCoordinator.getJobModel());
    verifyStartLifecycle();
    verifyPrepareWorkerExecution(jobModel, metadataResourceUtil, newMetadata, SINGLE_SSP_FANOUT);
    verify(this.jobCoordinatorListener).onNewJobModel(PROCESSOR_ID, jobModel);
  }

  @Test
  public void testSameJobModelAsPrevious() throws IOException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    setupJobCoordinatorMetadata(jobModel, jobModelConfig, ImmutableSet.of(), true);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);

    this.staticResourceJobCoordinator.start();
    assertEquals(jobModel, this.staticResourceJobCoordinator.getJobModel());
    verifyStartLifecycle();
    verifyPrepareWorkerExecution(jobModel, metadataResourceUtil, null, null);
    verify(this.jobCoordinatorListener).onNewJobModel(PROCESSOR_ID, jobModel);
  }

  @Test
  public void testNewDeploymentNewJobModel() throws IOException {
    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    JobCoordinatorMetadata newMetadata = setupJobCoordinatorMetadata(jobModel, jobModelConfig,
        ImmutableSet.of(JobMetadataChange.NEW_DEPLOYMENT, JobMetadataChange.JOB_MODEL), true);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);

    this.staticResourceJobCoordinator.start();
    assertEquals(jobModel, this.staticResourceJobCoordinator.getJobModel());
    verifyStartLifecycle();
    verifyPrepareWorkerExecution(jobModel, metadataResourceUtil, newMetadata, SINGLE_SSP_FANOUT);
    verify(this.jobCoordinatorListener).onNewJobModel(PROCESSOR_ID, jobModel);
  }

  @Test
  public void testStop() {
    this.staticResourceJobCoordinator.stop();

    verify(this.jobCoordinatorListener).onJobModelExpired();
    verify(this.coordinatorCommunication).stop();
    verify(this.startpointManager).stop();
    verify(this.systemAdmins).stop();
    verify(this.jobCoordinatorListener).onCoordinatorStop();
  }

  /**
   * Missing {@link StartpointManager} and {@link JobCoordinatorListener}.
   */
  @Test
  public void testStartMissingOptionalComponents() throws IOException {
    this.staticResourceJobCoordinator =
        spy(new StaticResourceJobCoordinator(PROCESSOR_ID, this.jobModelHelper, this.jobModelServingContext,
            this.coordinatorCommunication, this.jobCoordinatorMetadataManager, null, this.changelogStreamManager,
            this.metrics, this.systemAdmins, this.config));

    Config jobModelConfig = mock(Config.class);
    JobModel jobModel = setupJobModel(jobModelConfig);
    JobCoordinatorMetadata newMetadata = setupJobCoordinatorMetadata(jobModel, jobModelConfig,
        ImmutableSet.copyOf(Arrays.asList(JobMetadataChange.values())), false);
    MetadataResourceUtil metadataResourceUtil = metadataResourceUtil(jobModel);

    this.staticResourceJobCoordinator.start();
    assertEquals(jobModel, this.staticResourceJobCoordinator.getJobModel());
    verify(this.systemAdmins).start();
    verifyPrepareWorkerExecution(jobModel, metadataResourceUtil, newMetadata, null);
  }

  @Test
  public void testStopMissingOptionalComponents() {
    this.staticResourceJobCoordinator =
        spy(new StaticResourceJobCoordinator(PROCESSOR_ID, this.jobModelHelper, this.jobModelServingContext,
            this.coordinatorCommunication, this.jobCoordinatorMetadataManager, null, this.changelogStreamManager,
            this.metrics, this.systemAdmins, this.config));

    this.staticResourceJobCoordinator.stop();

    verify(this.coordinatorCommunication).stop();
    verify(this.systemAdmins).stop();
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

  private void verifyStartLifecycle() {
    verify(this.systemAdmins).start();
    verify(this.startpointManager).start();
  }

  /**
   * Common steps to verify when preparing workers for processing.
   * @param jobModel job model to be served for workers
   * @param metadataResourceUtil expected to be used for creating resources
   * @param newMetadata if not null, expected to be written to {@link JobCoordinatorMetadataManager}
   * @param expectedFanOut if not null, expected to be passed to {@link StartpointManager} for fan out
   */
  private void verifyPrepareWorkerExecution(JobModel jobModel, MetadataResourceUtil metadataResourceUtil,
      JobCoordinatorMetadata newMetadata, Map<TaskName, Set<SystemStreamPartition>> expectedFanOut) throws IOException {
    InOrder inOrder = inOrder(this.jobCoordinatorMetadataManager, this.jobModelServingContext, metadataResourceUtil,
        this.startpointManager, this.coordinatorCommunication);
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
  }
}