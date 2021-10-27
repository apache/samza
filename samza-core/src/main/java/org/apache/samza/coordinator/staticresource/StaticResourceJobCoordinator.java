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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.annotations.VisibleForTesting;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelHelper;
import org.apache.samza.coordinator.JobModelMonitors;
import org.apache.samza.coordinator.MetadataResourceUtil;
import org.apache.samza.coordinator.StreamPartitionCountMonitor;
import org.apache.samza.coordinator.StreamPartitionCountMonitorFactory;
import org.apache.samza.coordinator.StreamRegexMonitor;
import org.apache.samza.coordinator.StreamRegexMonitorFactory;
import org.apache.samza.coordinator.communication.CoordinatorCommunication;
import org.apache.samza.coordinator.communication.JobInfoServingContext;
import org.apache.samza.coordinator.lifecycle.JobRestartSignal;
import org.apache.samza.job.JobCoordinatorMetadata;
import org.apache.samza.job.JobMetadataChange;
import org.apache.samza.job.metadata.JobCoordinatorMetadataManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.JobModelUtil;
import org.apache.samza.logging.LoggingContextHolder;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.system.SystemAdmins;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles coordination with the workers for a Samza job. This includes generating the job model, managing metadata,
 * and coordinating with the workers when the job model changes.
 * This does certain similar things as the older ClusterBasedJobCoordinator, but one notable difference is that this
 * coordinator does no management of execution resources. It relies on an external component to manage those resources
 * for a Samza job.
 */
public class StaticResourceJobCoordinator implements JobCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(StaticResourceJobCoordinator.class);

  private final JobModelHelper jobModelHelper;
  private final JobInfoServingContext jobModelServingContext;
  private final CoordinatorCommunication coordinatorCommunication;
  private final JobCoordinatorMetadataManager jobCoordinatorMetadataManager;
  private final StreamPartitionCountMonitorFactory streamPartitionCountMonitorFactory;
  private final StreamRegexMonitorFactory streamRegexMonitorFactory;
  private final Optional<StartpointManager> startpointManager;
  private final ChangelogStreamManager changelogStreamManager;
  private final JobRestartSignal jobRestartSignal;
  private final MetricsRegistry metrics;
  private final SystemAdmins systemAdmins;
  private final String processorId;
  private final Config config;

  private Optional<JobCoordinatorListener> jobCoordinatorListener = Optional.empty();

  /**
   * Job model is calculated during {@link #start()}, so it is not immediately available.
   */
  private Optional<JobModel> currentJobModel = Optional.empty();
  /**
   * {@link JobModelMonitors} depend on job model, so they are only available after {@link #start()}.
   */
  private Optional<JobModelMonitors> currentJobModelMonitors = Optional.empty();
  /**
   * Keeps track of if the job coordinator has completed all preparation for running the job, including
   * publishing a new job model and starting the job model monitors.
   */
  private AtomicBoolean jobPreparationComplete = new AtomicBoolean(false);

  StaticResourceJobCoordinator(String processorId, JobModelHelper jobModelHelper,
      JobInfoServingContext jobModelServingContext, CoordinatorCommunication coordinatorCommunication,
      JobCoordinatorMetadataManager jobCoordinatorMetadataManager,
      StreamPartitionCountMonitorFactory streamPartitionCountMonitorFactory,
      StreamRegexMonitorFactory streamRegexMonitorFactory, StartpointManager startpointManager,
      ChangelogStreamManager changelogStreamManager, JobRestartSignal jobRestartSignal, MetricsRegistry metrics,
      SystemAdmins systemAdmins, Config config) {
    this.jobModelHelper = jobModelHelper;
    this.jobModelServingContext = jobModelServingContext;
    this.coordinatorCommunication = coordinatorCommunication;
    this.jobCoordinatorMetadataManager = jobCoordinatorMetadataManager;
    this.streamPartitionCountMonitorFactory = streamPartitionCountMonitorFactory;
    this.streamRegexMonitorFactory = streamRegexMonitorFactory;
    this.startpointManager = Optional.ofNullable(startpointManager);
    this.changelogStreamManager = changelogStreamManager;
    this.jobRestartSignal = jobRestartSignal;
    this.metrics = metrics;
    this.systemAdmins = systemAdmins;
    this.processorId = processorId;
    this.config = config;
  }

  @Override
  public void start() {
    LOG.info("Starting job coordinator");
    this.systemAdmins.start();
    this.startpointManager.ifPresent(StartpointManager::start);
    try {
      JobModel jobModel = newJobModel();
      doSetLoggingContextConfig(jobModel.getConfig());
      // monitors should be created right after job model is calculated (see jobModelMonitors() for more details)
      JobModelMonitors jobModelMonitors = jobModelMonitors(jobModel);
      JobCoordinatorMetadata newMetadata =
          this.jobCoordinatorMetadataManager.generateJobCoordinatorMetadata(jobModel, jobModel.getConfig());
      Set<JobMetadataChange> jobMetadataChanges = checkForMetadataChanges(newMetadata);
      if (!jobMetadataChanges.isEmpty() && !jobMetadataChanges.contains(JobMetadataChange.NEW_DEPLOYMENT)) {
        /*
         * If the job coordinator comes up, but not due to a new deployment, and the metadata changed, then trigger a
         * restart. This case applies if the job coordinator died and the job model needed to change while it was down.
         * If there were no metadata changes, then just let the current workers continue to run.
         * If there was a new deployment (which includes the case where the coordinator requested a restart), then we
         * rely on the external resource manager to make sure the previous workers restarted, so we don't need to
         * restart again.
         */
        LOG.info("Triggering job restart");
        this.jobRestartSignal.restartJob();
      } else {
        prepareWorkerExecution(jobModel, newMetadata, jobMetadataChanges);
        this.coordinatorCommunication.start();
        this.currentJobModel = Optional.of(jobModel);
        this.jobCoordinatorListener.ifPresent(listener -> listener.onNewJobModel(this.processorId, jobModel));
        this.currentJobModelMonitors = Optional.of(jobModelMonitors);
        jobModelMonitors.start();
        this.jobPreparationComplete.set(true);
      }
    } catch (Exception e) {
      LOG.error("Error while running job coordinator; exiting", e);
      throw new SamzaException("Error while running job coordinator", e);
    }
  }

  @Override
  public void stop() {
    try {
      this.jobCoordinatorListener.ifPresent(JobCoordinatorListener::onJobModelExpired);
      if (this.jobPreparationComplete.get()) {
        this.currentJobModelMonitors.ifPresent(JobModelMonitors::stop);
        this.coordinatorCommunication.stop();
      }
      this.startpointManager.ifPresent(StartpointManager::stop);
      this.systemAdmins.stop();
    } finally {
      this.jobCoordinatorListener.ifPresent(JobCoordinatorListener::onCoordinatorStop);
    }
  }

  @Override
  public String getProcessorId() {
    return this.processorId;
  }

  @Override
  public void setListener(JobCoordinatorListener jobCoordinatorListener) {
    this.jobCoordinatorListener = Optional.ofNullable(jobCoordinatorListener);
  }

  @Override
  public JobModel getJobModel() {
    return this.currentJobModel.orElse(null);
  }

  private JobModel newJobModel() {
    return this.jobModelHelper.newJobModel(this.config, this.changelogStreamManager.readPartitionMapping());
  }

  /*
   * Possible race condition: The partition count monitor queries for stream metadata when it is created, so if the
   * partition counts changed between the job model calculation and the creation of the partition count monitor, then
   * the monitor will not trigger an update to the job model. This method should be called right after calculating the
   * job model, in order to reduce the possible time in which a partition count change is missed. This issue also
   * exists in the older ClusterBasedJobCoordinator.
   * TODO This wouldn't be a problem if the partition count monitor used the job model to calculate initial metadata
   */
  private JobModelMonitors jobModelMonitors(JobModel jobModel) {
    StreamPartitionCountMonitor streamPartitionCountMonitor =
        this.streamPartitionCountMonitorFactory.build(jobModel.getConfig(),
          streamsChanged -> this.jobRestartSignal.restartJob());
    Optional<StreamRegexMonitor> streamRegexMonitor =
        this.streamRegexMonitorFactory.build(jobModel, jobModel.getConfig(),
          (initialInputSet, newInputStreams, regexesMonitored) -> this.jobRestartSignal.restartJob());
    return new JobModelMonitors(streamPartitionCountMonitor, streamRegexMonitor.orElse(null));
  }

  /**
   * This is a helper method so that we can verify it is called in testing.
   */
  @VisibleForTesting
  void doSetLoggingContextConfig(Config config) {
    LoggingContextHolder.INSTANCE.setConfig(config);
  }

  /**
   * Run set up steps so that workers can begin processing:
   * 1. Persist job coordinator metadata
   * 2. Publish new job model on coordinator-to-worker communication channel
   * 3. Create metadata resources
   * 4. Handle startpoints
   */
  private void prepareWorkerExecution(JobModel jobModel, JobCoordinatorMetadata newMetadata,
      Set<JobMetadataChange> jobMetadataChanges) throws IOException {
    if (!jobMetadataChanges.isEmpty()) {
      this.jobCoordinatorMetadataManager.writeJobCoordinatorMetadata(newMetadata);
    }
    this.jobModelServingContext.setJobModel(jobModel);

    metadataResourceUtil(jobModel).createResources();

    // the fan out trigger logic comes from ClusterBasedJobCoordinator, in which a new job model can trigger a fan out
    if (this.startpointManager.isPresent() && !jobMetadataChanges.isEmpty()) {
      this.startpointManager.get().fanOut(JobModelUtil.getTaskToSystemStreamPartitions(jobModel));
    }
  }

  @VisibleForTesting
  MetadataResourceUtil metadataResourceUtil(JobModel jobModel) {
    return new MetadataResourceUtil(jobModel, this.metrics, this.config);
  }

  private Set<JobMetadataChange> checkForMetadataChanges(JobCoordinatorMetadata newMetadata) {
    JobCoordinatorMetadata previousMetadata = this.jobCoordinatorMetadataManager.readJobCoordinatorMetadata();
    return this.jobCoordinatorMetadataManager.checkForMetadataChanges(newMetadata, previousMetadata);
  }
}
