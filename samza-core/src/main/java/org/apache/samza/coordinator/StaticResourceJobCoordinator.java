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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.annotations.VisibleForTesting;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.container.grouper.task.TaskPartitionAssignmentManager;
import org.apache.samza.coordinator.communication.CoordinatorCommunication;
import org.apache.samza.coordinator.communication.CoordinatorCommunicationContext;
import org.apache.samza.coordinator.communication.HttpCoordinatorToWorkerCommunicationFactory;
import org.apache.samza.coordinator.communication.JobModelServingContext;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.coordinator.stream.messages.SetJobCoordinatorMetadataMessage;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskModeMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskPartitionMapping;
import org.apache.samza.job.JobCoordinatorMetadata;
import org.apache.samza.job.JobMetadataChange;
import org.apache.samza.job.metadata.JobCoordinatorMetadataManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.JobModelUtil;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles coordination with the workers for a Samza job. This includes generating the job model, managing metadata,
 * and coordinating with the workers when the job model changes.
 * This does certain similar things as the older ClusterBasedJobCoordinator, but one notable difference is that this
 * coordinator does no management of execution resources. It relies on an external component to manage those resources
 * for a Samza job.
 */
public class StaticResourceJobCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(StaticResourceJobCoordinator.class);

  private final JobModelHelper jobModelHelper;
  private final JobModelServingContext jobModelServingContext;
  private final CoordinatorCommunication coordinatorCommunication;
  private final JobCoordinatorMetadataManager jobCoordinatorMetadataManager;
  /**
   * This can be null if startpoints are not enabled.
   */
  private final StartpointManager startpointManager;
  private final ChangelogStreamManager changelogStreamManager;
  private final MetricsRegistry metrics;
  private final SystemAdmins systemAdmins;
  private final Config config;

  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final AtomicBoolean shouldShutdown = new AtomicBoolean(false);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);

  public static StaticResourceJobCoordinator build(MetricsRegistry metrics, MetadataStore metadataStore,
      Config config) {
    JobModelServingContext jobModelServingContext = new JobModelServingContext();
    JobConfig jobConfig = new JobConfig(config);
    CoordinatorCommunicationContext context =
        new CoordinatorCommunicationContext(jobModelServingContext, config, metrics);
    CoordinatorCommunication coordinatorCommunication =
        new HttpCoordinatorToWorkerCommunicationFactory().coordinatorCommunication(context);
    JobCoordinatorMetadataManager jobCoordinatorMetadataManager = new JobCoordinatorMetadataManager(
        new NamespaceAwareCoordinatorStreamStore(metadataStore, SetJobCoordinatorMetadataMessage.TYPE),
        JobCoordinatorMetadataManager.ClusterType.NON_YARN, metrics);
    ChangelogStreamManager changelogStreamManager =
        new ChangelogStreamManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetChangelogMapping.TYPE));
    StartpointManager startpointManager =
        jobConfig.getStartpointEnabled() ? new StartpointManager(metadataStore) : null;
    SystemAdmins systemAdmins = new SystemAdmins(config, StaticResourceJobCoordinator.class.getSimpleName());
    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(systemAdmins, 0, SystemClock.instance());
    JobModelHelper jobModelHelper = buildJobModelHelper(metadataStore, streamMetadataCache);
    return new StaticResourceJobCoordinator(jobModelHelper, jobModelServingContext, coordinatorCommunication,
        jobCoordinatorMetadataManager, startpointManager, changelogStreamManager, metrics, systemAdmins, config);
  }

  @VisibleForTesting
  StaticResourceJobCoordinator(JobModelHelper jobModelHelper, JobModelServingContext jobModelServingContext,
      CoordinatorCommunication coordinatorCommunication, JobCoordinatorMetadataManager jobCoordinatorMetadataManager,
      StartpointManager startpointManager, ChangelogStreamManager changelogStreamManager, MetricsRegistry metrics,
      SystemAdmins systemAdmins, Config config) {
    this.jobModelHelper = jobModelHelper;
    this.jobModelServingContext = jobModelServingContext;
    this.coordinatorCommunication = coordinatorCommunication;
    this.jobCoordinatorMetadataManager = jobCoordinatorMetadataManager;
    this.startpointManager = startpointManager;
    this.changelogStreamManager = changelogStreamManager;
    this.metrics = metrics;
    this.systemAdmins = systemAdmins;
    this.config = config;
  }

  /**
   * Run the coordinator.
   */
  public void run() {
    if (!isStarted.compareAndSet(false, true)) {
      LOG.warn("Already running; not to going execute run() again");
      return;
    }
    LOG.info("Starting job coordinator");
    this.systemAdmins.start();
    this.startpointManager.start();
    try {
      JobModel jobModel = newJobModel();
      JobCoordinatorMetadata newMetadata =
          this.jobCoordinatorMetadataManager.generateJobCoordinatorMetadata(jobModel, jobModel.getConfig());
      Set<JobMetadataChange> jobMetadataChanges = checkForMetadataChanges(newMetadata);
      prepareWorkerExecution(jobModel, newMetadata, jobMetadataChanges);
      this.coordinatorCommunication.start();
      waitForShutdownQuietly();
    } catch (Exception e) {
      LOG.error("Error while running job coordinator; exiting", e);
      throw new SamzaException("Error while running job coordinator", e);
    } finally {
      this.coordinatorCommunication.stop();
      this.startpointManager.stop();
      this.systemAdmins.stop();
    }
  }

  /**
   * Set shutdown flag for coordinator and release any threads waiting for the shutdown.
   */
  public void signalShutdown() {
    if (this.shouldShutdown.compareAndSet(false, true)) {
      LOG.info("Shutdown signalled");
      this.shutdownLatch.countDown();
    }
  }

  private static JobModelHelper buildJobModelHelper(MetadataStore metadataStore,
      StreamMetadataCache streamMetadataCache) {
    LocalityManager localityManager =
        new LocalityManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetContainerHostMapping.TYPE));
    TaskAssignmentManager taskAssignmentManager =
        new TaskAssignmentManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetTaskContainerMapping.TYPE),
            new NamespaceAwareCoordinatorStreamStore(metadataStore, SetTaskModeMapping.TYPE));
    TaskPartitionAssignmentManager taskPartitionAssignmentManager = new TaskPartitionAssignmentManager(
        new NamespaceAwareCoordinatorStreamStore(metadataStore, SetTaskPartitionMapping.TYPE));
    return new JobModelHelper(localityManager, taskAssignmentManager, taskPartitionAssignmentManager,
        streamMetadataCache, JobModelCalculator.INSTANCE);
  }

  private JobModel newJobModel() {
    return this.jobModelHelper.newJobModel(this.config, this.changelogStreamManager.readPartitionMapping());
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
    if (this.startpointManager != null && !jobMetadataChanges.isEmpty()) {
      startpointManager.fanOut(JobModelUtil.getTaskToSystemStreamPartitions(jobModel));
    }
  }

  @VisibleForTesting
  MetadataResourceUtil metadataResourceUtil(JobModel jobModel) {
    return new MetadataResourceUtil(jobModel, this.metrics, this.config);
  }

  private void waitForShutdown() throws InterruptedException {
    LOG.info("Waiting for coordinator to be signalled for shutdown");
    boolean latchReleased = false;
    while (!latchReleased && !this.shouldShutdown.get()) {
      /*
       * Using a timeout as a defensive measure in case we are waiting for a shutdown but the latch is not triggered
       * for some reason.
       */
      latchReleased = this.shutdownLatch.await(15, TimeUnit.SECONDS);
    }
  }

  private Set<JobMetadataChange> checkForMetadataChanges(JobCoordinatorMetadata newMetadata) {
    JobCoordinatorMetadata previousMetadata = this.jobCoordinatorMetadataManager.readJobCoordinatorMetadata();
    return this.jobCoordinatorMetadataManager.checkForMetadataChanges(newMetadata, previousMetadata);
  }

  private void waitForShutdownQuietly() {
    try {
      waitForShutdown();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting to shutdown", e);
    }
  }
}
