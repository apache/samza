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

package org.apache.samza;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.runtime.ProcessorIdGenerator;
import org.apache.samza.scheduler.HeartbeatScheduler;
import org.apache.samza.scheduler.JMVersionUpgradeScheduler;
import org.apache.samza.scheduler.LeaderBarrierCompleteScheduler;
import org.apache.samza.scheduler.LeaderLivenessCheckScheduler;
import org.apache.samza.scheduler.LivenessCheckScheduler;
import org.apache.samza.scheduler.RenewLeaseScheduler;
import org.apache.samza.scheduler.SchedulerStateChangeListener;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.ClassLoaderHelper;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


/**
 * Class that provides coordination mechanism for Samza standalone in Azure.
 * Handles processor lifecycle through Azure blob and table storage. Orchestrates leader election.
 * The leader job coordinator generates partition mapping, writes shared data to the blob and manages rebalancing.
 */
public class AzureJobCoordinator implements JobCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(AzureJobCoordinator.class);
  private static final int METADATA_CACHE_TTL_MS = 5000;
  private static final String INITIAL_STATE = "UNASSIGNED";
  private final Consumer<String> errorHandler;
  private final AzureLeaderElector azureLeaderElector;
  private final BlobUtils leaderBlob;
  private final TableUtils table;
  private final Config config;
  private final String processorId;
  private final AzureClient client;
  private final AtomicReference<String> currentJMVersion;
  private final AtomicBoolean versionUpgradeDetected;
  private final HeartbeatScheduler heartbeat;
  private final JMVersionUpgradeScheduler versionUpgrade;
  private final LeaderLivenessCheckScheduler leaderAlive;
  private LivenessCheckScheduler liveness;
  private RenewLeaseScheduler renewLease;
  private LeaderBarrierCompleteScheduler leaderBarrierScheduler;
  private StreamMetadataCache streamMetadataCache = null;
  private JobCoordinatorListener coordinatorListener = null;
  private JobModel jobModel = null;

  /**
   * Creates an instance of Azure job coordinator, along with references to Azure leader elector, Azure Blob and Azure Table.
   * @param config User defined config
   */
  public AzureJobCoordinator(Config config) {
    //TODO: Cleanup previous values in the table when barrier times out.
    this.config = config;
    processorId = createProcessorId(config);
    currentJMVersion = new AtomicReference<>(INITIAL_STATE);
    AzureConfig azureConfig = new AzureConfig(config);
    client = new AzureClient(azureConfig.getAzureConnect());
    leaderBlob = new BlobUtils(client, azureConfig.getAzureContainerName(), azureConfig.getAzureBlobName(), azureConfig.getAzureBlobLength());
    errorHandler = (errorMsg) -> {
      LOG.error(errorMsg);
      stop();
    };
    table = new TableUtils(client, azureConfig.getAzureTableName(), INITIAL_STATE);
    azureLeaderElector = new AzureLeaderElector(new LeaseBlobManager(leaderBlob.getBlob()));
    azureLeaderElector.setLeaderElectorListener(new AzureLeaderElectorListener());
    versionUpgradeDetected = new AtomicBoolean(false);
    heartbeat = new HeartbeatScheduler(errorHandler, table, currentJMVersion, processorId);
    versionUpgrade = new JMVersionUpgradeScheduler(errorHandler, leaderBlob, currentJMVersion, versionUpgradeDetected, processorId);
    leaderAlive = new LeaderLivenessCheckScheduler(errorHandler, table, leaderBlob, currentJMVersion, INITIAL_STATE);
    leaderBarrierScheduler = null;
    renewLease = null;
    liveness = null;
  }

  @Override
  public void start() {

    LOG.info("Starting Azure job coordinator.");
    streamMetadataCache = StreamMetadataCache.apply(METADATA_CACHE_TTL_MS, config);
    table.addProcessorEntity(INITIAL_STATE, processorId, azureLeaderElector.amILeader());

    // Start scheduler for heartbeating
    LOG.info("Starting scheduler for heartbeating.");
    heartbeat.scheduleTask();

    azureLeaderElector.tryBecomeLeader();

    // Start scheduler to check for job model version upgrades
    LOG.info("Starting scheduler to check for job model version upgrades.");
    versionUpgrade.setStateChangeListener(createJMVersionUpgradeListener());
    versionUpgrade.scheduleTask();

    // Start scheduler to check for leader liveness
    LOG.info("Starting scheduler to check for leader liveness.");
    leaderAlive.setStateChangeListener(createLeaderLivenessListener());
    leaderAlive.scheduleTask();
  }

  @Override
  public void stop() {
    LOG.info("Shutting down Azure job coordinator.");

    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }

    // Resign leadership
    if (azureLeaderElector.amILeader()) {
      azureLeaderElector.resignLeadership();
    }

    // Shutdown all schedulers
    shutdownSchedulers();

    if (coordinatorListener != null) {
      coordinatorListener.onCoordinatorStop();
    }
  }

  @Override
  public String getProcessorId() {
    return processorId;
  }

  @Override
  public void setListener(JobCoordinatorListener listener) {
    this.coordinatorListener = listener;
  }

  @Override
  public JobModel getJobModel() {
    return jobModel;
  }

  private void shutdownSchedulers() {
    if (renewLease != null) {
      renewLease.shutdown();
    }
    if (leaderBarrierScheduler != null) {
      leaderBarrierScheduler.shutdown();
    }
    if (liveness != null) {
      liveness.shutdown();
    }
    heartbeat.shutdown();
    leaderAlive.shutdown();
    versionUpgrade.shutdown();
  }

  /**
   * Creates a listener for LeaderBarrierCompleteScheduler class.
   * Invoked by the leader when it detects that rebalancing has completed by polling the processor table.
   * Updates the barrier state on the blob to denote that the barrier has completed.
   * Cancels all future tasks scheduled by the LeaderBarrierComplete scheduler to check if barrier has completed.
   * @return an instance of SchedulerStateChangeListener.
   */
  private SchedulerStateChangeListener createLeaderBarrierCompleteListener(String nextJMVersion, AtomicBoolean barrierTimeout) {
    return () -> {
      versionUpgradeDetected.getAndSet(false);
      String state;
      if (barrierTimeout.get()) {
        LOG.error("Barrier timed out for version {}", nextJMVersion);
        state = BarrierState.TIMEOUT.name() + " " + nextJMVersion;
      } else {
        LOG.info("Leader detected barrier completion.");
        state = BarrierState.END.name() + " " + nextJMVersion;
      }
      if (!leaderBlob.publishBarrierState(state, azureLeaderElector.getLeaseId().get())) {
        LOG.info("Leader failed to publish the job model {}. Stopping the processor with PID: .", jobModel, processorId);
        stop();
        table.deleteProcessorEntity(currentJMVersion.get(), processorId);
      }
      leaderBarrierScheduler.shutdown();
    };
  }

  /**
   * Creates a listener for LivenessCheckScheduler class.
   * Invoked by the leader when the list of active processors in the system changes.
   * @return an instance of SchedulerStateChangeListener.
   */
  private SchedulerStateChangeListener createLivenessListener(AtomicReference<List<String>> liveProcessors) {
    return () -> {
      LOG.info("Leader detected change in list of live processors.");
      doOnProcessorChange(liveProcessors.get());
    };
  }

  /**
   * Creates a listener for JMVersionUpgradeScheduler class.
   * Invoked when the processor detects a job model version upgrade on the blob.
   * Stops listening for job model version upgrades until rebalancing achieved.
   * @return an instance of SchedulerStateChangeListener.
   */
  private SchedulerStateChangeListener createJMVersionUpgradeListener() {
    return () -> {
      LOG.info("Job model version upgrade detected.");
      versionUpgradeDetected.getAndSet(true);
      onNewJobModelAvailable(leaderBlob.getJobModelVersion());
    };
  }

  /**
   * Creates a listener for LeaderLivenessCheckScheduler class.
   * Invoked when an existing leader dies. Enables the JC to participate in leader election again.
   * @return an instance of SchedulerStateChangeListener.
   */
  private SchedulerStateChangeListener createLeaderLivenessListener() {
    return () -> {
      LOG.info("Existing leader died.");
      azureLeaderElector.tryBecomeLeader();
    };
  }

  /**
   * For each input stream specified in config, exactly determine its
   * partitions, returning a set of SystemStreamPartitions containing them all.
   */
  private Set<SystemStreamPartition> getInputStreamPartitions() {
    TaskConfig taskConfig = new TaskConfig(config);
    scala.collection.immutable.Set<SystemStream> inputSystemStreams = taskConfig.getInputStreams();

    // Get the set of partitions for each SystemStream from the stream metadata
    Set<SystemStreamPartition>
        sspSet = JavaConverters.mapAsJavaMapConverter(streamMetadataCache.getStreamMetadata(inputSystemStreams, true)).asJava()
        .entrySet()
        .stream()
        .flatMap(this::mapSSMToSSP)
        .collect(Collectors.toSet());

    return sspSet;
  }

  private Stream<SystemStreamPartition> mapSSMToSSP(Map.Entry<SystemStream, SystemStreamMetadata> ssMs) {
    return ssMs.getValue()
        .getSystemStreamPartitionMetadata()
        .keySet()
        .stream()
        .map(partition -> new SystemStreamPartition(ssMs.getKey(), partition));
  }

  /**
   * Gets a SystemStreamPartitionGrouper object from the configuration.
   */
  private SystemStreamPartitionGrouper getSystemStreamPartitionGrouper() {
    JobConfig jobConfig = new JobConfig(config);
    String factoryString = jobConfig.getSystemStreamPartitionGrouperFactory();
    SystemStreamPartitionGrouper grouper = Util.<SystemStreamPartitionGrouperFactory>getObj(factoryString).getSystemStreamPartitionGrouper(jobConfig);
    return grouper;
  }

  private int getMaxNumTasks() {
    // Do grouping to fetch TaskName to SSP mapping
    Set<SystemStreamPartition> allSystemStreamPartitions = getInputStreamPartitions();
    SystemStreamPartitionGrouper grouper = getSystemStreamPartitionGrouper();
    Map<TaskName, Set<SystemStreamPartition>> groups = grouper.group(allSystemStreamPartitions);
    LOG.info("SystemStreamPartitionGrouper " + grouper.toString() + " has grouped the SystemStreamPartitions into " + Integer.toString(groups.size()) +
        " tasks with the following taskNames: {}", groups.keySet());
    return groups.size();
  }

  /**
   * Called only by the leader, either when the processor becomes the leader, or when the list of live processors changes.
   * @param currentProcessorIds New updated list of processor IDs which caused the rebalancing.
   */
  private void doOnProcessorChange(List<String> currentProcessorIds) {
    // if list of processors is empty - it means we are called from 'onBecomeLeader'

    // Check if number of processors is greater than number of tasks
    List<String> initialProcessorIds = new ArrayList<>(currentProcessorIds);
    int numTasks = getMaxNumTasks();
    if (currentProcessorIds.size() > numTasks) {
      int iterator = 0;
      while (currentProcessorIds.size() != numTasks) {
        if (!currentProcessorIds.get(iterator).equals(processorId)) {
          currentProcessorIds.remove(iterator);
          iterator++;
        }
      }
    }
    LOG.info("currentProcessorIds = {}", currentProcessorIds);
    LOG.info("initialProcessorIds = {}", initialProcessorIds);

    String nextJMVersion;
    String prevJMVersion = currentJMVersion.get();
    JobModel prevJobModel = jobModel;
    AtomicBoolean barrierTimeout = new AtomicBoolean(false);

    if (currentProcessorIds.isEmpty()) {
      if (currentJMVersion.get().equals(INITIAL_STATE)) {
        nextJMVersion = "1";
      } else {
        nextJMVersion = Integer.toString(Integer.valueOf(prevJMVersion) + 1);
      }
      currentProcessorIds = new ArrayList<>(table.getActiveProcessorsList(currentJMVersion));
      initialProcessorIds = currentProcessorIds;
    } else {
      //Check if previous barrier not reached, then previous barrier times out.
      String blobJMV = leaderBlob.getJobModelVersion();
      nextJMVersion = Integer.toString(Integer.valueOf(prevJMVersion) + 1);
      if (blobJMV != null && Integer.valueOf(blobJMV) > Integer.valueOf(prevJMVersion)) {
        prevJMVersion = blobJMV;
        prevJobModel = leaderBlob.getJobModel();
        nextJMVersion = Integer.toString(Integer.valueOf(blobJMV) + 1);
        versionUpgradeDetected.getAndSet(false);
        leaderBarrierScheduler.shutdown();
        leaderBlob.publishBarrierState(BarrierState.TIMEOUT.name() + " " + blobJMV, azureLeaderElector.getLeaseId().get());
      }
    }

    // Generate the new JobModel
    JobModel newJobModel = JobModelManager.readJobModel(this.config, Collections.emptyMap(),
        null, streamMetadataCache, currentProcessorIds);
    LOG.info("pid=" + processorId + "Generated new Job Model. Version = " + nextJMVersion);

    // Publish the new job model
    boolean jmWrite = leaderBlob.publishJobModel(prevJobModel, newJobModel, prevJMVersion, nextJMVersion, azureLeaderElector.getLeaseId().get());
    // Publish barrier state
    boolean barrierWrite = leaderBlob.publishBarrierState(BarrierState.START.name() + " " + nextJMVersion, azureLeaderElector.getLeaseId().get());
    barrierTimeout.set(false);
    // Publish list of processors this function was called with
    boolean processorWrite = leaderBlob.publishLiveProcessorList(initialProcessorIds, azureLeaderElector.getLeaseId().get());

    //Shut down processor if write fails even after retries. These writes have an inherent retry policy.
    if (!jmWrite || !barrierWrite || !processorWrite) {
      LOG.info("Leader failed to publish the job model {}. Stopping the processor with PID: .", jobModel, processorId);
      stop();
      table.deleteProcessorEntity(currentJMVersion.get(), processorId);
    }

    LOG.info("pid=" + processorId + "Published new Job Model. Version = " + nextJMVersion);

    // Start scheduler to check if barrier reached
    long startTime = System.currentTimeMillis();
    leaderBarrierScheduler = new LeaderBarrierCompleteScheduler(errorHandler, table, nextJMVersion, initialProcessorIds, startTime, barrierTimeout, currentJMVersion, processorId);
    leaderBarrierScheduler.setStateChangeListener(createLeaderBarrierCompleteListener(nextJMVersion, barrierTimeout));
    leaderBarrierScheduler.scheduleTask();
  }

  /**
   * Called when the JC detects a job model version upgrade on the shared blob.
   * @param nextJMVersion The new job model version after rebalancing.
   */
  private void onNewJobModelAvailable(final String nextJMVersion) {
    LOG.info("pid=" + processorId + "new JobModel available with job model version {}", nextJMVersion);

    //Get the new job model from blob
    jobModel = leaderBlob.getJobModel();
    LOG.info("pid=" + processorId + ": new JobModel available. ver=" + nextJMVersion + "; jm = " + jobModel);

    if (!jobModel.getContainers().containsKey(processorId)) {
      LOG.info("JobModel: {} does not contain the processorId: {}. Stopping the processor.", jobModel, processorId);
      stop();
      table.deleteProcessorEntity(currentJMVersion.get(), processorId);
    } else {
      //Stop current work
      if (coordinatorListener != null) {
        coordinatorListener.onJobModelExpired();
      }
      // Add entry with new job model version to the processor table
      table.addProcessorEntity(nextJMVersion, processorId, azureLeaderElector.amILeader());

      // Start polling blob to check if barrier reached
      Random random = new Random();
      String blobBarrierState = leaderBlob.getBarrierState();
      while (true) {
        if (blobBarrierState.equals(BarrierState.END.name() + " " + nextJMVersion)) {
          LOG.info("Barrier completion detected by the worker for barrier version {}.", nextJMVersion);
          versionUpgradeDetected.getAndSet(false);
          onNewJobModelConfirmed(nextJMVersion);
          break;
        } else if (blobBarrierState.equals(BarrierState.TIMEOUT.name() + " " + nextJMVersion) ||
            (Integer.valueOf(leaderBlob.getJobModelVersion()) > Integer.valueOf(nextJMVersion))) {
          LOG.info("Barrier timed out for version number {}", nextJMVersion);
          versionUpgradeDetected.getAndSet(false);
          break;
        } else {
          try {
            Thread.sleep(random.nextInt(5000));
          } catch (InterruptedException e) {
            Thread.interrupted();
          }
          LOG.info("Checking for barrier state on the blob again...");
          blobBarrierState = leaderBlob.getBarrierState();
        }
      }
    }
  }

  /**
   * Called when the JC detects that the barrier has completed by checking the barrier state on the blob.
   * @param nextJMVersion The new job model version after rebalancing.
   */
  private void onNewJobModelConfirmed(final String nextJMVersion) {
    LOG.info("pid=" + processorId + "new version " + nextJMVersion + " of the job model got confirmed");

    // Delete previous value
    if (table.getEntity(currentJMVersion.get(), processorId) != null) {
      table.deleteProcessorEntity(currentJMVersion.get(), processorId);
    }
    if (table.getEntity(INITIAL_STATE, processorId) != null) {
      table.deleteProcessorEntity(INITIAL_STATE, processorId);
    }

    //Start heartbeating to new entry only when barrier reached.
    //Changing the current job model version enables that since we are heartbeating to a row identified by the current job model version.
    currentJMVersion.getAndSet(nextJMVersion);

    //Start the container with the new model
    if (coordinatorListener != null) {
      coordinatorListener.onNewJobModel(processorId, jobModel);
    }
  }

  private String createProcessorId(Config config) {
    // TODO: This check to be removed after 0.13+
    ApplicationConfig appConfig = new ApplicationConfig(config);
    if (appConfig.getProcessorId() != null) {
      return appConfig.getProcessorId();
    } else if (StringUtils.isNotBlank(appConfig.getAppProcessorIdGeneratorClass())) {
      ProcessorIdGenerator idGenerator =
          ClassLoaderHelper.fromClassName(appConfig.getAppProcessorIdGeneratorClass(), ProcessorIdGenerator.class);
      return idGenerator.generateProcessorId(config);
    } else {
      throw new ConfigException(String
          .format("Expected either %s or %s to be configured", ApplicationConfig.PROCESSOR_ID,
              ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS));
    }
  }

  public class AzureLeaderElectorListener implements LeaderElectorListener {
    /**
     * Keep renewing the lease and do the required tasks as a leader.
     */
    @Override
    public void onBecomingLeader() {
      // Update table to denote that it is a leader.
      table.updateIsLeader(currentJMVersion.get(), processorId, true);

      // Schedule a task to renew the lease after a fixed time interval
      LOG.info("Starting scheduler to keep renewing lease held by the leader.");
      renewLease = new RenewLeaseScheduler((errorMsg) -> {
          LOG.error(errorMsg);
          table.updateIsLeader(currentJMVersion.get(), processorId, false);
          azureLeaderElector.resignLeadership();
          renewLease.shutdown();
          liveness.shutdown();
        }, azureLeaderElector.getLeaseBlobManager(), azureLeaderElector.getLeaseId());
      renewLease.scheduleTask();

      doOnProcessorChange(new ArrayList<>());

      // Start scheduler to check for change in list of live processors
      LOG.info("Starting scheduler to check for change in list of live processors in the system.");
      liveness = new LivenessCheckScheduler(errorHandler, table, leaderBlob, currentJMVersion, processorId);
      liveness.setStateChangeListener(createLivenessListener(liveness.getLiveProcessors()));
      liveness.scheduleTask();
    }
  }
}