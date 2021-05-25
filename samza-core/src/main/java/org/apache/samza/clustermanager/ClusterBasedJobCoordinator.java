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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementMetadataStore;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementRequestAllocator;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.ExecutionContainerIdManager;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.InputStreamsDiscoveredException;
import org.apache.samza.coordinator.JobCoordinatorMetadataManager;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.MetadataResourceUtil;
import org.apache.samza.coordinator.PartitionChangeException;
import org.apache.samza.coordinator.StreamPartitionCountMonitor;
import org.apache.samza.coordinator.StreamRegexMonitor;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.coordinator.stream.messages.SetExecutionEnvContainerIdMapping;
import org.apache.samza.coordinator.stream.messages.SetJobCoordinatorMetadataMessage;
import org.apache.samza.job.JobCoordinatorMetadata;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.JobModelUtil;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.storage.StateBackendAdmin;
import org.apache.samza.storage.StateBackendFactory;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.DiagnosticsUtil;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a JobCoordinator that is completely independent of the underlying cluster
 * manager system. This {@link ClusterBasedJobCoordinator} handles functionality common
 * to both Yarn and Mesos. It takes care of
 *  1. Requesting resources from an underlying {@link ClusterResourceManager}.
 *  2. Ensuring that placement of processors to resources happens (as per whether host affinity
 *  is configured or not).
 *
 *  Any offer based cluster management system that must integrate with Samza will merely
 *  implement a {@link ResourceManagerFactory} and a {@link ClusterResourceManager}.
 *
 *  This class is not thread-safe. For safe access in multi-threaded context, invocations
 *  should be synchronized by the callers.
 *
 * TODO:
 * 1. Refactor ClusterResourceManager to also handle process liveness, process start
 * callbacks
 * 2. Refactor the JobModelReader to be an interface.
 * 3. Make ClusterBasedJobCoordinator implement the JobCoordinator API as in SAMZA-881.
 * 4. Refactor UI state variables.
 * 5. Unit tests.
 * 6. Document newly added configs.
 */
public class ClusterBasedJobCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterBasedJobCoordinator.class);
  private final static String METRICS_SOURCE_NAME = "ApplicationMaster";

  private final Config config;

  /**
   * State to track container failures, host-processor mappings
   */
  private final SamzaApplicationState state;

  //even though some of these can be converted to local variables, it will not be the case
  //as we add more methods to the JobCoordinator and completely implement SAMZA-881.

  /**
   * Handles callback for allocated containers, failed containers.
   */
  private final ContainerProcessManager containerProcessManager;

  /**
   * A JobModelManager to return and refresh the {@link org.apache.samza.job.model.JobModel} when required.
   */
  private final JobModelManager jobModelManager;

  /**
   * A ChangelogStreamManager to handle creation of changelog stream and map changelog stream partitions
   */
  private final ChangelogStreamManager changelogStreamManager;

  /*
   * The interval for polling the Task Manager for shutdown.
   */
  private final long jobCoordinatorSleepInterval;

  /*
   * Config specifies if a Jmx server should be started on this Job Coordinator
   */
  private final boolean isJmxEnabled;

  /**
   * Internal boolean to check if the job coordinator has already been started.
   */
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  /**
   * A boolean variable indicating whether the job has durable state stores in the configuration
   */
  private final boolean hasDurableStores;

  /**
   * The input topic partition count monitor
   */
  private final Optional<StreamPartitionCountMonitor> partitionMonitor;

  /**
   * The input stream regex monitor
   */
  private final Optional<StreamRegexMonitor> inputStreamRegexMonitor;

  /**
   * Container placement request dispatcher and metastore reader/writer
   */
  private final ContainerPlacementMetadataStore containerPlacementMetadataStore;
  private final ContainerPlacementRequestAllocator containerPlacementRequestAllocator;
  // Container Placement thread that reads requests from metastore
  private final Thread containerPlacementRequestAllocatorThread;

  /**
   * Metrics to track stats around container failures, needed containers etc.
   */
  private final MetricsRegistryMap metrics;
  private final MetadataStore metadataStore;

  private final SystemAdmins systemAdmins;
  private final LocalityManager localityManager;

  /**
   * Internal variable for the instance of {@link JmxServer}
   */
  private JmxServer jmxServer;

  /*
   * Denotes if the metadata changed across application attempts. Used only if job coordinator high availability is enabled
   */
  private boolean metadataChangedAcrossAttempts = false;

  /**
   * Variable to keep the callback exception
   */
  volatile private Exception coordinatorException = null;

  /**
   * Creates a new ClusterBasedJobCoordinator instance.
   * Invoke run() to actually run the job coordinator.
   *
   * @param metrics the registry for reporting metrics.
   * @param metadataStore metadata store to hold metadata.
   * @param fullJobConfig full job config.
   */
  public ClusterBasedJobCoordinator(MetricsRegistryMap metrics, MetadataStore metadataStore, Config fullJobConfig) {
    this.metrics = metrics;
    this.metadataStore = metadataStore;
    this.config = fullJobConfig;
    // build a JobModelManager and ChangelogStreamManager and perform partition assignments.
    this.changelogStreamManager = new ChangelogStreamManager(
        new NamespaceAwareCoordinatorStreamStore(metadataStore, SetChangelogMapping.TYPE));
    this.jobModelManager =
        JobModelManager.apply(config, changelogStreamManager.readPartitionMapping(), metadataStore, metrics);

    this.hasDurableStores = new StorageConfig(config).hasDurableStores();
    this.state = new SamzaApplicationState(jobModelManager);
    // The systemAdmins should be started before partitionMonitor can be used. And it should be stopped when this coordinator is stopped.
    this.systemAdmins = new SystemAdmins(config, this.getClass().getSimpleName());
    this.partitionMonitor = getPartitionCountMonitor(config, systemAdmins);

    Set<SystemStream> inputSystemStreams = JobModelUtil.getSystemStreams(jobModelManager.jobModel());
    this.inputStreamRegexMonitor = getInputRegexMonitor(config, systemAdmins, inputSystemStreams);

    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.isJmxEnabled = clusterManagerConfig.getJmxEnabledOnJobCoordinator();
    this.jobCoordinatorSleepInterval = clusterManagerConfig.getJobCoordinatorSleepInterval();
    this.localityManager =
        new LocalityManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetContainerHostMapping.TYPE));

    if (isApplicationMasterHighAvailabilityEnabled()) {
      ExecutionContainerIdManager executionContainerIdManager = new ExecutionContainerIdManager(
          new NamespaceAwareCoordinatorStreamStore(metadataStore, SetExecutionEnvContainerIdMapping.TYPE));
      state.processorToExecutionId.putAll(executionContainerIdManager.readExecutionEnvironmentContainerIdMapping());
      generateAndUpdateJobCoordinatorMetadata(jobModelManager.jobModel());
    }
    // build metastore for container placement messages
    containerPlacementMetadataStore = new ContainerPlacementMetadataStore(metadataStore);

    // build a container process Manager
    containerProcessManager = createContainerProcessManager();

    // build utils related to container placements
    containerPlacementRequestAllocator =
        new ContainerPlacementRequestAllocator(containerPlacementMetadataStore, containerProcessManager,
            new ApplicationConfig(config));
    this.containerPlacementRequestAllocatorThread =
        new Thread(containerPlacementRequestAllocator, "Samza-" + ContainerPlacementRequestAllocator.class.getSimpleName());
  }

  /**
   * Starts the JobCoordinator.
   */
  public void run() {
    if (!isStarted.compareAndSet(false, true)) {
      LOG.warn("Attempting to start an already started job coordinator. ");
      return;
    }
    // set up JmxServer (if jmx is enabled)
    if (isJmxEnabled) {
      jmxServer = new JmxServer();
      state.jmxUrl = jmxServer.getJmxUrl();
      state.jmxTunnelingUrl = jmxServer.getTunnelingJmxUrl();
    } else {
      jmxServer = null;
    }

    try {
      // initialize JobCoordinator state
      LOG.info("Starting cluster based job coordinator");

      // write the diagnostics metadata file
      String jobName = new JobConfig(config).getName().get();
      String jobId = new JobConfig(config).getJobId();
      Optional<String> execEnvContainerId = Optional.ofNullable(System.getenv("CONTAINER_ID"));
      DiagnosticsUtil.writeMetadataFile(jobName, jobId, METRICS_SOURCE_NAME, execEnvContainerId, config);

      //create necessary checkpoint and changelog streams, if not created
      JobModel jobModel = jobModelManager.jobModel();
      MetadataResourceUtil metadataResourceUtil = new MetadataResourceUtil(jobModel, this.metrics, config);
      metadataResourceUtil.createResources();

      // create all the resources required for state backend factories
      StorageConfig storageConfig = new StorageConfig(config);
      storageConfig.getBackupFactories().forEach(stateStorageBackendBackupFactory -> {
        StateBackendFactory stateBackendFactory =
            ReflectionUtil.getObj(stateStorageBackendBackupFactory, StateBackendFactory.class);
        StateBackendAdmin stateBackendAdmin = stateBackendFactory.getAdmin(jobModel, config);
        // Create resources required for state backend admin
        stateBackendAdmin.createResources();
        // Validate resources required for state backend admin
        stateBackendAdmin.validateResources();
      });

      /*
       * We fanout startpoint if and only if
       *  1. Startpoint is enabled in configuration
       *  2. If AM HA is enabled, fanout only if startpoint enabled and job coordinator metadata changed
       */
      if (shouldFanoutStartpoint()) {
        StartpointManager startpointManager = createStartpointManager();
        startpointManager.start();
        try {
          startpointManager.fanOut(JobModelUtil.getTaskToSystemStreamPartitions(jobModel));
        } finally {
          startpointManager.stop();
        }
      }

      // Remap changelog partitions to tasks
      Map<TaskName, Integer> prevPartitionMappings = changelogStreamManager.readPartitionMapping();

      Map<TaskName, Integer> taskPartitionMappings = new HashMap<>();
      Map<String, ContainerModel> containers = jobModel.getContainers();
      for (ContainerModel containerModel : containers.values()) {
        for (TaskModel taskModel : containerModel.getTasks().values()) {
          taskPartitionMappings.put(taskModel.getTaskName(), taskModel.getChangelogPartition().getPartitionId());
        }
      }

      changelogStreamManager.updatePartitionMapping(prevPartitionMappings, taskPartitionMappings);

      containerProcessManager.start();
      systemAdmins.start();
      partitionMonitor.ifPresent(StreamPartitionCountMonitor::start);
      inputStreamRegexMonitor.ifPresent(StreamRegexMonitor::start);

      // containerPlacementRequestAllocator thread has to start after the cpm is started
      LOG.info("Starting the container placement handler thread");
      containerPlacementMetadataStore.start();
      containerPlacementRequestAllocatorThread.start();

      boolean isInterrupted = false;

      while (!containerProcessManager.shouldShutdown()
          && !checkAndThrowException()
          && !isInterrupted
          && checkcontainerPlacementRequestAllocatorThreadIsAlive()) {
        try {
          Thread.sleep(jobCoordinatorSleepInterval);
        } catch (InterruptedException e) {
          isInterrupted = true;
          LOG.error("Interrupted in job coordinator loop", e);
          Thread.currentThread().interrupt();
        }
      }
    } catch (Throwable e) {
      LOG.error("Exception thrown in the JobCoordinator loop", e);
      throw new SamzaException(e);
    } finally {
      onShutDown();
    }
  }

  private boolean checkAndThrowException() throws Exception {
    if (coordinatorException != null) {
      throw coordinatorException;
    }
    return false;
  }

  private boolean checkcontainerPlacementRequestAllocatorThreadIsAlive() {
    if (containerPlacementRequestAllocatorThread.isAlive()) {
      return true;
    }
    LOG.info("{} thread is dead issuing a shutdown", containerPlacementRequestAllocatorThread.getName());
    return false;
  }

  /**
   * Generate the job coordinator metadata for current application attempt and checks for changes in the
   * metadata from the previous attempt and writes the updates metadata to coordinator stream.
   *
   * @param jobModel job model used to generate the job coordinator metadata
   */
  @VisibleForTesting
  void generateAndUpdateJobCoordinatorMetadata(JobModel jobModel) {
    JobCoordinatorMetadataManager jobCoordinatorMetadataManager = createJobCoordinatorMetadataManager();

    JobCoordinatorMetadata previousMetadata = jobCoordinatorMetadataManager.readJobCoordinatorMetadata();
    JobCoordinatorMetadata newMetadata = jobCoordinatorMetadataManager.generateJobCoordinatorMetadata(jobModel, config);
    if (jobCoordinatorMetadataManager.checkForMetadataChanges(newMetadata, previousMetadata)) {
      jobCoordinatorMetadataManager.writeJobCoordinatorMetadata(newMetadata);
      metadataChangedAcrossAttempts = true;
    }
  }

  /**
   * Stops all components of the JobCoordinator.
   */
  private void onShutDown() {
    try {
      partitionMonitor.ifPresent(StreamPartitionCountMonitor::stop);
      inputStreamRegexMonitor.ifPresent(StreamRegexMonitor::stop);
      systemAdmins.stop();
      shutDowncontainerPlacementRequestAllocatorAndUtils();
      containerProcessManager.stop();
      localityManager.close();
      metadataStore.close();
    } catch (Throwable e) {
      LOG.error("Exception while stopping cluster based job coordinator", e);
    }
    LOG.info("Stopped cluster based job coordinator");

    if (jmxServer != null) {
      try {
        jmxServer.stop();
        LOG.info("Stopped Jmx Server");
      } catch (Throwable e) {
        LOG.error("Exception while stopping jmx server", e);
      }
    }
  }

  private void shutDowncontainerPlacementRequestAllocatorAndUtils() {
    // Shutdown container placement handler
    containerPlacementRequestAllocator.stop();
    try {
      containerPlacementRequestAllocatorThread.join();
      LOG.info("Stopped container placement handler thread");
      containerPlacementMetadataStore.stop();
    } catch (InterruptedException ie) {
      LOG.error("Container Placement handler thread join threw an interrupted exception", ie);
      Thread.currentThread().interrupt();
    }
  }

  private Optional<StreamPartitionCountMonitor> getPartitionCountMonitor(Config config, SystemAdmins systemAdmins) {
    StreamMetadataCache streamMetadata = new StreamMetadataCache(systemAdmins, 0, SystemClock.instance());
    Set<SystemStream> inputStreamsToMonitor = new TaskConfig(config).getAllInputStreams();
    if (inputStreamsToMonitor.isEmpty()) {
      throw new SamzaException("Input streams to a job can not be empty.");
    }

    return Optional.of(new StreamPartitionCountMonitor(inputStreamsToMonitor, streamMetadata, metrics,
        new JobConfig(config).getMonitorPartitionChangeFrequency(), streamsChanged -> {
      // Fail the jobs with durable state store. Otherwise, application state.status remains UNDEFINED s.t. YARN job will be restarted
      if (hasDurableStores) {
        LOG.error("Input topic partition count changed in a job with durable state. Failing the job. " +
            "Changed topics: {}", streamsChanged.toString());
        state.status = SamzaApplicationState.SamzaAppStatus.FAILED;
      }
      coordinatorException = new PartitionChangeException("Input topic partition count changes detected for topics: " + streamsChanged.toString());
    }));
  }

  private Optional<StreamRegexMonitor> getInputRegexMonitor(Config config, SystemAdmins systemAdmins, Set<SystemStream> inputStreamsToMonitor) {
    JobConfig jobConfig = new JobConfig(config);

    // if input regex monitor is not enabled return empty
    if (jobConfig.getMonitorRegexDisabled()) {
      LOG.info("StreamRegexMonitor is disabled.");
      return Optional.empty();
    }

    StreamMetadataCache streamMetadata = new StreamMetadataCache(systemAdmins, 0, SystemClock.instance());
    if (inputStreamsToMonitor.isEmpty()) {
      throw new SamzaException("Input streams to a job can not be empty.");
    }

    // First list all rewriters
    Optional<String> rewritersList = jobConfig.getConfigRewriters();

    // if no rewriter is defined, there is nothing to monitor
    if (!rewritersList.isPresent()) {
      LOG.warn("No config rewriters are defined. No StreamRegexMonitor created.");
      return Optional.empty();
    }

    // Compile a map of each input-system to its corresponding input-monitor-regex patterns
    Map<String, Pattern> inputRegexesToMonitor = jobConfig.getMonitorRegexPatternMap(rewritersList.get());

    // if there are no regexes to monitor
    if (inputRegexesToMonitor.isEmpty()) {
      LOG.info("No input regexes are defined. No StreamRegexMonitor created.");
      return Optional.empty();
    }

    return Optional.of(new StreamRegexMonitor(inputStreamsToMonitor, inputRegexesToMonitor, streamMetadata, metrics,
        jobConfig.getMonitorRegexFrequency(), new StreamRegexMonitor.Callback() {
          @Override
          public void onInputStreamsChanged(Set<SystemStream> initialInputSet, Set<SystemStream> newInputStreams,
              Map<String, Pattern> regexesMonitored) {
            if (hasDurableStores) {
              LOG.error("New input system-streams discovered. Failing the job. New input streams: {}" +
                  " Existing input streams: {}", newInputStreams, inputStreamsToMonitor);
              state.status = SamzaApplicationState.SamzaAppStatus.FAILED;
            }
            coordinatorException = new InputStreamsDiscoveredException("New input streams discovered: " + newInputStreams);
          }
        }));
  }

  // The following two methods are package-private and for testing only
  @VisibleForTesting
  SamzaApplicationState.SamzaAppStatus getAppStatus() {
    // make sure to only return a unmodifiable copy of the status variable
    return state.status;
  }

  @VisibleForTesting
  StreamPartitionCountMonitor getPartitionMonitor() {
    return partitionMonitor.get();
  }

  @VisibleForTesting
  StartpointManager createStartpointManager() {
    return new StartpointManager(metadataStore);
  }

  @VisibleForTesting
  ContainerProcessManager createContainerProcessManager() {
    return new ContainerProcessManager(config, state, metrics, containerPlacementMetadataStore, localityManager,
        metadataChangedAcrossAttempts);
  }

  @VisibleForTesting
  JobCoordinatorMetadataManager createJobCoordinatorMetadataManager() {
    return new JobCoordinatorMetadataManager(new NamespaceAwareCoordinatorStreamStore(metadataStore,
        SetJobCoordinatorMetadataMessage.TYPE), JobCoordinatorMetadataManager.ClusterType.YARN, metrics);
  }

  @VisibleForTesting
  boolean isApplicationMasterHighAvailabilityEnabled() {
    return new JobConfig(config).getApplicationMasterHighAvailabilityEnabled();
  }

  @VisibleForTesting
  boolean isMetadataChangedAcrossAttempts() {
    return metadataChangedAcrossAttempts;
  }

  /**
   * We only fanout startpoint if and only if
   *  1. Startpoint is enabled
   *  2. If AM HA is enabled, fanout only if startpoint enabled and job coordinator metadata changed
   *
   * @return true if it satisfies above conditions, false otherwise
   */
  @VisibleForTesting
  boolean shouldFanoutStartpoint() {
    JobConfig jobConfig = new JobConfig(config);
    boolean startpointEnabled = jobConfig.getStartpointEnabled();

    return isApplicationMasterHighAvailabilityEnabled() ?
        startpointEnabled && isMetadataChangedAcrossAttempts() : startpointEnabled;
  }
}
