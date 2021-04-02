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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.GrouperMetadata;
import org.apache.samza.container.grouper.task.GrouperMetadataImpl;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.coordinator.MetadataResourceUtil;
import org.apache.samza.coordinator.StreamPartitionCountMonitor;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.JobModelUtil;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.runtime.LocationIdProvider;
import org.apache.samza.runtime.LocationIdProviderFactory;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.SystemClock;
import org.apache.samza.zk.ZkUtils.ProcessorNode;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobCoordinator for stand alone processor managed via Zookeeper.
 */
public class ZkJobCoordinator implements JobCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(ZkJobCoordinator.class);
  // TODO: MetadataCache timeout has to be 0 for the leader so that it can always have the latest information associated
  // with locality. Since host-affinity is not yet implemented, this can be fixed as part of SAMZA-1197
  private static final int METADATA_CACHE_TTL_MS = 5000;
  private static final int NUM_VERSIONS_TO_LEAVE = 10;

  // Action name when the JobModel version changes
  private static final String JOB_MODEL_VERSION_CHANGE = "JobModelVersionChange";

  // Action name when the Processor membership changes
  private static final String ON_PROCESSOR_CHANGE = "OnProcessorChange";

  /**
   * Cleanup process is started after every new job model generation is complete.
   * It deletes old versions of job model and the barrier.
   * How many to delete (or to leave) is controlled by @see org.apache.samza.zk.ZkJobCoordinator#NUM_VERSIONS_TO_LEAVE.
   **/
  private static final String ON_ZK_CLEANUP = "OnCleanUp";

  // Action name when the processor starts with last agreed job model upon start
  static final String START_WORK_WITH_LAST_ACTIVE_JOB_MODEL = "StartWorkWithLastActiveJobModel";

  private final ZkUtils zkUtils;
  private final String processorId;

  private final Config config;
  private final ZkJobCoordinatorMetrics metrics;
  private final AtomicBoolean initiatedShutdown = new AtomicBoolean(false);
  private final StreamMetadataCache streamMetadataCache;
  private final SystemAdmins systemAdmins;
  private final int debounceTimeMs;
  private final Map<TaskName, Integer> changeLogPartitionMap = new HashMap<>();
  private final LocationId locationId;
  private final MetadataStore jobModelMetadataStore;
  private final CoordinatorStreamStore coordinatorStreamStore;

  private JobCoordinatorListener coordinatorListener = null;
  // denotes the most recent job model agreed by the quorum
  private JobModel activeJobModel;
  // denotes job model that is latest but may have not reached consensus
  private JobModel latestJobModel;
  private boolean hasLoadedMetadataResources = false;
  private String cachedJobModelVersion = null;
  private ZkBarrierForVersionUpgrade barrier;
  private ZkLeaderElector leaderElector;

  @VisibleForTesting
  ZkSessionMetrics zkSessionMetrics;

  @VisibleForTesting
  ScheduleAfterDebounceTime debounceTimer;

  @VisibleForTesting
  StreamPartitionCountMonitor streamPartitionCountMonitor = null;

  ZkJobCoordinator(String processorId, Config config, MetricsRegistry metricsRegistry, ZkUtils zkUtils, MetadataStore jobModelMetadataStore, MetadataStore coordinatorStreamStore) {
    // TODO: When we consolidate metadata stores for standalone, this check can be removed. For now, we expect this type.
    //   Keeping method signature as MetadataStore to avoid public API changes in the future
    Preconditions.checkArgument(coordinatorStreamStore instanceof CoordinatorStreamStore);

    this.config = config;
    this.metrics = new ZkJobCoordinatorMetrics(metricsRegistry);
    this.zkSessionMetrics = new ZkSessionMetrics(metricsRegistry);
    this.processorId = processorId;
    this.zkUtils = zkUtils;
    // setup a listener for a session state change
    // we are mostly interested in "session closed" and "new session created" events
    zkUtils.getZkClient().subscribeStateChanges(new ZkSessionStateChangedListener());
    leaderElector = new ZkLeaderElector(processorId, zkUtils);
    leaderElector.setLeaderElectorListener(new LeaderElectorListenerImpl());
    this.debounceTimeMs = new JobConfig(config).getDebounceTimeMs();
    debounceTimer = new ScheduleAfterDebounceTime(processorId);
    debounceTimer.setScheduledTaskCallback(throwable -> {
      LOG.error("Received exception in debounce timer! Stopping the job coordinator", throwable);
      stop();
    });
    this.barrier =  new ZkBarrierForVersionUpgrade(zkUtils.getKeyBuilder().getJobModelVersionBarrierPrefix(), zkUtils, new ZkBarrierListenerImpl(), debounceTimer);
    systemAdmins = new SystemAdmins(config, this.getClass().getSimpleName());
    streamMetadataCache = new StreamMetadataCache(systemAdmins, METADATA_CACHE_TTL_MS, SystemClock.instance());
    LocationIdProviderFactory locationIdProviderFactory =
        ReflectionUtil.getObj(new JobConfig(config).getLocationIdProviderFactory(), LocationIdProviderFactory.class);
    LocationIdProvider locationIdProvider = locationIdProviderFactory.getLocationIdProvider(config);
    this.locationId = locationIdProvider.getLocationId();
    this.coordinatorStreamStore = (CoordinatorStreamStore) coordinatorStreamStore;
    this.jobModelMetadataStore = jobModelMetadataStore;
  }

  @Override
  public void start() {
    ZkKeyBuilder keyBuilder = zkUtils.getKeyBuilder();
    zkUtils.validateZkVersion();
    zkUtils.validatePaths(new String[]{
        keyBuilder.getProcessorsPath(),
        keyBuilder.getJobModelVersionPath(),
        keyBuilder.getActiveJobModelVersionPath(),
        keyBuilder.getJobModelPathPrefix(),
        keyBuilder.getTaskLocalityPath()});

    this.jobModelMetadataStore.init();
    systemAdmins.start();
    leaderElector.tryBecomeLeader();
    zkUtils.subscribeToJobModelVersionChange(new ZkJobModelVersionChangeHandler(zkUtils));

    if (new ZkConfig(config).getEnableStartupWithActiveJobModel()) {
      debounceTimer.scheduleAfterDebounceTime(START_WORK_WITH_LAST_ACTIVE_JOB_MODEL, 0,
          this::startWorkWithLastActiveJobModel);
    }
  }

  @Override
  public void stop() {
    // Make the shutdown idempotent
    if (initiatedShutdown.compareAndSet(false, true)) {

      LOG.info("Shutting down JobCoordinator.");
      boolean shutdownSuccessful = false;

      // Notify the metrics about abandoning the leadership. Moving it up the chain in the shutdown sequence so that
      // in case of unclean shutdown, we get notified about lack of leader and we can set up some alerts around the absence of leader.
      metrics.isLeader.set(0);

      try {
        // todo: what does it mean for coordinator listener to be null? why not have it part of constructor?
        if (coordinatorListener != null) {
          coordinatorListener.onJobModelExpired();
        }

        debounceTimer.stopScheduler();

        if (leaderElector.amILeader()) {
          LOG.info("Resigning leadership for processorId: " + processorId);
          leaderElector.resignLeadership();
        }

        LOG.info("Shutting down ZkUtils.");
        // close zk connection
        if (zkUtils != null) {
          zkUtils.close();
        }

        LOG.debug("Shutting down system admins.");
        systemAdmins.stop();

        if (streamPartitionCountMonitor != null) {
          streamPartitionCountMonitor.stop();
        }

        if (coordinatorListener != null) {
          coordinatorListener.onCoordinatorStop();
        }

        jobModelMetadataStore.close();
        shutdownSuccessful = true;
      } catch (Throwable t) {
        LOG.error("Encountered errors during job coordinator stop.", t);
        if (coordinatorListener != null) {
          coordinatorListener.onCoordinatorFailure(t);
        }
      } finally {
        LOG.info("Job Coordinator shutdown finished with ShutdownComplete=" + shutdownSuccessful);
      }
    } else {
      LOG.info("Job Coordinator shutdown is in progress!");
    }
  }

  @Override
  public void setListener(JobCoordinatorListener listener) {
    this.coordinatorListener = listener;
  }

  @Override
  public JobModel getJobModel() {
    return latestJobModel;
  }

  @Override
  public String getProcessorId() {
    return processorId;
  }

  /*
   * The leader handles notifications for two types of events:
   *   1. Changes to the current set of processors in the group.
   *   2. Changes to the set of participants who have subscribed the the barrier
   */
  public void onProcessorChange(List<String> processors) {
    if (leaderElector.amILeader()) {
      LOG.info("ZkJobCoordinator::onProcessorChange - list of processors changed. List size=" + processors.size());
      debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs, this::doOnProcessorChange);
    }
  }

  void doOnProcessorChange() {
    List<ProcessorNode> processorNodes = zkUtils.getAllProcessorNodes();

    List<String> currentProcessorIds = new ArrayList<>();
    for (ProcessorNode processorNode : processorNodes) {
      currentProcessorIds.add(processorNode.getProcessorData().getProcessorId());
    }

    Set<String> uniqueProcessorIds = new HashSet<>(currentProcessorIds);

    if (currentProcessorIds.size() != uniqueProcessorIds.size()) {
      LOG.info("Processors: {} has duplicates. Not generating JobModel.", currentProcessorIds);
      return;
    }

    // Generate the JobModel
    LOG.info("Generating new JobModel with processors: {}.", currentProcessorIds);
    JobModel newJobModel = generateNewJobModel(processorNodes);

    /*
     * Leader skips the rebalance even if there are changes in the quorum as long as the work assignment remains the same
     * across all the processors. The optimization is useful in the following scenarios
     *   1. The processor in the quorum restarts within the debounce window. Originally, this would trigger rebalance
     *      across the processors stopping and starting their work assignment which is detrimental to availability of
     *      the system. e.g. common scenario during rolling upgrades
     *   2. Processors in the quorum which don't have work assignment and their failures/restarts don't impact the
     *      quorum.
     */
    if (new ZkConfig(config).getEnableStartupWithActiveJobModel() &&
        JobModelUtil.compareContainerModels(newJobModel, activeJobModel)) {
      LOG.info("Skipping rebalance since there are no changes in work assignment");
      return;
    }

    // Create checkpoint and changelog streams if they don't exist
    if (!hasLoadedMetadataResources) {
      loadMetadataResources(newJobModel);
      hasLoadedMetadataResources = true;
    }

    // Assign the next version of JobModel
    String currentJMVersion = zkUtils.getJobModelVersion();
    String nextJMVersion = zkUtils.getNextJobModelVersion(currentJMVersion);
    LOG.info("pid=" + processorId + "Generated new JobModel with version: " + nextJMVersion + " and processors: " + currentProcessorIds);

    // Publish the new job model
    publishJobModelToMetadataStore(newJobModel, nextJMVersion);

    // Start the barrier for the job model update
    barrier.create(nextJMVersion, currentProcessorIds);

    // Notify all processors about the new JobModel by updating JobModel Version number
    zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);

    LOG.info("pid=" + processorId + "Published new Job Model. Version = " + nextJMVersion);

    debounceTimer.scheduleAfterDebounceTime(ON_ZK_CLEANUP, 0, () -> zkUtils.cleanupZK(NUM_VERSIONS_TO_LEAVE));
  }

  @VisibleForTesting
  void publishJobModelToMetadataStore(JobModel jobModel, String nextJMVersion) {
    JobModelUtil.writeJobModel(jobModel, nextJMVersion, jobModelMetadataStore);
  }

  @VisibleForTesting
  JobModel readJobModelFromMetadataStore(String zkJobModelVersion) {
    return JobModelUtil.readJobModel(zkJobModelVersion, jobModelMetadataStore);
  }

  /**
   * Stores the configuration of the job in the coordinator stream.
   */
  @VisibleForTesting
  void loadMetadataResources(JobModel jobModel) {
    try {
      MetadataResourceUtil metadataResourceUtil = createMetadataResourceUtil(jobModel, config);
      metadataResourceUtil.createResources();

      if (coordinatorStreamStore != null) {
        // TODO: SAMZA-2273 - publish configs async
        CoordinatorStreamValueSerde jsonSerde = new CoordinatorStreamValueSerde(SetConfig.TYPE);
        NamespaceAwareCoordinatorStreamStore configStore =
            new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetConfig.TYPE);
        for (Map.Entry<String, String> entry : config.entrySet()) {
          byte[] serializedValue = jsonSerde.toBytes(entry.getValue());
          configStore.put(entry.getKey(), serializedValue);
        }
        configStore.flush();

        if (new JobConfig(config).getStartpointEnabled()) {
          // fan out the startpoints
          StartpointManager startpointManager = createStartpointManager();
          startpointManager.start();
          try {
            startpointManager.fanOut(JobModelUtil.getTaskToSystemStreamPartitions(jobModel));
          } finally {
            startpointManager.stop();
          }
        }
      } else {
        LOG.warn("No metadata store registered to this job coordinator. Config not written to the metadata store and no Startpoints fan out.");
      }
    } catch (IOException ex) {
      throw new SamzaException(String.format("IO exception while loading metadata resources."), ex);
    }
  }

  @VisibleForTesting
  MetadataResourceUtil createMetadataResourceUtil(JobModel jobModel, Config config) {
    return new MetadataResourceUtil(jobModel, metrics.getMetricsRegistry(), config);
  }

  /**
   * Generate new JobModel when becoming a leader or the list of processor changed.
   */
  @VisibleForTesting
  JobModel generateNewJobModel(List<ProcessorNode> processorNodes) {
    String zkJobModelVersion = zkUtils.getJobModelVersion();
    // If JobModel exists in zookeeper && cached JobModel version is unequal to JobModel version stored in zookeeper.
    if (zkJobModelVersion != null && !Objects.equals(cachedJobModelVersion, zkJobModelVersion)) {
      JobModel jobModel = readJobModelFromMetadataStore(zkJobModelVersion);
      for (ContainerModel containerModel : jobModel.getContainers().values()) {
        containerModel.getTasks().forEach((taskName, taskModel) -> changeLogPartitionMap.put(taskName, taskModel.getChangelogPartition().getPartitionId()));
      }
      cachedJobModelVersion = zkJobModelVersion;
    }

    GrouperMetadata grouperMetadata = getGrouperMetadata(zkJobModelVersion, processorNodes);
    JobModel model = JobModelManager.readJobModel(config, changeLogPartitionMap, streamMetadataCache, grouperMetadata);
    return new JobModel(new MapConfig(), model.getContainers());
  }

  @VisibleForTesting
  StreamPartitionCountMonitor getPartitionCountMonitor() {
    StreamMetadataCache streamMetadata = new StreamMetadataCache(systemAdmins, 0, SystemClock.instance());
    Set<SystemStream> inputStreamsToMonitor = new TaskConfig(config).getAllInputStreams();

    return new StreamPartitionCountMonitor(
        inputStreamsToMonitor,
        streamMetadata,
        metrics.getMetricsRegistry(),
        new JobConfig(config).getMonitorPartitionChangeFrequency(),
      streamsChanged -> {
        if (leaderElector.amILeader()) {
          debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, 0, this::doOnProcessorChange);
        }
      });
  }

  @VisibleForTesting
  StartpointManager createStartpointManager() {
    // This method is for easy mocking.
    return new StartpointManager(coordinatorStreamStore);
  }

  /**
   * Check if the new job model contains a different work assignment for the processor compared the last active job
   * model. In case of different work assignment, expire the current job model by invoking the <i>onJobModelExpired</i>
   * on the registered {@link JobCoordinatorListener}.
   * At this phase, the job model is yet to be agreed by the quorum and hence, this optimization helps availability of
   * the processors in the event no changes in the work assignment.
   *
   * @param newJobModel new job model published by the leader
   */
  @VisibleForTesting
  void checkAndExpireJobModel(JobModel newJobModel) {
    Preconditions.checkNotNull(newJobModel, "JobModel cannot be null");
    if (coordinatorListener == null) {
      LOG.info("Skipping job model expiration since there are no active listeners");
      return;
    }

    LOG.info("Checking for work assignment changes for processor {} between active job model {} and new job model {}",
        processorId, activeJobModel, newJobModel);
    if (JobModelUtil.compareContainerModelForProcessor(processorId, activeJobModel, newJobModel)) {
      LOG.info("Skipping job model expiration for processor {} due to no change in work assignment.", processorId);
    } else {
      LOG.info("Work assignment changed for the processor {}. Notifying job model expiration to coordinator listener", processorId);
      coordinatorListener.onJobModelExpired();
    }
  }

  /**
   * Checks if the new job model contains a different work assignment for the processor compared to the last active
   * job model. In case of different work assignment, update the task locality of the tasks associated with the
   * processor and notify new job model to the registered {@link JobCoordinatorListener}.
   *
   * @param newJobModel new job model agreed by the quorum
   */
  @VisibleForTesting
  void onNewJobModel(JobModel newJobModel) {
    Preconditions.checkNotNull(newJobModel, "JobModel cannot be null. Failing onNewJobModel");
    // start the container with the new model
    if (!JobModelUtil.compareContainerModelForProcessor(processorId, activeJobModel, newJobModel)) {
      LOG.info("Work assignment changed for the processor {}. Updating task locality and notifying coordinator listener", processorId);
      if (newJobModel.getContainers().containsKey(processorId)) {
        for (TaskName taskName : JobModelUtil.getTaskNamesForProcessor(processorId, newJobModel)) {
          zkUtils.writeTaskLocality(taskName, locationId);
        }

        if (coordinatorListener != null) {
          coordinatorListener.onNewJobModel(processorId, newJobModel);
        }
      }
    } else {
      /*
       * The implication of work assignment remaining the same can be categorized into
       *   1. Processor part of the job model
       *   2. Processor not part of the job model.
       * For both the state of the processor remains what it was when the rebalance started. e.g.,
       *   [1] should continue to process its work assignment without any interruption as part of the rebalance. i.e.,
       *       there will be no expiration of the existing work (a.k.a samza container won't be stopped) and also no
       *       notification to StreamProcessor about the rebalance since work assignment didn't change.
       *   [2] should have no work and be idle processor and will continue to be idle.
       */
      LOG.info("Skipping onNewJobModel since there are no changes in work assignment.");
    }

    /*
     * Update the last active job model to new job model regardless of whether the work assignment for the processor
     * has changed or not. It is important to do it so that all the processors has a consistent view what the latest
     * active job model is.
     */
    activeJobModel = newJobModel;
  }

  @VisibleForTesting
  JobModel getActiveJobModel() {
    return activeJobModel;
  }

  @VisibleForTesting
  void setActiveJobModel(JobModel jobModel) {
    activeJobModel = jobModel;
  }

  @VisibleForTesting
  void setDebounceTimer(ScheduleAfterDebounceTime scheduleAfterDebounceTime) {
    debounceTimer = scheduleAfterDebounceTime;
  }

  @VisibleForTesting
  void setLeaderElector(ZkLeaderElector zkLeaderElector) {
    leaderElector = zkLeaderElector;
  }

  @VisibleForTesting
  void setZkBarrierUpgradeForVersion(ZkBarrierForVersionUpgrade barrierUpgradeForVersion) {
    barrier = barrierUpgradeForVersion;
  }

  /**
   * Start the processor with the last known active job model. It is safe to start with last active job model
   * version in all the scenarios unless in the event of concurrent rebalance. We define safe as a way to ensure that no
   * two processors in the quorum have overlapping work assignments.
   * In case of a concurrent rebalance there two scenarios
   *   1. Job model version update happens before processor registration
   *   2. Job model version update happens after processor registration
   * ZK guarantees FIFO order for client operations, the processor is guaranteed to see all the state up until its
   * own registration.
   * For scenario 1, due to above guarantee, the processor will not start with old assignment due to mismatch in
   * latest vs last active. (If there is no mismatch, the scenario reduces to one of the safe scenarios)
   *
   * For scenario 2, it is possible for the processor to not see the writes by the leader about job model version change
   * but will eventually receive a notification on the job model version change and act on it (potentially stop
   * the work assignment if its not part of the job model).
   *
   * In the scenario where the processor doesn't start with last active job model version, it will continue to follow
   * the old protocol where leader should get notified about the processor registration and potentially trigger
   * rebalance and notify about changes in work assignment after consensus.
   * TODO: SAMZA-2635: Rebalances in standalone doesn't handle DAG changes for restarted processor
   */
  @VisibleForTesting
  void startWorkWithLastActiveJobModel() {
    LOG.info("Starting the processor with the recent active job model");
    String lastActiveJobModelVersion = zkUtils.getLastActiveJobModelVersion();
    String latestJobModelVersion = zkUtils.getJobModelVersion();

    if (lastActiveJobModelVersion != null && lastActiveJobModelVersion.equals(latestJobModelVersion)) {
      final JobModel lastActiveJobModel = readJobModelFromMetadataStore(lastActiveJobModelVersion);

      /*
       * TODO: SAMZA-2645: Allow onNewJobModel as a valid state transition. Due to this limitation, we are forced
       *  to invoke onJobModelExpired even if there is nothing to expire.
       */
      checkAndExpireJobModel(lastActiveJobModel);
      onNewJobModel(lastActiveJobModel);
    }
  }

  /**
   * Builds the {@link GrouperMetadataImpl} based upon provided {@param jobModelVersion}
   * and {@param processorNodes}.
   * @param jobModelVersion the most recent jobModelVersion available in the zookeeper.
   * @param processorNodes the list of live processors in the zookeeper.
   * @return the built grouper metadata.
   */
  private GrouperMetadataImpl getGrouperMetadata(String jobModelVersion, List<ProcessorNode> processorNodes) {
    Map<TaskName, String> taskToProcessorId = new HashMap<>();
    Map<TaskName, List<SystemStreamPartition>> taskToSSPs = new HashMap<>();
    if (jobModelVersion != null) {
      JobModel jobModel = readJobModelFromMetadataStore(jobModelVersion);
      for (ContainerModel containerModel : jobModel.getContainers().values()) {
        for (TaskModel taskModel : containerModel.getTasks().values()) {
          taskToProcessorId.put(taskModel.getTaskName(), containerModel.getId());
          for (SystemStreamPartition partition : taskModel.getSystemStreamPartitions()) {
            taskToSSPs.computeIfAbsent(taskModel.getTaskName(), k -> new ArrayList<>());
            taskToSSPs.get(taskModel.getTaskName()).add(partition);
          }
        }
      }
    }

    Map<String, LocationId> processorLocality = new HashMap<>();
    for (ProcessorNode processorNode : processorNodes) {
      ProcessorData processorData = processorNode.getProcessorData();
      processorLocality.put(processorData.getProcessorId(), processorData.getLocationId());
    }

    Map<TaskName, LocationId> taskLocality = zkUtils.readTaskLocality();
    return new GrouperMetadataImpl(processorLocality, taskLocality, taskToSSPs, taskToProcessorId);
  }

  class LeaderElectorListenerImpl implements LeaderElectorListener {
    @Override
    public void onBecomingLeader() {
      LOG.info("ZkJobCoordinator::onBecomeLeader - I became the leader");
      metrics.isLeader.set(1);
      zkUtils.subscribeToProcessorChange(new ProcessorChangeHandler(zkUtils));
      if (!new StorageConfig(config).hasDurableStores()) {
        // 1. Stop if there's a existing StreamPartitionCountMonitor running.
        if (streamPartitionCountMonitor != null) {
          streamPartitionCountMonitor.stop();
        }
        // 2. Start a new instance of StreamPartitionCountMonitor.
        streamPartitionCountMonitor = getPartitionCountMonitor();
        streamPartitionCountMonitor.start();
      }
      debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs, ZkJobCoordinator.this::doOnProcessorChange);
    }
  }

  class ZkBarrierListenerImpl implements ZkBarrierListener {
    private final String barrierAction = "BarrierAction";

    private long startTime = 0;

    @Override
    public void onBarrierCreated(String version) {
      // Start the timer for rebalancing
      startTime = System.nanoTime();

      metrics.barrierCreation.inc();
      if (leaderElector.amILeader()) {
        debounceTimer.scheduleAfterDebounceTime(barrierAction, (new ZkConfig(config)).getZkBarrierTimeoutMs(), () -> barrier.expire(version));
      }
    }

    public void onBarrierStateChanged(final String version, ZkBarrierForVersionUpgrade.State state) {
      LOG.info("JobModel version " + version + " obtained consensus successfully!");
      metrics.barrierStateChange.inc();
      metrics.singleBarrierRebalancingTime.update(System.nanoTime() - startTime);
      if (ZkBarrierForVersionUpgrade.State.DONE.equals(state)) {
        debounceTimer.scheduleAfterDebounceTime(barrierAction, 0, () -> {
          LOG.info("pid=" + processorId + "new version " + version + " of the job model got confirmed");
          /*
           * Publish the active job model version separately to denote that the job model version is agreed by
           * the quorum. The active job model version is used by processors as an optimization during their startup
           * so that processors can start with the work assignment that was agreed by the quorum and allows the
           * leader to skip the rebalance if there is no change in the work assignment for the quorum across
           * quorum changes (processors leaving or joining)
           */
          if (leaderElector.amILeader()) {
            zkUtils.publishActiveJobModelVersion(version);
          }
          onNewJobModel(getJobModel());
        });
      } else {
        if (ZkBarrierForVersionUpgrade.State.TIMED_OUT.equals(state)) {
          // no-op for non-leaders
          // for leader: make sure we do not stop - so generate a new job model
          LOG.warn("Barrier for version " + version + " timed out.");
          if (leaderElector.amILeader()) {
            LOG.info("Leader will schedule a new job model generation");
            // actual actions to do are the same as onProcessorChange
            debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs, ZkJobCoordinator.this::doOnProcessorChange);
          }
        }
      }
    }

    @Override
    public void onBarrierError(String version, Throwable t) {
      LOG.error("Encountered error while attaining consensus on JobModel version " + version);
      metrics.barrierError.inc();
      stop();
    }
  }

  class ProcessorChangeHandler extends ZkUtils.GenerationAwareZkChildListener {

    public ProcessorChangeHandler(ZkUtils zkUtils) {
      super(zkUtils, "ProcessorChangeHandler");
    }

    /**
     * Called when the children of the given path changed.
     *
     * @param parentPath      The parent path
     * @param currentChildren The children or null if the root node (parent path) was deleted.
     * @throws Exception
     */
    @Override
    public void doHandleChildChange(String parentPath, List<String> currentChildren)
        throws Exception {
      if (currentChildren == null) {
        LOG.info("handleChildChange on path " + parentPath + " was invoked with NULL list of children");
      } else {
        LOG.info("ProcessorChangeHandler::handleChildChange - Path: {} Current Children: {} ", parentPath, currentChildren);
        onProcessorChange(currentChildren);
      }
    }
  }

  class ZkJobModelVersionChangeHandler extends ZkUtils.GenerationAwareZkDataListener {

    public ZkJobModelVersionChangeHandler(ZkUtils zkUtils) {
      super(zkUtils, "ZkJobModelVersionChangeHandler");
    }

    /**
     * Invoked when there is a change to the JobModelVersion z-node. It signifies that a new JobModel version is available.
     */
    @Override
    public void doHandleDataChange(String dataPath, Object data) {
      debounceTimer.scheduleAfterDebounceTime(JOB_MODEL_VERSION_CHANGE, 0, () -> {
        String jobModelVersion = (String) data;

        LOG.info("Got a notification for new JobModel version. Path = {} Version = {}", dataPath, data);

        latestJobModel = readJobModelFromMetadataStore(jobModelVersion);
        LOG.info("pid=" + processorId + ": new JobModel is available. Version =" + jobModelVersion + "; JobModel = " + latestJobModel);
        checkAndExpireJobModel(latestJobModel);
        // update ZK and wait for all the processors to get this new version
        barrier.join(jobModelVersion, processorId);
      });
    }

    @Override
    public void doHandleDataDeleted(String dataPath) {
      LOG.warn("JobModel version z-node has been deleted. Shutting down the coordinator" + dataPath);
      debounceTimer.scheduleAfterDebounceTime("JOB_MODEL_VERSION_DELETED", 0,  () -> stop());
    }
  }


  /// listener to handle ZK state change events
  @VisibleForTesting
  class ZkSessionStateChangedListener implements IZkStateListener {

    private static final String ZK_SESSION_ERROR = "ZK_SESSION_ERROR";
    private static final String ZK_SESSION_EXPIRED = "ZK_SESSION_EXPIRED";

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) {
      switch (state) {
        case Expired:
          // if the session has expired it means that all the registration's ephemeral nodes are gone.
          zkSessionMetrics.zkSessionExpirations.inc();
          LOG.warn("Got " + state.toString() + " event for processor=" + processorId + ". Stopping the container and unregister the processor node.");

          // increase generation of the ZK session. All the callbacks from the previous generation will be ignored.
          zkUtils.incGeneration();

          // reset all the values that might have been from the previous session (e.g ephemeral node path)
          zkUtils.unregister();
          if (leaderElector.amILeader()) {
            leaderElector.resignLeadership();
          }

          if (streamPartitionCountMonitor != null) {
            streamPartitionCountMonitor.stop();
          }

          /**
           * After this event, one amongst the following two things could potentially happen:
           * A. On successful reconnect to another zookeeper server in ensemble, this processor is going to
           * join the group again as new processor. In this case, retaining buffered events in debounceTimer will be unnecessary.
           * B. If zookeeper server is unreachable, handleSessionEstablishmentError callback will be triggered indicating
           * a error scenario. In this case, retaining buffered events in debounceTimer will be unnecessary.
           */
          LOG.info("Cancelling all scheduled actions in session expiration for processorId: {}.", processorId);
          debounceTimer.cancelAllScheduledActions();
          debounceTimer.scheduleAfterDebounceTime(ZK_SESSION_EXPIRED, 0, () -> {
            if (coordinatorListener != null) {
              coordinatorListener.onJobModelExpired();
            }
          });

          return;
        case Disconnected:
          // if the session has expired it means that all the registration's ephemeral nodes are gone.
          zkSessionMetrics.zkSessionDisconnects.inc();
          LOG.warn("Got " + state.toString() + " event for processor=" + processorId + ". Scheduling a coordinator stop.");

          // If the connection is not restored after debounceTimeMs, the process is considered dead.
          debounceTimer.scheduleAfterDebounceTime(ZK_SESSION_ERROR, new ZkConfig(config).getZkSessionTimeoutMs(), () -> stop());
          return;
        case AuthFailed:
        case NoSyncConnected:
        case Unknown:
          zkSessionMetrics.zkSessionErrors.inc();
          LOG.warn("Got unexpected failure event " + state.toString() + " for processor=" + processorId + ". Stopping the job coordinator.");
          debounceTimer.scheduleAfterDebounceTime(ZK_SESSION_ERROR, 0, () -> stop());
          return;
        case SyncConnected:
          zkSessionMetrics.zkSyncConnected.inc();
          LOG.info("Got syncconnected event for processor=" + processorId + ".");
          debounceTimer.cancelAction(ZK_SESSION_ERROR);
          return;
        default:
          // received SyncConnected, ConnectedReadOnly, and SaslAuthenticated. NoOp
          LOG.info("Got ZK event " + state.toString() + " for processor=" + processorId + ". Continue");
      }
    }

    @Override
    public void handleNewSession() {
      zkSessionMetrics.zkNewSessions.inc();
      LOG.info("Got new session created event for processor=" + processorId);
      debounceTimer.cancelAllScheduledActions();
      LOG.info("register zk controller for the new session");
      leaderElector.tryBecomeLeader();
      zkUtils.subscribeToJobModelVersionChange(new ZkJobModelVersionChangeHandler(zkUtils));
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error) {
      // this means we cannot connect to zookeeper to establish a session
      zkSessionMetrics.zkSessionErrors.inc();
      LOG.info("handleSessionEstablishmentError received for processor=" + processorId, error);
      debounceTimer.scheduleAfterDebounceTime(ZK_SESSION_ERROR, 0, () -> stop());
    }
  }

  @VisibleForTesting
  public ZkUtils getZkUtils() {
    return zkUtils;
  }
}
