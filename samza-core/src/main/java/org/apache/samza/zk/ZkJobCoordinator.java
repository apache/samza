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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.checkpoint.CheckpointManagerUtil;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.runtime.ProcessorIdGenerator;
import org.apache.samza.storage.ChangelogPartitionManager;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.util.ClassLoaderHelper;
import org.apache.samza.util.MetricsReporterLoader;
import org.apache.samza.util.SystemClock;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobCoordinator for stand alone processor managed via Zookeeper.
 */
public class ZkJobCoordinator implements JobCoordinator, ZkControllerListener {
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

  private final ZkUtils zkUtils;
  private final String processorId;
  private final ZkController zkController;

  private final Config config;
  private final ZkBarrierForVersionUpgrade barrier;
  private final ZkJobCoordinatorMetrics metrics;
  private final Map<String, MetricsReporter> reporters;

  private StreamMetadataCache streamMetadataCache = null;
  private SystemAdmins systemAdmins = null;
  private ScheduleAfterDebounceTime debounceTimer = null;
  private JobCoordinatorListener coordinatorListener = null;
  private JobModel newJobModel;
  private int debounceTimeMs;
  private boolean hasCreatedStreams = false;
  private String cachedJobModelVersion = null;
  private Map<TaskName, Integer> changeLogPartitionMap = new HashMap<>();

  ZkJobCoordinator(Config config, MetricsRegistry metricsRegistry, ZkUtils zkUtils) {
    this.config = config;

    this.metrics = new ZkJobCoordinatorMetrics(metricsRegistry);

    this.processorId = createProcessorId(config);
    this.zkUtils = zkUtils;
    // setup a listener for a session state change
    // we are mostly interested in "session closed" and "new session created" events
    zkUtils.getZkClient().subscribeStateChanges(new ZkSessionStateChangedListener());
    LeaderElector leaderElector = new ZkLeaderElector(processorId, zkUtils);
    leaderElector.setLeaderElectorListener(new LeaderElectorListenerImpl());
    this.zkController = new ZkControllerImpl(processorId, zkUtils, this, leaderElector);
    this.barrier =  new ZkBarrierForVersionUpgrade(
        zkUtils.getKeyBuilder().getJobModelVersionBarrierPrefix(),
        zkUtils,
        new ZkBarrierListenerImpl());
    this.debounceTimeMs = new JobConfig(config).getDebounceTimeMs();
    this.reporters = MetricsReporterLoader.getMetricsReporters(new MetricsConfig(config), processorId);
    debounceTimer = new ScheduleAfterDebounceTime();
    debounceTimer.setScheduledTaskCallback(throwable -> {
        LOG.error("Received exception from in JobCoordinator Processing!", throwable);
        stop();
      });
    systemAdmins = new SystemAdmins(config);
    streamMetadataCache = new StreamMetadataCache(systemAdmins, METADATA_CACHE_TTL_MS, SystemClock.instance());
  }

  @Override
  public void start() {
    startMetrics();
    systemAdmins.start();
    zkController.register();
  }

  @Override
  public synchronized void stop() {
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }
    //Setting the isLeader metric to false when the stream processor shuts down because it does not remain the leader anymore
    metrics.isLeader.set(false);
    debounceTimer.stopScheduler();
    zkController.stop();

    shutdownMetrics();
    if (coordinatorListener != null) {
      coordinatorListener.onCoordinatorStop();
    }
    systemAdmins.stop();
  }

  private void startMetrics() {
    for (MetricsReporter reporter: reporters.values()) {
      reporter.register("job-coordinator-" + processorId, (ReadableMetricsRegistry) metrics.getMetricsRegistry());
      reporter.start();
    }
  }

  private void shutdownMetrics() {
    for (MetricsReporter reporter: reporters.values()) {
      reporter.stop();
    }
  }

  @Override
  public void setListener(JobCoordinatorListener listener) {
    this.coordinatorListener = listener;
  }

  @Override
  public JobModel getJobModel() {
    return newJobModel;
  }

  @Override
  public String getProcessorId() {
    return processorId;
  }

  //////////////////////////////////////////////// LEADER stuff ///////////////////////////
  @Override
  public void onProcessorChange(List<String> processors) {
    LOG.info("ZkJobCoordinator::onProcessorChange - list of processors changed! List size=" + processors.size());
    debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs,
        () -> doOnProcessorChange(processors));
  }

  void doOnProcessorChange(List<String> processors) {
    // if list of processors is empty - it means we are called from 'onBecomeLeader'
    // TODO: Handle empty currentProcessorIds.
    List<String> currentProcessorIds = getActualProcessorIds(processors);
    Set<String> uniqueProcessorIds = new HashSet<String>(currentProcessorIds);

    if (currentProcessorIds.size() != uniqueProcessorIds.size()) {
      LOG.info("Processors: {} has duplicates. Not generating job model.", currentProcessorIds);
      return;
    }

    // Generate the JobModel
    JobModel jobModel = generateNewJobModel(currentProcessorIds);

    // Create checkpoint and changelog streams if they don't exist
    if (!hasCreatedStreams) {
      CheckpointManagerUtil.createAndInit(jobModel.getConfig(), metrics.getMetricsRegistry());

      // Pass in null Coordinator consumer and producer because ZK doesn't have coordinator streams.
      ChangelogPartitionManager changelogManager = new ChangelogPartitionManager(this.getClass().getSimpleName());
      changelogManager.createChangeLogStreams(jobModel.getConfig(), jobModel.maxChangeLogStreamPartitions);
      hasCreatedStreams = true;
    }

    // Assign the next version of JobModel
    String currentJMVersion = zkUtils.getJobModelVersion();
    String nextJMVersion = zkUtils.getNextJobModelVersion(currentJMVersion);
    LOG.info("pid=" + processorId + "Generated new Job Model. Version = " + nextJMVersion);

    // Publish the new job model
    zkUtils.publishJobModel(nextJMVersion, jobModel);

    // Start the barrier for the job model update
    barrier.create(nextJMVersion, currentProcessorIds);

    // Notify all processors about the new JobModel by updating JobModel Version number
    zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);

    LOG.info("pid=" + processorId + "Published new Job Model. Version = " + nextJMVersion);

    debounceTimer.scheduleAfterDebounceTime(ON_ZK_CLEANUP, 0, () -> zkUtils.cleanupZK(NUM_VERSIONS_TO_LEAVE));
  }

  @Override
  public void onNewJobModelAvailable(final String version) {
    debounceTimer.scheduleAfterDebounceTime(JOB_MODEL_VERSION_CHANGE, 0, () ->
      {
        LOG.info("pid=" + processorId + ": new JobModel available");
        // get the new job model from ZK
        newJobModel = zkUtils.getJobModel(version);
        LOG.info("pid=" + processorId + ": new JobModel available. ver=" + version + "; jm = " + newJobModel);

        if (!newJobModel.getContainers().containsKey(processorId)) {
          LOG.info("New JobModel does not contain pid={}. Stopping this processor. New JobModel: {}",
              processorId, newJobModel);
          stop();
        } else {
          // stop current work
          if (coordinatorListener != null) {
            coordinatorListener.onJobModelExpired();
          }
          // update ZK and wait for all the processors to get this new version
          barrier.join(version, processorId);
        }
      });
  }

  @Override
  public void onNewJobModelConfirmed(String version) {
    LOG.info("pid=" + processorId + "new version " + version + " of the job model got confirmed");
    // get the new Model
    JobModel jobModel = getJobModel();

    // start the container with the new model
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

  private List<String> getActualProcessorIds(List<String> processors) {
    if (processors.size() > 0) {
      // we should use this list
      // but it needs to be converted into PIDs, which is part of the data
      return zkUtils.getActiveProcessorsIDs(processors);
    } else {
      // get the current list of processors
      return zkUtils.getSortedActiveProcessorsIDs();
    }
  }

  /**
   * Generate new JobModel when becoming a leader or the list of processor changed.
   */
  private JobModel generateNewJobModel(List<String> processors) {
    String zkJobModelVersion = zkUtils.getJobModelVersion();
    // If JobModel exists in zookeeper && cached JobModel version is unequal to JobModel version stored in zookeeper.
    if (zkJobModelVersion != null && !Objects.equals(cachedJobModelVersion, zkJobModelVersion)) {
      JobModel jobModel = zkUtils.getJobModel(zkJobModelVersion);
      for (ContainerModel containerModel : jobModel.getContainers().values()) {
        containerModel.getTasks().forEach((taskName, taskModel) -> changeLogPartitionMap.put(taskName, taskModel.getChangelogPartition().getPartitionId()));
      }
      cachedJobModelVersion = zkJobModelVersion;
    }
    /**
     * Host affinity is not supported in standalone. Hence, LocalityManager(which is responsible for container
     * to host mapping) is passed in as null when building the jobModel.
     */
    return JobModelManager.readJobModel(this.config, changeLogPartitionMap, null, streamMetadataCache, processors);
  }

  class LeaderElectorListenerImpl implements LeaderElectorListener {
    @Override
    public void onBecomingLeader() {
      LOG.info("ZkJobCoordinator::onBecomeLeader - I became the leader!");
      metrics.isLeader.set(true);
      zkController.subscribeToProcessorChange();
      debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs, () ->
        {
          // actual actions to do are the same as onProcessorChange
          doOnProcessorChange(new ArrayList<>());
        });
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
      debounceTimer.scheduleAfterDebounceTime(
          barrierAction,
        (new ZkConfig(config)).getZkBarrierTimeoutMs(),
        () -> barrier.expire(version)
      );
    }

    public void onBarrierStateChanged(final String version, ZkBarrierForVersionUpgrade.State state) {
      LOG.info("JobModel version " + version + " obtained consensus successfully!");
      metrics.barrierStateChange.inc();
      metrics.singleBarrierRebalancingTime.update(System.nanoTime() - startTime);
      if (ZkBarrierForVersionUpgrade.State.DONE.equals(state)) {
        debounceTimer.scheduleAfterDebounceTime(barrierAction, 0, () -> onNewJobModelConfirmed(version));
      } else {
        if (ZkBarrierForVersionUpgrade.State.TIMED_OUT.equals(state)) {
          // no-op for non-leaders
          // for leader: make sure we do not stop - so generate a new job model
          LOG.warn("Barrier for version " + version + " timed out.");
          if (zkController.isLeader()) {
            LOG.info("Leader will schedule a new job model generation");
            debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs, () ->
              {
                // actual actions to do are the same as onProcessorChange
                doOnProcessorChange(new ArrayList<>());
              });
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

  /// listener to handle session expiration
  class ZkSessionStateChangedListener implements IZkStateListener {

    private static final String ZK_SESSION_ERROR = "ZK_SESSION_ERROR";

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state)
        throws Exception {
      if (state == Watcher.Event.KeeperState.Expired) {
        // if the session has expired it means that all the registration's ephemeral nodes are gone.
        LOG.warn("Got session expired event for processor=" + processorId);

        // increase generation of the ZK connection. All the callbacks from the previous generation will be ignored.
        zkUtils.incGeneration();

        if (coordinatorListener != null) {
          coordinatorListener.onJobModelExpired();
        }
        // reset all the values that might have been from the previous session (e.g ephemeral node path)
        zkUtils.unregister();

      }
    }

    @Override
    public void handleNewSession()
        throws Exception {
      LOG.info("Got new session created event for processor=" + processorId);

      LOG.info("register zk controller for the new session");
      zkController.register();
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error)
        throws Exception {
      // this means we cannot connect to zookeeper
      LOG.info("handleSessionEstablishmentError received for processor=" + processorId, error);
      debounceTimer.scheduleAfterDebounceTime(ZK_SESSION_ERROR, 0, () -> stop());
    }
  }

  @VisibleForTesting
  public ZkUtils getZkUtils() {
    return zkUtils;
  }
}
