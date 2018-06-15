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
import java.util.concurrent.atomic.AtomicBoolean;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.runtime.ProcessorIdGenerator;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.util.MetricsReporterLoader;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
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

  private final ZkUtils zkUtils;
  private final String processorId;

  private final Config config;
  private final ZkBarrierForVersionUpgrade barrier;
  private final ZkJobCoordinatorMetrics metrics;
  private final Map<String, MetricsReporter> reporters;
  private final ZkLeaderElector leaderElector;
  private final AtomicBoolean initiatedShutdown = new AtomicBoolean(false);
  private final StreamMetadataCache streamMetadataCache;
  private final SystemAdmins systemAdmins;
  private final int debounceTimeMs;
  private final Map<TaskName, Integer> changeLogPartitionMap = new HashMap<>();


  private JobCoordinatorListener coordinatorListener = null;
  private JobModel newJobModel;
  private boolean hasCreatedStreams = false;
  private String cachedJobModelVersion = null;

  @VisibleForTesting
  ScheduleAfterDebounceTime debounceTimer;

  ZkJobCoordinator(Config config, MetricsRegistry metricsRegistry, ZkUtils zkUtils) {
    this.config = config;

    this.metrics = new ZkJobCoordinatorMetrics(metricsRegistry);

    this.processorId = createProcessorId(config);
    this.zkUtils = zkUtils;
    // setup a listener for a session state change
    // we are mostly interested in "session closed" and "new session created" events
    zkUtils.getZkClient().subscribeStateChanges(new ZkSessionStateChangedListener());
    leaderElector = new ZkLeaderElector(processorId, zkUtils);
    leaderElector.setLeaderElectorListener(new LeaderElectorListenerImpl());
    this.debounceTimeMs = new JobConfig(config).getDebounceTimeMs();
    this.reporters = MetricsReporterLoader.getMetricsReporters(new MetricsConfig(config), processorId);
    debounceTimer = new ScheduleAfterDebounceTime(processorId);
    debounceTimer.setScheduledTaskCallback(throwable -> {
        LOG.error("Received exception in debounce timer! Stopping the job coordinator", throwable);
        stop();
      });
    this.barrier =  new ZkBarrierForVersionUpgrade(zkUtils.getKeyBuilder().getJobModelVersionBarrierPrefix(), zkUtils, new ZkBarrierListenerImpl(), debounceTimer);
    systemAdmins = new SystemAdmins(config);
    streamMetadataCache = new StreamMetadataCache(systemAdmins, METADATA_CACHE_TTL_MS, SystemClock.instance());
  }

  @Override
  public void start() {
    startMetrics();
    systemAdmins.start();
    ZkKeyBuilder keyBuilder = zkUtils.getKeyBuilder();
    zkUtils.validateZkVersion();
    zkUtils.validatePaths(new String[]{keyBuilder.getProcessorsPath(), keyBuilder.getJobModelVersionPath(), keyBuilder
        .getJobModelPathPrefix()});
    leaderElector.tryBecomeLeader();
    zkUtils.subscribeToJobModelVersionChange(new ZkJobModelVersionChangeHandler(zkUtils));
  }

  @Override
  public void stop() {
    // Make the shutdown idempotent
    if (initiatedShutdown.compareAndSet(false, true)) {

      LOG.info("Shutting down JobCoordinator.");
      boolean shutdownSuccessful = false;

      // Notify the metrics about abandoning the leadership. Moving it up the chain in the shutdown sequence so that
      // in case of unclean shutdown, we get notified about lack of leader and we can set up some alerts around the absence of leader.
      metrics.isLeader.set(false);

      try {
        // todo: what does it mean for coordinator listener to be null? why not have it part of constructor?
        if (coordinatorListener != null) {
          coordinatorListener.onJobModelExpired();
        }

        debounceTimer.stopScheduler();

        LOG.info("Shutting down Zk Client.");
        if (leaderElector.amILeader()) {
          leaderElector.resignLeadership();
        }

        // close zk connection
        if (zkUtils != null) {
          zkUtils.close();
        }

        LOG.debug("Shutting down system admins.");
        systemAdmins.stop();

        LOG.debug("Shutting down metrics.");
        shutdownMetrics();

        if (coordinatorListener != null) {
          coordinatorListener.onCoordinatorStop();
        }

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

  private void startMetrics() {
    for (MetricsReporter reporter : reporters.values()) {
      reporter.register("job-coordinator-" + processorId, (ReadableMetricsRegistry) metrics.getMetricsRegistry());
      reporter.start();
    }
  }

  private void shutdownMetrics() {
    for (MetricsReporter reporter : reporters.values()) {
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

  /*
   * The leader handles notifications for two types of events:
   *   1. Changes to the current set of processors in the group.
   *   2. Changes to the set of participants who have subscribed the the barrier
   */
  public void onProcessorChange(List<String> processors) {
    if (leaderElector.amILeader()) {
      LOG.info("ZkJobCoordinator::onProcessorChange - list of processors changed! List size=" + processors.size());
      debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs, () -> doOnProcessorChange(processors));
    }
  }

  void doOnProcessorChange(List<String> processors) {
    // if list of processors is empty - it means we are called from 'onBecomeLeader'
    // TODO: Handle empty currentProcessorIds.
    List<String> currentProcessorIds = zkUtils.getSortedActiveProcessorsIDs();
    Set<String> uniqueProcessorIds = new HashSet<>(currentProcessorIds);

    if (currentProcessorIds.size() != uniqueProcessorIds.size()) {
      LOG.info("Processors: {} has duplicates. Not generating JobModel.", currentProcessorIds);
      return;
    }

    // Generate the JobModel
    LOG.info("Generating new JobModel with processors: {}.", currentProcessorIds);
    JobModel jobModel = generateNewJobModel(currentProcessorIds);

    // Create checkpoint and changelog streams if they don't exist
    if (!hasCreatedStreams) {
      CheckpointManager checkpointManager = new TaskConfigJava(config).getCheckpointManager(metrics.getMetricsRegistry());
      if (checkpointManager != null) {
        checkpointManager.createResources();
      }

      // Pass in null Coordinator consumer and producer because ZK doesn't have coordinator streams.
      ChangelogStreamManager.createChangelogStreams(config, jobModel.maxChangeLogStreamPartitions);
      hasCreatedStreams = true;
    }

    // Assign the next version of JobModel
    String currentJMVersion = zkUtils.getJobModelVersion();
    String nextJMVersion = zkUtils.getNextJobModelVersion(currentJMVersion);
    LOG.info("pid=" + processorId + "Generated new JobModel with version: " + nextJMVersion + " and processors: " + currentProcessorIds);

    // Publish the new job model
    zkUtils.publishJobModel(nextJMVersion, jobModel);

    // Start the barrier for the job model update
    barrier.create(nextJMVersion, currentProcessorIds);

    // Notify all processors about the new JobModel by updating JobModel Version number
    zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);

    LOG.info("pid=" + processorId + "Published new Job Model. Version = " + nextJMVersion);

    debounceTimer.scheduleAfterDebounceTime(ON_ZK_CLEANUP, 0, () -> zkUtils.cleanupZK(NUM_VERSIONS_TO_LEAVE));
  }

  private String createProcessorId(Config config) {
    // TODO: This check to be removed after 0.13+
    ApplicationConfig appConfig = new ApplicationConfig(config);
    if (appConfig.getProcessorId() != null) {
      return appConfig.getProcessorId();
    } else if (StringUtils.isNotBlank(appConfig.getAppProcessorIdGeneratorClass())) {
      ProcessorIdGenerator idGenerator =
          Util.getObj(appConfig.getAppProcessorIdGeneratorClass(), ProcessorIdGenerator.class);
      return idGenerator.generateProcessorId(config);
    } else {
      throw new ConfigException(String
          .format("Expected either %s or %s to be configured", ApplicationConfig.PROCESSOR_ID,
              ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS));
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
    JobModel model = JobModelManager.readJobModel(this.config, changeLogPartitionMap, null, streamMetadataCache, processors);
    return new JobModel(new MapConfig(), model.getContainers());
  }

  class LeaderElectorListenerImpl implements LeaderElectorListener {
    @Override
    public void onBecomingLeader() {
      LOG.info("ZkJobCoordinator::onBecomeLeader - I became the leader!");
      metrics.isLeader.set(true);
      zkUtils.subscribeToProcessorChange(new ProcessorChangeHandler(zkUtils));
      debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs, () -> {
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

          // read the new Model
            JobModel jobModel = getJobModel();
            // start the container with the new model
            if (coordinatorListener != null) {
              coordinatorListener.onNewJobModel(processorId, jobModel);
            }
          });
      } else {
        if (ZkBarrierForVersionUpgrade.State.TIMED_OUT.equals(state)) {
          // no-op for non-leaders
          // for leader: make sure we do not stop - so generate a new job model
          LOG.warn("Barrier for version " + version + " timed out.");
          if (leaderElector.amILeader()) {
            LOG.info("Leader will schedule a new job model generation");
            debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs, () -> {
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

          newJobModel = zkUtils.getJobModel(jobModelVersion);
          LOG.info("pid=" + processorId + ": new JobModel is available. Version =" + jobModelVersion + "; JobModel = " + newJobModel);

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
            barrier.join(jobModelVersion, processorId);
          }
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
          LOG.warn("Got " + state.toString() + " event for processor=" + processorId + ". Stopping the container and unregister the processor node.");

          // increase generation of the ZK session. All the callbacks from the previous generation will be ignored.
          zkUtils.incGeneration();

          // reset all the values that might have been from the previous session (e.g ephemeral node path)
          zkUtils.unregister();
          if (leaderElector.amILeader()) {
            leaderElector.resignLeadership();
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
          LOG.warn("Got " + state.toString() + " event for processor=" + processorId + ". Scheduling a coordinator stop.");

          // If the connection is not restored after debounceTimeMs, the process is considered dead.
          debounceTimer.scheduleAfterDebounceTime(ZK_SESSION_ERROR, new ZkConfig(config).getZkSessionTimeoutMs(), () -> stop());
          return;
        case AuthFailed:
        case NoSyncConnected:
        case Unknown:
          LOG.warn("Got unexpected failure event " + state.toString() + " for processor=" + processorId + ". Stopping the job coordinator.");
          debounceTimer.scheduleAfterDebounceTime(ZK_SESSION_ERROR, 0, () -> stop());
          return;
        case SyncConnected:
          LOG.info("Got syncconnected event for processor=" + processorId + ".");
          debounceTimer.cancelAction(ZK_SESSION_ERROR);
          return;
        default:
          // received SyncConnected, ConnectedReadOnly, and SaslAuthenticated. NoOp
          LOG.info("Got ZK event " + state.toString() + " for processor=" + processorId + ". Continue");
          return;
      }
    }

    @Override
    public void handleNewSession() {
      LOG.info("Got new session created event for processor=" + processorId);
      debounceTimer.cancelAllScheduledActions();
      LOG.info("register zk controller for the new session");
      leaderElector.tryBecomeLeader();
      zkUtils.subscribeToJobModelVersionChange(new ZkJobModelVersionChangeHandler(zkUtils));
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error) {
      // this means we cannot connect to zookeeper to establish a session
      LOG.info("handleSessionEstablishmentError received for processor=" + processorId, error);
      debounceTimer.scheduleAfterDebounceTime(ZK_SESSION_ERROR, 0, () -> stop());
    }
  }

  @VisibleForTesting
  public ZkUtils getZkUtils() {
    return zkUtils;
  }
}
