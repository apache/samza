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

import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.BarrierForVersionUpgrade;
import org.apache.samza.coordinator.BarrierForVersionUpgradeListener;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.runtime.ProcessorIdGenerator;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.util.ClassLoaderHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * JobCoordinator for stand alone processor managed via Zookeeper.
 */
public class ZkJobCoordinator implements JobCoordinator, ZkControllerListener {
  private static final Logger LOG = LoggerFactory.getLogger(ZkJobCoordinator.class);
  // TODO: MetadataCache timeout has to be 0 for the leader so that it can always have the latest information associated
  // with locality. Since host-affinity is not yet implemented, this can be fixed as part of SAMZA-1197
  private static final int METADATA_CACHE_TTL_MS = 5000;

  private final ZkUtils zkUtils;
  private final String processorId;
  private final ZkController zkController;

  private final Config config;
  private final CoordinationUtils coordinationUtils;
  private final ZkBarrierForVersionUpgrade barrier;

  private StreamMetadataCache streamMetadataCache = null;
  private ScheduleAfterDebounceTime debounceTimer = null;
  private JobCoordinatorListener coordinatorListener = null;
  private JobModel newJobModel;

  public ZkJobCoordinator(Config config) {
    this.config = config;
    this.processorId = createProcessorId(config);
    this.coordinationUtils = new ZkCoordinationServiceFactory()
        .getCoordinationService(new ApplicationConfig(config).getGlobalAppId(), String.valueOf(processorId), config);
    this.zkUtils = ((ZkCoordinationUtils) coordinationUtils).getZkUtils();
    LeaderElector leaderElector = new ZkLeaderElector(processorId, zkUtils);
    leaderElector.setLeaderElectorListener(new LeaderElectorListenerImpl());
    this.zkController = new ZkControllerImpl(processorId, zkUtils, this, leaderElector);
    this.barrier =  new ZkBarrierForVersionUpgrade(
        zkUtils.getKeyBuilder().getJobModelVersionBarrierPrefix(),
        zkUtils,
        new ZkBarrierForVersionUpgradeListener());
  }

  @Override
  public void start() {
    streamMetadataCache = StreamMetadataCache.apply(METADATA_CACHE_TTL_MS, config);
    debounceTimer = new ScheduleAfterDebounceTime(throwable -> {
        LOG.error("Received exception from in JobCoordinator Processing!", throwable);
        stop();
      });

    zkController.register();
  }

  @Override
  public synchronized void stop() {
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }

    debounceTimer.stopScheduler();
    zkController.stop();

    if (coordinatorListener != null) {
      coordinatorListener.onCoordinatorStop();
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
    debounceTimer.scheduleAfterDebounceTime(ScheduleAfterDebounceTime.ON_PROCESSOR_CHANGE,
        ScheduleAfterDebounceTime.DEBOUNCE_TIME_MS, () -> doOnProcessorChange(processors));
  }

  void doOnProcessorChange(List<String> processors) {
    // if list of processors is empty - it means we are called from 'onBecomeLeader'
    List<String> currentProcessorIds = getActualProcessorIds(processors);

    // Generate the JobModel
    JobModel jobModel = generateNewJobModel(currentProcessorIds);

    // Assign the next version of JobModel
    String currentJMVersion  = zkUtils.getJobModelVersion();
    String nextJMVersion;
    if (currentJMVersion == null) {
      nextJMVersion = "1";
    } else {
      nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion) + 1);
    }
    LOG.info("pid=" + processorId + "Generated new Job Model. Version = " + nextJMVersion);

    // Publish the new job model
    zkUtils.publishJobModel(nextJMVersion, jobModel);

    // Start the barrier for the job model update
    barrier.start(nextJMVersion, currentProcessorIds);

    // Notify all processors about the new JobModel by updating JobModel Version number
    zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);

    LOG.info("pid=" + processorId + "Published new Job Model. Version = " + nextJMVersion);
  }

  @Override
  public void onNewJobModelAvailable(final String version) {
    debounceTimer.scheduleAfterDebounceTime(ScheduleAfterDebounceTime.JOB_MODEL_VERSION_CHANGE, 0, () ->
      {
        LOG.info("pid=" + processorId + "new JobModel available");
        // stop current work
        if (coordinatorListener != null) {
          coordinatorListener.onJobModelExpired();
        }
        // get the new job model from ZK
        newJobModel = zkUtils.getJobModel(version);

        LOG.info("pid=" + processorId + ": new JobModel available. ver=" + version + "; jm = " + newJobModel);

        // update ZK and wait for all the processors to get this new version
        barrier.joinBarrier(version, processorId);
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
    } else if (appConfig.getAppProcessorIdGeneratorClass() != null) {
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
    return JobModelManager.readJobModel(this.config, Collections.emptyMap(), null, streamMetadataCache,
        processors);
  }

  class LeaderElectorListenerImpl implements LeaderElectorListener {
    @Override
    public void onBecomingLeader() {
      LOG.info("ZkJobCoordinator::onBecomeLeader - I became the leader!");
      zkController.subscribeToProcessorChange();
      debounceTimer.scheduleAfterDebounceTime(
        ScheduleAfterDebounceTime.ON_PROCESSOR_CHANGE,
        ScheduleAfterDebounceTime.DEBOUNCE_TIME_MS, () -> {
          // actual actions to do are the same as onProcessorChange()
          doOnProcessorChange(new ArrayList<>());
        });
    }
  }

  class ZkBarrierForVersionUpgradeListener implements BarrierForVersionUpgradeListener {
    private final String barrierAction = "BarrierAction";
    @Override
    public void onBarrierCreated(String version) {
      debounceTimer.scheduleAfterDebounceTime(
          barrierAction,
        (new ZkConfig(config)).getZkBarrierTimeoutMs(),
        () -> barrier.expireBarrier(version)
      );
    }

    public void onBarrierStateChanged(final String version, BarrierForVersionUpgrade.State state) {
      LOG.info("JobModel version " + version + " obtained consensus successfully!");
      if (BarrierForVersionUpgrade.State.DONE.equals(state)) {
        debounceTimer.scheduleAfterDebounceTime(
            barrierAction,
          0,
          () -> onNewJobModelConfirmed(version));
      } else {
        if (BarrierForVersionUpgrade.State.TIMED_OUT.equals(state)) {
          // no-op
          // In our consensus model, if the Barrier is timed-out, then it means that one or more initial
          // participants failed to join. That means, they should have de-registered from "processors" list
          // and that would have triggered onProcessorChange action -> a new round of consensus.
          LOG.info("Barrier for version " + version + " timed out.");
        }
      }
    }

    @Override
    public void onBarrierError(String version, Throwable t) {
      LOG.error("Encountered error while attaining consensus on JobModel version " + version);
      stop();
    }
  }
}
