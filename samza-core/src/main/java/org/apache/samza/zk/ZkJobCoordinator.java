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
import org.apache.samza.coordinator.BarrierForVersionUpgrade;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * JobCoordinator for stand alone processor managed via Zookeeper.
 */
public class ZkJobCoordinator implements JobCoordinator, ZkControllerListener {
  private static final Logger LOG = LoggerFactory.getLogger(ZkJobCoordinator.class);
  private static final String JOB_MODEL_UPGRADE_BARRIER = "jobModelUpgradeBarrier";
  // TODO: MetadataCache timeout has to be 0 for the leader so that it can always have the latest information associated
  // with locality. Since host-affinity is not yet implemented, this can be fixed as part of SAMZA-1197
  private static final int METADATA_CACHE_TTL_MS = 5000;

  private final ZkUtils zkUtils;
  private final String processorId;
  private final ZkController zkController;

  private final Config config;
  private final CoordinationUtils coordinationUtils;

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

  public void doOnProcessorChange(List<String> processors) {
    // if list of processors is empty - it means we are called from 'onBecomeLeader'
    generateNewJobModel(processors);
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }
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
        LOG.info("pid=" + processorId + "new JobModel available.Container stopped.");
        // get the new job model
        newJobModel = zkUtils.getJobModel(version);

        LOG.info("pid=" + processorId + ": new JobModel available. ver=" + version + "; jm = " + newJobModel);

        // update ZK and wait for all the processors to get this new version
        ZkBarrierForVersionUpgrade barrier =
            (ZkBarrierForVersionUpgrade) coordinationUtils.getBarrier(JOB_MODEL_UPGRADE_BARRIER);
        barrier.waitForBarrier(version, processorId, () -> onNewJobModelConfirmed(version));
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

  /**
   * Generate new JobModel when becoming a leader or the list of processor changed.
   */
  private void generateNewJobModel(List<String> processors) {
    List<String> currentProcessorsIds;
    if (processors.size() > 0) {
      // we should use this list
      // but it needs to be converted into PIDs, which is part of the data
      currentProcessorsIds = zkUtils.getActiveProcessorsIDs(processors);
    } else {
      // get the current list of processors
      currentProcessorsIds = zkUtils.getSortedActiveProcessorsIDs();
    }

    // get the current version
    String currentJMVersion  = zkUtils.getJobModelVersion();
    String nextJMVersion;
    if (currentJMVersion == null) {
      LOG.info("pid=" + processorId + "generating first version of the model");
      nextJMVersion = "1";
    } else {
      nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion) + 1);
    }
    LOG.info("pid=" + processorId + "generating new model. Version = " + nextJMVersion);

    List<String> containerIds = new ArrayList<>(currentProcessorsIds.size());
    for (String processorPid : currentProcessorsIds) {
      containerIds.add(processorPid);
    }
    LOG.info("generate new job model: processorsIds: " + Arrays.toString(containerIds.toArray()));

    JobModel jobModel = JobModelManager.readJobModel(this.config, Collections.emptyMap(), null, streamMetadataCache,
        containerIds);

    LOG.info("pid=" + processorId + "Generated jobModel: " + jobModel);

    // publish the new job model first
    zkUtils.publishJobModel(nextJMVersion, jobModel);

    // start the barrier for the job model update
    BarrierForVersionUpgrade barrier = coordinationUtils.getBarrier(
        JOB_MODEL_UPGRADE_BARRIER);
    barrier.start(nextJMVersion, currentProcessorsIds);

    // publish new JobModel version
    zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);
    LOG.info("pid=" + processorId + "published new JobModel ver=" + nextJMVersion);
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
}
