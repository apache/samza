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

import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.coordinator.BarrierForVersionUpgrade;
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JobCoordinator for stand alone processor managed via Zookeeper.
 */
public class ZkJobCoordinator implements JobCoordinator, ZkControllerListener {
  private static final Logger log = LoggerFactory.getLogger(ZkJobCoordinator.class);
  private static final String JOB_MODEL_UPGRADE_BARRIER = "jobModelUpgradeBarrier";

  private final ZkUtils zkUtils;
  private final String processorId;
  private final ZkController zkController;
  private final ScheduleAfterDebounceTime debounceTimer;
  private final StreamMetadataCache  streamMetadataCache;
  private final Config config;
  private final CoordinationUtils coordinationUtils;

  private JobCoordinatorListener coordinatorListener = null;
  private JobModel newJobModel;

  public ZkJobCoordinator(String processorId, Config config, ScheduleAfterDebounceTime debounceTimer) {
    this.processorId = processorId;
    this.debounceTimer = debounceTimer;
    this.config = config;

    this.coordinationUtils = Util.
        <CoordinationServiceFactory>getObj(
            new JobCoordinatorConfig(config)
                .getJobCoordinationServiceFactoryClassName())
        .getCoordinationService(new ApplicationConfig(config).getGlobalAppId(), String.valueOf(processorId), config);

    this.zkUtils = ((ZkCoordinationUtils) coordinationUtils).getZkUtils();
    LeaderElector leaderElector = new ZkLeaderElector(this.processorId, zkUtils, debounceTimer);
    leaderElector.setLeaderElectorListener(new LeaderElectorListenerImpl());

    this.zkController = new ZkControllerImpl(processorId, zkUtils, debounceTimer, this, leaderElector);
    streamMetadataCache = getStreamMetadataCache();
  }

  private StreamMetadataCache getStreamMetadataCache() {
    // model generation - NEEDS TO BE REVIEWED
    JavaSystemConfig systemConfig = new JavaSystemConfig(this.config);
    Map<String, SystemAdmin> systemAdmins = new HashMap<>();
    for (String systemName: systemConfig.getSystemNames()) {
      String systemFactoryClassName = systemConfig.getSystemFactory(systemName);
      if (systemFactoryClassName == null) {
        String msg = String.format("A stream uses system %s, which is missing from the configuration.", systemName);
        log.error(msg);
        throw new SamzaException(msg);
      }
      SystemFactory systemFactory = Util.getObj(systemFactoryClassName);
      systemAdmins.put(systemName, systemFactory.getAdmin(systemName, this.config));
    }

    return new StreamMetadataCache(Util.<String, SystemAdmin>javaMapAsScalaMap(systemAdmins), 5000, SystemClock.instance());
  }

  @Override
  public void start() {
    zkController.register();
  }

  @Override
  public void stop() {
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }
    zkController.stop();
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
    return newJobModel;
  }

  //////////////////////////////////////////////// LEADER stuff ///////////////////////////
  @Override
  public void onProcessorChange(List<String> processors) {
    debounceTimer.scheduleAfterDebounceTime(
        ScheduleAfterDebounceTime.ON_PROCESSOR_CHANGE,
        ScheduleAfterDebounceTime.DEBOUNCE_TIME_MS, () -> {
        log.info("ZkJobCoordinator::onProcessorChange - list of processors changed! List size=" + processors.size());
        // if list of processors is empty - it means we are called from 'onBecomeLeader'
        generateNewJobModel(processors);
        if (coordinatorListener != null) {
          coordinatorListener.onJobModelExpired();
        }
      });
  }

  @Override
  public void onNewJobModelAvailable(final String version) {
    log.info("pid=" + processorId + "new JobModel available");
    // stop current work
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }
    log.info("pid=" + processorId + "new JobModel available.Container stopped.");
    // get the new job model
    newJobModel = zkUtils.getJobModel(version);

    log.info("pid=" + processorId + ": new JobModel available. ver=" + version + "; jm = " + newJobModel);

    // update ZK and wait for all the processors to get this new version
    ZkBarrierForVersionUpgrade barrier = (ZkBarrierForVersionUpgrade) coordinationUtils.getBarrier(
        JOB_MODEL_UPGRADE_BARRIER);
    barrier.waitForBarrier(version, processorId, new Runnable() {
      @Override
      public void run() {
        onNewJobModelConfirmed(version);
      }
    });
  }

  @Override
  public void onNewJobModelConfirmed(String version) {
    log.info("pid=" + processorId + "new version " + version + " of the job model got confirmed");
    // get the new Model
    JobModel jobModel = getJobModel();
    log.info("pid=" + processorId + "got the new job model in JobModelConfirmed =" + jobModel);

    // start the container with the new model
    if (coordinatorListener != null) {
      coordinatorListener.onNewJobModel(processorId, jobModel);
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
      log.info("pid=" + processorId + "generating first version of the model");
      nextJMVersion = "1";
    } else {
      nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion) + 1);
    }
    log.info("pid=" + processorId + "generating new model. Version = " + nextJMVersion);

    List<String> containerIds = new ArrayList<>(currentProcessorsIds.size());
    for (String processorPid : currentProcessorsIds) {
      containerIds.add(processorPid);
    }
    log.info("generate new job model: processorsIds: " + Arrays.toString(containerIds.toArray()));

    JobModel jobModel = JobModelManager.readJobModel(this.config, Collections.emptyMap(), null, streamMetadataCache,
        containerIds);

    log.info("pid=" + processorId + "Generated jobModel: " + jobModel);

    // publish the new job model first
    zkUtils.publishJobModel(nextJMVersion, jobModel);
    log.info("pid=" + processorId + "published new JobModel ver=" + nextJMVersion + ";jm=" + jobModel);

    // start the barrier for the job model update
    BarrierForVersionUpgrade barrier = coordinationUtils.getBarrier(
        JOB_MODEL_UPGRADE_BARRIER);
    barrier.start(nextJMVersion, currentProcessorsIds);

    // publish new JobModel version
    zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);
    log.info("pid=" + processorId + "published new JobModel ver=" + nextJMVersion);
  }

  class LeaderElectorListenerImpl implements LeaderElectorListener {
    @Override
    public void onBecomingLeader() {
      log.info("ZkJobCoordinator::onBecomeLeader - I became the leader!");
      zkController.subscribeToProcessorChange();
      // actual actions to do are the same as onProcessorChange()
      onProcessorChange(new ArrayList<>());
    }
  }
}
