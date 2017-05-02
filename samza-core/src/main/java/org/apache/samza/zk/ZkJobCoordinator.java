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
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.coordinator.BarrierForVersionUpgrade;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.runtime.ProcessorIdGenerator;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.util.*;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkJobCoordinator.class);
  private static final String JOB_MODEL_UPGRADE_BARRIER = "jobModelUpgradeBarrier";

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
    streamMetadataCache = getStreamMetadataCache();
    debounceTimer = new ScheduleAfterDebounceTime(exception -> {
      LOGGER.error("Received exception from in JobCoordinator Processing!");
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
    LOGGER.info("ZkJobCoordinator::onProcessorChange - list of processors changed! List size=" + processors.size());
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
      LOGGER.info("pid=" + processorId + "new JobModel available");
      // stop current work
      if (coordinatorListener != null) {
        coordinatorListener.onJobModelExpired();
      }
      LOGGER.info("pid=" + processorId + "new JobModel available.Container stopped.");
      // get the new job model
      newJobModel = zkUtils.getJobModel(version);

      LOGGER.info("pid=" + processorId + ": new JobModel available. ver=" + version + "; jm = " + newJobModel);

      // update ZK and wait for all the processors to get this new version
      ZkBarrierForVersionUpgrade barrier = (ZkBarrierForVersionUpgrade) coordinationUtils.getBarrier(
          JOB_MODEL_UPGRADE_BARRIER);
      barrier.waitForBarrier(version, processorId, () -> onNewJobModelConfirmed(version));
    });
  }

  @Override
  public void onNewJobModelConfirmed(String version) {
    LOGGER.info("pid=" + processorId + "new version " + version + " of the job model got confirmed");
    // get the new Model
    JobModel jobModel = getJobModel();
    LOGGER.info("pid=" + processorId + "got the new job model in JobModelConfirmed =" + jobModel);

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
      LOGGER.info("pid=" + processorId + "generating first version of the model");
      nextJMVersion = "1";
    } else {
      nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion) + 1);
    }
    LOGGER.info("pid=" + processorId + "generating new model. Version = " + nextJMVersion);

    List<String> containerIds = new ArrayList<>(currentProcessorsIds.size());
    for (String processorPid : currentProcessorsIds) {
      containerIds.add(processorPid);
    }
    LOGGER.info("generate new job model: processorsIds: " + Arrays.toString(containerIds.toArray()));

    JobModel jobModel = JobModelManager.readJobModel(this.config, Collections.emptyMap(), null, streamMetadataCache,
        containerIds);

    LOGGER.info("pid=" + processorId + "Generated jobModel: " + jobModel);

    // publish the new job model first
    zkUtils.publishJobModel(nextJMVersion, jobModel);
    LOGGER.info("pid=" + processorId + "published new JobModel ver=" + nextJMVersion + ";jm=" + jobModel);

    // start the barrier for the job model update
    BarrierForVersionUpgrade barrier = coordinationUtils.getBarrier(
        JOB_MODEL_UPGRADE_BARRIER);
    barrier.start(nextJMVersion, currentProcessorsIds);

    // publish new JobModel version
    zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);
    LOGGER.info("pid=" + processorId + "published new JobModel ver=" + nextJMVersion);
  }

  private StreamMetadataCache getStreamMetadataCache() {
    // model generation - NEEDS TO BE REVIEWED
    JavaSystemConfig systemConfig = new JavaSystemConfig(this.config);
    Map<String, SystemAdmin> systemAdmins = new HashMap<>();
    for (String systemName: systemConfig.getSystemNames()) {
      String systemFactoryClassName = systemConfig.getSystemFactory(systemName);
      if (systemFactoryClassName == null) {
        String msg = String.format("A stream uses system %s, which is missing from the configuration.", systemName);
        LOGGER.error(msg);
        throw new SamzaException(msg);
      }
      SystemFactory systemFactory = Util.getObj(systemFactoryClassName);
      systemAdmins.put(systemName, systemFactory.getAdmin(systemName, this.config));
    }

    return new StreamMetadataCache(Util.<String, SystemAdmin>javaMapAsScalaMap(systemAdmins), 5000, SystemClock.instance());
  }

  class LeaderElectorListenerImpl implements LeaderElectorListener {
    @Override
    public void onBecomingLeader() {
      LOGGER.info("ZkJobCoordinator::onBecomeLeader - I became the leader!");
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
