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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.processor.SamzaContainerController;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.util.ProcessorIdUtil;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobCoordinator for stand alone processor managed via Zookeeper.
 */
public class ZkJobCoordinator implements JobCoordinator, ZkControllerListener {
  private static final Logger log = LoggerFactory.getLogger(ZkJobCoordinator.class);
  private static final String JOB_MODEL_VERSION_BARRIER = "JobModelVersion";

  private final ZkUtils zkUtils;
  private final String processorId;

  private final ZkController zkController;
  private final SamzaContainerController containerController;
  private final ScheduleAfterDebounceTime debounceTimer;
  private final StreamMetadataCache  streamMetadataCache;
  private final ZkKeyBuilder keyBuilder;
  private final Config config;
  private final CoordinationUtils coordinationUtils;

  private JobModel newJobModel;
  private String newJobModelVersion;  // version published in ZK (by the leader)
  private JobModel jobModel;

  public ZkJobCoordinator(String groupId, Config config, ScheduleAfterDebounceTime debounceTimer, ZkUtils zkUtils,
                          SamzaContainerController containerController) {
    this.processorId = ProcessorIdUtil.generateProcessorId(config);
    this.zkUtils = zkUtils;
    this.keyBuilder = zkUtils.getKeyBuilder();
    this.debounceTimer = debounceTimer;
    this.containerController = containerController;
    this.zkController = new ZkControllerImpl(processorId, zkUtils, debounceTimer, this);
    this.config = config;
    this.coordinationUtils = new ZkCoordinationServiceFactory().getCoordinationService(groupId, String.valueOf(processorId), config);

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
    zkController.stop();
    if (containerController != null)
      containerController.stopContainer();
  }

  @Override
  public boolean awaitStart(long timeoutMs)
      throws InterruptedException {
    return containerController.awaitStart(timeoutMs);
  }

  @Override
  public String getProcessorId() {
    return processorId;
  }

  @Override
  public JobModel getJobModel() {
    return newJobModel;
  }

  //////////////////////////////////////////////// LEADER stuff ///////////////////////////
  @Override
  public void onBecomeLeader() {
    log.info("ZkJobCoordinator::onBecomeLeader - I became the leader!");

    List<String> emptyList = new ArrayList<>();

    // actual actions to do are the same as onProcessorChange()
    debounceTimer.scheduleAfterDebounceTime(ScheduleAfterDebounceTime.ON_PROCESSOR_CHANGE,
        ScheduleAfterDebounceTime.DEBOUNCE_TIME_MS, () -> onProcessorChange(emptyList));
  }

  @Override
  public void onProcessorChange(List<String> processorIds) {
    log.info("ZkJobCoordinator::onProcessorChange - Processors changed! List: " + Arrays.toString(processorIds.toArray()));
    generateNewJobModel();
  }

  @Override
  public void onNewJobModelAvailable(final String version) {
    newJobModelVersion = version;
    log.info("pid=" + processorId + "new JobModel available");
    // stop current work
    containerController.stopContainer();
    log.info("pid=" + processorId + "new JobModel available.Container stopped.");
    // get the new job model
    newJobModel = zkUtils.getJobModel(version);
    log.info("pid=" + processorId + "new JobModel available. ver=" + version + "; jm = " + newJobModel);

    String currentPath = zkUtils.getEphemeralPath();
    String zkProcessorId = keyBuilder.parseIdFromPath(currentPath);

    // update ZK and wait for all the processors to get this new version
    ZkBarrierForVersionUpgrade barrier = (ZkBarrierForVersionUpgrade) coordinationUtils.getBarrier(JOB_MODEL_VERSION_BARRIER);
    barrier.waitForBarrier(version, String.valueOf(zkProcessorId), new Runnable() {
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
    containerController.startContainer(jobModel.getContainers().get(processorId), jobModel.getConfig(),
        jobModel.maxChangeLogStreamPartitions);
  }

  /**
   * Generate new JobModel when becoming a leader or the list of processor changed.
   */
  private void generateNewJobModel() {
    // get the current list of processors
    List<String> currentProcessors = zkUtils.getSortedActiveProcessors();

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

    List<String> containerIds = new ArrayList<>();
    for (String processor : currentProcessors) {
      String zkProcessorId = ZkKeyBuilder.parseIdFromPath(processor);
      containerIds.add(zkProcessorId);
    }
    log.info("generate new job model: processorsIds: " + Arrays.toString(containerIds.toArray()));

    jobModel = JobModelManager.readJobModel(this.config, Collections.emptyMap(), null, streamMetadataCache,
        containerIds);

    log.info("pid=" + processorId + "Generated jobModel: " + jobModel);

    // publish the new job model first
    zkUtils.publishJobModel(nextJMVersion, jobModel);
    log.info("pid=" + processorId + "published new JobModel ver=" + nextJMVersion + ";jm=" + jobModel);

    // start the barrier for the job model update
    ZkBarrierForVersionUpgrade barrier = (ZkBarrierForVersionUpgrade) coordinationUtils.getBarrier(JOB_MODEL_VERSION_BARRIER);
    barrier.start(nextJMVersion, currentProcessors);

    // publish new JobModel version
    zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);
    log.info("pid=" + processorId + "published new JobModel ver=" + nextJMVersion);
  }
}
