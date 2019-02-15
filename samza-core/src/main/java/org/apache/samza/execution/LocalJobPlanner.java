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
package org.apache.samza.execution;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.DistributedLockWithState;
import org.apache.samza.system.StreamSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Temporarily helper class with specific implementation of {@link JobPlanner#prepareJobs()}
 * for standalone Samza processors.
 *
 * TODO: we need to consolidate this with {@link ExecutionPlanner} after SAMZA-1811.
 */
public class LocalJobPlanner extends JobPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(LocalJobPlanner.class);
  private static final String APPLICATION_RUNNER_PATH_SUFFIX = "/ApplicationRunnerData";

  private final String uid = UUID.randomUUID().toString();

  public LocalJobPlanner(ApplicationDescriptorImpl<? extends ApplicationDescriptor> descriptor) {
    super(descriptor);
  }

  @Override
  public List<JobConfig> prepareJobs() {
    // for high-level DAG, generating the plan and job configs
    // 1. initialize and plan
    ExecutionPlan plan = getExecutionPlan();

    String executionPlanJson = "";
    try {
      executionPlanJson = plan.getPlanAsJson();
    } catch (Exception e) {
      throw new SamzaException("Failed to create plan JSON.", e);
    }
    writePlanJsonFile(executionPlanJson);
    LOG.info("Execution Plan: \n" + executionPlanJson);
    String planId = String.valueOf(executionPlanJson.hashCode());

    List<JobConfig> jobConfigs = plan.getJobConfigs();
    if (jobConfigs.isEmpty()) {
      throw new SamzaException("No jobs in the plan.");
    }

    // 2. create the necessary streams
    // TODO: System generated intermediate streams should have robust naming scheme. See SAMZA-1391
    // TODO: this works for single-job applications. For multi-job applications, ExecutionPlan should return an AppConfig
    // to be used for the whole application
    JobConfig jobConfig = jobConfigs.get(0);
    StreamManager streamManager = null;
    try {
      // create the StreamManager to create intermediate streams in the plan
      streamManager = buildAndStartStreamManager(jobConfig);
      createStreams(planId, plan.getIntermediateStreams(), streamManager);
    } finally {
      if (streamManager != null) {
        streamManager.stop();
      }
    }
    return jobConfigs;
  }

  /**
   * Create intermediate streams using {@link StreamManager}.
   * If {@link CoordinationUtils} is provided, this function will first invoke leader election, and the leader
   * will create the streams. All the runner processes will wait on the latch that is released after the leader finishes
   * stream creation.
   * @param planId a unique identifier representing the plan used for coordination purpose
   * @param intStreams list of intermediate {@link StreamSpec}s
   * @param streamManager the {@link StreamManager} used to create streams
   */
  private void createStreams(String planId, List<StreamSpec> intStreams, StreamManager streamManager) {
    if (intStreams.isEmpty()) {
      LOG.info("Set of intermediate streams is empty. Nothing to create.");
      return;
    }
    LOG.info("A single processor must create the intermediate streams. Processor {} will attempt to acquire the lock.", uid);
    // Move the scope of coordination utils within stream creation to address long idle connection problem.
    // Refer SAMZA-1385 for more details
    JobCoordinatorConfig jcConfig = new JobCoordinatorConfig(userConfig);
    String coordinationId = new ApplicationConfig(userConfig).getGlobalAppId() + APPLICATION_RUNNER_PATH_SUFFIX;
    CoordinationUtils coordinationUtils =
        jcConfig.getCoordinationUtilsFactory().getCoordinationUtils(coordinationId, uid, userConfig);
    if (coordinationUtils == null) {
      LOG.warn("Processor {} failed to create utils. Each processor will attempt to create streams.", uid);
      // each application process will try creating the streams, which
      // requires stream creation to be idempotent
      streamManager.createStreams(intStreams);
      return;
    }

    DistributedLockWithState lockWithState = coordinationUtils.getLockWithState(planId);
    try {
      // check if the processor needs to go through leader election and stream creation
      if (lockWithState.lockIfNotSet(1000, TimeUnit.MILLISECONDS)) {
        LOG.info("lock acquired for streams creation by " + uid);
        streamManager.createStreams(intStreams);
        lockWithState.unlockAndSet();
      } else {
        LOG.info("Processor {} did not obtain the lock for streams creation. They must've been created by another processor.", uid);
      }
    } catch (TimeoutException e) {
      String msg = String.format("Processor {} failed to get the lock for stream initialization", uid);
      throw new SamzaException(msg, e);
    } finally {
      coordinationUtils.close();
    }
  }
}
