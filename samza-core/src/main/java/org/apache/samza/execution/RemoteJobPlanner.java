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
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Temporary helper class with specific implementation of {@link JobPlanner#prepareJobs()}
 * for remote-launched Samza processors (e.g. in YARN).
 *
 * TODO: we need to consolidate this class with {@link ExecutionPlanner} after SAMZA-1811.
 */
public class RemoteJobPlanner extends JobPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteJobPlanner.class);

  public RemoteJobPlanner(ApplicationDescriptorImpl<? extends ApplicationDescriptor> descriptor) {
    super(descriptor);
  }

  @Override
  public List<JobConfig> prepareJobs() {
    // for high-level DAG, generate the plan and job configs
    // TODO: run.id needs to be set for standalone: SAMZA-1531
    // run.id is based on current system time with the most significant bits in UUID (8 digits) to avoid collision
    String runId = String.valueOf(System.currentTimeMillis()) + "-" + UUID.randomUUID().toString().substring(0, 8);
    LOG.info("The run id for this run is {}", runId);

    ClassLoader classLoader = getClass().getClassLoader();

    // 1. initialize and plan
    ExecutionPlan plan = getExecutionPlan(runId, classLoader);
    try {
      writePlanJsonFile(plan.getPlanAsJson());
    } catch (Exception e) {
      throw new SamzaException("Failed to create plan JSON.", e);
    }

    List<JobConfig> jobConfigs = plan.getJobConfigs();
    if (jobConfigs.isEmpty()) {
      throw new SamzaException("No jobs in the plan.");
    }

    // 2. create the necessary streams
    // TODO: this works for single-job applications. For multi-job applications, ExecutionPlan should return an AppConfig
    // to be used for the whole application
    JobConfig jobConfig = jobConfigs.get(0);
    StreamManager streamManager = null;
    try {
      // create the StreamManager to create intermediate streams in the plan
      streamManager = buildAndStartStreamManager(jobConfig);
      if (plan.getApplicationConfig().getAppMode() == ApplicationConfig.ApplicationMode.BATCH) {
        streamManager.clearStreamsFromPreviousRun(getConfigFromPrevRun(), classLoader);
      }
      streamManager.createStreams(plan.getIntermediateStreams());
    } finally {
      if (streamManager != null) {
        streamManager.stop();
      }
    }
    return jobConfigs;
  }

  private Config getConfigFromPrevRun() {
    CoordinatorStreamSystemConsumer consumer = new CoordinatorStreamSystemConsumer(userConfig, new MetricsRegistryMap());
    consumer.register();
    consumer.start();
    consumer.bootstrap();
    consumer.stop();

    Config cfg = consumer.getConfig();
    LOG.info("Previous config is: " + cfg.toString());
    return cfg;
  }
}
