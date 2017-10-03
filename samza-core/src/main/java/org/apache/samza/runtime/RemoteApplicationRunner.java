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

package org.apache.samza.runtime;

import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in a remote cluster
 */
public class RemoteApplicationRunner extends AbstractApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteApplicationRunner.class);

  public RemoteApplicationRunner(Config config) {
    super(config);
  }

  @Override
  public void runTask() {
    throw new UnsupportedOperationException("Running StreamTask is not implemented for RemoteReplicationRunner");
  }

  /**
   * Run the {@link StreamApplication} on the remote cluster
   * @param app a StreamApplication
   */
  @Override
  public void run(StreamApplication app) {
    try {
      // TODO: this is a tmp solution and the run.id generation will be addressed in another JIRA
      String runId = String.valueOf(System.currentTimeMillis());
      LOG.info("The run id for this run is {}", runId);

      // 1. initialize and plan
      ExecutionPlan plan = getExecutionPlan(app, runId);
      writePlanJsonFile(plan.getPlanAsJson());

      // 2. create the necessary streams
      if (plan.getApplicationConfig().getAppMode() == ApplicationConfig.ApplicationMode.BATCH) {
        getStreamManager().clearStreamsFromPreviousRun(getConfigFromPrevRun());
      }
      getStreamManager().createStreams(plan.getIntermediateStreams());

      // 3. submit jobs for remote execution
      plan.getJobConfigs().forEach(jobConfig -> {
          LOG.info("Starting job {} with config {}", jobConfig.getName(), jobConfig);
          JobRunner runner = new JobRunner(jobConfig);
          runner.run(true);
        });
    } catch (Throwable t) {
      throw new SamzaException("Failed to run application", t);
    }
  }

  @Override
  public void kill(StreamApplication app) {
    try {
      ExecutionPlan plan = getExecutionPlan(app);

      plan.getJobConfigs().forEach(jobConfig -> {
          LOG.info("Killing job {}", jobConfig.getName());
          JobRunner runner = new JobRunner(jobConfig);
          runner.kill();
        });
    } catch (Throwable t) {
      throw new SamzaException("Failed to kill application", t);
    }
  }

  @Override
  public ApplicationStatus status(StreamApplication app) {
    try {
      boolean hasNewJobs = false;
      boolean hasRunningJobs = false;
      ApplicationStatus unsuccessfulFinishStatus = null;

      ExecutionPlan plan = getExecutionPlan(app);
      for (JobConfig jobConfig : plan.getJobConfigs()) {
        JobRunner runner = new JobRunner(jobConfig);
        ApplicationStatus status = runner.status();
        LOG.debug("Status is {} for job {}", new Object[]{status, jobConfig.getName()});

        switch (status.getStatusCode()) {
          case New:
            hasNewJobs = true;
            break;
          case Running:
            hasRunningJobs = true;
            break;
          case UnsuccessfulFinish:
            unsuccessfulFinishStatus = status;
            break;
          case SuccessfulFinish:
            break;
          default:
            // Do nothing
        }
      }

      if (hasNewJobs) {
        // There are jobs not started, report as New
        return ApplicationStatus.New;
      } else if (hasRunningJobs) {
        // All jobs are started, some are running
        return ApplicationStatus.Running;
      } else if (unsuccessfulFinishStatus != null) {
        // All jobs are finished, some are not successful
        return unsuccessfulFinishStatus;
      } else {
        // All jobs are finished successfully
        return ApplicationStatus.SuccessfulFinish;
      }
    } catch (Throwable t) {
      throw new SamzaException("Failed to get status for application", t);
    }
  }

  private Config getConfigFromPrevRun() {
    CoordinatorStreamSystemFactory coordinatorStreamSystemFactory = new CoordinatorStreamSystemFactory();
    CoordinatorStreamSystemConsumer consumer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(
        config, new MetricsRegistryMap());
    consumer.register();
    consumer.start();
    consumer.bootstrap();
    consumer.stop();

    Config cfg = consumer.getConfig();
    LOG.info("Previous config is: " + cfg.toString());
    return cfg;
  }
}
