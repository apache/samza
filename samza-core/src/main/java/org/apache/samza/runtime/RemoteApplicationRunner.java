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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in a remote cluster
 */
public class RemoteApplicationRunner extends AbstractApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteApplicationRunner.class);
  private Map<StreamApplication, List<JobRunner>> jobRunnersByApp = new HashMap<>();

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
      super.run(app);

      if (jobRunnersByApp.containsKey(app)) {
        throw new SamzaException("Stream application is already running");
      }
      List<JobRunner> jobRunners = new ArrayList<>();

      // TODO: run.id needs to be set for standalone: SAMZA-1531
      // run.id is based on current system time with the most significant bits in UUID (8 digits) to avoid collision
      String runId = String.valueOf(System.currentTimeMillis()) + "-" + UUID.randomUUID().toString().substring(0, 8);
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
          jobRunners.add(runner);
          runner.run(true);
        });
      jobRunnersByApp.put(app, jobRunners);
    } catch (Throwable t) {
      throw new SamzaException("Failed to run application", t);
    }
  }

  @Override
  public void kill(StreamApplication app) {
    if (!jobRunnersByApp.containsKey(app)) {
      throw new SamzaException("Stream application is not running or has already been killed");
    }

    try {
      jobRunnersByApp.get(app).forEach(runner -> {
          LOG.info("Killing job {}", new JobConfig(runner.config()).getName());
          runner.kill();
        });
      super.kill(app);
      jobRunnersByApp.remove(app);
    } catch (Throwable t) {
      throw new SamzaException("Failed to kill application", t);
    }
  }

  @Override
  public ApplicationStatus status(StreamApplication app) {
    if (!jobRunnersByApp.containsKey(app)) {
      throw new SamzaException("Stream application is not running or has already been killed");
    }

    try {
      boolean hasNewJobs = false;
      boolean hasRunningJobs = false;
      ApplicationStatus unsuccessfulFinishStatus = null;

      for (JobRunner runner : jobRunnersByApp.get(app)) {
        ApplicationStatus status = runner.status();
        LOG.debug("Status is {} for job {}", new Object[]{status, new JobConfig(runner.config()).getName()});

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
    CoordinatorStreamSystemConsumer consumer = new CoordinatorStreamSystemConsumer(config, new MetricsRegistryMap());
    consumer.register();
    consumer.start();
    consumer.bootstrap();
    consumer.stop();

    Config cfg = consumer.getConfig();
    LOG.info("Previous config is: " + cfg.toString());
    return cfg;
  }
}
