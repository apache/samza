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

import java.time.Duration;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationInternal;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.system.SystemAdmins;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.job.ApplicationStatus.*;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in a remote cluster
 */
public class RemoteApplicationRunner extends AbstractApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteApplicationRunner.class);
  private static final long DEFAULT_SLEEP_DURATION_MS = 2000;

  private final StreamManager streamManager;
  private final ExecutionPlanner planner;

  public RemoteApplicationRunner(Config config) {
    super(config);
    this.streamManager = new StreamManager(new SystemAdmins(config));
    this.planner = new ExecutionPlanner(config, this.streamManager);
  }

  @Deprecated
  public void runTask() {
    throw new UnsupportedOperationException("Running StreamTask is not implemented for RemoteReplicationRunner");
  }

  @Override
  public void waitForFinish(StreamApplication app) {
    throw new UnsupportedOperationException("waitForFinish is not supported in RemoteApplicationRunner");
  }

  @Override
  public boolean waitForFinish(StreamApplication app, Duration timeout) {
    throw new UnsupportedOperationException("waitForFinish is not supported in RemoteApplicationRunner");
  }

  @Override
  public void run(StreamApplication userApp) {
    StreamApplicationInternal app = new StreamApplicationInternal(userApp);
    try {
      // 1. initialize and plan
      OperatorSpecGraph specGraph = app.getStreamGraphSpec().getOperatorSpecGraph();
      ExecutionPlan plan = getExecutionPlan(specGraph);
      writePlanJsonFile(plan.getPlanAsJson());

      // 2. create the necessary streams
      this.streamManager.createStreams(plan.getIntermediateStreams());

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

  private ExecutionPlan getExecutionPlan(OperatorSpecGraph specGraph) throws Exception {
    return this.planner.plan(specGraph);
  }

  @Override
  public void kill(StreamApplication userApp) {
    StreamApplicationInternal app = new StreamApplicationInternal(userApp);
    try {
      OperatorSpecGraph specGraph = app.getStreamGraphSpec().getOperatorSpecGraph();
      ExecutionPlan plan = getExecutionPlan(specGraph);

      plan.getJobConfigs().forEach(jobConfig -> {
          LOG.info("Killing job {}", jobConfig.getName());
          JobRunner runner = new JobRunner(jobConfig);
          runner.kill();
        });
      super.kill(userApp);
    } catch (Throwable t) {
      throw new SamzaException("Failed to kill application", t);
    }
  }

  @Override
  public ApplicationStatus status(StreamApplication userApp) {
    StreamApplicationInternal app = new StreamApplicationInternal(userApp);
    try {
      boolean hasNewJobs = false;
      boolean hasRunningJobs = false;
      ApplicationStatus unsuccessfulFinishStatus = null;

      OperatorSpecGraph specGraph = app.getStreamGraphSpec().getOperatorSpecGraph();
      ExecutionPlan plan = getExecutionPlan(specGraph);
      for (JobConfig jobConfig : plan.getJobConfigs()) {
        ApplicationStatus status = getApplicationStatus(jobConfig);

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
        return New;
      } else if (hasRunningJobs) {
        // All jobs are started, some are running
        return Running;
      } else if (unsuccessfulFinishStatus != null) {
        // All jobs are finished, some are not successful
        return unsuccessfulFinishStatus;
      } else {
        // All jobs are finished successfully
        return SuccessfulFinish;
      }
    } catch (Throwable t) {
      throw new SamzaException("Failed to get status for application", t);
    }
  }

  /* package private */ ApplicationStatus getApplicationStatus(JobConfig jobConfig) {
    JobRunner runner = new JobRunner(jobConfig);
    ApplicationStatus status = runner.status();
    LOG.debug("Status is {} for job {}", new Object[]{status, jobConfig.getName()});
    return status;
  }

  /**
   * Waits until the application finishes.
   */
  public void waitForFinish() {
    waitForFinish(Duration.ofMillis(0));
  }

  /**
   * Waits for {@code timeout} duration for the application to finish.
   * If timeout &lt; 1, blocks the caller indefinitely.
   *
   * @param timeout time to wait for the application to finish
   * @return true - application finished before timeout
   *         false - otherwise
   */
  public boolean waitForFinish(Duration timeout) {
    JobConfig jobConfig = new JobConfig(config);
    boolean finished = true;
    long timeoutInMs = timeout.toMillis();
    long startTimeInMs = System.currentTimeMillis();
    long timeElapsed = 0L;

    long sleepDurationInMs = timeoutInMs < 1 ?
        DEFAULT_SLEEP_DURATION_MS : Math.min(timeoutInMs, DEFAULT_SLEEP_DURATION_MS);
    ApplicationStatus status;

    try {
      while (timeoutInMs < 1 || timeElapsed <= timeoutInMs) {
        status = getApplicationStatus(jobConfig);
        if (status == SuccessfulFinish || status == UnsuccessfulFinish) {
          LOG.info("Application finished with status {}", status);
          break;
        }

        Thread.sleep(sleepDurationInMs);
        timeElapsed = System.currentTimeMillis() - startTimeInMs;
      }

      if (timeElapsed > timeoutInMs) {
        LOG.warn("Timed out waiting for application to finish.");
        finished = false;
      }
    } catch (Exception e) {
      LOG.error("Error waiting for application to finish", e);
      throw new SamzaException(e);
    }

    return finished;
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
