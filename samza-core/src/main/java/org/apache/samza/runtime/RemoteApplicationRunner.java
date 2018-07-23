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
import java.util.UUID;
import org.apache.samza.SamzaException;
import org.apache.samza.application.internal.StreamAppSpecImpl;
import org.apache.samza.application.internal.TaskAppSpecImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.runtime.internal.ApplicationRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.job.ApplicationStatus.*;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in a remote cluster
 */
public class RemoteApplicationRunner extends AbstractApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteApplicationRunner.class);
  private static final long DEFAULT_SLEEP_DURATION_MS = 2000;

  public RemoteApplicationRunner(Config config) {
    super(config);
  }

  @Override
  protected ApplicationLifecycle getTaskAppLifecycle(TaskAppSpecImpl appSpec) {
    return new TaskAppLifecycle(appSpec);
  }

  @Override
  protected ApplicationLifecycle getStreamAppLifecycle(StreamAppSpecImpl appSpec) {
    return new StreamAppLifecycle(appSpec);
  }

  class TaskAppLifecycle implements ApplicationLifecycle {
    final TaskAppSpecImpl taskApp;
    final JobRunner jobRunner;

    TaskAppLifecycle(TaskAppSpecImpl appSpec) {
      this.taskApp = appSpec;
      this.jobRunner = new JobRunner(config);
    }

    @Override
    public void run() {
      jobRunner.run(true);
    }

    @Override
    public void kill() {
      jobRunner.kill();
    }

    @Override
    public ApplicationStatus status() {
      return jobRunner.status();
    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return RemoteApplicationRunner.this.waitForFinish(timeout);
    }

  }

  class StreamAppLifecycle implements ApplicationLifecycle {
    final StreamAppSpecImpl streamApp;

    StreamAppLifecycle(StreamAppSpecImpl appSpec) {
      this.streamApp = appSpec;
    }

    @Override
    public void run() {
      StreamManager streamManager = null;
      try {
        streamManager = buildAndStartStreamManager();
        // TODO: start.id needs to be set for standalone: SAMZA-1531
        // start.id is based on current system time with the most significant bits in UUID (8 digits) to avoid collision
        String runId = String.valueOf(System.currentTimeMillis()) + "-" + UUID.randomUUID().toString().substring(0, 8);
        LOG.info("The start id for this start is {}", runId);

        // 1. initialize and plan
        ExecutionPlan plan = getExecutionPlan(((StreamGraphSpec) streamApp.getGraph()).getOperatorSpecGraph(), runId, streamManager);
        writePlanJsonFile(plan.getPlanAsJson());

        // 2. create the necessary streams
        if (plan.getApplicationConfig().getAppMode() == ApplicationConfig.ApplicationMode.BATCH) {
          streamManager.clearStreamsFromPreviousRun(getConfigFromPrevRun());
        }
        streamManager.createStreams(plan.getIntermediateStreams());

        // 3. submit jobs for remote execution
        plan.getJobConfigs().forEach(jobConfig -> {
          LOG.info("Starting job {} with config {}", jobConfig.getName(), jobConfig);
          JobRunner runner = new JobRunner(jobConfig);
          runner.run(true);
        });
      } catch (Throwable t) {
        throw new SamzaException("Failed to start application", t);
      } finally {
        if (streamManager != null) {
          streamManager.stop();
        }
      }
    }

    @Override
    public void kill() {
      StreamManager streamManager = null;
      try {
        streamManager = buildAndStartStreamManager();
        ExecutionPlan plan = getExecutionPlan(((StreamGraphSpec) streamApp.getGraph()).getOperatorSpecGraph(), streamManager);

        plan.getJobConfigs().forEach(jobConfig -> {
          LOG.info("Killing job {}", jobConfig.getName());
          JobRunner runner = new JobRunner(jobConfig);
          runner.kill();
        });
      } catch (Throwable t) {
        throw new SamzaException("Failed to stop application", t);
      } finally {
        if (streamManager != null) {
          streamManager.stop();
        }
      }
    }

    @Override
    public ApplicationStatus status() {
      StreamManager streamManager = null;
      try {
        boolean hasNewJobs = false;
        boolean hasRunningJobs = false;
        ApplicationStatus unsuccessfulFinishStatus = null;

        streamManager = buildAndStartStreamManager();
        ExecutionPlan plan = getExecutionPlan(((StreamGraphSpec) streamApp.getGraph()).getOperatorSpecGraph(), streamManager);
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
      } finally {
        if (streamManager != null) {
          streamManager.stop();
        }
      }
    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return RemoteApplicationRunner.this.waitForFinish(timeout);
    }

  }

  /* package private */ ApplicationStatus getApplicationStatus(JobConfig jobConfig) {
    JobRunner runner = new JobRunner(jobConfig);
    ApplicationStatus status = runner.status();
    LOG.debug("Status is {} for job {}", new Object[]{status, jobConfig.getName()});
    return status;
  }

  private boolean waitForFinish(Duration timeout) {
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
