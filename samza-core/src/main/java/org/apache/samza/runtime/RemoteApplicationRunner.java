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
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.AsyncStreamTaskApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationInternal;
import org.apache.samza.application.StreamTaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in a remote cluster
 */
public class RemoteApplicationRunner extends ApplicationRunnerBase {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteApplicationRunner.class);

  public RemoteApplicationRunner(Config config) {
    super(config);
  }

  @Deprecated
  public void runTask() {
    throw new UnsupportedOperationException("Running StreamTask is not implemented for RemoteReplicationRunner");
  }

  @Override
  protected ApplicationRuntimeInstance getRuntimeInstance(ApplicationBase streamApp) {
    if (streamApp instanceof StreamApplication) {
      return new StreamAppRunner((StreamApplication) streamApp);
    }

    if (streamApp instanceof StreamTaskApplication) {
      return new StreamTaskAppRunner((StreamTaskApplication) streamApp);
    }

    if (streamApp instanceof AsyncStreamTaskApplication) {
      return new AsyncStreamTaskAppRunner((AsyncStreamTaskApplication) streamApp);
    }

    throw new IllegalArgumentException("Application type " + streamApp.getClass().getCanonicalName() + " is not supported by RemoteApplicationRunner");
  }

  public void waitForFinish() {
    // TODO: add life cycle listner and the corresponding wait listner for local process to shutdown
  }

  private class StreamTaskAppRunner implements ApplicationRuntimeInstance {
    private final StreamTaskApplication app;

    StreamTaskAppRunner(StreamTaskApplication app) {
      this.app = app;
    }

    @Override
    public void run() {

    }

    @Override
    public void kill() {

    }

    @Override
    public ApplicationStatus status() {
      return null;
    }

    @Override
    public void waitForFinish() {

    }
  }

  private class AsyncStreamTaskAppRunner implements ApplicationRuntimeInstance {
    private final AsyncStreamTaskApplication app;

    AsyncStreamTaskAppRunner(AsyncStreamTaskApplication app) {
      this.app = app;
    }

    @Override
    public void run() {

    }

    @Override
    public void kill() {

    }

    @Override
    public ApplicationStatus status() {
      return null;
    }

    @Override
    public void waitForFinish() {

    }
  }

  private class StreamAppRunner implements ApplicationRuntimeInstance {
    private final StreamApplication app;
    private final StreamManager streamManager;
    private final ExecutionPlanner planner;

    StreamAppRunner(StreamApplication app) {
      this.app = app;
      this.streamManager = new StreamManager(new JavaSystemConfig(config).getSystemAdmins());
      this.planner = new ExecutionPlanner(config, streamManager);
    }

    @Override
    public void run() {
      try {
        // 1. initialize and plan
        ExecutionPlan plan = getExecutionPlan(app);
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

    private ExecutionPlan getExecutionPlan(StreamApplication app) throws Exception {
      return this.planner.plan(new StreamApplicationInternal(app).getStreamGraphImpl());
    }

    @Override
    public void kill() {
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
    public ApplicationStatus status() {
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

    @Override
    public void waitForFinish() {

    }
  }
}
