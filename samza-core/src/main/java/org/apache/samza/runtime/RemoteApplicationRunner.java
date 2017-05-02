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
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in a remote cluster
 */
public class RemoteApplicationRunner extends AbstractApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(RemoteApplicationRunner.class);

  public RemoteApplicationRunner(Config config) {
    super(config);
  }

  /**
   * Run the {@link StreamApplication} on the remote cluster
   * @param app a StreamApplication
   */
  @Override
  public void run(StreamApplication app) {
    try {
      // 1. initialize and plan
      ExecutionPlan plan = getExecutionPlan(app);
      writePlanJsonFile(plan.getPlanAsJson());

      // 2. create the necessary streams
      getStreamManager().createStreams(plan.getIntermediateStreams());

      // 3. submit jobs for remote execution
      plan.getJobConfigs().forEach(jobConfig -> {
          log.info("Starting job {} with config {}", jobConfig.getName(), jobConfig);
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
          log.info("Killing job {}", jobConfig.getName());
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
      boolean finished = false;
      boolean unsuccessfulFinish = false;

      ExecutionPlan plan = getExecutionPlan(app);
      for (JobConfig jobConfig : plan.getJobConfigs()) {
        JobRunner runner = new JobRunner(jobConfig);
        ApplicationStatus status = runner.status();
        log.debug("Status is {} for job {}", new Object[]{status, jobConfig.getName()});

        switch (status) {
          case Running:
            return ApplicationStatus.Running;
          case UnsuccessfulFinish:
            unsuccessfulFinish = true;
          case SuccessfulFinish:
            finished = true;
            break;
          default:
            // Do nothing
        }
      }

      if (unsuccessfulFinish) {
        return ApplicationStatus.UnsuccessfulFinish;
      } else if (finished) {
        return ApplicationStatus.SuccessfulFinish;
      }
      return ApplicationStatus.New;
    } catch (Throwable t) {
      throw new SamzaException("Failed to get status for application", t);
    }
  }

}
