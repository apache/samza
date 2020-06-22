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
import java.util.List;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.execution.JobPlanner;
import org.apache.samza.execution.RemoteJobPlanner;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.job.ApplicationStatus.*;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in a remote cluster
 */
public class RemoteApplicationRunner implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteApplicationRunner.class);
  private static final long DEFAULT_SLEEP_DURATION_MS = 2000;

  private final SamzaApplication app;
  private final Config config;

  /**
   * Constructors a {@link RemoteApplicationRunner} to run the {@code app} with the {@code config}.
   *
   * @param app application to run
   * @param config configuration for the application
   */
  public RemoteApplicationRunner(SamzaApplication app, Config config) {
    this.app = app;
    this.config = config;
  }

  @Override
  public void run(ExternalContext externalContext) {
    if (new JobConfig(config).getConfigLoaderFactory().isPresent()) {
      JobRunner runner = new JobRunner(JobPlanner.generateSingleJobConfig(config));
      runner.submit();
      return;
    }

    // TODO SAMZA-2432: Clean this up once SAMZA-2405 is completed when legacy flow is removed.
    try {
      JobPlanner planner = new RemoteJobPlanner(ApplicationDescriptorUtil.getAppDescriptor(app, config));
      List<JobConfig> jobConfigs = planner.prepareJobs();
      if (jobConfigs.isEmpty()) {
        throw new SamzaException("No jobs to run.");
      }

      // 3. submit jobs for remote execution
      jobConfigs.forEach(jobConfig -> {
        LOG.info("Starting job {} with config {}", jobConfig.getName(), jobConfig);
        JobRunner runner = new JobRunner(jobConfig);
        runner.run(true);
      });
    } catch (Throwable t) {
      throw new SamzaException("Failed to run application", t);
    }
  }

  @Override
  public void kill() {
    // since currently we only support single actual remote job, we can get its status without
    // building the execution plan.
    try {
      JobConfig jc = new JobConfig(JobPlanner.generateSingleJobConfig(config));
      LOG.info("Killing job {}", jc.getName());
      JobRunner runner = new JobRunner(jc);
      runner.kill();
    } catch (Throwable t) {
      throw new SamzaException("Failed to kill application", t);
    }
  }

  @Override
  public ApplicationStatus status() {
    // since currently we only support single actual remote job, we can get its status without
    // building the execution plan
    try {
      JobConfig jc = new JobConfig(JobPlanner.generateSingleJobConfig(config));
      return getApplicationStatus(jc);
    } catch (Throwable t) {
      throw new SamzaException("Failed to get status for application", t);
    }
  }

  @Override
  public void waitForFinish() {
    waitForFinish(Duration.ofMillis(0));
  }

  @Override
  public boolean waitForFinish(Duration timeout) {
    JobConfig jobConfig = new JobConfig(JobPlanner.generateSingleJobConfig(config));
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

  /* package private */ ApplicationStatus getApplicationStatus(JobConfig jobConfig) {
    JobRunner runner = new JobRunner(jobConfig);
    ApplicationStatus status = runner.status();
    LOG.debug("Status is {} for job {}", new Object[]{status, jobConfig.getName()});
    return status;
  }
}
