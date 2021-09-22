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

import java.util.Optional;
import java.util.Random;
import org.apache.samza.SamzaException;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.logging.LoggingContextHolder;
import org.apache.samza.util.SamzaUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Launches and manages the lifecycle for {@link SamzaContainer}s in YARN.
 */
public class LocalContainerRunner {
  private static final Logger log = LoggerFactory.getLogger(LocalContainerRunner.class);

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(
        new SamzaUncaughtExceptionHandler(() -> {
          log.info("Exiting process now.");
          System.exit(1);
        }));

    String containerId = System.getenv(ShellCommandConfig.ENV_CONTAINER_ID);
    System.out.println(String.format("Container ID: %s", containerId));

    String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL);
    System.out.println(String.format("Coordinator URL: %s", coordinatorUrl));

    Optional<String> execEnvContainerId = Optional.ofNullable(System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID));

    int delay = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;
    JobModel jobModel = SamzaContainer.readJobModel(coordinatorUrl, delay);
    Config config = jobModel.getConfig();

    // this call is also in LocalContainerRunner, but adding this here allows more logs to get handled by Samza loggers
    LoggingContextHolder.INSTANCE.setConfig(config);
    log.info(String.format("Got container ID: %s", containerId));
    log.info(String.format("Got coordinator URL: %s", coordinatorUrl));

    JobConfig jobConfig = new JobConfig(config);
    String jobName = jobConfig.getName()
        .orElseThrow(() -> new SamzaException(String.format("Config %s is missing", JobConfig.JOB_NAME)));
    String jobId = jobConfig.getJobId();

    ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc =
        ApplicationDescriptorUtil.getAppDescriptor(ApplicationUtil.fromConfig(config), config);

    ContainerLaunchUtil.run(appDesc, jobName, jobId, containerId, execEnvContainerId, jobModel);
  }

}
