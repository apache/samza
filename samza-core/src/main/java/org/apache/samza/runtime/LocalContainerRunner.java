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

import java.io.File;
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
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.util.FileUtil;
import org.apache.samza.util.SamzaUncaughtExceptionHandler;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import scala.Option;


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

    String containerId = System.getenv(ShellCommandConfig.ENV_CONTAINER_ID());
    log.info(String.format("Got container ID: %s", containerId));
    System.out.println(String.format("Container ID: %s", containerId));

    String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
    log.info(String.format("Got coordinator URL: %s", coordinatorUrl));
    System.out.println(String.format("Coordinator URL: %s", coordinatorUrl));

    Optional<String> execEnvContainerId = Optional.ofNullable(System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID()));

    int delay = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;
    JobModel jobModel = SamzaContainer.readJobModel(coordinatorUrl, delay);
    Config config = jobModel.getConfig();
    JobConfig jobConfig = new JobConfig(config);
    if (jobConfig.getName().isEmpty()) {
      throw new SamzaException("can not find the job name");
    }
    String jobName = jobConfig.getName().get();
    String jobId = jobConfig.getJobId();
    MDC.put("containerName", "samza-container-" + containerId);
    MDC.put("jobName", jobName);
    MDC.put("jobId", jobId);

    writeMetadataFile(jobName, jobId, containerId, execEnvContainerId, config);

    ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc =
        ApplicationDescriptorUtil.getAppDescriptor(ApplicationUtil.fromConfig(config), config);

    ContainerLaunchUtil.run(appDesc, jobName, jobId, containerId, execEnvContainerId, jobModel);
  }

  public static void writeMetadataFile(String jobName, String jobId, String containerId,
      Optional<String> execEnvContainerId, Config config) throws Exception {

    Option<File> metadataFile = new JobConfig(config).getMetadataFile(Option.apply(execEnvContainerId.orElse(null)));

    if (metadataFile.isDefined()) {

      StringBuilder metadata = new StringBuilder("Version: 1");
      metadata.append(System.lineSeparator());
      MetricsHeader metricsHeader =
          new MetricsHeader(jobName, jobId, "samza-container-" + containerId, execEnvContainerId.orElse(""), LocalContainerRunner.class.getName(),
              Util.getTaskClassVersion(config), Util.getSamzaVersion(), Util.getLocalHost().getHostName(),
              System.currentTimeMillis(), System.currentTimeMillis());

      MetricsSnapshot metricsSnapshot = new MetricsSnapshot(metricsHeader, new Metrics());
      metadata.append("MetricsSnapshot: ");
      metadata.append(SamzaObjectMapper.getObjectMapper().writeValueAsString(metricsSnapshot));
      FileUtil.writeToTextFile(metadataFile.get(), metadata.toString(), false);
    } else {
      log.info("Skipping writing metadata file.");
    }
  }
}
