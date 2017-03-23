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

import java.util.HashMap;
import java.util.Random;
import org.apache.log4j.MDC;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.ScalaToJavaUtils;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LocalContainerRunner is the local runner for Yarn {@link SamzaContainer}s. It is an intermediate step to
 * have a local runner for yarn before we consolidate the Yarn container and coordination into a
 * {@link org.apache.samza.processor.StreamProcessor}. This class will be replaced by the {@link org.apache.samza.processor.StreamProcessor}
 * local runner once that's done.
 *
 * Since we don't have the {@link org.apache.samza.coordinator.JobCoordinator} implementation in Yarn, the components (jobModel and containerId)
 * are directly inside the runner.
 */
public class LocalContainerRunner extends AbstractApplicationRunner {
  private static final Logger log = LoggerFactory.getLogger(LocalContainerRunner.class);
  private final JobModel jobModel;
  private final Integer containerId;

  public LocalContainerRunner(JobModel jobModel, Integer containerId) {
    super(jobModel.getConfig());
    this.jobModel = jobModel;
    this.containerId = containerId;
  }

  @Override
  public void run(StreamApplication streamApp) {
    JmxServer jmxServer = null;
    try {
      jmxServer = new JmxServer();
      ContainerModel containerModel = jobModel.getContainers().get(containerId);
      Object taskFactory = TaskFactoryUtil.createTaskFactory(config, streamApp, this);

      SamzaContainer container = SamzaContainer$.MODULE$.apply(
          containerModel.getContainerId(),
          containerModel,
          config,
          jobModel.maxChangeLogStreamPartitions,
          SamzaContainer.getLocalityManager(containerId, config),
          jmxServer,
          Util.javaMapAsScalaMap(new HashMap<String, MetricsReporter>()),
          taskFactory,
          this);

      container.run();
    } finally {
      if (jmxServer != null) {
        jmxServer.stop();
      }
    }
  }


  public static void main(String[] args) throws Exception {
    setExceptionHandler(() -> System.exit(1));

    Integer containerId = Integer.valueOf(System.getenv(ShellCommandConfig.ENV_CONTAINER_ID()));
    log.info(String.format("Got container ID: %d", containerId));
    String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
    log.info(String.format("Got coordinator URL: %s", coordinatorUrl));
    int delay = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;
    JobModel jobModel = SamzaContainer.readJobModel(coordinatorUrl, delay);
    Config config = jobModel.getConfig();
    JobConfig jobConfig = new JobConfig(config);
    if (jobConfig.getName().isEmpty()) {
      throw new SamzaException("can not find the job name");
    }
    String jobName = jobConfig.getName().get();
    String jobId = jobConfig.getJobId().getOrElse(ScalaToJavaUtils.defaultValue("1"));
    MDC.put("containerName", "samza-container-" + containerId);
    MDC.put("jobName", jobName);
    MDC.put("jobId", jobId);

    StreamApplication streamApp = TaskFactoryUtil.createStreamApplication(config);
    new LocalContainerRunner(jobModel, containerId).run(streamApp);
  }

  /* package private */ static void setExceptionHandler(Runnable runnable) {
    Thread.UncaughtExceptionHandler exceptionHandler = (t, e) -> {
      log.error(String.format("Uncaught exception in thread (name=%s). Exiting process now.", t.getName(), e));
      e.printStackTrace(System.err);
      runnable.run();
    };
    Thread.setDefaultUncaughtExceptionHandler(exceptionHandler);
  }
}
