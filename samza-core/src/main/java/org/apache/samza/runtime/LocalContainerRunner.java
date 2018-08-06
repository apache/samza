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
import org.slf4j.MDC;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.ContainerHeartbeatClient;
import org.apache.samza.container.ContainerHeartbeatMonitor;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.util.SamzaUncaughtExceptionHandler;
import org.apache.samza.container.SamzaContainerListener;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.ScalaJavaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launches and manages the lifecycle for {@link SamzaContainer}s in YARN.
 */
public class LocalContainerRunner {
  private static final Logger log = LoggerFactory.getLogger(LocalContainerRunner.class);
  private static volatile Throwable containerRunnerException = null;

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

    int delay = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;
    JobModel jobModel = SamzaContainer.readJobModel(coordinatorUrl, delay);
    Config config = jobModel.getConfig();
    JobConfig jobConfig = new JobConfig(config);
    if (jobConfig.getName().isEmpty()) {
      throw new SamzaException("can not find the job name");
    }
    String jobName = jobConfig.getName().get();
    String jobId = jobConfig.getJobId().getOrElse(ScalaJavaUtil.defaultValue("1"));
    MDC.put("containerName", "samza-container-" + containerId);
    MDC.put("jobName", jobName);
    MDC.put("jobId", jobId);

    StreamApplication streamApp = TaskFactoryUtil.createStreamApplication(config);
    Object taskFactory = getTaskFactory(streamApp, config);
    run(taskFactory, containerId, jobModel, config);

    System.exit(0);
  }

  private static void run(Object taskFactory, String containerId, JobModel jobModel, Config config) {
    SamzaContainer container = SamzaContainer$.MODULE$.apply(
        containerId,
        jobModel,
        config,
        ScalaJavaUtil.toScalaMap(new HashMap<>()),
        taskFactory);

    container.setContainerListener(
        new SamzaContainerListener() {
          @Override
          public void onContainerStart() {
            log.info("Container Started");
          }

          @Override
          public void onContainerStop() {
            log.info("Container Stopped");
          }

          @Override
          public void onContainerFailed(Throwable t) {
            log.info("Container Failed");
            containerRunnerException = t;
          }
        });

    ContainerHeartbeatMonitor heartbeatMonitor = createContainerHeartbeatMonitor(container);
    if (heartbeatMonitor != null) {
      heartbeatMonitor.start();
    }

    container.run();
    if (heartbeatMonitor != null) {
      heartbeatMonitor.stop();
    }

    if (containerRunnerException != null) {
      log.error("Container stopped with Exception. Exiting process now.", containerRunnerException);
      System.exit(1);
    }
  }

  private static Object getTaskFactory(StreamApplication streamApp, Config config) {
    if (streamApp != null) {
      StreamGraphSpec graphSpec = new StreamGraphSpec(config);
      streamApp.init(graphSpec, config);
      return TaskFactoryUtil.createTaskFactory(graphSpec.getOperatorSpecGraph(), graphSpec.getContextManager());
    }
    return TaskFactoryUtil.createTaskFactory(config);
  }

  /**
   * Creates a new container heartbeat monitor if possible.
   * @param container the container to monitor
   * @return a new {@link ContainerHeartbeatMonitor} instance, or null if could not create one
   */
  private static ContainerHeartbeatMonitor createContainerHeartbeatMonitor(SamzaContainer container) {
    String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
    String executionEnvContainerId = System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID());
    if (executionEnvContainerId != null) {
      log.info("Got execution environment container id: {}", executionEnvContainerId);
      return new ContainerHeartbeatMonitor(() -> {
          try {
            container.shutdown();
            containerRunnerException = new SamzaException("Container shutdown due to expired heartbeat");
          } catch (Exception e) {
            log.error("Heartbeat monitor failed to shutdown the container gracefully. Exiting process.", e);
            System.exit(1);
          }
        }, new ContainerHeartbeatClient(coordinatorUrl, executionEnvContainerId));
    } else {
      log.warn("Execution environment container id not set. Container heartbeat monitor will not be created");
      return null;
    }
  }
}
