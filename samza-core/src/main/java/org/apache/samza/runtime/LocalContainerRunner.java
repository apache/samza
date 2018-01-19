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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Random;
import org.apache.log4j.MDC;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.ContainerHeartbeatClient;
import org.apache.samza.container.ContainerHeartbeatMonitor;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.container.SamzaContainerExceptionHandler;
import org.apache.samza.container.SamzaContainerListener;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.util.ScalaToJavaUtils.defaultValue;

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
  private static JobModel jobModel = null;
  private final String containerId;
  private volatile Throwable containerRunnerException = null;
  private ContainerHeartbeatMonitor containerHeartbeatMonitor;
  private SamzaContainer container;

  // Default constructor invoked through user-defined main method
  public LocalContainerRunner(Config config) {
    super(config);
    this.containerId = config.getOrDefault(ShellCommandConfig.ENV_CONTAINER_ID(), System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL()));
    log.info(String.format("Got container ID: %s", containerId));
  }

  // constructor that is invoked via LocalContainerRunner#main() or ThreadJobFactory
  public LocalContainerRunner(JobModel jobModel, String containerId) {
    super(jobModel.getConfig());
    setJobModel(jobModel);
    this.containerId = containerId;
    log.info(String.format("Got container ID: %s", containerId));
  }

  private static void setJobModel(JobModel jobModel) {
    LocalContainerRunner.jobModel = jobModel;
  }

  private static JobModel getJobModel() {
    if (jobModel == null) {
      String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
      log.info(String.format("Got coordinator URL: %s", coordinatorUrl));
      int delay = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;
      jobModel = SamzaContainer.readJobModel(coordinatorUrl, delay);
    }
    return jobModel;
  }

  @Override
  public void runTask() {
    // validation
    String taskName = new TaskConfig(config).getTaskClass().getOrElse(defaultValue(null));
    if (taskName == null) {
      throw new SamzaException("Neither APP nor task.class are defined defined");
    }
    log.info("LocalContainerRunner will run " + taskName);

    this.runContainerOnly(TaskFactoryUtil.createTaskFactory(config, null, this), getJobModel());
  }

  @Override
  public ApplicationRuntimeResult run(StreamApplication app) {
    Thread.setDefaultUncaughtExceptionHandler(
        new SamzaContainerExceptionHandler(() -> {
          log.info("Exiting process now.");
          System.exit(1);
        }));

    this.runContainerOnly(TaskFactoryUtil.createTaskFactory(config, new StreamApplicationInternal(app), this), getJobModel());

    return new NoOpRuntimeResult();
  }

  @Override
  public ApplicationRuntimeResult kill(StreamApplication userApp) {
    // Ultimately this class probably won't end up extending ApplicationRunner, so this will be deleted
    throw new UnsupportedOperationException();
  }

  @Override
  public ApplicationStatus status(StreamApplication userApp) {
    // Ultimately this class probably won't end up extending ApplicationRunner, so this will be deleted
    throw new UnsupportedOperationException();
  }

  private void startContainerHeartbeatMonitor() {
    String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
    String executionEnvContainerId = System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID());
    if (executionEnvContainerId != null) {
      log.info("Got execution environment container id: {}", executionEnvContainerId);
      containerHeartbeatMonitor = new ContainerHeartbeatMonitor(() -> {
          container.shutdown();
          containerRunnerException = new SamzaException("Container shutdown due to expired heartbeat");
        }, new ContainerHeartbeatClient(coordinatorUrl, executionEnvContainerId));
      containerHeartbeatMonitor.start();
    } else {
      containerHeartbeatMonitor = null;
      log.warn("executionEnvContainerId not set. Container heartbeat monitor will not be started");
    }
  }

  private void stopContainerHeartbeatMonitor() {
    if (containerHeartbeatMonitor != null) {
      containerHeartbeatMonitor.stop();
    }
  }

  private void runContainerOnly(Object taskFactory, JobModel jobModel) {
    JobConfig jobConfig = new JobConfig(jobModel.getConfig());
    if (jobConfig.getName().isEmpty()) {
      throw new SamzaException("can not find the job name");
    }
    String jobName = jobConfig.getName().get();
    String jobId = jobConfig.getJobId().getOrElse(defaultValue("1"));
    MDC.put("containerName", "samza-container-" + containerId);
    MDC.put("jobName", jobName);
    MDC.put("jobId", jobId);

    container = SamzaContainer$.MODULE$.apply(
        containerId,
        jobModel,
        jobModel.getConfig(),
        Util.javaMapAsScalaMap(new HashMap<>()),
        taskFactory);
    container.setContainerListener(
        new SamzaContainerListener() {
          @Override
          public void onContainerStart() {
            log.info("Container Started");
          }

          @Override
          public void onContainerStop(boolean invokedExternally) {
            log.info("Container Stopped");
          }

          @Override
          public void onContainerFailed(Throwable t) {
            log.info("Container Failed");
            containerRunnerException = t;
          }
        });
    startContainerHeartbeatMonitor();
    container.run();
    stopContainerHeartbeatMonitor();
    if (containerRunnerException != null) {
      log.error("Container stopped with Exception. Exiting process now.", containerRunnerException);
      System.exit(1);
    }
  }

  public static void main(String[] args) throws Exception {
    Config config = getJobModel().getConfig();
    Thread.setDefaultUncaughtExceptionHandler(
        new SamzaContainerExceptionHandler(() -> {
          log.info("Exiting process now.");
          System.exit(1);
        }));

    StreamApplication.AppConfig appConfig = new StreamApplication.AppConfig(config);
    if (appConfig.getAppClass() != null && !appConfig.getAppClass().isEmpty()) {
      Class<?> cls = Class.forName(appConfig.getAppClass());
      Method mainMethod = cls.getMethod("main", String[].class);
      // enforce that the application runner for the user application is LocalContainerRunner
      mainMethod.invoke(null, (Object) new String[] {"--config", String.format("%s=%s", StreamApplication.AppConfig.RUNNER_CONFIG, LocalContainerRunner.class.getName())});
    } else {
      // low-level task containers
      LocalContainerRunner runner = new LocalContainerRunner(getJobModel(), System.getenv(ShellCommandConfig.ENV_CONTAINER_ID()));
      runner.runTask();
    }
  }

}
