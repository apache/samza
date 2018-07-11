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
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.log4j.MDC;
import org.apache.samza.SamzaException;
import org.apache.samza.application.ApplicationRunnable;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.internal.ApplicationSpec;
import org.apache.samza.application.internal.StreamApplicationRuntime;
import org.apache.samza.application.internal.StreamApplicationSpec;
import org.apache.samza.application.internal.TaskApplicationRuntime;
import org.apache.samza.application.internal.TaskApplicationSpec;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.ContainerHeartbeatClient;
import org.apache.samza.container.ContainerHeartbeatMonitor;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.container.SamzaContainerListener;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.SamzaUncaughtExceptionHandler;
import org.apache.samza.util.ScalaJavaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.util.ScalaJavaUtil.*;

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
  private final String containerId;
  private volatile Throwable containerRunnerException = null;
  private ContainerHeartbeatMonitor containerHeartbeatMonitor;
  private SamzaContainer container;

  public LocalContainerRunner(JobModel jobModel, String containerId) {
    super(jobModel.getConfig());
    this.jobModel = jobModel;
    this.containerId = containerId;
  }

  @Override
  protected ApplicationRunnable getTaskAppRunnable(TaskApplicationSpec appSpec) {
    return new TaskAppRunnable(appSpec);
  }

  @Override
  protected ApplicationRunnable getStreamAppRunnable(StreamApplicationSpec appSpec) {
    return new StreamAppRunnable(appSpec);
  }

  class TaskAppRunnable implements ApplicationRunnable {
    final TaskApplicationRuntime taskApp;

    TaskAppRunnable(TaskApplicationSpec taskApp) {
      this.taskApp = new TaskApplicationRuntime(taskApp);
    }

    @Override
    public void run() {
      Object taskFactory = this.taskApp.getTaskFactory();

      container = SamzaContainer$.MODULE$.apply(
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

    @Override
    public void kill() {
      // Ultimately this class probably won't end up extending ApplicationRunner, so this will be deleted
      throw new UnsupportedOperationException();
    }

    @Override
    public ApplicationStatus status() {
      // Ultimately this class probably won't end up extending ApplicationRunner, so this will be deleted
      throw new UnsupportedOperationException();
    }

    @Override
    public void waitForFinish() {

    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return false;
    }

    @Override
    public ApplicationRunnable withMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
      // Ultimately this class probably won't end up extending ApplicationRunner, so this will be deleted
      throw new UnsupportedOperationException();
    }
  }

  class StreamAppRunnable implements ApplicationRunnable {
    final StreamApplicationRuntime streamApp;

    StreamAppRunnable(StreamApplicationSpec streamApp) {
      this.streamApp = new StreamApplicationRuntime(streamApp);
    }

    @Override
    public void run() {
      Object taskFactory = TaskFactoryUtil.createTaskFactory(streamApp.getStreamGraphSpec().getOperatorSpecGraph(),
          streamApp.getStreamGraphSpec().getContextManager());

      container = SamzaContainer$.MODULE$.apply(
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

    @Override
    public void kill() {
      // Ultimately this class probably won't end up extending ApplicationRunner, so this will be deleted
      throw new UnsupportedOperationException();
    }

    @Override
    public ApplicationStatus status() {
      // Ultimately this class probably won't end up extending ApplicationRunner, so this will be deleted
      throw new UnsupportedOperationException();
    }

    @Override
    public void waitForFinish() {

    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return false;
    }

    @Override
    public ApplicationRunnable withMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
      throw new UnsupportedOperationException();
    }
  }

  // only invoked by legacy applications w/o user-defined main
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
    String jobId = jobConfig.getJobId().getOrElse(defaultValue("1"));
    MDC.put("containerName", "samza-container-" + containerId);
    MDC.put("jobName", jobName);
    MDC.put("jobId", jobId);

    ApplicationSpec.AppConfig appConfig = new ApplicationSpec.AppConfig(config);

    LocalContainerRunner runner = new LocalContainerRunner(jobModel, containerId);
    ApplicationSpec appSpec = null;
    if (appConfig.getAppClass() != null && !appConfig.getAppClass().isEmpty()) {
      // add configuration-factory and configuration-path to the command line options and invoke the user defined main class
      // write the complete configuration to a local file in property file format
      StreamGraphSpec streamGraph = new StreamGraphSpec(runner, config);
      StreamApplication userApp = (StreamApplication) Class.forName(appConfig.getAppClass()).newInstance();
      userApp.init(streamGraph, config);
      appSpec = new StreamApplicationSpec(streamGraph, config);
    } else {
      appSpec = new TaskApplicationSpec((TaskFactory) TaskFactoryUtil.createTaskFactory(config), config);
    }

    runner.run(appSpec);
    runner.waitForFinish(appSpec);
  }

  private void startContainerHeartbeatMonitor() {
    String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
    String executionEnvContainerId = System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID());
    if (executionEnvContainerId != null) {
      log.info("Got execution environment container id: {}", executionEnvContainerId);
      containerHeartbeatMonitor = new ContainerHeartbeatMonitor(() -> {
          try {
            container.shutdown();
            containerRunnerException = new SamzaException("Container shutdown due to expired heartbeat");
          } catch (Exception e) {
            log.error("Heartbeat monitor failed to shutdown the container gracefully. Exiting process.", e);
            System.exit(1);
          }
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

}
