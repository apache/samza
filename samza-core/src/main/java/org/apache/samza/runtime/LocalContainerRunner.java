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
import java.util.HashMap;
import java.util.Random;
import org.apache.log4j.MDC;
import org.apache.samza.SamzaException;
import org.apache.samza.application.internal.ApplicationBuilder;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.internal.StreamApplicationBuilder;
import org.apache.samza.application.internal.TaskApplicationBuilder;
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
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.runtime.internal.StreamApplicationSpec;
import org.apache.samza.runtime.internal.TaskApplicationSpec;
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
  protected ApplicationLifecycle getTaskAppLifecycle(TaskApplicationSpec appSpec) {
    return new TaskAppLifecycle(appSpec);
  }

  @Override
  protected ApplicationLifecycle getStreamAppLifecycle(StreamApplicationSpec appSpec) {
    return new StreamAppLifecycle(appSpec);
  }

  class TaskAppLifecycle implements ApplicationLifecycle {
    final TaskApplicationSpec taskApp;

    TaskAppLifecycle(TaskApplicationSpec taskApp) {
      this.taskApp = taskApp;
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
    public boolean waitForFinish(Duration timeout) {
      return false;
    }

  }

  class StreamAppLifecycle implements ApplicationLifecycle {
    final StreamApplicationSpec streamApp;

    StreamAppLifecycle(StreamApplicationSpec streamApp) {
      this.streamApp = streamApp;
    }

    @Override
    public void run() {
      Object taskFactory = TaskFactoryUtil.createTaskFactory(((StreamGraphSpec) streamApp.getGraph()).getOperatorSpecGraph(),
          ((StreamGraphSpec) streamApp.getGraph()).getContextManager());

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
    public boolean waitForFinish(Duration timeout) {
      return false;
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

    ApplicationBuilder.AppConfig appConfig = new ApplicationBuilder.AppConfig(config);

    LocalContainerRunner runner = new LocalContainerRunner(jobModel, containerId);

    ApplicationBuilder appSpec = appConfig.getAppClass() != null && !appConfig.getAppClass().isEmpty() ?
        new StreamApplicationBuilder((StreamApplication) Class.forName(appConfig.getAppClass()).newInstance(), config) :
        // TODO: Need to deal with 1) new TaskApplication implemention that populates inputStreams and outputStreams by the user;
        // 2) legacy task application that only has input streams specified in config
        new TaskApplicationBuilder((appBuilder, cfg) -> appBuilder.setTaskFactory((TaskFactory) TaskFactoryUtil.createTaskFactory(cfg)), config);

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
