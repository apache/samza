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
import java.util.Map;
import java.util.Random;
import org.apache.log4j.MDC;
import org.apache.samza.SamzaException;
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.ApplicationClassUtils;
import org.apache.samza.application.ApplicationSpec;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.internal.StreamAppSpecImpl;
import org.apache.samza.application.internal.TaskAppSpecImpl;
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
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskFactory;
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

  LocalContainerRunner(JobModel jobModel, String containerId) {
    super(jobModel.getConfig());
    this.jobModel = jobModel;
    this.containerId = containerId;
  }

  private class TaskAppExecutable implements AppRuntimeExecutable {
    private final TaskAppSpecImpl taskApp;

    private TaskAppExecutable(TaskAppSpecImpl taskApp) {
      this.taskApp = taskApp;
    }

    @Override
    public void run() {
      TaskFactory taskFactory = this.taskApp.getTaskFactory();

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
            public void onContainerStop() {
              log.info("Container Stopped");
            }

            @Override
            public void onContainerFailed(Throwable t) {
              log.info("Container Failed");
              containerRunnerException = t;
            }
          });

      taskApp.getProcessorLifecycleListner().beforeStart();
      taskApp.getProcessorLifecycleListner().afterStart();
      startContainerHeartbeatMonitor();
      container.run();
      stopContainerHeartbeatMonitor();
      taskApp.getProcessorLifecycleListner().beforeStop();
      taskApp.getProcessorLifecycleListner().afterStop();

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

  private class StreamAppExecutable implements AppRuntimeExecutable {
    private final StreamAppSpecImpl streamApp;

    private StreamAppExecutable(StreamAppSpecImpl streamApp) {
      this.streamApp = streamApp;
    }

    @Override
    public void run() {
      TaskFactory taskFactory = () -> new StreamOperatorTask(((StreamGraphSpec) streamApp.getGraph()).getOperatorSpecGraph(),
          streamApp.getContextManager());

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
            public void onContainerStop() {
              log.info("Container Stopped");
            }

            @Override
            public void onContainerFailed(Throwable t) {
              log.info("Container Failed");
              containerRunnerException = t;
            }
          });

      streamApp.getProcessorLifecycleListner().beforeStart();
      streamApp.getProcessorLifecycleListner().afterStart();
      startContainerHeartbeatMonitor();
      container.run();
      stopContainerHeartbeatMonitor();
      streamApp.getProcessorLifecycleListner().beforeStop();
      streamApp.getProcessorLifecycleListner().afterStop();

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

  private static class ContainerAppRuntimeImpl implements ApplicationRuntime {
    private final ApplicationSpec appSpec;
    private final LocalContainerRunner runner;

    public ContainerAppRuntimeImpl(ApplicationSpec appSpec, LocalContainerRunner runner) {
      this.appSpec = appSpec;
      this.runner = runner;
    }

    @Override
    public void run() {
      this.runner.run(appSpec);
    }

    @Override
    public void kill() {
      this.runner.kill(appSpec);
    }

    @Override
    public ApplicationStatus status() {
      return this.runner.status(appSpec);
    }

    @Override
    public void waitForFinish() {
      this.runner.waitForFinish(appSpec, Duration.ofSeconds(0));
    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return this.runner.waitForFinish(appSpec, timeout);
    }

    @Override
    public void addMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
      this.runner.addMetricsReporters(metricsReporters);
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

    ApplicationRuntime appSpec = createAppRuntime(ApplicationClassUtils.fromConfig(config),
        new LocalContainerRunner(jobModel, containerId), config);
    appSpec.run();
    appSpec.waitForFinish();
  }

  @Override
  protected AppRuntimeExecutable getTaskAppRuntimeExecutable(TaskAppSpecImpl appSpec) {
    return new TaskAppExecutable(appSpec);
  }

  @Override
  protected AppRuntimeExecutable getStreamAppRuntimeExecutable(StreamAppSpecImpl appSpec) {
    return new StreamAppExecutable(appSpec);
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

  private static ApplicationRuntime createAppRuntime(ApplicationBase userApp, LocalContainerRunner runner, Config config) {
    if (userApp instanceof StreamApplication) {
      return new ContainerAppRuntimeImpl(new StreamAppSpecImpl((StreamApplication) userApp, config), runner);
    }
    if (userApp instanceof TaskApplication) {
      return new ContainerAppRuntimeImpl(new TaskAppSpecImpl((TaskApplication) userApp, config), runner);
    }

    throw new IllegalArgumentException(String.format("Invalid user application class %s. Only StreamApplication and TaskApplication "
        + "are supported", userApp.getClass().getName()));
  }

}
