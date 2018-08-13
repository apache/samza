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
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.ApplicationClassUtils;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.internal.AppDescriptorImpl;
import org.apache.samza.application.internal.StreamAppDescriptorImpl;
import org.apache.samza.application.internal.TaskAppDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.ContainerHeartbeatClient;
import org.apache.samza.container.ContainerHeartbeatMonitor;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.container.SamzaContainerListener;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.SamzaUncaughtExceptionHandler;
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

    AppDescriptorImpl appDesc = getAppDesc(config);
    run(appDesc, containerId, jobModel, config);

    System.exit(0);
  }

  private static class LocalContainerLifecycleListener implements SamzaContainerListener {
    private final ProcessorLifecycleListener pListener;

    LocalContainerLifecycleListener(ProcessorLifecycleListener pListener) {
      this.pListener = pListener;
    }

    @Override
    public void beforeStart() {
      log.info("Starting Local Container");
      pListener.beforeStart();
    }

    @Override
    public void onContainerStart() {
      log.info("Local Container Started");
      pListener.afterStart();
    }

    @Override
    public void beforeStop() {
      log.info("Stopping Local Container");
      pListener.beforeStop();
    }

    @Override
    public void onContainerStop() {
      log.info("Container Stopped Successfully");
      pListener.afterStop(null);
    }

    @Override
    public void onContainerFailed(Throwable t) {
      log.info("Container Stopped with Failure");
      containerRunnerException = t;
      pListener.afterStop(t);
    }
  }

  private static AppDescriptorImpl getAppDesc(Config config) {
    ApplicationBase userApp = ApplicationClassUtils.fromConfig(config);
    return userApp instanceof StreamApplication ? new StreamAppDescriptorImpl((StreamApplication) userApp, config) :
        new TaskAppDescriptorImpl((TaskApplication) userApp, config);
  }

  private static void run(AppDescriptorImpl appDesc, String containerId, JobModel jobModel, Config config) {
    TaskFactory taskFactory = getTaskFactory(appDesc);
    SamzaContainer container = SamzaContainer$.MODULE$.apply(
        containerId,
        jobModel,
        config,
        ScalaJavaUtil.toScalaMap(new HashMap<>()),
        taskFactory);

    JobConfig jobConfig = new JobConfig(config);
    ProcessorContext pContext = () -> String.format("%s-%s-%s", jobConfig.getName(), jobConfig.getJobId(), containerId);
    ProcessorLifecycleListener pListener = appDesc.getProcessorLifecycleListenerFactory().createInstance(pContext, config);

    // TODO: this is a temporary solution to inject the lifecycle listeners before we fix SAMZA-1168
    container.setContainerListener(new LocalContainerLifecycleListener(pListener));

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

  private static TaskFactory getTaskFactory(AppDescriptorImpl appDesc) {
    if (appDesc instanceof StreamAppDescriptorImpl) {
      StreamAppDescriptorImpl streamAppDesc = (StreamAppDescriptorImpl) appDesc;
      return TaskFactoryUtil.createTaskFactory(((StreamGraphSpec) streamAppDesc.getGraph()).getOperatorSpecGraph(),
          streamAppDesc.getContextManager());
    }
    return ((TaskAppDescriptorImpl) appDesc).getTaskFactory();
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
