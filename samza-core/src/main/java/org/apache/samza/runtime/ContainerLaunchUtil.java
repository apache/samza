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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.ContainerHeartbeatMonitor;
import org.apache.samza.container.ExecutionContainerIdManager;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.context.JobContextImpl;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.coordinator.stream.messages.SetExecutionEnvContainerIdMapping;
import org.apache.samza.diagnostics.DiagnosticsManager;
import org.apache.samza.drain.DrainMonitor;
import org.apache.samza.environment.EnvironmentVariables;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.logging.LoggingContextHolder;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.DiagnosticsUtil;
import org.apache.samza.util.ScalaJavaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import scala.Option;


public class ContainerLaunchUtil {
  private static final Logger log = LoggerFactory.getLogger(ContainerLaunchUtil.class);

  private static volatile Throwable containerRunnerException = null;

  /**
   * This method launches a Samza container in a managed cluster and is invoked by BeamContainerRunner.
   * Any change here needs to take Beam into account.
   */
  public static void run(ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,  String containerId, JobModel jobModel) {
    Optional<String> executionEnvContainerId =
        Optional.ofNullable(System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID));
    Optional<String> samzaEpochId = Optional.ofNullable(System.getenv(EnvironmentVariables.SAMZA_EPOCH_ID));
    JobConfig jobConfig = new JobConfig(jobModel.getConfig());
    ContainerLaunchUtil.run(appDesc, jobConfig.getName().get(), jobConfig.getJobId(), containerId,
        executionEnvContainerId, samzaEpochId, jobModel);
  }

  /**
   * This method launches a Samza container in a managed cluster, e.g. Yarn.
   */
  public static void run(
      ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
      String jobName,
      String jobId,
      String containerId,
      Optional<String> executionEnvContainerId,
      Optional<String> samzaEpochId,
      JobModel jobModel) {
    Config config = jobModel.getConfig();

    // logging setup: MDC, logging context
    MDC.put("containerName", "samza-container-" + containerId);
    MDC.put("jobName", jobName);
    MDC.put("jobId", jobId);
    LoggingContextHolder.INSTANCE.setConfig(jobModel.getConfig());

    DiagnosticsUtil.writeMetadataFile(jobName, jobId, containerId, executionEnvContainerId, config);
    int exitCode = run(appDesc, jobName, jobId, containerId, executionEnvContainerId, samzaEpochId, jobModel, config,
        buildExternalContext(config));

    exitProcess(exitCode);
  }

  @VisibleForTesting
  static int run(
      ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
      String jobName,
      String jobId,
      String containerId,
      Optional<String> executionEnvContainerId,
      Optional<String> samzaEpochId,
      JobModel jobModel,
      Config config,
      Optional<ExternalContext> externalContextOptional) {
    CoordinatorStreamStore coordinatorStreamStore = buildCoordinatorStreamStore(config, new MetricsRegistryMap());
    coordinatorStreamStore.init();

    /*
     * We track the exit code and only trigger exit in the finally block to make sure we are able to execute all the
     * clean up steps. Prior implementation had short circuited exit causing some of the clean up steps to be missed.
     */
    int exitCode = 0;

    try {
      TaskFactory taskFactory = TaskFactoryUtil.getTaskFactory(appDesc);
      LocalityManager localityManager = new LocalityManager(new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetContainerHostMapping.TYPE));

      // StartpointManager wraps the coordinatorStreamStore in the namespaces internally
      StartpointManager startpointManager = null;
      if (new JobConfig(config).getStartpointEnabled()) {
        startpointManager = new StartpointManager(coordinatorStreamStore);
      }

      Map<String, MetricsReporter> metricsReporters = loadMetricsReporters(appDesc, containerId, config);

      // Creating diagnostics manager and reporter, and wiring it respectively
      Optional<DiagnosticsManager> diagnosticsManager =
          DiagnosticsUtil.buildDiagnosticsManager(jobName, jobId, jobModel, containerId, executionEnvContainerId,
              samzaEpochId, config);
      MetricsRegistryMap metricsRegistryMap = new MetricsRegistryMap();

      DrainMonitor drainMonitor = null;
      JobConfig jobConfig = new JobConfig(config);
      if (jobConfig.getDrainMonitorEnabled()) {
        drainMonitor = new DrainMonitor(coordinatorStreamStore, config);
      }

      SamzaContainer container = SamzaContainer$.MODULE$.apply(
          containerId, jobModel,
          ScalaJavaUtil.toScalaMap(metricsReporters),
          metricsRegistryMap,
          taskFactory,
          JobContextImpl.fromConfigWithDefaults(config, jobModel),
          Option.apply(appDesc.getApplicationContainerContextFactory().orElse(null)),
          Option.apply(appDesc.getApplicationTaskContextFactory().orElse(null)),
          Option.apply(externalContextOptional.orElse(null)),
          localityManager,
          startpointManager,
          Option.apply(diagnosticsManager.orElse(null)),
          drainMonitor);

      ProcessorLifecycleListener processorLifecycleListener = appDesc.getProcessorLifecycleListenerFactory()
          .createInstance(new ProcessorContext() { }, config);
      ClusterBasedProcessorLifecycleListener
          listener = new ClusterBasedProcessorLifecycleListener(config, processorLifecycleListener, container::shutdown);
      container.setContainerListener(listener);

      ContainerHeartbeatMonitor heartbeatMonitor = createContainerHeartbeatMonitor(container,
          new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetConfig.TYPE), config);
      if (heartbeatMonitor != null) {
        heartbeatMonitor.start();
      }

      if (new JobConfig(config).getApplicationMasterHighAvailabilityEnabled()) {
        executionEnvContainerId.ifPresent(execEnvContainerId -> {
          ExecutionContainerIdManager executionContainerIdManager = new ExecutionContainerIdManager(
              new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetExecutionEnvContainerIdMapping.TYPE));
          executionContainerIdManager.writeExecutionEnvironmentContainerIdMapping(containerId, execEnvContainerId);
        });
      }

      container.run();
      if (heartbeatMonitor != null) {
        heartbeatMonitor.stop();
      }

      // Check to see if the HeartbeatMonitor has set an exception before
      // overriding the value with what the listener returns
      if (containerRunnerException == null) {
        containerRunnerException = listener.getContainerException();
      }

      if (containerRunnerException != null) {
        log.error("Container stopped with Exception. Exiting process now.", containerRunnerException);
        exitCode = 1;
      }
    } catch (Throwable e) {
      /*
       * Two separate log statements are intended to print the entire stack trace as part of the logs. Using
       * single log statement with custom format requires explicitly fetching stack trace and null checks which makes
       * the code slightly hard to read in comparison with the current choice.
       */
      log.error("Exiting the process due to", e);
      log.error("Container runner exception: ", containerRunnerException);
      exitCode = 1;
    } finally {
      coordinatorStreamStore.close();
      return exitCode;
    }
  }

  @VisibleForTesting
  static CoordinatorStreamStore buildCoordinatorStreamStore(Config config, MetricsRegistryMap metricsRegistryMap) {
    return new CoordinatorStreamStore(config, metricsRegistryMap);
  }

  @VisibleForTesting
  static void exitProcess(int status) {
    System.exit(status);
  }

  private static Optional<ExternalContext> buildExternalContext(Config config) {
    /*
     * By default, use an empty ExternalContext here. In a custom fork of Samza, this can be implemented to pass
     * a non-empty ExternalContext to SamzaContainer. Only config should be used to build the external context. In the
     * future, components like the application descriptor may not be available to LocalContainerRunner.
     */
    return Optional.empty();
  }

  // TODO: this is going away when SAMZA-1168 is done and the initialization of metrics reporters are done via
  // LocalApplicationRunner#createStreamProcessor()
  private static Map<String, MetricsReporter> loadMetricsReporters(
      ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc, String containerId, Config config) {
    Map<String, MetricsReporter> reporters = new HashMap<>();
    appDesc.getMetricsReporterFactories().forEach((name, factory) ->
        reporters.put(name, factory.getMetricsReporter(name, containerId, config)));
    return reporters;
  }

  /**
   * Creates a new container heartbeat monitor if possible.
   * @param container the container to monitor
   * @param coordinatorStreamStore the metadata store to fetch coordinator url from
   * @param config the job configuration
   * @return a new {@link ContainerHeartbeatMonitor} instance, or null if could not create one
   */
  private static ContainerHeartbeatMonitor createContainerHeartbeatMonitor(SamzaContainer container,
      MetadataStore coordinatorStreamStore, Config config) {
    if (new JobConfig(config).getContainerHeartbeatMonitorEnabled()) {
      String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL);
      String executionEnvContainerId = System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID);
      if (executionEnvContainerId != null) {
        log.info("Got execution environment container id for container heartbeat monitor: {}", executionEnvContainerId);
        return new ContainerHeartbeatMonitor(() -> {
          try {
            container.shutdown();
            containerRunnerException = new SamzaException("Container shutdown due to expired heartbeat");
          } catch (Exception e) {
            log.error("Heartbeat monitor failed to shutdown the container gracefully. Exiting process.", e);
            System.exit(1);
          }
        }, coordinatorUrl, executionEnvContainerId, coordinatorStreamStore, config);
      } else {
        log.warn("Container heartbeat monitor is enabled, but execution environment container id is not set. "
            + "Container heartbeat monitor will not be created");
        return null;
      }
    } else {
      log.info("Container heartbeat monitor is disabled");
      return null;
    }
  }
}
