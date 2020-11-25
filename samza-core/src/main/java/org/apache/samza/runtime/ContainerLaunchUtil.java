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
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.ContainerHeartbeatMonitor;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.context.JobContextImpl;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.diagnostics.DiagnosticsManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.reporter.MetricsSnapshotReporter;
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
    Optional<String> execEnvContainerId = Optional.ofNullable(System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID));
    JobConfig jobConfig = new JobConfig(jobModel.getConfig());
    ContainerLaunchUtil.run(appDesc, jobConfig.getName().get(), jobConfig.getJobId(), containerId, execEnvContainerId, jobModel);
  }

  /**
   * This method launches a Samza container in a managed cluster, e.g. Yarn.
   */
  public static void run(
      ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
      String jobName, String jobId, String containerId, Optional<String> execEnvContainerId,
      JobModel jobModel) {

    // populate MDC for logging
    MDC.put("containerName", "samza-container-" + containerId);
    MDC.put("jobName", jobName);
    MDC.put("jobId", jobId);


    Config config = jobModel.getConfig();
    DiagnosticsUtil.writeMetadataFile(jobName, jobId, containerId, execEnvContainerId, config);
    run(appDesc, jobName, jobId, containerId, execEnvContainerId, jobModel, config, buildExternalContext(config));

    System.exit(0);
  }

  private static void run(
      ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
      String jobName,
      String jobId,
      String containerId,
      Optional<String> execEnvContainerId,
      JobModel jobModel,
      Config config,
      Optional<ExternalContext> externalContextOptional) {
    CoordinatorStreamStore coordinatorStreamStore = new CoordinatorStreamStore(config, new MetricsRegistryMap());
    coordinatorStreamStore.init();

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
      Optional<Pair<DiagnosticsManager, MetricsSnapshotReporter>> diagnosticsManagerReporterPair = DiagnosticsUtil.buildDiagnosticsManager(jobName, jobId, jobModel, containerId, execEnvContainerId, config);
      Option<DiagnosticsManager> diagnosticsManager = Option.empty();
      if (diagnosticsManagerReporterPair.isPresent()) {
        diagnosticsManager = Option.apply(diagnosticsManagerReporterPair.get().getKey());
        metricsReporters.put(MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS, diagnosticsManagerReporterPair.get().getValue());
      }

      SamzaContainer container = SamzaContainer$.MODULE$.apply(
          containerId, jobModel,
          ScalaJavaUtil.toScalaMap(metricsReporters),
          taskFactory,
          JobContextImpl.fromConfigWithDefaults(config, jobModel),
          Option.apply(appDesc.getApplicationContainerContextFactory().orElse(null)),
          Option.apply(appDesc.getApplicationTaskContextFactory().orElse(null)),
          Option.apply(externalContextOptional.orElse(null)),
          localityManager,
          startpointManager,
          diagnosticsManager);

      ProcessorLifecycleListener processorLifecycleListener = appDesc.getProcessorLifecycleListenerFactory()
          .createInstance(new ProcessorContext() { }, config);
      ClusterBasedProcessorLifecycleListener
          listener = new ClusterBasedProcessorLifecycleListener(config, processorLifecycleListener, container::shutdown);
      container.setContainerListener(listener);

      JobConfig jobConfig = new JobConfig(config);
      ContainerHeartbeatMonitor heartbeatMonitor =
          createContainerHeartbeatMonitor(container,
              new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetConfig.TYPE),
              jobConfig.getApplicationMasterHighAvailabilityEnabled(), jobConfig.getContainerHeartbeatRetryCount(),
              jobConfig.getContainerHeartbeatRetrySleepDurationMs());
      if (heartbeatMonitor != null) {
        heartbeatMonitor.start();
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
        System.exit(1);
      }
    } finally {
      coordinatorStreamStore.close();
    }
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
   * @param isApplicaitonMasterHighAvailabilityEnabled whether AM HA is enabled to fetch new AM url
   * @param retryCount number of times to retry connecting to new AM when heartbeat expires
   * @param sleepDurationForReconnectWithAM sleep duration between retries to connect to new AM when heartbeat expires
   * @return a new {@link ContainerHeartbeatMonitor} instance, or null if could not create one
   */
  private static ContainerHeartbeatMonitor createContainerHeartbeatMonitor(SamzaContainer container,
      MetadataStore coordinatorStreamStore, boolean isApplicaitonMasterHighAvailabilityEnabled, long retryCount,
      long sleepDurationForReconnectWithAM) {
    String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL);
    String executionEnvContainerId = System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID);
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
      }, coordinatorUrl, executionEnvContainerId, coordinatorStreamStore, isApplicaitonMasterHighAvailabilityEnabled,
          retryCount, sleepDurationForReconnectWithAM);
    } else {
      log.warn("Execution environment container id not set. Container heartbeat monitor will not be created");
      return null;
    }
  }
}
