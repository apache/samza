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
import java.util.Optional;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.ContainerHeartbeatClient;
import org.apache.samza.container.ContainerHeartbeatMonitor;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.container.SamzaContainerListener;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.context.JobContextImpl;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.diagnostics.DiagnosticsManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.reporter.MetricsSnapshotReporter;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.ScalaJavaUtil;
import org.apache.samza.util.StreamUtil;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.runtime.AbstractFunction0;


public class ContainerLaunchUtil {
  private static final Logger log = LoggerFactory.getLogger(ContainerLaunchUtil.class);

  private static volatile Throwable containerRunnerException = null;

  /**
   * This method launches a Samza container in a managed cluster, e.g. Yarn.
   *
   * NOTE: this util method is also invoked by Beam SamzaRunner.
   * Any change here needs to take Beam into account.
   */
  public static void run(
      ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
      String jobName, String jobId, String containerId, Optional<String> execEnvContainerId,
      JobModel jobModel) {

    Config config = jobModel.getConfig();
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
      Optional<StartpointManager> startpointManager = Optional.empty();
      if (new JobConfig(config).getStartpointMetadataStoreFactory() != null) {
        startpointManager = Optional.of(new StartpointManager(new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, StartpointManager.NAMESPACE)));
      }

      Map<String, MetricsReporter> customReporters = loadMetricsReporters(appDesc, containerId, config);

      // Creating diagnostics manager and reporter, and wiring it respectively
      Optional<Tuple2<DiagnosticsManager, MetricsSnapshotReporter>> diagnosticsManagerReporterPair = buildDiagnosticsManager(jobName, jobId, containerId, execEnvContainerId, config);
      Option<DiagnosticsManager> diagnosticsManager = Option.empty();
      if (diagnosticsManagerReporterPair.isPresent()) {
        diagnosticsManager = Option.apply(diagnosticsManagerReporterPair.get()._1());
        customReporters.put(MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS(), diagnosticsManagerReporterPair.get()._2());
      }

      SamzaContainer container = SamzaContainer$.MODULE$.apply(
          containerId, jobModel,
          ScalaJavaUtil.toScalaMap(customReporters),
          taskFactory,
          JobContextImpl.fromConfigWithDefaults(config),
          Option.apply(appDesc.getApplicationContainerContextFactory().orElse(null)),
          Option.apply(appDesc.getApplicationTaskContextFactory().orElse(null)),
          Option.apply(externalContextOptional.orElse(null)), localityManager, startpointManager.orElse(null), diagnosticsManager);

      ProcessorLifecycleListener listener = appDesc.getProcessorLifecycleListenerFactory()
          .createInstance(new ProcessorContext() { }, config);

      container.setContainerListener(
          new SamzaContainerListener() {
            @Override
            public void beforeStart() {
              log.info("Before starting the container.");
              listener.beforeStart();
            }

            @Override
            public void afterStart() {
              log.info("Container Started");
              listener.afterStart();
            }

            @Override
            public void afterStop() {
              log.info("Container Stopped");
              listener.afterStop();
            }

            @Override
            public void afterFailure(Throwable t) {
              log.info("Container Failed");
              containerRunnerException = t;
              listener.afterFailure(t);
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
    } finally {
      coordinatorStreamStore.close();
    }
  }

  /**
   * Create a pair of DiagnosticsManager and Reporter for the given jobName, jobId, containerId, and execEnvContainerId,
   * if diagnostics is enabled.
   * execEnvContainerId is the ID assigned to the container by the cluster manager (e.g., YARN).
   */
  public static Optional<Tuple2<DiagnosticsManager, MetricsSnapshotReporter>> buildDiagnosticsManager(String jobName, String jobId,
      String containerId, Optional<String> execEnvContainerId, Config config) {

    Optional<Tuple2<DiagnosticsManager, MetricsSnapshotReporter>> diagnosticsManagerReporterPair = Optional.empty();

    if (new JobConfig(config).getDiagnosticsEnabled()) {
      String diagnosticsReporterName = MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS();

      Integer publishInterval = Integer.parseInt(new MetricsConfig(config).getMetricsSnapshotReporterInterval(diagnosticsReporterName));
      String taskClassVersion = Util.getTaskClassVersion(config);
      String samzaVersion = Util.getSamzaVersion();
      String hostName = Util.getLocalHost().getHostName();
      Option<String> blacklist = new MetricsConfig(config).getMetricsSnapshotReporterBlacklist(diagnosticsReporterName);
      Option<String> diagnosticsReporterStreamName = new MetricsConfig(config).getMetricsSnapshotReporterStream(diagnosticsReporterName);

      if (diagnosticsReporterStreamName.isEmpty()) {
        throw new ConfigException("Missing required config: " + String.format(MetricsConfig.METRICS_SNAPSHOT_REPORTER_STREAM(), diagnosticsReporterName));
      }

      SystemStream diagnosticsSystemStream = StreamUtil.getSystemStreamFromNames(diagnosticsReporterStreamName.get());

      Optional<String> diagnosticsSystemFactoryName = new SystemConfig(config).getSystemFactory(diagnosticsSystemStream.getSystem());
      if (!diagnosticsSystemFactoryName.isPresent()) {
        throw new SamzaException("A stream uses system " + diagnosticsSystemStream.getSystem() + ", which is missing from the configuration.");
      }

      SystemFactory systemFactory = Util.getObj(diagnosticsSystemFactoryName.get(), SystemFactory.class);
      SystemProducer systemProducer = systemFactory.getProducer(diagnosticsSystemStream.getSystem(), config, new MetricsRegistryMap());
      DiagnosticsManager diagnosticsManager = new DiagnosticsManager(jobName, jobId, containerId, execEnvContainerId.orElse(""), taskClassVersion,
              samzaVersion, hostName, diagnosticsSystemStream, systemProducer, Duration.ofMillis(new TaskConfigJava(config).getShutdownMs()));

      MetricsSnapshotReporter diagnosticsReporter =
          new MetricsSnapshotReporter(systemProducer, diagnosticsSystemStream, publishInterval, jobName, jobId,
              "samza-container-" + containerId, taskClassVersion, samzaVersion, hostName, new MetricsSnapshotSerdeV2(),
              blacklist, new AbstractFunction0<Object>() {
                @Override
                 public Object apply() {
                    return System.currentTimeMillis();
                  }
              });

      diagnosticsManagerReporterPair = Optional.of(new Tuple2<>(diagnosticsManager, diagnosticsReporter));
    }

    return diagnosticsManagerReporterPair;
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
