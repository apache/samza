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
package org.apache.samza.clustermanager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import com.google.common.annotations.VisibleForTesting;
import org.apache.samza.SamzaException;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.coordinator.NoProcessorJobCoordinatorListener;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.execution.RemoteJobPlanner;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.DiagnosticsUtil;
import org.apache.samza.util.MetricsReporterLoader;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Util class to launch and run {@link ClusterBasedJobCoordinator}.
 * This util is being used by both high/low and beam API Samza jobs.
 */
public class JobCoordinatorLaunchUtil {
  private static final Logger LOG = LoggerFactory.getLogger(JobCoordinatorLaunchUtil.class);
  private static final String JOB_COORDINATOR_SOURCE_NAME = "JobCoordinator";
  /**
   * There is no processor associated with this job coordinator, so adding a placeholder value.
   */
  private static final String JOB_COORDINATOR_PROCESSOR_ID_PLACEHOLDER = "samza-job-coordinator";

  /**
   * Run {@link ClusterBasedJobCoordinator} with full job config.
   *
   * @param app SamzaApplication to run.
   * @param config full job config.
   */
  @SuppressWarnings("rawtypes")
  public static void run(SamzaApplication app, Config config) {
    // Execute planning
    ApplicationDescriptorImpl<? extends ApplicationDescriptor>
        appDesc = ApplicationDescriptorUtil.getAppDescriptor(app, config);
    RemoteJobPlanner planner = new RemoteJobPlanner(appDesc);
    List<JobConfig> jobConfigs = planner.prepareJobs();

    if (jobConfigs.size() != 1) {
      throw new SamzaException("Only support single remote job is supported.");
    }

    Config fullConfig = jobConfigs.get(0);
    // Create coordinator stream if does not exist before fetching launch config from it.
    CoordinatorStreamUtil.createCoordinatorStream(fullConfig);
    MetricsRegistryMap metrics = new MetricsRegistryMap();
    MetadataStore
        metadataStore = new CoordinatorStreamStore(CoordinatorStreamUtil.buildCoordinatorStreamConfig(fullConfig), metrics);
    // MetadataStore will be closed in ClusterBasedJobCoordinator#onShutDown
    // initialization of MetadataStore can be moved to ClusterBasedJobCoordinator after we clean up
    // ClusterBasedJobCoordinator#createFromMetadataStore
    metadataStore.init();
    // Reads extra launch config from metadata store.
    Config launchConfig = CoordinatorStreamUtil.readLaunchConfigFromCoordinatorStream(fullConfig, metadataStore);
    Config finalConfig = new MapConfig(launchConfig, fullConfig);

    // This needs to be consistent with RemoteApplicationRunner#run where JobRunner#submit to be called instead of JobRunner#run
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(finalConfig, true);
    DiagnosticsUtil.createDiagnosticsStream(finalConfig);

    Optional<String> jobCoordinatorFactoryClassName =
        new JobCoordinatorConfig(config).getOptionalJobCoordinatorFactoryClassName();
    if (jobCoordinatorFactoryClassName.isPresent()) {
      runJobCoordinator(jobCoordinatorFactoryClassName.get(), metrics, metadataStore, finalConfig);
    } else {
      ClusterBasedJobCoordinator jc = new ClusterBasedJobCoordinator(metrics, metadataStore, finalConfig);
      jc.run();
    }
  }

  private static void runJobCoordinator(String jobCoordinatorClassName, MetricsRegistryMap metrics,
      MetadataStore metadataStore, Config finalConfig) {
    JobCoordinatorFactory jobCoordinatorFactory =
        ReflectionUtil.getObj(jobCoordinatorClassName, JobCoordinatorFactory.class);
    JobCoordinator jobCoordinator =
        jobCoordinatorFactory.getJobCoordinator(JOB_COORDINATOR_PROCESSOR_ID_PLACEHOLDER, finalConfig, metrics,
            metadataStore);
    addShutdownHook(jobCoordinator);
    Map<String, MetricsReporter> metricsReporters =
        MetricsReporterLoader.getMetricsReporters(new MetricsConfig(finalConfig), JOB_COORDINATOR_SOURCE_NAME);
    metricsReporters.values()
        .forEach(metricsReporter -> metricsReporter.register(JOB_COORDINATOR_SOURCE_NAME, metrics));
    metricsReporters.values().forEach(MetricsReporter::start);
    CountDownLatch waitForShutdownLatch = new CountDownLatch(1);
    jobCoordinator.setListener(new NoProcessorJobCoordinatorListener(waitForShutdownLatch));
    jobCoordinator.start();
    try {
      waitForShutdownLatch.await();
    } catch (InterruptedException e) {
      String errorMessage = "Error while waiting for coordinator to complete";
      LOG.error(errorMessage, e);
      throw new SamzaException(errorMessage, e);
    } finally {
      metricsReporters.values().forEach(MetricsReporter::stop);
    }
  }

  /**
   * This is a separate method so it can be stubbed in tests, since adding a real shutdown hook will cause the hook to
   * added to the test suite JVM.
   */
  @VisibleForTesting
  static void addShutdownHook(JobCoordinator jobCoordinator) {
    Runtime.getRuntime()
        .addShutdownHook(new Thread(jobCoordinator::stop, "Samza Job Coordinator Shutdown Hook Thread"));
  }

  private JobCoordinatorLaunchUtil() {}
}
