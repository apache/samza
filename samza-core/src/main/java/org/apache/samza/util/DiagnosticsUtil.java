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
package org.apache.samza.util;

import java.io.File;
import java.time.Duration;
import java.util.Optional;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.diagnostics.DiagnosticsManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.runtime.LocalContainerRunner;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DiagnosticsUtil {
  private static final Logger log = LoggerFactory.getLogger(DiagnosticsUtil.class);
  private static final String DIAGNOSTICS_STREAM_ID = "samza-diagnostics-stream-id";

  // Write a file in the samza.log.dir named {exec-env-container-id}.metadata that contains
  // metadata about the container such as containerId, jobName, jobId, hostname, timestamp, version info, and others.
  // The file contents are serialized using {@link JsonSerde}.
  public static void writeMetadataFile(String jobName, String jobId, String containerId,
      Optional<String> execEnvContainerId, Config config) {

    Optional<File> metadataFile = JobConfig.getMetadataFile(execEnvContainerId.orElse(null));

    if (metadataFile.isPresent()) {
      MetricsHeader metricsHeader =
          new MetricsHeader(jobName, jobId, "samza-container-" + containerId, execEnvContainerId.orElse(""),
              LocalContainerRunner.class.getName(), Util.getTaskClassVersion(config), Util.getSamzaVersion(),
              Util.getLocalHost().getHostName(), System.currentTimeMillis(), System.currentTimeMillis());

      class MetadataFileContents {
        public final String version;
        public final String metricsSnapshot;

        public MetadataFileContents(String version, String metricsSnapshot) {
          this.version = version;
          this.metricsSnapshot = metricsSnapshot;
        }
      }

      MetricsSnapshot metricsSnapshot = new MetricsSnapshot(metricsHeader, new Metrics());
      MetadataFileContents metadataFileContents =
          new MetadataFileContents("1", new String(new MetricsSnapshotSerdeV2().toBytes(metricsSnapshot)));
      new FileUtil().writeToTextFile(metadataFile.get(), new String(new JsonSerde<>().toBytes(metadataFileContents)), false);
    } else {
      log.info("Skipping writing metadata file.");
    }
  }

  /**
   * Create a pair of DiagnosticsManager and Reporter for the given jobName, jobId, containerId, and execEnvContainerId,
   * if diagnostics is enabled.
   * execEnvContainerId is the ID assigned to the container by the cluster manager (e.g., YARN).
   */
  public static Optional<DiagnosticsManager> buildDiagnosticsManager(String jobName,
      String jobId, JobModel jobModel, String containerId, Optional<String> execEnvContainerId, Config config) {

    JobConfig jobConfig = new JobConfig(config);
    MetricsConfig metricsConfig = new MetricsConfig(config);
    Optional<DiagnosticsManager> diagnosticsManagerOptional = Optional.empty();

    if (jobConfig.getDiagnosticsEnabled()) {
      ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
      int containerMemoryMb = clusterManagerConfig.getContainerMemoryMb();
      int containerNumCores = clusterManagerConfig.getNumCores();
      long maxHeapSizeBytes = Runtime.getRuntime().maxMemory();
      int containerThreadPoolSize = jobConfig.getThreadPoolSize();
      String taskClassVersion = Util.getTaskClassVersion(config);
      String samzaVersion = Util.getSamzaVersion();
      String hostName = Util.getLocalHost().getHostName();
      Optional<String> diagnosticsReporterStreamName =
          metricsConfig.getMetricsSnapshotReporterStream(MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS);

      if (!diagnosticsReporterStreamName.isPresent()) {
        throw new ConfigException(
            "Missing required config: " + String.format(MetricsConfig.METRICS_SNAPSHOT_REPORTER_STREAM,
                MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS));
      }
      SystemStream diagnosticsSystemStream = StreamUtil.getSystemStreamFromNames(diagnosticsReporterStreamName.get());

      // Create a SystemProducer for DiagnosticsManager. This producer is used by the DiagnosticsManager
      // to write to the same stream as the MetricsSnapshotReporter called `diagnosticsreporter`.
      Optional<String> diagnosticsSystemFactoryName =
          new SystemConfig(config).getSystemFactory(diagnosticsSystemStream.getSystem());
      if (!diagnosticsSystemFactoryName.isPresent()) {
        throw new SamzaException("Missing factory in config for system " + diagnosticsSystemStream.getSystem());
      }
      SystemFactory systemFactory = ReflectionUtil.getObj(diagnosticsSystemFactoryName.get(), SystemFactory.class);
      SystemProducer systemProducer =
          systemFactory.getProducer(diagnosticsSystemStream.getSystem(), config, new MetricsRegistryMap(),
              DiagnosticsUtil.class.getSimpleName());

      DiagnosticsManager diagnosticsManager =
          new DiagnosticsManager(jobName, jobId, jobModel.getContainers(), containerMemoryMb, containerNumCores,
              new StorageConfig(config).getNumPersistentStores(), maxHeapSizeBytes, containerThreadPoolSize,
              containerId, execEnvContainerId.orElse(""), taskClassVersion, samzaVersion, hostName,
              diagnosticsSystemStream, systemProducer,
              Duration.ofMillis(new TaskConfig(config).getShutdownMs()), jobConfig.getAutosizingEnabled(), config);

      diagnosticsManagerOptional = Optional.of(diagnosticsManager);
    }

    return diagnosticsManagerOptional;
  }

  public static void createDiagnosticsStream(Config config) {
    if (!new JobConfig(config).getDiagnosticsEnabled()) {
      return;
    }
    // if diagnostics is enabled, create diagnostics stream if it doesnt exist

    String diagnosticsSystemStreamName = new MetricsConfig(config)
        .getMetricsSnapshotReporterStream(MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS)
        .orElseThrow(() -> new ConfigException("Missing required config: " +
            String.format(MetricsConfig.METRICS_SNAPSHOT_REPORTER_STREAM,
                MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS)));

    SystemStream diagnosticsSystemStream = StreamUtil.getSystemStreamFromNames(diagnosticsSystemStreamName);
    SystemConfig systemConfig = new SystemConfig(config);
    SystemAdmin diagnosticsSysAdmin = systemConfig.getSystemFactories().get(diagnosticsSystemStream.getSystem())
        .getAdmin(diagnosticsSystemStream.getSystem(), config, DiagnosticsUtil.class.getSimpleName());
    StreamSpec diagnosticsStreamSpec = new StreamSpec(DIAGNOSTICS_STREAM_ID, diagnosticsSystemStream.getStream(),
        diagnosticsSystemStream.getSystem(), new StreamConfig(config).getStreamProperties(DIAGNOSTICS_STREAM_ID));

    log.info("Creating diagnostics stream {}", diagnosticsSystemStream.getStream());
    diagnosticsSysAdmin.start();

    if (diagnosticsSysAdmin.createStream(diagnosticsStreamSpec)) {
      log.info("Created diagnostics stream {}", diagnosticsSystemStream.getStream());
    } else {
      log.info("Diagnostics stream {} already exists", diagnosticsSystemStream.getStream());
    }

    diagnosticsSysAdmin.stop();
  }
}
