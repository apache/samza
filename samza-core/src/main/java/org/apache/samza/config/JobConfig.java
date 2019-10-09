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
package org.apache.samza.config;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.samza.SamzaException;
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory;
import org.apache.samza.container.grouper.stream.HashSystemStreamPartitionMapperFactory;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamMetadataStoreFactory;
import org.apache.samza.runtime.DefaultLocationIdProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobConfig extends MapConfig {
  private static final Logger LOG = LoggerFactory.getLogger(JobConfig.class);

  public static final String STREAM_JOB_FACTORY_CLASS = "job.factory.class";

  /**
   * job.config.rewriters is a CSV list of config rewriter names. Each name is determined
   * by the %s value in job.config.rewriter.%s.class. For example, if you define
   * job.config.rewriter.some-regex.class=org.apache.samza.config.RegExTopicGenerator,
   * then the rewriter config would be set to job.config.rewriters = some-regex.
   */
  public static final String CONFIG_REWRITERS = "job.config.rewriters";
  public static final String CONFIG_REWRITER_CLASS = "job.config.rewriter.%s.class";

  public static final String JOB_NAME = "job.name";
  public static final String JOB_ID = "job.id";
  static final String DEFAULT_JOB_ID = "1";

  public static final String SAMZA_FWK_PATH = "samza.fwk.path";
  public static final String SAMZA_FWK_VERSION = "samza.fwk.version";

  public static final String JOB_COORDINATOR_SYSTEM = "job.coordinator.system";
  public static final String JOB_DEFAULT_SYSTEM = "job.default.system";

  public static final String JOB_JMX_ENABLED = "job.jmx.enabled";
  public static final String JOB_CONTAINER_COUNT = "job.container.count";
  static final int DEFAULT_JOB_CONTAINER_COUNT = 1;
  public static final String JOB_CONTAINER_THREAD_POOL_SIZE = "job.container.thread.pool.size";
  public static final String JOB_INTERMEDIATE_STREAM_PARTITIONS = "job.intermediate.stream.partitions";

  public static final String JOB_DEBOUNCE_TIME_MS = "job.debounce.time.ms";
  static final int DEFAULT_DEBOUNCE_TIME_MS = 20000;

  public static final String SSP_INPUT_EXPANSION_ENABLED = "job.systemstreampartition.input.expansion.enabled";
  public static final boolean DEFAULT_INPUT_EXPANSION_ENABLED = true;

  public static final String SSP_GROUPER_FACTORY = "job.systemstreampartition.grouper.factory";
  public static final String SSP_MATCHER_CLASS = "job.systemstreampartition.matcher.class";
  public static final String SSP_MATCHER_CLASS_REGEX = "org.apache.samza.system.RegexSystemStreamPartitionMatcher";
  public static final String SSP_MATCHER_CLASS_RANGE = "org.apache.samza.system.RangeSystemStreamPartitionMatcher";
  public static final String SSP_MATCHER_CONFIG_REGEX = "job.systemstreampartition.matcher.config.regex";
  public static final String SSP_MATCHER_CONFIG_RANGES = "job.systemstreampartition.matcher.config.ranges";
  public static final String SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX =
      "job.systemstreampartition.matcher.config.job.factory.regex";
  static final String DEFAULT_SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX =
      "org\\.apache\\.samza\\.job\\.local(.*ProcessJobFactory|.*ThreadJobFactory)";
  public static final String SYSTEM_STREAM_PARTITION_MAPPER_FACTORY = "job.system.stream.partition.mapper.factory";

  // number of partitions in the checkpoint stream should be 1. But sometimes,
  // if a stream was created(automatically) with the wrong number of partitions(default number of partitions
  // for new streams), there is no easy fix for the user (topic deletion or reducing of number of partitions
  // is not yet supported, and auto-creation of the topics cannot be always easily tuned off).
  // So we add a setting that allows for the job to continue even though number of partitions is not 1.
  public static final String JOB_FAIL_CHECKPOINT_VALIDATION = "job.checkpoint.validation.enabled";
  public static final String MONITOR_PARTITION_CHANGE = "job.coordinator.monitor-partition-change";
  public static final String MONITOR_PARTITION_CHANGE_FREQUENCY_MS =
      "job.coordinator.monitor-partition-change.frequency.ms";
  static final int DEFAULT_MONITOR_PARTITION_CHANGE_FREQUENCY_MS = 300000;

  public static final String MONITOR_INPUT_REGEX_FREQUENCY_MS = "job.coordinator.monitor-input-regex.frequency.ms";
  static final int DEFAULT_MONITOR_INPUT_REGEX_FREQUENCY_MS = 300000;

  public static final String REGEX_RESOLVED_STREAMS = "job.config.rewriter.%s.regex";
  public static final String REGEX_RESOLVED_SYSTEM = "job.config.rewriter.%s.system";
  public static final String REGEX_INHERITED_CONFIG = "job.config.rewriter.%s.config";

  public static final String JOB_SECURITY_MANAGER_FACTORY = "job.security.manager.factory";

  public static final String METADATA_STORE_FACTORY = "metadata.store.factory";
  public static final String STARTPOINT_METADATA_STORE_FACTORY = "startpoint.metadata.store.factory";

  public static final String LOCATION_ID_PROVIDER_FACTORY = "locationid.provider.factory";

  // Processor Config Constants
  public static final String PROCESSOR_ID = "processor.id";
  public static final String PROCESSOR_LIST = "processor.list";

  // Represents the store path for non-changelog stores.
  public static final String JOB_NON_LOGGED_STORE_BASE_DIR = "job.non-logged.store.base.dir";

  // Represents the store path for stores with changelog enabled. Typically the stores are not cleaned up
  // across application restarts
  public static final String JOB_LOGGED_STORE_BASE_DIR = "job.logged.store.base.dir";

  // Enables diagnostic appender for logging exception events
  public static final String JOB_DIAGNOSTICS_ENABLED = "job.diagnostics.enabled";

  // Enables standby tasks
  public static final String STANDBY_TASKS_REPLICATION_FACTOR = "job.standbytasks.replication.factor";
  static final int DEFAULT_STANDBY_TASKS_REPLICATION_FACTOR = 1;

  // Naming format and directory for container.metadata file
  public static final String CONTAINER_METADATA_FILENAME_FORMAT = "%s.metadata"; // Filename: <containerID>.metadata
  public static final String CONTAINER_METADATA_DIRECTORY_SYS_PROPERTY = "samza.log.dir";

  public static final String COORDINATOR_STREAM_FACTORY = "job.coordinatorstream.config.factory";
  public static final String DEFAULT_COORDINATOR_STREAM_CONFIG_FACTORY = "org.apache.samza.util.DefaultCoordinatorStreamConfigFactory";

  public JobConfig(Config config) {
    super(config);
  }

  public Optional<String> getName() {
    return Optional.ofNullable(get(JOB_NAME));
  }

  public String getCoordinatorSystemName() {
    return Optional.ofNullable(getCoordinatorSystemNameOrNull())
        .orElseThrow(() -> new ConfigException(
            "Missing job.coordinator.system configuration. Cannot proceed with job execution."));
  }

  /**
   * Gets the System to use for reading/writing the coordinator stream. Uses the following precedence.
   *
   * 1. If job.coordinator.system is defined, that value is used.
   * 2. If job.default.system is defined, that value is used.
   * 3. None
   */
  public String getCoordinatorSystemNameOrNull() {
    return Optional.ofNullable(get(JOB_COORDINATOR_SYSTEM)).orElseGet(() -> getDefaultSystem().orElse(null));
  }

  public Optional<String> getDefaultSystem() {
    return Optional.ofNullable(get(JOB_DEFAULT_SYSTEM));
  }

  public int getContainerCount() {
    Optional<String> jobContainerCountValue = Optional.ofNullable(get(JOB_CONTAINER_COUNT));
    if (jobContainerCountValue.isPresent()) {
      return Integer.parseInt(jobContainerCountValue.get());
    } else {
      // To maintain backwards compatibility, honor yarn.container.count for now.
      // TODO get rid of this in a future release.
      Optional<String> yarnContainerCountValue = Optional.ofNullable(get("yarn.container.count"));
      if (yarnContainerCountValue.isPresent()) {
        LOG.warn("Configuration 'yarn.container.count' is deprecated. Please use {}.", JOB_CONTAINER_COUNT);
        return Integer.parseInt(yarnContainerCountValue.get());
      } else {
        return DEFAULT_JOB_CONTAINER_COUNT;
      }
    }
  }

  public int getMonitorRegexFrequency() {
    return getInt(MONITOR_INPUT_REGEX_FREQUENCY_MS, DEFAULT_MONITOR_INPUT_REGEX_FREQUENCY_MS);
  }

  public boolean getMonitorRegexDisabled() {
    return getMonitorRegexFrequency() <= 0;
  }

  public int getMonitorPartitionChangeFrequency() {
    return getInt(MONITOR_PARTITION_CHANGE_FREQUENCY_MS, DEFAULT_MONITOR_PARTITION_CHANGE_FREQUENCY_MS);
  }

  /**
   * Compile a map of each input-system to its corresponding input-monitor-regex patterns.
   */
  public Map<String, Pattern> getMonitorRegexPatternMap(String rewritersList) {
    Map<String, Pattern> inputRegexesToMonitor = new HashMap<>();
    Stream.of(rewritersList.split(",")).forEach(rewriterName -> {
        Optional<String> rewriterSystem = getRegexResolvedSystem(rewriterName);
        Optional<String> rewriterRegex = getRegexResolvedStreams(rewriterName);
        if (rewriterSystem.isPresent() && rewriterRegex.isPresent()) {
          Pattern newPatternForSystem;
          Pattern existingPatternForSystem = inputRegexesToMonitor.get(rewriterSystem.get());
          if (existingPatternForSystem == null) {
            newPatternForSystem = Pattern.compile(rewriterRegex.get());
          } else {
            newPatternForSystem =
                Pattern.compile(String.join("|", existingPatternForSystem.pattern(), rewriterRegex.get()));
          }
          inputRegexesToMonitor.put(rewriterSystem.get(), newPatternForSystem);
        }
      });
    return inputRegexesToMonitor;
  }

  public Optional<String> getRegexResolvedStreams(String rewriterName) {
    return Optional.ofNullable(get(String.format(REGEX_RESOLVED_STREAMS, rewriterName)));
  }

  public Optional<String> getRegexResolvedSystem(String rewriterName) {
    return Optional.ofNullable(get(String.format(REGEX_RESOLVED_SYSTEM, rewriterName)));
  }

  public Config getRegexResolvedInheritedConfig(String rewriterName) {
    return subset(String.format(REGEX_INHERITED_CONFIG, rewriterName) + ".", true);
  }

  public Optional<String> getStreamJobFactoryClass() {
    return Optional.ofNullable(get(STREAM_JOB_FACTORY_CLASS));
  }

  public String getJobId() {
    return Optional.ofNullable(get(JOB_ID)).orElse(DEFAULT_JOB_ID);
  }

  public boolean failOnCheckpointValidation() {
    return getBoolean(JOB_FAIL_CHECKPOINT_VALIDATION, true);
  }

  public Optional<String> getConfigRewriters() {
    return Optional.ofNullable(get(CONFIG_REWRITERS));
  }

  public Optional<String> getConfigRewriterClass(String name) {
    return Optional.ofNullable(get(String.format(CONFIG_REWRITER_CLASS, name)));
  }

  public boolean isSSPGrouperProxyEnabled() {
    return getBoolean(SSP_INPUT_EXPANSION_ENABLED, DEFAULT_INPUT_EXPANSION_ENABLED);
  }

  public String getSystemStreamPartitionGrouperFactory() {
    return Optional.ofNullable(get(SSP_GROUPER_FACTORY)).orElseGet(GroupByPartitionFactory.class::getName);
  }

  public String getLocationIdProviderFactory() {
    return Optional.ofNullable(get(LOCATION_ID_PROVIDER_FACTORY))
        .orElseGet(DefaultLocationIdProviderFactory.class::getName);
  }

  public Optional<String> getSecurityManagerFactory() {
    return Optional.ofNullable(get(JOB_SECURITY_MANAGER_FACTORY));
  }

  public Optional<String> getSSPMatcherClass() {
    return Optional.ofNullable(get(SSP_MATCHER_CLASS));
  }

  public String getSSPMatcherConfigRegex() {
    return Optional.ofNullable(get(SSP_MATCHER_CONFIG_REGEX))
        .orElseThrow(
            () -> new SamzaException(String.format("Missing required configuration: '%s'", SSP_MATCHER_CONFIG_REGEX)));
  }

  public String getSSPMatcherConfigRanges() {
    return Optional.ofNullable(get(SSP_MATCHER_CONFIG_RANGES))
        .orElseThrow(
            () -> new SamzaException(String.format("Missing required configuration: '%s'", SSP_MATCHER_CONFIG_RANGES)));
  }

  public String getSSPMatcherConfigJobFactoryRegex() {
    return get(SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX, DEFAULT_SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX);
  }

  public int getThreadPoolSize() {
    return getInt(JOB_CONTAINER_THREAD_POOL_SIZE, 0);
  }

  public int getDebounceTimeMs() {
    return getInt(JOB_DEBOUNCE_TIME_MS, DEFAULT_DEBOUNCE_TIME_MS);
  }

  public Optional<String> getNonLoggedStorePath() {
    return Optional.ofNullable(get(JOB_NON_LOGGED_STORE_BASE_DIR));
  }

  public Optional<String> getLoggedStorePath() {
    return Optional.ofNullable(get(JOB_LOGGED_STORE_BASE_DIR));
  }

  public String getMetadataStoreFactory() {
    return get(METADATA_STORE_FACTORY, CoordinatorStreamMetadataStoreFactory.class.getName());
  }

  public boolean getDiagnosticsEnabled() {
    return getBoolean(JOB_DIAGNOSTICS_ENABLED, false);
  }

  public boolean getJMXEnabled() {
    return getBoolean(JOB_JMX_ENABLED, true);
  }

  public String getSystemStreamPartitionMapperFactoryName() {
    return get(SYSTEM_STREAM_PARTITION_MAPPER_FACTORY, HashSystemStreamPartitionMapperFactory.class.getName());
  }

  public int getStandbyTaskReplicationFactor() {
    return getInt(STANDBY_TASKS_REPLICATION_FACTOR, DEFAULT_STANDBY_TASKS_REPLICATION_FACTOR);
  }

  public boolean getStandbyTasksEnabled() {
    return getStandbyTaskReplicationFactor() > 1;
  }

  /**
   * Reads the config to figure out if split deployment is enabled and fwk directory is setup
   * @return fwk + "/" + version, or empty string if fwk path is not specified
   */
  public static String getFwkPath(Config config) {
    String fwkPath = config.get(SAMZA_FWK_PATH, "");
    String fwkVersion = config.get(SAMZA_FWK_VERSION);
    if (fwkVersion == null || fwkVersion.isEmpty()) {
      fwkVersion = "STABLE";
    }
    if (!fwkPath.isEmpty()) {
      fwkPath = fwkPath + File.separator + fwkVersion;
    }
    return fwkPath;
  }

  /**
   * The metadata file is written in a {@code exec-env-container-id}.metadata file in the log-dir of the container.
   * Here the {@code exec-env-container-id} refers to the ID assigned by the cluster manager (e.g., YARN) to the container,
   * which uniquely identifies a container's lifecycle.
   */
  public static Optional<File> getMetadataFile(String execEnvContainerId) {
    String dir = System.getProperty(CONTAINER_METADATA_DIRECTORY_SYS_PROPERTY);
    if (dir == null || execEnvContainerId == null) {
      return Optional.empty();
    } else {
      return Optional.of(
          new File(dir, String.format(CONTAINER_METADATA_FILENAME_FORMAT, execEnvContainerId)));
    }
  }

  /**
   * Get coordinatorStreamFactory according to the configs
   * @return the name of coordinatorStreamFactory
   */
  public String getCoordinatorStreamFactory() {
    return get(COORDINATOR_STREAM_FACTORY, DEFAULT_COORDINATOR_STREAM_CONFIG_FACTORY);
  }

}