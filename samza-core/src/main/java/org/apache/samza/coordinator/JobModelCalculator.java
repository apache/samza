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
package org.apache.samza.coordinator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.RegExTopicGenerator;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.SSPGrouperProxy;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory;
import org.apache.samza.container.grouper.task.GrouperMetadata;
import org.apache.samza.container.grouper.task.TaskNameGrouperFactory;
import org.apache.samza.container.grouper.task.TaskNameGrouperProxy;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionMatcher;
import org.apache.samza.util.ConfigUtil;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


public class JobModelCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(JobModelCalculator.class);
  public static final JobModelCalculator INSTANCE = new JobModelCalculator();

  private JobModelCalculator() {
  }

  /**
   * Does the following:
   * 1. Fetches metadata of the input streams defined in configuration through {@code streamMetadataCache}.
   * 2. Applies the SSP grouper and task name grouper defined in the configuration to build the {@link JobModel}.
   * @param originalConfig the configuration of the job.
   * @param changeLogPartitionMapping the task to changelog partition mapping of the job.
   * @param streamMetadataCache the cache that holds the partition metadata of the input streams.
   * @param grouperMetadata provides the historical metadata of the application.
   * @return the built {@link JobModel}.
   */
  public JobModel calculateJobModel(Config originalConfig, Map<TaskName, Integer> changeLogPartitionMapping,
      StreamMetadataCache streamMetadataCache, GrouperMetadata grouperMetadata) {
    // refresh config if enabled regex topic rewriter
    Config refreshedConfig = refreshConfigByRegexTopicRewriter(originalConfig);

    TaskConfig taskConfig = new TaskConfig(refreshedConfig);
    // Do grouping to fetch TaskName to SSP mapping
    Set<SystemStreamPartition> allSystemStreamPartitions =
        getMatchedInputStreamPartitions(refreshedConfig, streamMetadataCache);

    // processor list is required by some of the groupers. So, let's pass them as part of the config.
    // Copy the config and add the processor list to the config copy.
    // TODO: It is non-ideal to have config as a medium to transmit the locality information; especially, if the locality information evolves. Evaluate options on using context objects to pass dependent components.
    Map<String, String> configMap = new HashMap<>(refreshedConfig);
    configMap.put(JobConfig.PROCESSOR_LIST, String.join(",", grouperMetadata.getProcessorLocality().keySet()));
    SystemStreamPartitionGrouper grouper = getSystemStreamPartitionGrouper(new MapConfig(configMap));

    JobConfig jobConfig = new JobConfig(refreshedConfig);

    Map<TaskName, Set<SystemStreamPartition>> groups;
    if (jobConfig.isSSPGrouperProxyEnabled()) {
      SSPGrouperProxy sspGrouperProxy = new SSPGrouperProxy(refreshedConfig, grouper);
      groups = sspGrouperProxy.group(allSystemStreamPartitions, grouperMetadata);
    } else {
      LOG.warn(String.format(
          "SSPGrouperProxy is disabled (%s = false). Stateful jobs may produce erroneous results if this is not enabled.",
          JobConfig.SSP_INPUT_EXPANSION_ENABLED));
      groups = grouper.group(allSystemStreamPartitions);
    }
    LOG.info(String.format(
        "SystemStreamPartitionGrouper %s has grouped the SystemStreamPartitions into %d tasks with the following taskNames: %s",
        grouper, groups.size(), groups));

    // If no mappings are present (first time the job is running) we return -1, this will allow 0 to be the first change
    // mapping.
    int maxChangelogPartitionId = changeLogPartitionMapping.values().stream().max(Comparator.naturalOrder()).orElse(-1);
    // Sort the groups prior to assigning the changelog mapping so that the mapping is reproducible and intuitive
    TreeMap<TaskName, Set<SystemStreamPartition>> sortedGroups = new TreeMap<>(groups);
    Set<TaskModel> taskModels = new HashSet<>();
    for (Map.Entry<TaskName, Set<SystemStreamPartition>> group : sortedGroups.entrySet()) {
      TaskName taskName = group.getKey();
      Set<SystemStreamPartition> systemStreamPartitions = group.getValue();
      Optional<Integer> changelogPartitionId = Optional.ofNullable(changeLogPartitionMapping.get(taskName));
      Partition changelogPartition;
      if (changelogPartitionId.isPresent()) {
        changelogPartition = new Partition(changelogPartitionId.get());
      } else {
        // If we've never seen this TaskName before, then assign it a new changelog partition.
        maxChangelogPartitionId++;
        LOG.info(
            String.format("New task %s is being assigned changelog partition %s.", taskName, maxChangelogPartitionId));
        changelogPartition = new Partition(maxChangelogPartitionId);
      }
      taskModels.add(new TaskModel(taskName, systemStreamPartitions, changelogPartition));
    }

    // Here is where we should put in a pluggable option for the SSPTaskNameGrouper for locality, load-balancing, etc.
    TaskNameGrouperFactory containerGrouperFactory =
        ReflectionUtil.getObj(taskConfig.getTaskNameGrouperFactory(), TaskNameGrouperFactory.class);
    boolean standbyTasksEnabled = jobConfig.getStandbyTasksEnabled();
    int standbyTaskReplicationFactor = jobConfig.getStandbyTaskReplicationFactor();
    TaskNameGrouperProxy taskNameGrouperProxy =
        new TaskNameGrouperProxy(containerGrouperFactory.build(refreshedConfig), standbyTasksEnabled,
            standbyTaskReplicationFactor);
    Set<ContainerModel> containerModels;
    boolean isHostAffinityEnabled = new ClusterManagerConfig(refreshedConfig).getHostAffinityEnabled();
    if (isHostAffinityEnabled) {
      containerModels = taskNameGrouperProxy.group(taskModels, grouperMetadata);
    } else {
      containerModels =
          taskNameGrouperProxy.group(taskModels, new ArrayList<>(grouperMetadata.getProcessorLocality().keySet()));
    }

    Map<String, ContainerModel> containerMap =
        containerModels.stream().collect(Collectors.toMap(ContainerModel::getId, Function.identity()));
    return new JobModel(refreshedConfig, containerMap);
  }

  /**
   * Refresh Kafka topic list used as input streams if enabled {@link org.apache.samza.config.RegExTopicGenerator}
   * @param originalConfig Samza job config
   * @return refreshed config
   */
  private static Config refreshConfigByRegexTopicRewriter(Config originalConfig) {
    JobConfig jobConfig = new JobConfig(originalConfig);
    Optional<String> configRewriters = jobConfig.getConfigRewriters();
    Config resultConfig = originalConfig;
    if (configRewriters.isPresent()) {
      for (String rewriterName : configRewriters.get().split(",")) {
        String rewriterClass = jobConfig.getConfigRewriterClass(rewriterName)
            .orElseThrow(() -> new ConfigException(
                String.format("Unable to find class config for config rewriter %s.", rewriterName)));
        if (rewriterClass.equalsIgnoreCase(RegExTopicGenerator.class.getName())) {
          resultConfig = ConfigUtil.applyRewriter(resultConfig, rewriterName);
        }
      }
    }
    return resultConfig;
  }

  /**
   * Builds the input {@see SystemStreamPartition} based upon the {@param config} defined by the user.
   * @param config configuration to fetch the metadata of the input streams.
   * @param streamMetadataCache required to query the partition metadata of the input streams.
   * @return the input SystemStreamPartitions of the job.
   */
  private static Set<SystemStreamPartition> getMatchedInputStreamPartitions(Config config,
      StreamMetadataCache streamMetadataCache) {
    Set<SystemStreamPartition> allSystemStreamPartitions = getInputStreamPartitions(config, streamMetadataCache);
    JobConfig jobConfig = new JobConfig(config);
    Optional<String> sspMatcherClassName = jobConfig.getSSPMatcherClass();
    if (sspMatcherClassName.isPresent()) {
      String sspMatcherConfigJobFactoryRegex = jobConfig.getSSPMatcherConfigJobFactoryRegex();
      Optional<String> streamJobFactoryClass = jobConfig.getStreamJobFactoryClass();
      if (streamJobFactoryClass.isPresent() && Pattern.matches(sspMatcherConfigJobFactoryRegex,
          streamJobFactoryClass.get())) {
        LOG.info(String.format("before match: allSystemStreamPartitions.size = %s", allSystemStreamPartitions.size()));
        SystemStreamPartitionMatcher sspMatcher =
            ReflectionUtil.getObj(sspMatcherClassName.get(), SystemStreamPartitionMatcher.class);
        Set<SystemStreamPartition> matchedPartitions = sspMatcher.filter(allSystemStreamPartitions, config);
        // Usually a small set hence ok to log at info level
        LOG.info(String.format("after match: matchedPartitions = %s", matchedPartitions));
        return matchedPartitions;
      }
    }
    return allSystemStreamPartitions;
  }

  /**
   * Finds the {@see SystemStreamPartitionGrouperFactory} from the {@param config}. Instantiates the
   * {@see SystemStreamPartitionGrouper} object through the factory.
   * @param config the configuration of the samza job.
   * @return the instantiated {@see SystemStreamPartitionGrouper}.
   */
  private static SystemStreamPartitionGrouper getSystemStreamPartitionGrouper(Config config) {
    String factoryString = new JobConfig(config).getSystemStreamPartitionGrouperFactory();
    SystemStreamPartitionGrouperFactory factory =
        ReflectionUtil.getObj(factoryString, SystemStreamPartitionGrouperFactory.class);
    return factory.getSystemStreamPartitionGrouper(config);
  }

  /**
   * Computes the input system stream partitions of a samza job using the provided {@param config}
   * and {@param streamMetadataCache}.
   * @param config the configuration of the job.
   * @param streamMetadataCache to query the partition metadata of the input streams.
   * @return the input {@see SystemStreamPartition} of the samza job.
   */
  private static Set<SystemStreamPartition> getInputStreamPartitions(Config config,
      StreamMetadataCache streamMetadataCache) {
    TaskConfig taskConfig = new TaskConfig(config);
    // Get the set of partitions for each SystemStream from the stream metadata
    Map<SystemStream, SystemStreamMetadata> allMetadata = JavaConverters.mapAsJavaMapConverter(
        streamMetadataCache.getStreamMetadata(
            JavaConverters.asScalaSetConverter(taskConfig.getInputStreams()).asScala().toSet(), true)).asJava();
    Set<SystemStreamPartition> inputStreamPartitions = new HashSet<>();
    allMetadata.forEach((systemStream, systemStreamMetadata) -> systemStreamMetadata.getSystemStreamPartitionMetadata()
        .keySet()
        .forEach(partition -> inputStreamPartitions.add(new SystemStreamPartition(systemStream, partition))));
    return inputStreamPartitions;
  }
}
