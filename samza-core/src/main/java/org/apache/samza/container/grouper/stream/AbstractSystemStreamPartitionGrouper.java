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
package org.apache.samza.container.grouper.stream;

import com.google.common.base.Preconditions;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * An abstract implementation of {@link SystemStreamPartitionGrouper} that provides a stream expansion
 * aware task to partition assignments.
 */
@InterfaceStability.Evolving
public abstract class AbstractSystemStreamPartitionGrouper implements SystemStreamPartitionGrouper {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSystemStreamPartitionGrouper.class);

  private final StreamPartitionMapper streamPartitionMapper;
  private final Set<SystemStreamPartition> broadcastSystemStreamPartitions;

  public AbstractSystemStreamPartitionGrouper(Config config) {
    Preconditions.checkNotNull(config);
    this.broadcastSystemStreamPartitions = new TaskConfigJava(config).getBroadcastSystemStreamPartitions();
    this.streamPartitionMapper = getPartitionExpansionAlgorithm(config);
  }

  /**
   * {@inheritDoc}
   *
   * 1. Invokes the sub-class {@link org.apache.samza.container.grouper.task.TaskNameGrouper#group(Set)} method to generate the task to partition assignments.
   * 2. Uses the {@link StreamPartitionMapper} to compute the previous {@link SystemStreamPartition} for every partition of the expanded streams.
   * 3. Uses the previous, current task to partition assignments and result of {@link StreamPartitionMapper} to redistribute the expanded {@link SystemStreamPartition}'s
   * to correct tasks after the stream expansion.
   */
  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> ssps, GrouperContext grouperContext) {
    Map<TaskName, Set<SystemStreamPartition>> currentTaskAssignments = group(ssps);

    if (grouperContext.getPreviousTaskToSSPAssignment().isEmpty()) {
      LOGGER.info("Previous task to partition assignment does not exist. Using the result from the group method.");
      return currentTaskAssignments;
    }

    Map<SystemStreamPartition, TaskName> previousSSPToTask = getPreviousSSPToTaskMapping(grouperContext);

    Map<TaskName, PartitionGroup> taskToPartitionGroup = new HashMap<>();
    currentTaskAssignments.forEach((taskName, systemStreamPartitions) -> taskToPartitionGroup.put(taskName, new PartitionGroup(taskName, systemStreamPartitions)));

    Map<SystemStream, Integer> previousStreamToPartitionCount = getStreamToPartitionCount(grouperContext.getPreviousTaskToSSPAssignment());
    Map<SystemStream, Integer> currentStreamToPartitionCount = getStreamToPartitionCount(currentTaskAssignments);

    for (Map.Entry<TaskName, Set<SystemStreamPartition>> entry : currentTaskAssignments.entrySet()) {
      TaskName currentlyAssignedTask = entry.getKey();
      for (SystemStreamPartition currentSystemStreamPartition : entry.getValue()) {
        if (broadcastSystemStreamPartitions.contains(currentSystemStreamPartition)) {
          LOGGER.info("SystemStreamPartition: {} is part of broadcast stream. Skipping reassignment.", currentSystemStreamPartition);
        } else {
          SystemStream systemStream = currentSystemStreamPartition.getSystemStream();

          Integer previousStreamPartitionCount = previousStreamToPartitionCount.getOrDefault(systemStream, 0);
          Integer currentStreamPartitionCount = currentStreamToPartitionCount.getOrDefault(systemStream, 0);

          if (previousStreamPartitionCount > 0 && previousStreamPartitionCount < currentStreamPartitionCount
              && currentStreamPartitionCount % previousStreamPartitionCount == 0) {
            LOGGER.info("SystemStream: {} has expanded from: {} to: {}. Performing partition reassignment.", systemStream, previousStreamPartitionCount, currentStreamPartitionCount);

            SystemStreamPartition previousSystemStreamPartition = streamPartitionMapper.getSSPBeforeExpansion(currentSystemStreamPartition, previousStreamPartitionCount, currentStreamPartitionCount);
            TaskName previouslyAssignedTask = previousSSPToTask.get(previousSystemStreamPartition);

            LOGGER.info("Moving systemStreamPartition: {} from task: {} to task: {}.", currentSystemStreamPartition, currentlyAssignedTask, previouslyAssignedTask);

            taskToPartitionGroup.get(currentlyAssignedTask).removeSSP(currentSystemStreamPartition);
            taskToPartitionGroup.get(previouslyAssignedTask).addSSP(currentSystemStreamPartition);
          }
        }
      }
    }

    Map<TaskName, Set<SystemStreamPartition>> taskToSystemStreamPartitions = new HashMap<>();
    for (PartitionGroup partitionGroup : taskToPartitionGroup.values()) {
      if (!partitionGroup.systemStreamPartitions.isEmpty()) {
        taskToSystemStreamPartitions.put(partitionGroup.taskName, partitionGroup.systemStreamPartitions);
      }
    }
    return taskToSystemStreamPartitions;
  }

  /**
   * Computes a mapping from the stream name to partition count using the provided {@param taskToSSPAssignment}.
   * @param taskToSSPAssignment the {@link TaskName} to {@link SystemStreamPartition}'s assignment of the job.
   * @return a mapping from {@link SystemStream} to the number of partitions of the stream.
   */
  private Map<SystemStream, Integer> getStreamToPartitionCount(Map<TaskName, Set<SystemStreamPartition>> taskToSSPAssignment) {
    Map<SystemStream, Integer> streamToPartitionCount = new HashMap<>();
    taskToSSPAssignment.forEach((taskName, systemStreamPartitions) -> {
        systemStreamPartitions.forEach(systemStreamPartition -> {
            SystemStream systemStream = systemStreamPartition.getSystemStream();
            streamToPartitionCount.put(systemStream, streamToPartitionCount.getOrDefault(systemStream, 0) + 1);
          });
      });

    return streamToPartitionCount;
  }

  /**
   * Computes a mapping from the {@link SystemStreamPartition} to {@link TaskName} using the provided {@param grouperContext}
   * @param grouperContext the grouper context that contains relevant historical metadata about the job.
   * @return a mapping from {@link SystemStreamPartition} to {@link TaskName}.
   */
  private Map<SystemStreamPartition, TaskName> getPreviousSSPToTaskMapping(GrouperContext grouperContext) {
    Map<SystemStreamPartition, TaskName> sspToTaskMapping = new HashMap<>();
    Map<TaskName, Set<SystemStreamPartition>> previousTaskToSSPAssignment = grouperContext.getPreviousTaskToSSPAssignment();
    previousTaskToSSPAssignment.forEach((taskName, systemStreamPartitions) -> {
        systemStreamPartitions.forEach(systemStreamPartition -> {
            if (!broadcastSystemStreamPartitions.contains(systemStreamPartition)) {
              sspToTaskMapping.put(systemStreamPartition, taskName);
            }
          });
      });
    return sspToTaskMapping;
  }

  /**
   * Creates a instance of {@link StreamPartitionMapper} using the partition expansion factory class
   * defined in the {@param config}.
   * @param config the configuration of the samza job.
   * @return the instantiated {@link StreamPartitionMapper} object.
   */
  private StreamPartitionMapper getPartitionExpansionAlgorithm(Config config) {
    JobConfig jobConfig = new JobConfig(config);
    String partitionExpansionAlgorithmClass = jobConfig.getStreamPartitionMapperFactory();
    StreamPartitionMapperFactory
        expansionAlgorithmFactory = Util.getObj(partitionExpansionAlgorithmClass, StreamPartitionMapperFactory.class);
    return expansionAlgorithmFactory.getPartitionExpansionAlgorithm(config, new MetricsRegistryMap());
  }

  /**
   * A mutable group of {@link SystemStreamPartition} and {@link TaskName}.
   * Used to hold the interim result until the final task assignments are known.
   */
  private static class PartitionGroup {
    private TaskName taskName;
    private Set<SystemStreamPartition> systemStreamPartitions;

    PartitionGroup(TaskName taskName, Set<SystemStreamPartition> systemStreamPartitions) {
      Preconditions.checkNotNull(taskName);
      Preconditions.checkNotNull(systemStreamPartitions);
      this.taskName = taskName;
      this.systemStreamPartitions = new HashSet<>(systemStreamPartitions);
    }

    void removeSSP(SystemStreamPartition systemStreamPartition) {
      systemStreamPartitions.remove(systemStreamPartition);
    }

    void addSSP(SystemStreamPartition systemStreamPartition) {
      systemStreamPartitions.add(systemStreamPartition);
    }
  }
}
