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

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import com.google.common.base.Preconditions;
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

/**
 * Provides a stream expansion aware task to partition assignments on top of a custom implementation
 * of the {@link SystemStreamPartitionGrouper}.
 */
public class SSPGrouperProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(SSPGrouperProxy.class);

  private final SystemStreamPartitionMapper systemStreamPartitionMapper;
  private final Set<SystemStreamPartition> broadcastSystemStreamPartitions;
  private final SystemStreamPartitionGrouper grouper;

  public SSPGrouperProxy(Config config, SystemStreamPartitionGrouper grouper) {
    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(grouper);
    this.grouper = grouper;
    this.broadcastSystemStreamPartitions = new TaskConfigJava(config).getBroadcastSystemStreamPartitions();
    this.systemStreamPartitionMapper = getSystemStreamPartitionMapper(config);
  }

  /**
   * 1. Invokes the sub-class {@link org.apache.samza.container.grouper.task.TaskNameGrouper#group(Set)} method to generate the task to partition assignments.
   * 2. Uses the {@link SystemStreamPartitionMapper} to compute the previous {@link SystemStreamPartition} for every partition of the expanded or contracted streams.
   * 3. Uses the previous, current task to partition assignments and result of {@link SystemStreamPartitionMapper} to redistribute the expanded {@link SystemStreamPartition}'s
   * to correct tasks after the stream expansion or contraction.
   * @param ssps the input system stream partitions of the job.
   * @param grouperContext the grouper context holding metadata for the previous job execution.
   * @return the grouped {@link TaskName} to {@link SystemStreamPartition} assignments.
   */
  public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> ssps, GrouperContext grouperContext) {
    Map<TaskName, Set<SystemStreamPartition>> currentTaskAssignments = grouper.group(ssps);

    if (grouperContext.getPreviousTaskToSSPAssignment().isEmpty()) {
      LOGGER.info("Previous task to partition assignment does not exist. Using the result from the group method.");
      return currentTaskAssignments;
    }

    Map<SystemStreamPartition, TaskName> previousSSPToTask = getPreviousSSPToTaskMapping(grouperContext);

    Map<TaskName, PartitionGroup> taskToPartitionGroup = new HashMap<>();
    currentTaskAssignments.forEach((taskName, systemStreamPartitions) -> taskToPartitionGroup.put(taskName, new PartitionGroup(taskName, systemStreamPartitions)));

    Map<SystemStream, Integer> previousStreamToPartitionCount = getSystemStreamToPartitionCount(grouperContext.getPreviousTaskToSSPAssignment());
    Map<SystemStream, Integer> currentStreamToPartitionCount = getSystemStreamToPartitionCount(currentTaskAssignments);

    for (Map.Entry<TaskName, Set<SystemStreamPartition>> entry : currentTaskAssignments.entrySet()) {
      TaskName currentlyAssignedTask = entry.getKey();
      for (SystemStreamPartition currentSystemStreamPartition : entry.getValue()) {
        if (broadcastSystemStreamPartitions.contains(currentSystemStreamPartition)) {
          LOGGER.info("SystemStreamPartition: {} is part of broadcast stream. Skipping reassignment.", currentSystemStreamPartition);
        } else {
          SystemStream systemStream = currentSystemStreamPartition.getSystemStream();

          Integer previousStreamPartitionCount = previousStreamToPartitionCount.getOrDefault(systemStream, 0);
          Integer currentStreamPartitionCount = currentStreamToPartitionCount.getOrDefault(systemStream, 0);

          if (previousStreamPartitionCount > 0 && !currentStreamPartitionCount.equals(previousStreamPartitionCount)) {
            LOGGER.info("Partition count of system stream: {} had changed from: {} to: {} partitions. Performing partition reassignment.", systemStream, previousStreamPartitionCount, currentStreamPartitionCount);

            SystemStreamPartition previousSystemStreamPartition = systemStreamPartitionMapper.getPreviousSSP(currentSystemStreamPartition, previousStreamPartitionCount, currentStreamPartitionCount);
            TaskName previouslyAssignedTask = previousSSPToTask.get(previousSystemStreamPartition);

            LOGGER.info("Moving systemStreamPartition: {} from task: {} to task: {}.", currentSystemStreamPartition, currentlyAssignedTask, previouslyAssignedTask);

            taskToPartitionGroup.get(currentlyAssignedTask).removeSSP(currentSystemStreamPartition);
            taskToPartitionGroup.get(previouslyAssignedTask).addSSP(currentSystemStreamPartition);
          } else {
            LOGGER.debug("No partition change in SystemStream: {}. Skipping partition reassignment.", systemStream);
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
   * Computes a mapping from system stream to partition count using the provided {@param taskToSSPAssignment}.
   * @param taskToSSPAssignment the {@link TaskName} to {@link SystemStreamPartition}'s assignment of the job.
   * @return a mapping from {@link SystemStream} to the number of partitions of the stream.
   */
  private Map<SystemStream, Integer> getSystemStreamToPartitionCount(Map<TaskName, Set<SystemStreamPartition>> taskToSSPAssignment) {
    Map<SystemStream, Integer> systemStreamToPartitionCount = new HashMap<>();
    taskToSSPAssignment.forEach((taskName, systemStreamPartitions) -> {
        systemStreamPartitions.forEach(systemStreamPartition -> {
            SystemStream systemStream = systemStreamPartition.getSystemStream();
            systemStreamToPartitionCount.put(systemStream, systemStreamToPartitionCount.getOrDefault(systemStream, 0) + 1);
          });
      });

    return systemStreamToPartitionCount;
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
   * Creates a instance of {@link SystemStreamPartitionMapper} using the stream partition expansion factory class
   * defined in the {@param config}.
   * @param config the configuration of the samza job.
   * @return the instantiated {@link SystemStreamPartitionMapper} object.
   */
  private SystemStreamPartitionMapper getSystemStreamPartitionMapper(Config config) {
    JobConfig jobConfig = new JobConfig(config);
    String systemStreamPartitionMapperClass = jobConfig.getSystemStreamPartitionMapperFactoryName();
    SystemStreamPartitionMapperFactory systemStreamPartitionMapperFactory = Util.getObj(systemStreamPartitionMapperClass, SystemStreamPartitionMapperFactory.class);
    return systemStreamPartitionMapperFactory.getStreamPartitionMapper(config, new MetricsRegistryMap());
  }

  /**
   * A mutable group of {@link SystemStreamPartition} and {@link TaskName}.
   * Used to hold the interim results until the final task assignments are known.
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
