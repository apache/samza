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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

public class GroupBySystemStreamPartitionWithFixedTaskNum extends GroupBySystemStreamPartition implements SystemStreamPartitionGrouperWithFixedTaskNum {
  /**
   * A constructor that accepts job config as the parameter
   *
   * @param config job config
   */
  public GroupBySystemStreamPartitionWithFixedTaskNum(Config config) {
    super(config);
  }

  private Map<String, Set<String>> groupTaskByStream(Map<SystemStreamPartition, String> systemStreamPartitionToTaskAssignment) {
    Map<String, Set<String>> streamToTasks = new HashMap<>();
    for (Map.Entry<SystemStreamPartition, String> entry: systemStreamPartitionToTaskAssignment.entrySet()) {
      SystemStreamPartition systemStreamPartition = entry.getKey();
      if (!streamToTasks.containsKey(systemStreamPartition.getStream()))
        streamToTasks.put(systemStreamPartition.getStream(), new HashSet<>());
      streamToTasks.get(systemStreamPartition.getStream()).add(entry.getValue());
    }
    return streamToTasks;
  }

  private Map<String, Integer> getPartitionCountByStream(Set<SystemStreamPartition> systemStreamPartitions) {
    Map<String, Integer> streamToPartitionCount = new HashMap<>();
    for (SystemStreamPartition systemStreamPartition: systemStreamPartitions) {
      int partitionCount = 1;
      if (streamToPartitionCount.containsKey(systemStreamPartition.getStream()))
        partitionCount = streamToPartitionCount.get(systemStreamPartition.getStream()) + 1;
      streamToPartitionCount.put(systemStreamPartition.getStream(), partitionCount);
    }
    return streamToPartitionCount;
  }

  private boolean isDoubled(int previousPartitionCount, int currentPartitionCount) {
    if (previousPartitionCount <= 0 || currentPartitionCount <= 0 || previousPartitionCount > currentPartitionCount)
      return false;
    while (previousPartitionCount < currentPartitionCount)
      previousPartitionCount *= 2;
    return previousPartitionCount == currentPartitionCount;
  }

  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(
      Map<SystemStreamPartition, String> previousSystemStreamPartitionToTask,
      Set<SystemStreamPartition> systemStreamPartitions) {

    Map<String, Set<String>> previousStreamToTasks = groupTaskByStream(previousSystemStreamPartitionToTask);
    Map<String, Integer> currentStreamToPartitionCount = getPartitionCountByStream(systemStreamPartitions);
    Map<TaskName, Set<SystemStreamPartition>> taskToSystemStreamPartitions = new HashMap<>();

    for (SystemStreamPartition systemStreamPartition : systemStreamPartitions) {
      if (broadcastStreams.contains(systemStreamPartition)) {
        continue;
      }

      int currentPartitionId = systemStreamPartition.getPartition().getPartitionId();
      TaskName taskName = new TaskName(systemStreamPartition.toString());
      Set<String> previousTaskSet = previousStreamToTasks.get(systemStreamPartition.getStream());
      int previousPartitionCount = previousTaskSet != null ? previousTaskSet.size() : 0;

      if (previousPartitionCount > 0) {
        int currentPartitionCount = currentStreamToPartitionCount.get(systemStreamPartition.getStream());
        if (!isDoubled(previousPartitionCount, currentPartitionCount))
          throw new SamzaException("Partition of the " + systemStreamPartition.getSystemStream() +
              " has increased from " + previousPartitionCount + " to " + currentPartitionCount + " which is not allowed");

        int previousPartitionId = currentPartitionId % previousPartitionCount;
        SystemStreamPartition previousSystemStreamPartition = new SystemStreamPartition(
            systemStreamPartition.getSystemStream(), new Partition(previousPartitionId));
        taskName = new TaskName(previousSystemStreamPartitionToTask.get(previousSystemStreamPartition));
      }

      if (!taskToSystemStreamPartitions.containsKey(taskName)) {
        taskToSystemStreamPartitions.put(taskName, new HashSet<>());
      }
      taskToSystemStreamPartitions.get(taskName).add(systemStreamPartition);
    }

    // assign the broadcast streams to all the taskNames
    if (!broadcastStreams.isEmpty()) {
      for (Set<SystemStreamPartition> value : taskToSystemStreamPartitions.values()) {
        value.addAll(broadcastStreams);
      }
    }

    return taskToSystemStreamPartitions;
  }

}
