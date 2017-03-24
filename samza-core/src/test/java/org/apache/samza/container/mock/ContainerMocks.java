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
package org.apache.samza.container.mock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;


public class ContainerMocks {
  /**
   * Note: This is artificial. It makes assumptions about task grouping, changelog partition mapping, etc.
   * that will likely not be true at runtime, but are not the focus of this test.
   */
  public static Set<ContainerModel> generateContainerModels(int numContainers, int taskCount) {
    Set<ContainerModel> models = new HashSet<>(numContainers);
    int[] taskCountPerContainer = calculateTaskCountPerContainer(taskCount, numContainers);
    int j = 0;
    for (int i = 0; i < numContainers; i++) {
      int[] partitions = new int[taskCountPerContainer[i]];
      for (int k = 0; k < taskCountPerContainer[i]; k++) {
        partitions[k] = j + k;
      }
      j += taskCountPerContainer[i];

      models.add(createContainerModel(i, partitions));
    }
    return models;
  }

  public static Map<String, String> generateTaskAssignments(int numContainers, int taskCount) {
    Map<String, String> mapping = new HashMap<>(taskCount);
    Set<ContainerModel> containers = generateContainerModels(numContainers, taskCount);
    for (ContainerModel container : containers) {
      for (TaskName taskName : container.getTasks().keySet()) {
        mapping.put(taskName.getTaskName(), container.getProcessorId());
      }
    }
    return mapping;
  }

  public static int[] calculateTaskCountPerContainer(int taskCount, int currentContainerCount) {
    int[] newTaskCountPerContainer = new int[currentContainerCount];
    for (int i = 0; i < currentContainerCount; i++) {
      newTaskCountPerContainer[i] = taskCount / currentContainerCount;
      if (taskCount % currentContainerCount > i) {
        newTaskCountPerContainer[i]++;
      }
    }
    return newTaskCountPerContainer;
  }

  public static ContainerModel createContainerModel(int containerId, int[] partitions) {
    Map<TaskName, TaskModel> tasks = new HashMap<>();
    for (int partition : partitions) {
      tasks.put(getTaskName(partition), getTaskModel(partition));
    }
    return new ContainerModel(containerId, tasks);
  }

  public static Set<TaskModel> generateTaskModels(int[] partitions) {
    Set<TaskModel> models = new HashSet<>(partitions.length);
    for (int partition : partitions) {
      models.add(getTaskModel(partition));
    }
    return models;
  }

  public static Set<TaskModel> generateTaskModels(int count) {
    Set<TaskModel> taskModels = new HashSet<>();
    for (int i = 0; i < count; i++) {
      taskModels.add(getTaskModel(i));
    }
    return taskModels;
  }

  public static TaskModel getTaskModel(int partitionId) {
    return new TaskModel(getTaskName(partitionId),
        new HashSet<>(
            Arrays.asList(new SystemStreamPartition[]{new SystemStreamPartition("System", "Stream", new Partition(partitionId))})),
        new Partition(partitionId));
  }

  public static TaskName getTaskName(int partitionId) {
    return new TaskName("Partition " + partitionId);
  }

  // Inclusive of both indices
  public static int[] range(int from, int to) {
    int[] values = new int[to - from + 1];
    for (int i = 0; i < values.length; i++) {
      values[i] = from++;
    }
    return values;
  }

  public static Map<String, String> generateTaskContainerMapping(Set<ContainerModel> containers) {
    Map<String, String> taskMapping = new HashMap<>();
    for (ContainerModel container : containers) {
      for (TaskName taskName : container.getTasks().keySet()) {
        taskMapping.put(taskName.getTaskName(), container.getProcessorId());
      }
    }
    return taskMapping;
  }
}
