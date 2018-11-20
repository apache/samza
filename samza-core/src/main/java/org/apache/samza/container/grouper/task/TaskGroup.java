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
package org.apache.samza.container.grouper.task;

import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;

import java.util.*;

/**
 * A mutable group of tasks and an associated container id.
 *
 * Used as a temporary mutable container until the final ContainerModel is known.
 */
class TaskGroup {
  final List<String> taskNames = new LinkedList<>();
  final String containerId;

  TaskGroup(String containerId, List<String> taskNames) {
    this.containerId = containerId;
    Collections.sort(taskNames); // For consistency because the taskNames came from a Map
    this.taskNames.addAll(taskNames);
  }

  public String getContainerId() {
    return containerId;
  }

  public void addTaskName(String taskName) {
    taskNames.add(taskName);
  }

  public String removeLastTask() {
    return taskNames.remove(taskNames.size() - 1);
  }

  public int size() {
    return taskNames.size();
  }

  /**
   * Converts the {@link TaskGroup} list to a set of ContainerModel.
   *
   * @param taskModels    the TaskModels to assign to the ContainerModels.
   * @param taskGroups    the TaskGroups defining how the tasks should be grouped.
   * @return              a set of ContainerModels.
   */
  public static Set<ContainerModel> buildContainerModels(Set<TaskModel> taskModels, Collection<TaskGroup> taskGroups) {
    // Map task names to models
    Map<String, TaskModel> taskNameToModel = new HashMap<>();
    for (TaskModel model : taskModels) {
      taskNameToModel.put(model.getTaskName().getTaskName(), model);
    }

    // Build container models
    Set<ContainerModel> containerModels = new HashSet<>();
    for (TaskGroup container : taskGroups) {
      Map<TaskName, TaskModel> containerTaskModels = new HashMap<>();
      for (String taskName : container.taskNames) {
        TaskModel model = taskNameToModel.get(taskName);
        containerTaskModels.put(model.getTaskName(), model);
      }
      containerModels.add(new ContainerModel(container.containerId, containerTaskModels));
    }

    return Collections.unmodifiableSet(containerModels);
  }
}
