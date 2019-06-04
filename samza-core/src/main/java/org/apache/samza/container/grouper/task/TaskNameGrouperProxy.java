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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.clustermanager.StandbyTaskUtil;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This TaskNameGrouperProxy wraps around any given TaskNameGrouper.
 * In addition to apply the provided grouper, this grouper also generates
 * generates Standby-tasks and adds them to separate dedicated containers.
 * It adds (r-1) Standby-Tasks for each active task, where r is the replication factor.
 * Hence it adds r-1 additional containers for each regular container.
 *
 * All Standby-tasks are assigned a TaskName with a "Standby" prefix.
 * The new containers carrying Standby tasks that are added are assigned containerIDs corresponding to its
 * active container, e.g., activeContainerID-replicaNumber
 * For e.g.,
 *
 * If the initial container model map is:
 *
 * Container 0 : (Partition 0, Partition 1)
 * Container 1 : (Partition 2, Partition 3)
 * with replicationFactor = 3
 *
 * The generated containerModel map is:
 * Container 0 : (Partition 0, Partition 1)
 * Container 1 : (Partition 2, Partition 3)
 * Container 0-0 : (Standby-Partition 0-0, Standby-Partition 1-0)
 * Container 1-0 : (Standby-Partition 2-0, Standby-Partition 3-0)
 * Container 0-1 : (Standby-Partition 0-1, Standby-Partition 1-1)
 * Container 1-1 : (Standby-Partition 2-1, Standby-Partition 3-1)
 */
public class TaskNameGrouperProxy {

  private static final Logger LOG = LoggerFactory.getLogger(TaskNameGrouperProxy.class);
  private final TaskNameGrouper taskNameGrouper;
  private final boolean standbyTasksEnabled;
  private final int replicationFactor;

  public TaskNameGrouperProxy(TaskNameGrouper taskNameGrouper, boolean standbyTasksEnabled,
      int replicationFactor) {
    this.taskNameGrouper = taskNameGrouper;
    this.standbyTasksEnabled = standbyTasksEnabled;
    this.replicationFactor = replicationFactor;
  }

  public Set<ContainerModel> group(Set<TaskModel> taskModels, GrouperMetadata grouperMetadata) {
    if (this.standbyTasksEnabled) {
      return generateStandbyTasks(this.taskNameGrouper.group(taskModels, grouperMetadata), replicationFactor);
    } else {
      return this.taskNameGrouper.group(taskModels, grouperMetadata);
    }
  }

  public Set<ContainerModel> group(Set<TaskModel> taskModels, List<String> containersIds) {
    if (this.standbyTasksEnabled) {
      return generateStandbyTasks(this.taskNameGrouper.group(taskModels, containersIds), replicationFactor);
    } else {
      return this.taskNameGrouper.group(taskModels, containersIds);
    }
  }

  /**
   *  Generate a container model map with standby tasks added and grouped into buddy containers.
   *  Package-private for testing.
   *
   * @param containerModels The initial container model map.
   * @param replicationFactor The desired replication factor, if the replication-factor is n, we add n-1 standby tasks for each active task.
   * @return The generated map of containerModels with added containers, and the initial regular containers
   */
  Set<ContainerModel> generateStandbyTasks(Set<ContainerModel> containerModels, int replicationFactor) {
    LOG.info("Received current containerModel map : {}, replicationFactor : {}", containerModels, replicationFactor);
    Set<ContainerModel> buddyContainers = new HashSet<>();

    for (ContainerModel activeContainer : containerModels) {
      for (int replicaNum = 0; replicaNum < replicationFactor - 1; replicaNum++) {
        String buddyContainerId = StandbyTaskUtil.getStandbyContainerId(activeContainer.getId(), replicaNum);

        ContainerModel buddyContainerModel =
            new ContainerModel(buddyContainerId, getTaskModelForBuddyContainer(activeContainer.getTasks(), replicaNum));

        buddyContainers.add(buddyContainerModel);
      }
    }

    LOG.info("Adding buddy containers : {}", buddyContainers);
    buddyContainers.addAll(containerModels);
    return buddyContainers;
  }

  // Helper method to populate the container model for a buddy container.
  private static Map<TaskName, TaskModel> getTaskModelForBuddyContainer(
      Map<TaskName, TaskModel> activeContainerTaskModel, int replicaNum) {
    Map<TaskName, TaskModel> standbyTaskModels = new HashMap<>();

    for (TaskName taskName : activeContainerTaskModel.keySet()) {
      TaskName standbyTaskName = StandbyTaskUtil.getStandbyTaskName(taskName, replicaNum);
      TaskModel standbyTaskModel =
          new TaskModel(standbyTaskName, activeContainerTaskModel.get(taskName).getSystemStreamPartitions(),
              activeContainerTaskModel.get(taskName).getChangelogPartition(), TaskMode.Standby);
      standbyTaskModels.put(standbyTaskName, standbyTaskModel);
    }

    LOG.info("Generated standbyTaskModels : {} for active task models : {}", standbyTaskModels,
        activeContainerTaskModel);
    return standbyTaskModels;
  }
}
