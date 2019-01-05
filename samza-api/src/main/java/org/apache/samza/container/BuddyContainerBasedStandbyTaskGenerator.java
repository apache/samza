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
package org.apache.samza.container;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BuddyContainerBasedStandbyTaskGenerator implements StandbyTaskGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(BuddyContainerBasedStandbyTaskGenerator.class);

  private static final String CONTAINER_ID_SEPARATOR = "-";
  private static final String STANDBY_TASKNAME_PREFIX = "Standby ";

  @Override
  public Map<String, ContainerModel> provisionStandbyTasks(Map<String, ContainerModel> containerModels,
      int replicationFactor) {
    LOG.debug("Received current containerModel map : {}, replicationFactor : {}", containerModels, replicationFactor);
    Map<String, ContainerModel> buddyContainerMap = new HashMap<>();

    for (String activeContainerId : containerModels.keySet()) {
      // if the replication-factor is n, we add n-1 standby tasks for each active task
      for (int i = 0; i < replicationFactor - 1; i++) {
        String buddyContainerId = getBuddyContainerId(activeContainerId, i);

        ContainerModel buddyContainerModel = new ContainerModel(buddyContainerId,
            getTaskModelForBuddyContainer(containerModels.get(activeContainerId).getTasks()));

        buddyContainerMap.put(buddyContainerId, buddyContainerModel);
      }
    }
    LOG.info("Adding buddy containers : {}", buddyContainerMap);

    buddyContainerMap.putAll(containerModels);
    return buddyContainerMap;
  }

  private static Map<TaskName, TaskModel> getTaskModelForBuddyContainer(
      Map<TaskName, TaskModel> activeContainerTaskModel) {
    Map<TaskName, TaskModel> standbyTaskModels = new HashMap<>();

    for (TaskName taskName : activeContainerTaskModel.keySet()) {
      TaskName standbyTaskName = getStandbyTaskName(taskName);
      TaskModel standbyTaskModel =
          new TaskModel(standbyTaskName, activeContainerTaskModel.get(taskName).getSystemStreamPartitions(),
              activeContainerTaskModel.get(taskName).getChangelogPartition());

      standbyTaskModels.put(standbyTaskName, standbyTaskModel);
    }

    LOG.info("Generated standbyTaskModels : {} for active task models : {}", standbyTaskModels,
        activeContainerTaskModel);
    return standbyTaskModels;
  }

  // generate buddy containerIDs by appending the replica-number to the active-container's id
  private final static String getBuddyContainerId(String activeContainerId, int replicaNumber) {
    return activeContainerId.concat(CONTAINER_ID_SEPARATOR).concat(String.valueOf(replicaNumber));
  }

  private final static TaskName getStandbyTaskName(TaskName activeTaskName) {
    return new TaskName(STANDBY_TASKNAME_PREFIX.concat(activeTaskName.getTaskName()));
  }
}
