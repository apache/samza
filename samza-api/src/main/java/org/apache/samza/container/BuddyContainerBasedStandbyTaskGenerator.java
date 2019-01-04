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
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BuddyContainerBasedStandbyTaskGenerator implements StandbyTaskGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(BuddyContainerBasedStandbyTaskGenerator.class);

  @Override
  public Map<String, ContainerModel> provisionStandbyTasks(Map<String, ContainerModel> containerModels,
      int taskReplicationFactor) {

    LOG.debug("Received current containerModel map : {}, taskReplicationFactor : {}", containerModels,
        taskReplicationFactor);

    Map<String, ContainerModel> buddyContainerMap = new HashMap<>();
    int numberOfActiveContainers = containerModels.size();

    for (String activeContainerId : containerModels.keySet()) {

      for (int i = 1; i <= taskReplicationFactor; i++) {

        // generate buddy containerIDs by adding the numContainers to the activeContainer's id
        // e.g., if activeContainersIDs = 2,3 numContainers = 2 and rf = 3
        // this will generate buddyContainerIds as 4 & 6 for activeCont-2 and 5 & 6 for activeCont-3
        String buddyContainerId = String.valueOf(Integer.parseInt(activeContainerId) + i * numberOfActiveContainers);
        ContainerModel buddyContainerModel = new ContainerModel(buddyContainerId,
            getTaskModelForBuddyContainer(containerModels.get(activeContainerId).getTasks()));

        buddyContainerMap.put(buddyContainerId, buddyContainerModel);
      }
    }

    LOG.debug("Adding buddy containers : {}", buddyContainerMap);

    containerModels.putAll(buddyContainerMap);
    return containerModels;
  }

  private static Map<TaskName, TaskModel> getTaskModelForBuddyContainer(
      Map<TaskName, TaskModel> activeContainerTaskModel) {
    Map<TaskName, TaskModel> buddyTaskModels = new HashMap<>();

    for (TaskName taskName : activeContainerTaskModel.keySet()) {

      TaskName buddyTaskName = new TaskName("Standby " + taskName.getTaskName());

      TaskModel buddyTaskModel =
          new TaskModel(buddyTaskName, activeContainerTaskModel.get(taskName).getSystemStreamPartitions(),
              activeContainerTaskModel.get(taskName).getChangelogPartition());

      buddyTaskModels.put(buddyTaskName, buddyTaskModel);
    }

    LOG.debug("Generated buddytaskModels : {} for active task models : {}", buddyTaskModels, activeContainerTaskModel);

    return buddyTaskModels;
  }
}
