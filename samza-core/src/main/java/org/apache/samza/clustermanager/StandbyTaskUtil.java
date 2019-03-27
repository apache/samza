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
package org.apache.samza.clustermanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskMode;


/**
 * Collection of util methods used for performing Standby-aware Container allocation in YARN.
 */
public class StandbyTaskUtil {
  private static final String STANDBY_CONTAINER_ID_SEPARATOR = "-";
  private static final String TASKNAME_SEPARATOR = "-";
  private static final String STANDBY_TASKNAME_PREFIX = "Standby";

  /**
   * Returns true if the containerName implies a standby container, false otherwise.
   * @param containerID The desired containerID
   * @return
   */
  public static boolean isStandbyContainer(String containerID) {
    return containerID.contains(STANDBY_CONTAINER_ID_SEPARATOR);
  }

  // Helper method to generate buddy containerIDs by appending the replica-number to the active-container's id.
  public final static String getStandbyContainerId(String activeContainerId, int replicaNumber) {
    return activeContainerId.concat(STANDBY_CONTAINER_ID_SEPARATOR).concat(String.valueOf(replicaNumber));
  }

  // Helper method to generate active container's ID by removing the replica-number from the standby container's id.
  public final static String getActiveContainerId(String standbyContainerID) {
    return standbyContainerID.split(STANDBY_CONTAINER_ID_SEPARATOR)[0];
  }

  // Helper method to get the standby task name by prefixing "Standby" to the corresponding active task's name.
  public final static TaskName getStandbyTaskName(TaskName activeTaskName, int replicaNum) {
    return new TaskName(STANDBY_TASKNAME_PREFIX.concat(TASKNAME_SEPARATOR)
        .concat(activeTaskName.getTaskName())
        .concat(TASKNAME_SEPARATOR)
        .concat(String.valueOf(replicaNum)));
  }

  // Helper method to get the active task name by stripping the prefix "Standby" from the standby task name.
  public final static TaskName getActiveTaskName(TaskName standbyTaskName) {
    return new TaskName(standbyTaskName.getTaskName().split(TASKNAME_SEPARATOR)[1]);
  }

  /**
   *  Given a containerID and job model, it returns the containerids of all containers that either have
   *  a. standby tasks corresponding to active tasks on the given container, or
   *  b. have active tasks corresponding to standby tasks on the given container.
   *  This is used to ensure that an active task and all its corresponding standby tasks are on separate hosts, and
   *  standby tasks corresponding to the same active task are on separate hosts.
   */
  public static List<String> getStandbyContainerConstraints(String containerID, JobModel jobModel) {

    ContainerModel givenContainerModel = jobModel.getContainers().get(containerID);
    List<String> containerIDsWithStandbyConstraints = new ArrayList<>();

    // iterate over all containerModels in the jobModel
    for (ContainerModel containerModel : jobModel.getContainers().values()) {

      // add to list if active and standby tasks on the two containerModels overlap
      if (!givenContainerModel.equals(containerModel) && checkTaskOverlap(givenContainerModel, containerModel)) {
        containerIDsWithStandbyConstraints.add(containerModel.getId());
      }
    }
    return containerIDsWithStandbyConstraints;
  }

  // Helper method that checks if tasks on the two containerModels overlap
  private static boolean checkTaskOverlap(ContainerModel containerModel1, ContainerModel containerModel2) {
    Set<TaskName> activeTasksOnContainer1 = getCorrespondingActiveTasks(containerModel1);
    Set<TaskName> activeTasksOnContainer2 = getCorrespondingActiveTasks(containerModel2);
    return !Collections.disjoint(activeTasksOnContainer1, activeTasksOnContainer2);
  }

  // Helper method that returns the active tasks corresponding to all standby tasks on a container, including any already-active tasks on the container
  private static Set<TaskName> getCorrespondingActiveTasks(ContainerModel containerModel) {
    Set<TaskName> tasksInActiveMode = getAllTasks(containerModel, TaskMode.Active);
    tasksInActiveMode.addAll(getAllTasks(containerModel, TaskMode.Standby).stream()
        .map(taskName -> getActiveTaskName(taskName))
        .collect(Collectors.toSet()));
    return tasksInActiveMode;
  }

  // Helper method to getAllTaskModels of this container in the given taskMode
  private static Set<TaskName> getAllTasks(ContainerModel containerModel, TaskMode taskMode) {
    return containerModel.getTasks()
        .values()
        .stream()
        .filter(e -> e.getTaskMode().equals(taskMode))
        .map(taskModel -> taskModel.getTaskName())
        .collect(Collectors.toSet());
  }

}
