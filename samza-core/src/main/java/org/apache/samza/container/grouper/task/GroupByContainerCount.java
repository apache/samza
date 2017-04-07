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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.SamzaException;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Group the SSP taskNames by dividing the number of taskNames into the number
 * of containers (n) and assigning n taskNames to each container as returned by
 * iterating over the keys in the map of taskNames (whatever that ordering
 * happens to be). No consideration is given towards locality, even distribution
 * of aggregate SSPs within a container, even distribution of the number of
 * taskNames between containers, etc.
 */
public class GroupByContainerCount implements BalancingTaskNameGrouper {
  private static final Logger log = LoggerFactory.getLogger(GroupByContainerCount.class);
  private final int containerCount;

  public GroupByContainerCount(int containerCount) {
    if (containerCount <= 0) throw new IllegalArgumentException("Must have at least one container");
    this.containerCount = containerCount;
  }

  @Override
  public Set<ContainerModel> group(Set<TaskModel> tasks) {

    validateTasks(tasks);

    // Sort tasks by taskName.
    List<TaskModel> sortedTasks = new ArrayList<>(tasks);
    Collections.sort(sortedTasks);

    // Map every task to a container in round-robin fashion.
    Map<TaskName, TaskModel>[] taskGroups = new Map[containerCount];
    for (int i = 0; i < containerCount; i++) {
      taskGroups[i] = new HashMap<>();
    }
    for (int i = 0; i < sortedTasks.size(); i++) {
      TaskModel tm = sortedTasks.get(i);
      taskGroups[i % containerCount].put(tm.getTaskName(), tm);
    }

    // Convert to a Set of ContainerModel
    Set<ContainerModel> containerModels = new HashSet<>();
    for (int i = 0; i < containerCount; i++) {
      containerModels.add(new ContainerModel(String.valueOf(i), i, taskGroups[i]));
    }

    return Collections.unmodifiableSet(containerModels);
  }

  @Override
  public Set<ContainerModel> balance(Set<TaskModel> tasks, LocalityManager localityManager) {

    validateTasks(tasks);

    TaskAssignmentManager taskAssignmentManager = localityManager.getTaskAssignmentManager();
    List<TaskGroup> containers = getPreviousContainers(taskAssignmentManager, tasks.size());
    if (containers == null || containers.size() == 1 || containerCount == 1) {
      log.info("Balancing does not apply. Invoking grouper.");
      Set<ContainerModel> models = group(tasks);
      saveTaskAssignments(models, taskAssignmentManager);
      return models;
    }

    int prevContainerCount = containers.size();
    int containerDelta = containerCount - prevContainerCount;
    if (containerDelta == 0) {
      log.info("Container count has not changed. Reusing previous container models.");
      return buildContainerModels(tasks, containers);
    }
    log.info("Container count changed from {} to {}. Balancing tasks.", prevContainerCount, containerCount);

    // Calculate the expected task count per container
    int[] expectedTaskCountPerContainer = calculateTaskCountPerContainer(tasks.size(), prevContainerCount, containerCount);

    // Collect excess tasks from over-assigned containers
    List<String> taskNamesToReassign = new LinkedList<>();
    for (int i = 0; i < prevContainerCount; i++) {
      TaskGroup taskGroup = containers.get(i);
      while (taskGroup.size() > expectedTaskCountPerContainer[i]) {
        taskNamesToReassign.add(taskGroup.removeTask());
      }
    }

    // Assign tasks to the under-assigned containers
    if (containerDelta > 0) {
      List<TaskGroup> newContainers = createContainers(prevContainerCount, containerCount);
      containers.addAll(newContainers);
    } else {
      containers = containers.subList(0, containerCount);
    }
    assignTasksToContainers(expectedTaskCountPerContainer, taskNamesToReassign, containers);

    // Transform containers to containerModel
    Set<ContainerModel> models = buildContainerModels(tasks, containers);

    // Save the results
    saveTaskAssignments(models, taskAssignmentManager);

    return models;
  }

  /**
   * Reads the task-container mapping from the provided {@link TaskAssignmentManager} and returns a
   * list of TaskGroups, ordered ascending by containerId.
   *
   * @param taskAssignmentManager the {@link TaskAssignmentManager} that will be used to retrieve the previous mapping.
   * @param taskCount             the number of tasks, for validation against the persisted tasks.
   * @return                      a list of TaskGroups, ordered ascending by containerId or {@code null}
   *                              if the previous mapping doesn't exist or isn't usable.
   */
  private List<TaskGroup> getPreviousContainers(TaskAssignmentManager taskAssignmentManager, int taskCount) {
    Map<String, String> taskToContainerId = taskAssignmentManager.readTaskAssignment();
    taskToContainerId.values().forEach(id -> {
        try {
          int intId = Integer.parseInt(id);
        } catch (NumberFormatException nfe) {
          throw new SamzaException("GroupByContainerCount cannot handle non-integer processorIds!", nfe);
        }
      });
    if (taskToContainerId.isEmpty()) {
      log.info("No task assignment map was saved.");
      return null;
    } else if (taskCount != taskToContainerId.size()) {
      log.warn(
          "Current task count {} does not match saved task count {}. Stateful jobs may observe misalignment of keys!",
          taskCount, taskToContainerId.size());
      // If the tasks changed, then the partition-task grouping is also likely changed and we can't handle that
      // without a much more complicated mapping. Further, the partition count may have changed, which means
      // input message keys are likely reshuffled w.r.t. partitions, so the local state may not contain necessary
      // data associated with the incoming keys. Warn the user and default to grouper
      // In this scenario the tasks may have been reduced, so we need to delete all the existing messages
      taskAssignmentManager.deleteTaskContainerMappings(taskToContainerId.keySet());
      return null;
    }

    List<TaskGroup> containers;
    try {
      containers = getOrderedContainers(taskToContainerId);
    } catch (Exception e) {
      log.error("Exception while parsing task mapping", e);
      return null;
    }
    return containers;
  }

  /**
   * Saves the task assignments specified by containers using the provided TaskAssignementManager.
   *
   * @param containers            the set of containers from which the task assignments will be saved.
   * @param taskAssignmentManager the {@link TaskAssignmentManager} that will be used to save the mappings.
   */
  private void saveTaskAssignments(Set<ContainerModel> containers, TaskAssignmentManager taskAssignmentManager) {
    for (ContainerModel container : containers) {
      for (TaskName taskName : container.getTasks().keySet()) {
        taskAssignmentManager.writeTaskContainerMapping(taskName.getTaskName(), container.getProcessorId());
      }
    }
  }

  /**
   * Verifies the input tasks argument and throws {@link IllegalArgumentException} if it is invalid.
   *
   * @param tasks the tasks to validate.
   */
  private void validateTasks(Set<TaskModel> tasks) {
    if (tasks.size() <= 0)
      throw new IllegalArgumentException("No tasks found. Likely due to no input partitions. Can't run a job with no tasks.");

    if (tasks.size() < containerCount)
      throw new IllegalArgumentException(String.format(
          "Your container count (%s) is larger than your task count (%s). Can't have containers with nothing to do, so aborting.",
          containerCount,
          tasks.size()));
  }

  /**
   * Creates a list of empty {@link TaskGroup} instances for a range of container id's
   * from the start(inclusive) to end(exclusive) container id.
   *
   * @param startContainerId  the first container id for which a TaskGroup is needed.
   * @param endContainerId    the first container id AFTER the last TaskGroup that is needed.
   * @return                  a set of empty TaskGroup instances corresponding to the range
   *                          [startContainerId, endContainerId)
   */
  private List<TaskGroup> createContainers(int startContainerId, int endContainerId) {
    List<TaskGroup> containers = new ArrayList<>(endContainerId - startContainerId);
    for (int i = startContainerId; i < endContainerId; i++) {
      TaskGroup taskGroup = new TaskGroup(String.valueOf(i), new ArrayList<String>());
      containers.add(taskGroup);
    }
    return containers;
  }

  /**
   * Assigns tasks from the specified list to containers that have fewer containers than indicated
   * in taskCountPerContainer.
   *
   * @param taskCountPerContainer the expected number of tasks for each container.
   * @param taskNamesToAssign     the list of tasks to assign to the containers.
   * @param containers            the containers (as {@link TaskGroup}) to which the tasks will be assigned.
   */
  // TODO: Change logic from using int arrays to a Map<String, Integer> (id -> taskCount)
  private void assignTasksToContainers(int[] taskCountPerContainer, List<String> taskNamesToAssign,
      List<TaskGroup> containers) {
    for (TaskGroup taskGroup : containers) {
      for (int j = taskGroup.size(); j < taskCountPerContainer[Integer.valueOf(taskGroup.getContainerId())]; j++) {
        String taskName = taskNamesToAssign.remove(0);
        taskGroup.addTaskName(taskName);
        log.info("Assigned task {} to container {}", taskName, taskGroup.getContainerId());
      }
    }
  }

  /**
   * Calculates the expected number of tasks for each container. The count is generated for
   * max(oldContainerCount, newContainerCount) s.t. if the container count has decreased,
   * the excess containers will have a count == 0, indicating that any tasks assigned to
   * them should be reassigned.
   *
   * @param taskCount             the number of tasks to divide among the containers.
   * @param prevContainerCount    the previous number of containers.
   * @param currentContainerCount the current number of containers.
   * @return                      the expected number of tasks for each container.
   */
  private int[] calculateTaskCountPerContainer(int taskCount, int prevContainerCount, int currentContainerCount) {
    int[] newTaskCountPerContainer = new int[Math.max(currentContainerCount, prevContainerCount)];
    Arrays.fill(newTaskCountPerContainer, 0);

    for (int i = 0; i < currentContainerCount; i++) {
      newTaskCountPerContainer[i] = taskCount / currentContainerCount;
      if (taskCount % currentContainerCount > i) {
        newTaskCountPerContainer[i]++;
      }
    }
    return newTaskCountPerContainer;
  }

  /**
   * Translates the list of TaskGroup instances to a set of ContainerModel instances, using the
   * set of TaskModel instances.
   *
   * @param tasks             the TaskModels to assign to the ContainerModels.
   * @param containerTasks    the TaskGroups defining how the tasks should be grouped.
   * @return                  a mutable set of ContainerModels.
   */
  private Set<ContainerModel> buildContainerModels(Set<TaskModel> tasks, List<TaskGroup> containerTasks) {
    // Map task names to models
    Map<String, TaskModel> taskNameToModel = new HashMap<>();
    for (TaskModel model : tasks) {
      taskNameToModel.put(model.getTaskName().getTaskName(), model);
    }

    // Build container models
    Set<ContainerModel> containerModels = new HashSet<>();
    for (TaskGroup container : containerTasks) {
      Map<TaskName, TaskModel> containerTaskModels = new HashMap<>();
      for (String taskName : container.taskNames) {
        TaskModel model = taskNameToModel.get(taskName);
        containerTaskModels.put(model.getTaskName(), model);
      }
      containerModels.add(
          new ContainerModel(container.containerId, Integer.valueOf(container.containerId), containerTaskModels));
    }
    return Collections.unmodifiableSet(containerModels);
  }

  /**
   * Converts the task->containerId map to an ordered list of {@link TaskGroup} instances.
   *
   * @param taskToContainerId a map from each task name to the containerId to which it is assigned.
   * @return                  a list of TaskGroups ordered ascending by containerId.
   */
  private List<TaskGroup> getOrderedContainers(Map<String, String> taskToContainerId) {
    log.debug("Got task to container map: {}", taskToContainerId);

    // Group tasks by container Id
    HashMap<String, List<String>> containerIdToTaskNames = new HashMap<>();
    for (Map.Entry<String, String> entry : taskToContainerId.entrySet()) {
      String taskName = entry.getKey();
      String containerId = entry.getValue();
      List<String> taskNames = containerIdToTaskNames.get(containerId);
      if (taskNames == null) {
        taskNames = new ArrayList<>();
        containerIdToTaskNames.put(containerId, taskNames);
      }
      taskNames.add(taskName);
    }

    // Build container tasks
    List<TaskGroup> containerTasks = new ArrayList<>(containerIdToTaskNames.size());
    for (int i = 0; i < containerIdToTaskNames.size(); i++) {
      if (containerIdToTaskNames.get(String.valueOf(i)) == null) throw new IllegalStateException("Task mapping is missing container: " + i);
      containerTasks.add(new TaskGroup(String.valueOf(i), containerIdToTaskNames.get(String.valueOf(i))));
    }

    return containerTasks;
  }

  /**
   * A mutable group of tasks and an associated container id.
   *
   * Used as a temporary mutable container until the final ContainerModel is known.
   */
  private static class TaskGroup {
    private final List<String> taskNames = new LinkedList<>();
    private final String containerId;

    private TaskGroup(String containerId, List<String> taskNames) {
      this.containerId = containerId;
      Collections.sort(taskNames);        // For consistency because the taskNames came from a Map
      this.taskNames.addAll(taskNames);
    }

    public String getContainerId() {
      return containerId;
    }

    public void addTaskName(String taskName) {
      taskNames.add(taskName);
    }

    public String removeTask() {
      return taskNames.remove(taskNames.size() - 1);
    }

    public int size() {
      return taskNames.size();
    }
  }
}
