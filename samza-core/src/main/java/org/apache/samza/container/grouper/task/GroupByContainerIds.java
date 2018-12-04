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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.commons.collections4.MapUtils;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.runtime.LocationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple grouper.
 * It exposes two group methods - one that assumes sequential container numbers and one that gets a set of container
 * IDs as an argument. Please note - this first implementation ignores locality information.
 */
public class GroupByContainerIds implements TaskNameGrouper {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByContainerIds.class);

  private final int startContainerCount;
  public GroupByContainerIds(int count) {
    this.startContainerCount = count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<ContainerModel> group(Set<TaskModel> tasks) {
    List<String> containerIds = new ArrayList<>(startContainerCount);
    for (int i = 0; i < startContainerCount; i++) {
      containerIds.add(String.valueOf(i));
    }
    return group(tasks, containerIds);
  }

  /**
   * {@inheritDoc}
   *
   * When number of taskModels are less than number of available containerIds,
   * then chooses then selects the lexicographically least `x` containerIds.
   *
   * Otherwise, assigns the tasks to the available containerIds in a round robin fashion
   * preserving the containerId in the final assignment.
   */
  @Override
  public Set<ContainerModel> group(Set<TaskModel> tasks, List<String> containerIds) {
    if (containerIds == null)
      return this.group(tasks);

    if (containerIds.isEmpty())
      throw new IllegalArgumentException("Must have at least one container");

    if (tasks.isEmpty())
      throw new IllegalArgumentException("cannot group an empty set. containerIds=" + Arrays
          .toString(containerIds.toArray()));

    if (containerIds.size() > tasks.size()) {
      LOG.warn("Number of containers: {} is greater than number of tasks: {}.",  containerIds.size(), tasks.size());
      /**
       * Choose lexicographically least `x` containerIds(where x = tasks.size()).
       */
      containerIds = containerIds.stream()
                                   .sorted()
                                   .limit(tasks.size())
                                   .collect(Collectors.toList());
      LOG.info("Generating containerModel with containers: {}.", containerIds);
    }

    int containerCount = containerIds.size();

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
      containerModels.add(new ContainerModel(containerIds.get(i), taskGroups[i]));
    }

    return Collections.unmodifiableSet(containerModels);
  }

  /**
   * {@inheritDoc}
   *
   * When the are `t` tasks and `p` processors, where t >= p, a fair task distribution should ideally assign
   * (t / p) tasks to each processor. In addition to guaranteeing a fair distribution, this {@link TaskNameGrouper}
   * implementation generates a locationId aware task assignment to processors where it makes best efforts in assigning
   * the tasks to processors with the same locality.
   *
   * Task assignment to processors is accomplished through the following two phases:
   *
   * 1. In the first phase, each task(T) is assigned to a processor(P) that satisfies the following constraints:
   *    A. The processor(P) should have the same locality of the task(T).
   *    B. Number of tasks already assigned to the processor should be less than the (number of tasks / number of processors).
   *
   * 2. Each unassigned task from phase 1 are then mapped to any processor with task count less than the
   * (number of tasks / number of processors). When no such processor exists, then the unassigned
   * task is mapped to any processor from available processors in a round robin fashion.
   */
  @Override
  public Set<ContainerModel> group(Set<TaskModel> taskModels, MetadataProvider metadataProvider) {
    // Validate that the task models are not empty.
    Map<TaskName, LocationId> taskLocality = metadataProvider.getTaskLocality();
    Preconditions.checkArgument(!taskModels.isEmpty(), "No tasks found. Likely due to no input partitions. Can't run a job with no tasks.");

    // Invoke the default grouper when the processor locality does not exist.
    if (MapUtils.isEmpty(metadataProvider.getProcessorLocality())) {
      LOG.info("ProcessorLocality is empty. Generating with the default group method.");
      return group(taskModels, new ArrayList<>());
    }

    Map<String, LocationId> processorLocality = new TreeMap<>(metadataProvider.getProcessorLocality());
    /**
     * When there're more task models than processors then choose the lexicographically least `x` processors(where x = tasks.size()).
     */
    if (processorLocality.size() > taskModels.size()) {
      processorLocality = processorLocality.entrySet()
                                           .stream()
                                           .limit(taskModels.size())
                                           .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    Map<LocationId, List<String>> locationIdToProcessors = new HashMap<>();
    Map<String, TaskGroup> processorIdToTaskGroup = new HashMap<>();

    // Generate the {@see LocationId} to processors mapping and processorId to {@see TaskGroup} mapping.
    processorLocality.forEach((processorId, locationId) -> {
        List<String> processorIds = locationIdToProcessors.getOrDefault(locationId, new ArrayList<>());
        processorIds.add(processorId);
        locationIdToProcessors.put(locationId, processorIds);
        processorIdToTaskGroup.put(processorId, new TaskGroup(processorId, new ArrayList<>()));
      });

    int numTasksPerProcessor = taskModels.size() / processorLocality.size();
    Set<TaskName> assignedTasks = new HashSet<>();

    /**
     * A processor is considered under-assigned when number of tasks assigned to it is less than
     * (number of tasks / number of processors).
     * Map the tasks to the under-assigned processors with same locality.
     */
    for (TaskModel taskModel : taskModels) {
      LocationId taskLocationId = taskLocality.get(taskModel.getTaskName());
      if (taskLocationId != null) {
        List<String> processorIds = locationIdToProcessors.getOrDefault(taskLocationId, new ArrayList<>());
        for (String processorId : processorIds) {
          TaskGroup taskGroup = processorIdToTaskGroup.get(processorId);
          if (taskGroup.size() < numTasksPerProcessor) {
            taskGroup.addTaskName(taskModel.getTaskName().getTaskName());
            assignedTasks.add(taskModel.getTaskName());
            break;
          }
        }
      }
    }

    /**
     * In some scenarios, the task either might not have any previous locality or might not have any
     * processor that maps to its previous locality. This cyclic processorId's iterator helps us in
     * those scenarios to assign the processorIds to those kind of tasks in a round robin fashion.
     */
    Iterator<String> processorIdsCyclicIterator = Iterators.cycle(processorLocality.keySet());
    Collection<TaskGroup> taskGroups = processorIdToTaskGroup.values();

    /**
     * For the tasks left over from the previous stage, map them to any under-assigned processor.
     * When a under-assigned processor doesn't exist, then map them to any processor from available
     * processor in round robin fashion.
     */
    for (TaskModel taskModel : taskModels) {
      if (!assignedTasks.contains(taskModel.getTaskName())) {
        Optional<TaskGroup> underAssignedTaskGroup = taskGroups.stream()
                .filter(taskGroup -> taskGroup.size() < numTasksPerProcessor)
                .findFirst();
        if (underAssignedTaskGroup.isPresent()) {
          underAssignedTaskGroup.get().addTaskName(taskModel.getTaskName().getTaskName());
        } else {
          TaskGroup taskGroup = processorIdToTaskGroup.get(processorIdsCyclicIterator.next());
          taskGroup.addTaskName(taskModel.getTaskName().getTaskName());
        }
        assignedTasks.add(taskModel.getTaskName());
      }
    }

    return TaskGroup.buildContainerModels(taskModels, taskGroups);
  }
}
