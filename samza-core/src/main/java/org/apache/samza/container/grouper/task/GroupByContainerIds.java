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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
   */
  @Override
  public Set<ContainerModel> group(Set<TaskModel> taskModels, GrouperContext grouperContext) {
    Map<TaskName, LocationId> taskLocality = grouperContext.getTaskLocality();

    Preconditions.checkArgument(!taskModels.isEmpty(), "No tasks found. Likely due to no input partitions. Can't run a job with no tasks.");

    if (MapUtils.isEmpty(grouperContext.getProcessorLocality())) {
      LOG.info("ProcessorLocality is empty. Generating with the default group method.");
      return group(taskModels, new ArrayList<>());
    }

    Map<String, LocationId> processorLocality;
    if (grouperContext.getProcessorLocality().size() > taskModels.size()) {
      processorLocality = grouperContext.getProcessorLocality()
                                        .entrySet()
                                        .stream()
                                        .sorted(Comparator.comparing(Map.Entry::getKey))
                                        .limit(taskModels.size())
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } else {
      processorLocality = grouperContext.getProcessorLocality();
    }

    Map<LocationId, List<String>> locationIdToProcessors = new HashMap<>();
    Map<String, TaskGroup> processorIdToTaskGroup = new HashMap<>();

    processorLocality.forEach((processorId, locationId) -> {
        List<String> processorIds = locationIdToProcessors.getOrDefault(locationId, new ArrayList<>());
        processorIds.add(processorId);
        locationIdToProcessors.put(locationId, processorIds);
        processorIdToTaskGroup.put(processorId, new TaskGroup(processorId, new ArrayList<>()));
      });

    int numTasksPerProcessor = taskModels.size() / processorLocality.size();
    Set<TaskName> assignedTasks = new HashSet<>();
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

    Iterator<String> processorIdsCyclicIterator = Iterators.cycle(processorLocality.keySet());
    Collection<TaskGroup> taskGroups = processorIdToTaskGroup.values();
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

    return buildContainerModels(taskModels, taskGroups);
  }


  /**
   * Converts the {@link TaskGroup} list to a set of ContainerModel.
   *
   * @param tasks         the TaskModels to assign to the ContainerModels.
   * @param taskGroups    the TaskGroups defining how the tasks should be grouped.
   * @return              a set of ContainerModels.
   */
  private Set<ContainerModel> buildContainerModels(Set<TaskModel> tasks, Collection<TaskGroup> taskGroups) {
    // Map task names to models
    Map<String, TaskModel> taskNameToModel = new HashMap<>();
    for (TaskModel model : tasks) {
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
