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

import java.util.Arrays;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Simple grouper.
 * It exposes two group methods - one that assumes sequential container numbers and one that gets a set of container
 * IDs as an argument. Please note - this first implementation ignores locality information.
 */
public class GroupByContainerIds implements TaskNameGrouper {
  private final int startContainerCount;
  public GroupByContainerIds(int count) {
    this.startContainerCount = count;
  }

  @Override
  public Set<ContainerModel> group(Set<TaskModel> tasks) {
    if (tasks.isEmpty())
      throw new IllegalArgumentException("cannot group an empty set");

    if (startContainerCount > tasks.size())
      throw new IllegalArgumentException("number of containers="  + startContainerCount + " is bigger than number of tasks=" + tasks.size());

    List<String> containerIds = new ArrayList<>(startContainerCount);
    for (int i = 0; i < startContainerCount; i++) {
      containerIds.add(String.valueOf(i));
    }
    return group(tasks, containerIds);
  }

  public Set<ContainerModel> group(Set<TaskModel> tasks, List<String> containersIds) {
    if (tasks.isEmpty())
      throw new IllegalArgumentException("cannot group an empty set. containersIds=" + Arrays
          .toString(containersIds.toArray()));

    if (containersIds.size() > tasks.size())
      throw new IllegalArgumentException("number of containers "  + containersIds.size() + " is bigger than number of tasks " + tasks.size());

    if (containersIds == null)
      return this.group(tasks);

    int containerCount = containersIds.size();

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
      containerModels.add(new ContainerModel(String.valueOf(containersIds.get(i)), taskGroups[i]));
    }

    return Collections.unmodifiableSet(containerModels);
  }
}
