package org.apache.samza.container.grouper.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;


public class SimpleGroupByContainerCount implements TaskNameGrouper {
  private final int startContainerCount;
  public SimpleGroupByContainerCount() {
    this.startContainerCount = 1;
  }
  public SimpleGroupByContainerCount(int containerCount) {
    if (containerCount <= 0) throw new IllegalArgumentException("Must have at least one container");
    this.startContainerCount = containerCount;
  }

  @Override
  public Set<ContainerModel> group(Set<TaskModel> tasks) {
    List<Integer> containerIds = new ArrayList<>(startContainerCount);
    for (int i = 0; i < startContainerCount; i++) {
      containerIds.add(i);
    }
    return group(tasks, containerIds);
  }

  public Set<ContainerModel> group(Set<TaskModel> tasks, List<Integer> containersIds) {
    if(containersIds == null)
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
      containerModels.add(new ContainerModel(containersIds.get(i), taskGroups[i]));
    }

    return Collections.unmodifiableSet(containerModels);
  }
}
