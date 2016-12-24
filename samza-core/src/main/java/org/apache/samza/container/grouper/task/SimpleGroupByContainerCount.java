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


public class SimpleGroupByContainerCount implements BalancingTaskNameGrouper {
  private final int containerCount;
  public SimpleGroupByContainerCount() {
    this.containerCount = 1;
  }
  public SimpleGroupByContainerCount(int containerCount) {
    if (containerCount <= 0) throw new IllegalArgumentException("Must have at least one container");
    this.containerCount = containerCount;
  }

  @Override
  public Set<ContainerModel> group(Set<TaskModel> tasks) {

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
      containerModels.add(new ContainerModel(i, taskGroups[i]));
    }

    return Collections.unmodifiableSet(containerModels);
  }

  @Override
  public Set<ContainerModel> balance(Set<TaskModel> tasks, LocalityManager localityManager) {
    return null;
  }
}
