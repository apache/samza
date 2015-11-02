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

package org.apache.samza.job.model;

import java.util.Collections;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;


/**
 * <p>
 * The data model used to represent a task. The model is used in the job
 * coordinator and SamzaContainer to determine how to execute Samza jobs.
 * </p>
 *
 * <p>
 * The hierarchy for a Samza's job data model is that jobs have containers, and
 * containers have tasks. Each data model contains relevant information, such as
 * an id, partition information, etc.
 * </p>
 */
public class TaskModel implements Comparable<TaskModel> {
  private final TaskName taskName;
  private final Set<SystemStreamPartition> systemStreamPartitions;
  private final Partition changelogPartition;

  public TaskModel(TaskName taskName, Set<SystemStreamPartition> systemStreamPartitions, Partition changelogPartition) {
    this.taskName = taskName;
    this.systemStreamPartitions = Collections.unmodifiableSet(systemStreamPartitions);
    this.changelogPartition = changelogPartition;
  }

  public TaskName getTaskName() {
    return taskName;
  }

  public Set<SystemStreamPartition> getSystemStreamPartitions() {
    return systemStreamPartitions;
  }

  public Partition getChangelogPartition() {
    return changelogPartition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TaskModel taskModel = (TaskModel) o;

    if (!changelogPartition.equals(taskModel.changelogPartition)) {
      return false;
    }
    if (!systemStreamPartitions.equals(taskModel.systemStreamPartitions)) {
      return false;
    }
    if (!taskName.equals(taskModel.taskName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = taskName.hashCode();
    result = 31 * result + systemStreamPartitions.hashCode();
    result = 31 * result + changelogPartition.hashCode();
    return result;
  }

  @Override

  public String toString() {
    return "TaskModel [taskName=" + taskName + ", systemStreamPartitions=" + systemStreamPartitions + ", changeLogPartition=" + changelogPartition + "]";
  }

  public int compareTo(TaskModel other) {
    return taskName.compareTo(other.getTaskName());
  }
}
