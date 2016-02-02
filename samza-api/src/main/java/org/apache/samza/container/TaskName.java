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

/**
 * A unique identifier of a set of a SystemStreamPartitions that have been grouped by
 * a {@link org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper}.  The
 * SystemStreamPartitionGrouper determines the TaskName for each set it creates. The TaskName class
 * should only contain the taskName. This is necessary because the ChangelogManager assumes that the taskName is
 * unique enough to identify this class, and uses it to store it in the underlying coordinator stream (as a key).
 */
public class TaskName implements Comparable<TaskName> {
  private final String taskName;

  public String getTaskName() {
    return taskName;
  }

  public TaskName(String taskName) {
    this.taskName = taskName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TaskName taskName1 = (TaskName) o;

    if (!taskName.equals(taskName1.taskName)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return taskName.hashCode();
  }

  @Override
  public String toString() {
    return taskName;
  }

  @Override
  public int compareTo(TaskName that) {
    return taskName.compareTo(that.taskName);
  }
}
