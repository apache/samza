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
package org.apache.samza.startpoint;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;


class StartpointKey {
  private final SystemStreamPartition systemStreamPartition;
  private final TaskName taskName;

  StartpointKey(SystemStreamPartition systemStreamPartition) {
    this(systemStreamPartition, null);
  }

  StartpointKey(SystemStreamPartition systemStreamPartition, TaskName taskName) {
    this.systemStreamPartition = systemStreamPartition;
    this.taskName = taskName;
  }

  SystemStreamPartition getSystemStreamPartition() {
    return systemStreamPartition;
  }

  TaskName getTaskName() {
    return taskName;
  }

  @Override
  public String toString() {
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    listBuilder.add("StartpointKey")
        .add(systemStreamPartition.getSystem())
        .add(systemStreamPartition.getStream())
        .add(Integer.toString(systemStreamPartition.getPartition().getPartitionId()));

    if (taskName != null && StringUtils.isNotBlank(taskName.getTaskName())) {
      listBuilder.add(taskName.getTaskName());
    }

    return String.join(":", listBuilder.build());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StartpointKey that = (StartpointKey) o;
    return Objects.equal(systemStreamPartition, that.systemStreamPartition) && Objects.equal(taskName, that.taskName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(systemStreamPartition, taskName);
  }
}
