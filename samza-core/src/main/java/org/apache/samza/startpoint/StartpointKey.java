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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;


final class StartpointKey {
  private final SystemStreamPartition systemStreamPartition;
  private final TaskName taskName;

  // Constructs a startpoint key with SSP. This means the key will apply to all tasks that are mapped to this SSP
  StartpointKey(SystemStreamPartition systemStreamPartition) {
    this(systemStreamPartition, null);
  }

  // Constructs a startpoint key with SSP and a task.
  StartpointKey(SystemStreamPartition systemStreamPartition, TaskName taskName) {
    this.systemStreamPartition = systemStreamPartition;
    this.taskName = taskName;
  }

  @JsonIgnore
  SystemStreamPartition getSystemStreamPartition() {
    return systemStreamPartition;
  }

  @JsonProperty("system")
  String getSystem() {
    return systemStreamPartition.getSystem();
  }

  @JsonProperty("stream")
  String getStream() {
    return systemStreamPartition.getStream();
  }

  @JsonProperty("partition")
  Integer getPartition() {
    return systemStreamPartition.getPartition().getPartitionId();
  }

  @JsonProperty("taskName")
  String getTaskName() {
    return taskName != null ? taskName.getTaskName() : null;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
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
