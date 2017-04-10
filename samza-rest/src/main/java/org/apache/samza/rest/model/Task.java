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
package org.apache.samza.rest.model;

import java.util.List;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Provides a client view of a samza task.
 * Includes the preferred preferredHost, taskName, containerId, containerId and list of partitions.
 *
 */
public class Task {

  // preferred host of the task
  private String preferredHost;

  // name of the task
  private String taskName;

  // containerId of the samza container in which the task is running
  private String containerId;

  // list of partitions that belong to the task.
  private List<Partition> partitions;

  // list of stores that are associated with the task.
  private List<String> storeNames;

  public Task() {
  }

  public Task(@JsonProperty("preferredHost") String preferredHost,
              @JsonProperty("taskName") String taskName,
              @JsonProperty("containerId") String containerId,
              @JsonProperty("partitions") List<Partition> partitions,
              @JsonProperty("storeNames") List<String> storeNames) {
    this.preferredHost = preferredHost;
    this.taskName = taskName;
    this.containerId = containerId;
    this.partitions = partitions;
    this.storeNames = storeNames;
  }

  public String getPreferredHost() {
    return preferredHost;
  }

  public void setPreferredHost(String preferredHost) {
    this.preferredHost = preferredHost;
  }

  public String getContainerId() {
    return containerId;
  }

  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  public List<Partition> getPartitions() {
    return partitions;
  }

  public void setPartitions(List<Partition> partitions) {
    this.partitions = partitions;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public List<String> getStoreNames() {
    return storeNames;
  }

  public void setStoreNames(List<String> storeNames) {
    this.storeNames = storeNames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Task)) {
      return false;
    }

    Task task = (Task) o;

    if (containerId != null && containerId.equals(task.containerId)) {
      return false;
    }
    if (!preferredHost.equals(task.preferredHost)) {
      return false;
    }
    if (!taskName.equals(task.taskName)) {
      return false;
    }
    if (!partitions.equals(task.partitions)) {
      return false;
    }
    return storeNames.equals(task.storeNames);
  }

  @Override
  public int hashCode() {
    int result = preferredHost.hashCode();
    result = 31 * result + taskName.hashCode();
    result = 31 * result + containerId.hashCode();
    result = 31 * result + partitions.hashCode();
    result = 31 * result + storeNames.hashCode();
    return result;
  }
}
