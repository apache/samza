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

import org.apache.samza.container.TaskName;

import java.util.Collections;
import java.util.Map;

/**
 * <p>
 * The data model is used to define which TaskModels a SamzaContainer should
 * process. The model is used in the job coordinator and SamzaContainer to
 * determine how to execute Samza jobs.
 * </p>
 *
 * <p>
 * The hierarchy for a Samza's job data model is that jobs have containers, and
 * containers have tasks. Each data model contains relevant information, such as
 * an id, partition information, etc.
 * </p>
 * <p>
 * <b>Note</b>: This class has a natural ordering that is inconsistent with equals.
 * </p>
 */
public class ContainerModel {
  @Deprecated
  private final int containerId;
  private final String processorId;
  private final Map<TaskName, TaskModel> tasks;

  public ContainerModel(String processorId, int containerId, Map<TaskName, TaskModel> tasks) {
    this.containerId = containerId;
    if (processorId == null) {
      this.processorId = String.valueOf(containerId);
    } else {
      this.processorId = processorId;
    }
    this.tasks = Collections.unmodifiableMap(tasks);
  }

  @Deprecated
  public int getContainerId() {
    return containerId;
  }

  public String getProcessorId() {
    return processorId;
  }

  public Map<TaskName, TaskModel> getTasks() {
    return tasks;
  }

  @Override
  public String toString() {
    return "ContainerModel [processorId=" + processorId + ", tasks=" + tasks + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((processorId == null) ? 0 : processorId.hashCode());
    result = prime * result + ((tasks == null) ? 0 : tasks.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContainerModel other = (ContainerModel) obj;
    if (!processorId.equals(other.processorId))
      return false;
    if (tasks == null) {
      if (other.tasks != null)
        return false;
    } else if (!tasks.equals(other.tasks))
      return false;
    return true;
  }

}
