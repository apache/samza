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

import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.runtime.LocationId;

import java.util.Map;
import java.util.Set;

/**
 * State that is required to group the taskModels to containerModels in a samza job.
 * Holds the task models and the locality information of the processors and tasks.
 */
public class TaskNameGrouperContext {
  private final Set<TaskModel> taskModels;
  private final Map<TaskName, LocationId> taskLocality;
  private final Map<String, LocationId> processorLocality;

  public TaskNameGrouperContext(Set<TaskModel> taskModels, Map<TaskName, LocationId> taskLocality, Map<String, LocationId> processorLocality) {
    this.taskModels = taskModels;
    this.taskLocality = taskLocality;
    this.processorLocality = processorLocality;
  }

  public Set<TaskModel> getTaskModels() {
    return taskModels;
  }

  public Map<TaskName, LocationId> getTaskLocality() {
    return taskLocality;
  }

  public Map<String, LocationId> getProcessorLocality() {
    return processorLocality;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TaskNameGrouperContext that = (TaskNameGrouperContext) o;

    if (taskModels != null ? !taskModels.equals(that.taskModels) : that.taskModels != null) return false;
    if (taskLocality != null ? !taskLocality.equals(that.taskLocality) : that.taskLocality != null) return false;
    return processorLocality != null ? processorLocality.equals(that.processorLocality) : that.processorLocality == null;
  }

  @Override
  public int hashCode() {
    int result = taskModels != null ? taskModels.hashCode() : 0;
    result = 31 * result + (taskLocality != null ? taskLocality.hashCode() : 0);
    result = 31 * result + (processorLocality != null ? processorLocality.hashCode() : 0);
    return result;
  }

}
