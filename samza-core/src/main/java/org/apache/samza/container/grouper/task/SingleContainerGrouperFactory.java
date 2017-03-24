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

import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SingleContainerGrouperFactory implements TaskNameGrouperFactory {
  @Override
  public TaskNameGrouper build(Config config) {
    return new SingleContainerGrouper(config.get(JobConfig.PROCESSOR_ID()));
  }
}

class SingleContainerGrouper implements TaskNameGrouper {
  private final String containerId;

  SingleContainerGrouper(String containerId) {
    this.containerId = containerId;
  }

  @Override
  public Set<ContainerModel> group(Set<TaskModel> taskModels) {
    Map<TaskName, TaskModel> taskNameTaskModelMap = new HashMap<>();
    for (TaskModel taskModel: taskModels) {
      taskNameTaskModelMap.put(taskModel.getTaskName(), taskModel);
    }
    ContainerModel containerModel = new ContainerModel(containerId, taskNameTaskModelMap);
    return Collections.singleton(containerModel);
  }
}
