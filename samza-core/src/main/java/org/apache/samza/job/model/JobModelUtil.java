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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;


/**
 * Utility class for the {@link JobModel}
 */
public class JobModelUtil {
  private final JobModel jobModel;

  public JobModelUtil(JobModel jobModel) {
    Preconditions.checkArgument(jobModel != null, "JobModel cannot be null");

    this.jobModel = jobModel;
  }

  /**
   * Extracts the map of {@link SystemStreamPartition}s to {@link TaskName} from the {@link JobModel}
   *
   * @return the extracted map
   */
  public Map<TaskName, Set<SystemStreamPartition>> getTaskToSystemStreamPartitions() {
    Map<String, ContainerModel> containers = jobModel.getContainers();
    HashMap<TaskName, Set<SystemStreamPartition>> taskToSSPs = new HashMap<>();
    for (ContainerModel containerModel : containers.values()) {
      for (TaskName taskName : containerModel.getTasks().keySet()) {
        TaskModel taskModel = containerModel.getTasks().get(taskName);
        if (taskModel.getTaskMode() != TaskMode.Active) {
          // Avoid duplicate tasks
          continue;
        }
        taskToSSPs.putIfAbsent(taskName, new HashSet<>());
        taskToSSPs.get(taskName).addAll(taskModel.getSystemStreamPartitions());
      }
    }
    return taskToSSPs;
  }
}
