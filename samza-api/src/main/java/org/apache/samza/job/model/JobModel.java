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
import java.util.Map;
import org.apache.samza.config.Config;

/**
 * <p>
 * The data model used to represent a Samza job. The model is used in the job
 * coordinator and SamzaContainer to determine how to execute Samza jobs.
 * </p>
 *
 * <p>
 * The hierarchy for a Samza's job data model is that jobs have containers, and
 * containers have tasks. Each data model contains relevant information, such as
 * an id, partition information, etc.
 * </p>
 */
public class JobModel {
  private final Config config;
  private final Map<String, ContainerModel> containers;

  public int maxChangeLogStreamPartitions;

  public JobModel(Config config, Map<String, ContainerModel> containers) {
    this.config = config;
    this.containers = Collections.unmodifiableMap(containers);

    // Compute the number of change log stream partitions as the maximum partition-id
    // of all total number of tasks of the job; Increment by 1 because partition ids
    // start from 0 while we need the absolute count.
    this.maxChangeLogStreamPartitions = 0;
    for (ContainerModel container: containers.values()) {
      for (TaskModel task: container.getTasks().values()) {
        if (this.maxChangeLogStreamPartitions < task.getChangelogPartition().getPartitionId() + 1)
          this.maxChangeLogStreamPartitions = task.getChangelogPartition().getPartitionId() + 1;
      }
    }
  }

  public int getMaxChangeLogStreamPartitions() {
    return maxChangeLogStreamPartitions;
  }

  public Config getConfig() {
    return config;
  }

  public Map<String, ContainerModel> getContainers() {
    return containers;
  }

  @Override
  public String toString() {
    return "JobModel [config=" + config + ", containers=" + containers + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((config == null) ? 0 : config.hashCode());
    result = prime * result + ((containers == null) ? 0 : containers.hashCode());
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
    JobModel other = (JobModel) obj;
    if (config == null) {
      if (other.config != null)
        return false;
    } else if (!config.equals(other.config))
      return false;
    if (containers == null) {
      if (other.containers != null)
        return false;
    } else if (!containers.equals(other.containers))
      return false;
    return true;
  }
}
