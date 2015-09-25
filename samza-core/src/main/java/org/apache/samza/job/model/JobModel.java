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
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;

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
  private static final String EMPTY_STRING = "";
  private final Config config;
  private final Map<Integer, ContainerModel> containers;

  private final LocalityManager localityManager;
  private Map<Integer, String> localityMappings = new HashMap<Integer, String>();

  public int maxChangeLogStreamPartitions;

  public JobModel(Config config, Map<Integer, ContainerModel> containers) {
    this(config, containers, null);
  }

  public JobModel(Config config, Map<Integer, ContainerModel> containers, LocalityManager localityManager) {
    this.config = config;
    this.containers = Collections.unmodifiableMap(containers);
    this.localityManager = localityManager;

    if (localityManager == null) {
      for (Integer containerId : containers.keySet()) {
        localityMappings.put(containerId, null);
      }
    } else {
      populateContainerLocalityMappings();
    }


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

  public Config getConfig() {
    return config;
  }

  /**
   * Returns the container to host mapping for a given container ID and mapping key
   *
   * @param containerId the ID of the container
   * @param key mapping key which is one of the keys declared in {@link org.apache.samza.coordinator.stream.messages.SetContainerHostMapping}
   * @return the value if it exists for a given container and key, otherwise an empty string
   */
  public String getContainerToHostValue(Integer containerId, String key) {
    if (localityManager == null) {
      return EMPTY_STRING;
    }
    final Map<String, String> mappings = localityManager.readContainerLocality().get(containerId);
    if (mappings == null) {
      return EMPTY_STRING;
    }
    if (!mappings.containsKey(key)) {
      return EMPTY_STRING;
    }
    return mappings.get(key);
  }

  private void populateContainerLocalityMappings() {
    Map<Integer, Map<String, String>> allMappings = localityManager.readContainerLocality();
    for (Integer containerId: containers.keySet()) {
      if (allMappings.containsKey(containerId)) {
        localityMappings.put(containerId, allMappings.get(containerId).get(SetContainerHostMapping.HOST_KEY));
      } else {
        localityMappings.put(containerId, null);
      }
    }
  }

  public Map<Integer, String> getAllContainerLocality() {
    if (localityManager != null) {
      populateContainerLocalityMappings();
    }
    return localityMappings;
  }

  public Map<Integer, ContainerModel> getContainers() {
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
