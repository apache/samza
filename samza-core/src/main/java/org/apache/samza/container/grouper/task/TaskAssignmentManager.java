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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskModeMapping;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.serializers.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task assignment Manager is used to persist and read the task-to-container
 * assignment information from the coordinator stream.
 * */
public class TaskAssignmentManager {
  private static final Logger LOG = LoggerFactory.getLogger(TaskAssignmentManager.class);

  private final Map<String, String> taskNameToContainerId = new HashMap<>();
  private final Serde<String> containerIdSerde;
  private final Serde<String> taskModeSerde;

  private MetadataStore taskContainerMappingMetadataStore;
  private MetadataStore taskModeMappingMetadataStore;

  /**
   * Builds the TaskAssignmentManager based upon the provided {@link MetadataStore} that is instantiated.
   * Setting up a metadata store instance is expensive which requires opening multiple connections
   * and reading tons of information. Fully instantiated metadata store is taken as a constructor argument
   * to reuse it across different utility classes. Uses the {@link CoordinatorStreamValueSerde} to serialize
   * messages before reading/writing into metadata store.
   *
   * @param taskContainerMappingMetadataStore an instance of {@link MetadataStore} used to read/write the task to container assignments.
   * @param taskModeMappingMetadataStore an instance of {@link MetadataStore} used to read/write the task to mode  assignments.
   */
  public TaskAssignmentManager(MetadataStore taskContainerMappingMetadataStore, MetadataStore taskModeMappingMetadataStore) {
    Preconditions.checkNotNull(taskContainerMappingMetadataStore, "Metadata store cannot be null");
    Preconditions.checkNotNull(taskModeMappingMetadataStore, "Metadata store cannot be null");

    this.taskModeMappingMetadataStore = taskModeMappingMetadataStore;
    this.taskContainerMappingMetadataStore = taskContainerMappingMetadataStore;
    this.containerIdSerde = new CoordinatorStreamValueSerde(SetTaskContainerMapping.TYPE);
    this.taskModeSerde = new CoordinatorStreamValueSerde(SetTaskModeMapping.TYPE);
  }

  /**
   * Method to allow read container task information from {@link MetadataStore}. This method is used in {@link org.apache.samza.coordinator.JobModelManager}.
   *
   * @return the map of taskName: containerId
   */
  public Map<String, String> readTaskAssignment() {
    taskNameToContainerId.clear();
    taskContainerMappingMetadataStore.all().forEach((taskName, valueBytes) -> {
      String containerId = containerIdSerde.fromBytes(valueBytes);
      if (containerId != null) {
        taskNameToContainerId.put(taskName, containerId);
      }
      LOG.debug("Assignment for task {}: {}", taskName, containerId);
    });
    return Collections.unmodifiableMap(new HashMap<>(taskNameToContainerId));
  }

  public Map<TaskName, TaskMode> readTaskModes() {
    Map<TaskName, TaskMode> taskModeMap = new HashMap<>();
    taskModeMappingMetadataStore.all().forEach((taskName, valueBytes) -> {
      String taskMode = taskModeSerde.fromBytes(valueBytes);
      if (taskMode != null) {
        taskModeMap.put(new TaskName(taskName), TaskMode.valueOf(taskMode));
      }
      LOG.debug("Task mode assignment for task {}: {}", taskName, taskMode);
    });
    return Collections.unmodifiableMap(new HashMap<>(taskModeMap));
  }

  /**
   * Method to batch write task container info to {@link MetadataStore}.
   * @param mappings the task and container mappings: (ContainerId, (TaskName, TaskMode))
   */
  public void writeTaskContainerMappings(Map<String, Map<String, TaskMode>> mappings) {
    for (String containerId : mappings.keySet()) {
      Map<String, TaskMode> tasks = mappings.get(containerId);
      for (String taskName : tasks.keySet()) {
        TaskMode taskMode = tasks.get(taskName);
        LOG.info("Storing task: {} and container ID: {} into metadata store", taskName, containerId);
        String existingContainerId = taskNameToContainerId.get(taskName);
        if (existingContainerId != null && !existingContainerId.equals(containerId)) {
          LOG.info("Task \"{}\" in mode {} moved from container {} to container {}", new Object[]{taskName, taskMode, existingContainerId, containerId});
        } else {
          LOG.debug("Task \"{}\" in mode {} assigned to container {}", taskName, taskMode, containerId);
        }

        if (containerId == null) {
          taskContainerMappingMetadataStore.delete(taskName);
          taskModeMappingMetadataStore.delete(taskName);
          taskNameToContainerId.remove(taskName);
        } else {
          taskContainerMappingMetadataStore.put(taskName, containerIdSerde.toBytes(containerId));
          taskModeMappingMetadataStore.put(taskName, taskModeSerde.toBytes(taskMode.toString()));
          taskNameToContainerId.put(taskName, containerId);
        }
      }
    }
    taskContainerMappingMetadataStore.flush();
    taskModeMappingMetadataStore.flush();
  }

  /**
   * Deletes the task container info from the {@link MetadataStore} for the task names.
   *
   * @param taskNames the task names for which the mapping will be deleted.
   */
  public void deleteTaskContainerMappings(Iterable<String> taskNames) {
    for (String taskName : taskNames) {
      taskContainerMappingMetadataStore.delete(taskName);
      taskModeMappingMetadataStore.delete(taskName);
      taskNameToContainerId.remove(taskName);
    }
    taskContainerMappingMetadataStore.flush();
    taskModeMappingMetadataStore.flush();
  }

  public void close() {
    taskContainerMappingMetadataStore.close();
    taskModeMappingMetadataStore.close();
  }
}
