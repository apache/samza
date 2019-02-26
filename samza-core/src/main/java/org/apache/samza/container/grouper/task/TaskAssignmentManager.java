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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.CoordinatorStreamKeySerde;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskModeMapping;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task assignment Manager is used to persist and read the task-to-container
 * assignment information from the coordinator stream.
 * */
public class TaskAssignmentManager {
  private static final Logger LOG = LoggerFactory.getLogger(TaskAssignmentManager.class);

  private final Map<String, String> taskNameToContainerId = new HashMap<>();
  private final Serde<String> keySerde;
  private final Serde<String> containerIdSerde;
  private final Serde<String> taskModeSerde;

  private MetadataStore taskContainerMappingMetadataStore;
  private MetadataStore taskModeMappingMetadataStore;

  /**
   * Builds the TaskAssignmentManager based upon {@link Config} and {@link MetricsRegistry}.
   * Uses {@link CoordinatorStreamValueSerde} to serialize messages before reading/writing
   * into the metadata store.
   *
   * @param config the configuration required for setting up metadata store.
   * @param metricsRegistry the registry for reporting metrics.
   */
  public TaskAssignmentManager(Config config, MetricsRegistry metricsRegistry) {
    this(config, metricsRegistry, new CoordinatorStreamKeySerde(SetTaskContainerMapping.TYPE),
         new CoordinatorStreamValueSerde(SetTaskContainerMapping.TYPE), new CoordinatorStreamValueSerde(SetTaskModeMapping.TYPE));
  }

  /**
   * Builds the LocalityManager based upon {@link Config} and {@link MetricsRegistry}.
   *
   * Uses keySerde, containerIdSerde to serialize/deserialize (key, value) pairs before reading/writing
   * into {@link MetadataStore}.
   *
   * Key and value serializer are different for yarn(uses CoordinatorStreamMessage) and standalone(uses native
   * ObjectOutputStream for serialization) modes.
   * @param config the configuration required for setting up metadata store.
   * @param metricsRegistry the registry for reporting metrics.
   * @param keySerde the key serializer.
   * @param containerIdSerde the value serializer.
   * @param taskModeSerde the task-mode serializer.
   */
  public TaskAssignmentManager(Config config, MetricsRegistry metricsRegistry, Serde<String> keySerde, Serde<String> containerIdSerde, Serde<String> taskModeSerde) {
    this.keySerde = keySerde;
    this.containerIdSerde = containerIdSerde;
    this.taskModeSerde = taskModeSerde;

    MetadataStoreFactory metadataStoreFactory = Util.getObj(new JobConfig(config).getMetadataStoreFactory(), MetadataStoreFactory.class);
    this.taskModeMappingMetadataStore = metadataStoreFactory.getMetadataStore(SetTaskModeMapping.TYPE, config, metricsRegistry);
    this.taskContainerMappingMetadataStore = metadataStoreFactory.getMetadataStore(SetTaskContainerMapping.TYPE, config, metricsRegistry);
    this.taskModeMappingMetadataStore.init();
    this.taskContainerMappingMetadataStore.init();
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
   * Method to write task container info to {@link MetadataStore}.
   *
   * @param taskName    the task name
   * @param containerId the SamzaContainer ID or {@code null} to delete the mapping
   * @param taskMode the mode of the task
   */
  public void writeTaskContainerMapping(String taskName, String containerId, TaskMode taskMode) {
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
  }

  public void close() {
    taskContainerMappingMetadataStore.close();
    taskModeMappingMetadataStore.close();
  }
}
