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
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.coordinator.stream.CoordinatorStreamKeySerde;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
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

  private final Config config;
  private SamzaContainerContext containerContext;
  private final Serde<String> keySerde;
  private final Serde<String> valueSerde;
  private MetadataStore metadataStore;
  private Map<String, String> taskNameToContainerId = new HashMap<>();

  /**
   * Builds the TaskAssignmentManager based upon {@link Config} and {@link MetricsRegistry}.
   * Uses {@link CoordinatorStreamKeySerde} and {@link CoordinatorStreamValueSerde} to
   * serialize messages before reading/writing into coordinator stream.
   *
   * @param config denotes the configuration required for setting up metadata store.
   * @param metricsRegistry the registry for reporting metrics in metadata store.
   */
  public TaskAssignmentManager(Config config, MetricsRegistry metricsRegistry) {
    this(config, metricsRegistry, new CoordinatorStreamKeySerde(SetTaskContainerMapping.TYPE),
         new CoordinatorStreamValueSerde(config, SetTaskContainerMapping.TYPE));
  }

  /**
   * Builds the LocalityManager based upon {@link Config} and {@link MetricsRegistry}.
   *
   * Uses keySerde, valueSerde to serialize/deserialize (key, value) pairs before reading/writing
   * into metadata store.
   *
   * Key and value serializer are different for yarn(uses CoordinatorStreamMessage) and standalone(uses native
   * ObjectOutputStream for serialization) modes.
   * @param config denotes the configuration required for setting up metadata store.
   * @param metricsRegistry denotes the registry for reporting metrics in metadata store.
   * @param keySerde denotes the key serializer.
   * @param valueSerde denotes the value serializer.
   */
  public TaskAssignmentManager(Config config, MetricsRegistry metricsRegistry, Serde<String> keySerde, Serde<String> valueSerde) {
    this.config = config;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    MetadataStoreFactory metadataStoreFactory = Util.getObj(new JobConfig(config).getMetadataStoreFactory(), MetadataStoreFactory.class);
    this.metadataStore = metadataStoreFactory.getMetadataStore(SetTaskContainerMapping.TYPE, config, metricsRegistry);
  }

  public void init(SamzaContainerContext containerContext) {
    this.containerContext = containerContext;
    this.metadataStore.init(containerContext);
  }

  /**
   * Method to allow read container task information from coordinator stream. This method is used
   * in {@link org.apache.samza.coordinator.JobModelManager}.
   *
   * @return the map of taskName: containerId
   */
  public Map<String, String> readTaskAssignment() {
    taskNameToContainerId.clear();
    metadataStore.all().forEach((keyBytes, valueBytes) -> {
        String taskName = keySerde.fromBytes(keyBytes);
        String containerId = valueSerde.fromBytes(valueBytes);
        if (containerId != null) {
          taskNameToContainerId.put(taskName, containerId);
        }
        LOG.debug("Assignment for task {}: {}", taskName, containerId);
      });
    return Collections.unmodifiableMap(new HashMap<>(taskNameToContainerId));
  }

  /**
   * Method to write task container info to coordinator stream.
   *
   * @param taskName    the task name
   * @param containerId the SamzaContainer ID or {@code null} to delete the mapping
   */
  public void writeTaskContainerMapping(String taskName, String containerId) {
    String existingContainerId = taskNameToContainerId.get(taskName);
    if (existingContainerId != null && !existingContainerId.equals(containerId)) {
      LOG.info("Task \"{}\" moved from container {} to container {}", new Object[]{taskName, existingContainerId, containerId});
    } else {
      LOG.debug("Task \"{}\" assigned to container {}", taskName, containerId);
    }

    if (containerId == null) {
      metadataStore.remove(keySerde.toBytes(taskName));
      taskNameToContainerId.remove(taskName);
    } else {
      metadataStore.put(keySerde.toBytes(taskName), valueSerde.toBytes(containerId));
      taskNameToContainerId.put(taskName, containerId);
    }
  }

  /**
   * Deletes the task container info from the coordinator stream for each of the specified task names.
   *
   * @param taskNames the task names for which the mapping will be deleted.
   */
  public void deleteTaskContainerMappings(Iterable<String> taskNames) {
    for (String taskName : taskNames) {
      metadataStore.remove(keySerde.toBytes(taskName));
    }
  }

  public void close() {
    metadataStore.close();
  }
}
