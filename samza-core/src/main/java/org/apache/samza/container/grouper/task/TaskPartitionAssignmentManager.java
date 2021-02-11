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
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetTaskPartitionMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemStreamPartition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used to persist and read the task-to-partition assignment information
 * into the metadata store.
 */
public class TaskPartitionAssignmentManager {

  private static final Logger LOG = LoggerFactory.getLogger(TaskPartitionAssignmentManager.class);

  private final ObjectMapper taskNamesMapper = SamzaObjectMapper.getObjectMapper();
  private final ObjectMapper sspMapper = SamzaObjectMapper.getObjectMapper();

  private final Serde<String> valueSerde;
  private final MetadataStore metadataStore;

  /**
   * Builds the TaskPartitionAssignmentManager based upon the provided {@link MetadataStore} that is instantiated.
   * Setting up a metadata store instance is expensive which requires opening multiple connections
   * and reading tons of information. Fully instantiated metadata store is taken as a constructor argument
   * to reuse it across different utility classes. Uses the {@link CoordinatorStreamValueSerde} to serialize
   * messages before reading/writing into metadata store.
   *
   * @param metadataStore an instance of {@link MetadataStore} used to read/write the task to partition assignments.
   */
  public TaskPartitionAssignmentManager(MetadataStore metadataStore) {
    Preconditions.checkNotNull(metadataStore, "Metdatastore cannot be null.");

    this.metadataStore = metadataStore;
    this.valueSerde = new CoordinatorStreamValueSerde(SetTaskPartitionMapping.TYPE);
  }

  /**
   * Stores the task names to {@link SystemStreamPartition} assignments to the metadata store.
   * @param sspToTaskNameMapping the mapped assignments to write to the metadata store. If the task name list is empty,
   *                             then the entry is deleted from the metadata store.
   */
  public void writeTaskPartitionAssignments(Map<SystemStreamPartition, List<String>> sspToTaskNameMapping) {
    for (SystemStreamPartition partition: sspToTaskNameMapping.keySet()) {
      List<String> taskNames = sspToTaskNameMapping.get(partition);
      // For broadcast streams, a input system stream partition will be mapped to more than one tasks in a
      // SamzaContainer. Rather than storing taskName to list of SystemStreamPartitions in metadata store, here
      // systemStreamPartition to list of taskNames is stored. This was done due to 1 MB limit on value size in kafka.
      String serializedSSPAsJson = serializeSSPToJson(partition);
      if (taskNames == null || taskNames.isEmpty()) {
        LOG.info("Deleting the key: {} from the metadata store.", partition);
        metadataStore.delete(serializedSSPAsJson);
      } else {
        try {
          String taskNamesAsString = taskNamesMapper.writeValueAsString(taskNames);
          byte[] taskNamesAsBytes = valueSerde.toBytes(taskNamesAsString);
          LOG.info("Storing the partition: {} and taskNames: {} into the metadata store.", serializedSSPAsJson, taskNames);
          metadataStore.put(serializedSSPAsJson, taskNamesAsBytes);
        } catch (Exception e) {
          throw new SamzaException("Exception occurred when writing task to partition assignment.", e);
        }
      }
    }
    metadataStore.flush();
  }

  /**
   * Reads the task partition assignments from the underlying storage layer.
   * @return the task partition assignments.
   */
  public Map<SystemStreamPartition, List<String>> readTaskPartitionAssignments() {
    try {
      Map<SystemStreamPartition, List<String>> sspToTaskNamesMap = new HashMap<>();
      Map<String, byte[]> allMetadataEntries = metadataStore.all();
      for (Map.Entry<String, byte[]> entry : allMetadataEntries.entrySet()) {
        SystemStreamPartition systemStreamPartition = deserializeSSPFromJson(entry.getKey());
        String taskNamesAsJson = valueSerde.fromBytes(entry.getValue());
        List<String> taskNames = taskNamesMapper.readValue(taskNamesAsJson, new TypeReference<List<String>>() { });
        sspToTaskNamesMap.put(systemStreamPartition, taskNames);
      }
      return sspToTaskNamesMap;
    } catch (Exception e) {
      throw new SamzaException("Exception occurred when reading task partition assignments.", e);
    }
  }

  /**
   * Deletes the system stream partitions from the underlying metadata store.
   * @param systemStreamPartitions the system stream partitions to delete.
   */
  public void delete(Iterable<SystemStreamPartition> systemStreamPartitions) {
    for (SystemStreamPartition systemStreamPartition : systemStreamPartitions) {
      LOG.info("Deleting the partition: {} from store.", systemStreamPartition);
      String serializedSSPAsJson = serializeSSPToJson(systemStreamPartition);
      metadataStore.delete(serializedSSPAsJson);
    }
    metadataStore.flush();
  }

  /**
   * Closes the connections with the underlying metadata store.
   */
  public void close() {
    metadataStore.close();
  }

  /**
   * Serializes the {@param SystemStreamPartition} to json string.
   * @param systemStreamPartition represents the input system stream partition.
   * @return the SystemStreamPartition serialized to json.
   */
  private String serializeSSPToJson(SystemStreamPartition systemStreamPartition) {
    try {
      return sspMapper.writeValueAsString(systemStreamPartition);
    } catch (IOException e) {
      throw new SamzaException(String.format("Exception occurred when serializing the partition: %s", systemStreamPartition), e);
    }
  }

  /**
   * Deserializes the {@param sspAsJson} in json format to {@link SystemStreamPartition}.
   * @param sspAsJson the serialized SystemStreamPartition in json format.
   * @return the deserialized SystemStreamPartition.
   */
  private SystemStreamPartition deserializeSSPFromJson(String sspAsJson) {
    try {
      return sspMapper.readValue(sspAsJson, SystemStreamPartition.class);
    } catch (IOException e) {
      throw new SamzaException(String.format("Exception occurred when deserializing the partition: %s", sspAsJson), e);
    }
  }
}
