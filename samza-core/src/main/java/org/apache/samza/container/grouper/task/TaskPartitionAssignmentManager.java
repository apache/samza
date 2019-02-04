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

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetTaskPartitionMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
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

  private final ObjectMapper taskNameMapper = SamzaObjectMapper.getObjectMapper();
  private final ObjectMapper sspMapper = SamzaObjectMapper.getObjectMapper();

  private final Serde<String> valueSerde;
  private final MetadataStore metadataStore;

  /**
   * Instantiates the task partition assignment manager with the provided metricsRegistry
   * and config.
   * @param config the configuration required for connecting with the metadata store.
   * @param metricsRegistry the registry to create and report custom metrics.
   */
  public TaskPartitionAssignmentManager(Config config, MetricsRegistry metricsRegistry) {
    this(config, metricsRegistry, new CoordinatorStreamValueSerde(SetTaskPartitionMapping.TYPE));
  }

  TaskPartitionAssignmentManager(Config config, MetricsRegistry metricsRegistry, Serde<String> valueSerde) {
    this.valueSerde = valueSerde;
    MetadataStoreFactory metadataStoreFactory = Util.getObj(new JobConfig(config).getMetadataStoreFactory(), MetadataStoreFactory.class);
    this.metadataStore = metadataStoreFactory.getMetadataStore(SetTaskPartitionMapping.TYPE, config, metricsRegistry);
    this.metadataStore.init();
  }

  /**
   * Stores the task to partition assignments to the metadata store.
   * @param partition the system stream partition.
   * @param taskNames the task names to which the partition is assigned to.
   */
  public void writeTaskPartitionAssignment(SystemStreamPartition partition, List<String> taskNames) {
    // For broadcast streams, a input system stream partition will be mapped to more than one tasks in a
    // SamzaContainer. Rather than storing taskName to list of SystemStreamPartitions in metadata store, here
    // systemStreamPartition to list of taskNames is stored. This was done due to 1 MB limit on value size in kafka.
    String serializedKey = getKey(partition);
    if (taskNames == null || taskNames.isEmpty()) {
      LOG.info("Deleting the key: {} from the metadata store.", partition);
      metadataStore.delete(serializedKey);
    } else {
      try {
        String taskNameAsString = taskNameMapper.writeValueAsString(taskNames);
        byte[] taskNameAsBytes = valueSerde.toBytes(taskNameAsString);
        LOG.info("Storing the partition: {} and taskNames: {} into the metadata store.", serializedKey, taskNames);
        metadataStore.put(serializedKey, taskNameAsBytes);
      } catch (Exception e) {
        throw new SamzaException("Exception occurred when writing task to partition assignment.", e);
      }
    }
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
        LOG.info("Trying to deserialize the system stream partition: {}", entry.getKey());
        SystemStreamPartition systemStreamPartition = getSystemStreamPartition(entry.getKey());
        String taskNameAsJson = valueSerde.fromBytes(entry.getValue());
        List<String> taskNames = taskNameMapper.readValue(taskNameAsJson, new TypeReference<List<String>>() { });
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
      String sspKey = getKey(systemStreamPartition);
      metadataStore.delete(sspKey);
    }
  }

  /**
   * Closes the connections with the underlying metadata store.
   */
  public void close() {
    metadataStore.close();
  }

  private String getKey(SystemStreamPartition systemStreamPartition) {
    try {
      return sspMapper.writeValueAsString(systemStreamPartition);
    } catch (IOException e) {
      throw new SamzaException(String.format("Exception occurred when serializing the partition: %s", systemStreamPartition), e);
    }
  }

  private SystemStreamPartition getSystemStreamPartition(String partitionAsString) {
    try {
      return sspMapper.readValue(partitionAsString, SystemStreamPartition.class);
    } catch (IOException e) {
      throw new SamzaException(String.format("Exception occurred when deserializing the partition: %s", partitionAsString), e);
    }
  }
}
