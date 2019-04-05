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

package org.apache.samza.storage;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.StreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for creating the changelog stream. Used for reading, writing
 * and updating the task to changelog stream partition association in metadata store.
 */
public class ChangelogStreamManager {

  private static final Logger LOG = LoggerFactory.getLogger(ChangelogStreamManager.class);

  private final MetadataStore metadataStore;
  private final CoordinatorStreamValueSerde valueSerde;

  /**
   * Builds the ChangelogStreamManager based upon the provided {@link MetadataStore} that is instantiated.
   * Setting up a metadata store instance is expensive which requires opening multiple connections
   * and reading tons of information. Fully instantiated metadata store is taken as a constructor argument
   * to reuse it across different utility classes. Uses the {@link CoordinatorStreamValueSerde} to serialize
   * messages before reading/writing into metadata store.
   *
   * @param metadataStore an instance of {@link MetadataStore} to read/write the container locality.
   */
  public ChangelogStreamManager(MetadataStore metadataStore) {
    this.metadataStore = metadataStore;
    this.valueSerde = new CoordinatorStreamValueSerde(SetChangelogMapping.TYPE);
  }

  /**
   * Reads the taskName to changelog partition assignments from the {@link MetadataStore}.
   *
   * @return TaskName to change LOG partition mapping, or an empty map if there were no messages.
   */
  public Map<TaskName, Integer> readPartitionMapping() {
    LOG.debug("Reading changelog partition information");
    final Map<TaskName, Integer> changelogMapping = new HashMap<>();
    metadataStore.all().forEach((taskName, partitionIdAsBytes) -> {
        String partitionId = valueSerde.fromBytes(partitionIdAsBytes);
        LOG.debug("TaskName: {} is mapped to {}", taskName, partitionId);
        if (partitionId != null) {
          changelogMapping.put(new TaskName(taskName), Integer.valueOf(partitionId));
        }
      });
    return changelogMapping;
  }

  /**
   * Writes the taskName to changelog partition assignments to the {@link MetadataStore}.
   * @param changelogEntries a map of the taskName to the changelog partition to be written to
   *                         metadata store.
   */
  public void writePartitionMapping(Map<TaskName, Integer> changelogEntries) {
    LOG.debug("Updating changelog information with: ");
    for (Map.Entry<TaskName, Integer> entry : changelogEntries.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      String taskName = entry.getKey().getTaskName();
      if (entry.getValue() != null) {
        String changeLogPartitionId = String.valueOf(entry.getValue());
        LOG.debug("TaskName: {} to Partition: {}", taskName, entry.getValue());
        metadataStore.put(taskName, valueSerde.toBytes(changeLogPartitionId));
      } else {
        LOG.debug("Deleting the TaskName: {}", taskName);
        metadataStore.delete(taskName);
      }
    }
  }

  /**
   * Merges the previous and the new taskName to changelog partition mapping.
   * Writes the merged taskName to partition mapping to {@link MetadataStore}.
   * @param prevChangelogEntries The previous map of taskName to changelog partition.
   * @param newChangelogEntries The new map of taskName to changelog partition.
   */
  public void updatePartitionMapping(Map<TaskName, Integer> prevChangelogEntries,
      Map<TaskName, Integer> newChangelogEntries) {
    Map<TaskName, Integer> combinedEntries = new HashMap<>(newChangelogEntries);
    combinedEntries.putAll(prevChangelogEntries);
    writePartitionMapping(combinedEntries);
  }

  /**
   * Creates and validates the changelog streams of a samza job.
   *
   * @param config the configuration with changelog info.
   * @param maxChangeLogStreamPartitions the maximum number of changelog stream partitions to create.
   */
  public static void createChangelogStreams(Config config, int maxChangeLogStreamPartitions) {
    // Get changelog store config
    JavaStorageConfig storageConfig = new JavaStorageConfig(config);
    Map<String, SystemStream> storeNameSystemStreamMapping = storageConfig.getStoreNames()
        .stream()
        .filter(name -> StringUtils.isNotBlank(storageConfig.getChangelogStream(name)))
        .collect(Collectors.toMap(name -> name,
            name -> StreamUtil.getSystemStreamFromNames(storageConfig.getChangelogStream(name))));

    // Get SystemAdmin for changelog store's system and attempt to create the stream
    SystemConfig systemConfig = new SystemConfig(config);
    storeNameSystemStreamMapping.forEach((storeName, systemStream) -> {
        // Load system admin for this system.
        SystemAdmin systemAdmin = systemConfig.getSystemAdmin(systemStream.getSystem());

        if (systemAdmin == null) {
          throw new SamzaException(String.format(
              "Error creating changelog. Changelog on store %s uses system %s, which is missing from the configuration.",
              storeName, systemStream.getSystem()));
        }

        StreamSpec changelogSpec =
            StreamSpec.createChangeLogStreamSpec(systemStream.getStream(), systemStream.getSystem(),
                maxChangeLogStreamPartitions);

        systemAdmin.start();

        if (systemAdmin.createStream(changelogSpec)) {
          LOG.info(String.format("created changelog stream %s.", systemStream.getStream()));
        } else {
          LOG.info(String.format("changelog stream %s already exists.", systemStream.getStream()));
        }
        systemAdmin.validateStream(changelogSpec);

        if (storageConfig.getAccessLogEnabled(storeName)) {
          String accesslogStream = storageConfig.getAccessLogStream(systemStream.getStream());
          StreamSpec accesslogSpec =
              new StreamSpec(accesslogStream, accesslogStream, systemStream.getSystem(), maxChangeLogStreamPartitions);
          systemAdmin.createStream(accesslogSpec);
          systemAdmin.validateStream(accesslogSpec);
        }

        systemAdmin.stop();
      });
  }
}
