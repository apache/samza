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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.coordinator.stream.CoordinatorStreamManager;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.StreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Changelog manager creates the changelog stream. If a coordinator stream manager is provided,
 * it can be used to read, write and update the changelog stream partition-to-task mapping.
 */
public class ChangelogStreamManager {

  private static final Logger LOG = LoggerFactory.getLogger(ChangelogStreamManager.class);
  // This is legacy for changelog. Need to investigate what happens if you use a different source name
  private static final String SOURCE = "JobModelManager";

  private final CoordinatorStreamManager coordinatorStreamManager;

  /**
   * Construct changelog manager with a bootstrapped coordinator stream.
   *
   * @param coordinatorStreamManager Coordinator stream manager.
   */
  public ChangelogStreamManager(CoordinatorStreamManager coordinatorStreamManager) {
    this.coordinatorStreamManager = coordinatorStreamManager;
  }

  /**
   * Read the taskName to partition mapping that is being maintained by this ChangelogManager
   * @return TaskName to change LOG partition mapping, or an empty map if there were no messages.
   */
  public Map<TaskName, Integer> readPartitionMapping() {
    LOG.debug("Reading changelog partition information");
    final HashMap<TaskName, Integer> changelogMapping = new HashMap<>();
    for (CoordinatorStreamMessage coordinatorStreamMessage : coordinatorStreamManager.getBootstrappedStream(SetChangelogMapping.TYPE)) {
      SetChangelogMapping changelogMapEntry = new SetChangelogMapping(coordinatorStreamMessage);
      changelogMapping.put(new TaskName(changelogMapEntry.getTaskName()), changelogMapEntry.getPartition());
      LOG.debug("TaskName: {} is mapped to {}", changelogMapEntry.getTaskName(), changelogMapEntry.getPartition());
    }
    return changelogMapping;
  }

  /**
   * Write the taskName to partition mapping.
   * @param changelogEntries The entries that needs to be written to the coordinator stream, the map takes the taskName
   *                         and it's corresponding changelog partition.
   */
  public void writePartitionMapping(Map<TaskName, Integer> changelogEntries) {
    LOG.debug("Updating changelog information with: ");
    for (Map.Entry<TaskName, Integer> entry : changelogEntries.entrySet()) {
      LOG.debug("TaskName: {} to Partition: {}", entry.getKey().getTaskName(), entry.getValue());
      coordinatorStreamManager.send(new SetChangelogMapping(SOURCE, entry.getKey().getTaskName(), entry.getValue()));
    }
  }

  /**
   * Merge previous and new taskName to partition mapping and write it.
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
   * Utility method to create and validate changelog streams. The method is static because it does not require an
   * instance of the {@link CoordinatorStreamManager}
   * @param config Config with changelog info
   * @param maxChangeLogStreamPartitions Maximum changelog stream partitions to create
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
    JavaSystemConfig systemConfig = new JavaSystemConfig(config);
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
