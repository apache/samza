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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage.SetChangelogMapping;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Changelog manager is used to persist and read the changelog information from the coordinator stream.
 */
public class ChangelogPartitionManager {

  private static final Logger log = LoggerFactory.getLogger(ChangelogPartitionManager.class);
  private final CoordinatorStreamSystemProducer coordinatorStreamProducer;
  private final CoordinatorStreamSystemConsumer coordinatorStreamConsumer;
  private boolean isCoordinatorConsumerRegistered = false;
  private String source;

  public ChangelogPartitionManager(CoordinatorStreamSystemProducer coordinatorStreamProducer,
      CoordinatorStreamSystemConsumer coordinatorStreamConsumer) {
    this.coordinatorStreamConsumer = coordinatorStreamConsumer;
    this.coordinatorStreamProducer = coordinatorStreamProducer;
    this.source = "Unknown";
  }

  public ChangelogPartitionManager(CoordinatorStreamSystemProducer coordinatorStreamProducer,
      CoordinatorStreamSystemConsumer coordinatorStreamConsumer,
      String source) {
    this(coordinatorStreamProducer, coordinatorStreamConsumer);
    this.source = source;
  }

  public void start() {
    coordinatorStreamProducer.start();
    coordinatorStreamConsumer.start();
  }

  public void stop() {
    coordinatorStreamConsumer.stop();
    coordinatorStreamProducer.stop();
  }

  /**
   * Registers this manager to write changelog mapping for a particular task.
   * @param taskName The taskname to be registered for changelog mapping.
   */
  public void register(TaskName taskName) {
    log.debug("Adding taskName {} to {}", taskName, this);
    if(!isCoordinatorConsumerRegistered) {
      coordinatorStreamConsumer.register();
      isCoordinatorConsumerRegistered = true;
    }
    coordinatorStreamProducer.register(taskName.getTaskName());
  }

  /**
   * Read the taskName to partition mapping that is being maintained by this ChangelogManager
   * @return TaskName to change log partition mapping, or an empty map if there were no messages.
   */
  public Map<TaskName, Integer> readChangeLogPartitionMapping() {
    log.debug("Reading changelog partition information");
    Set<CoordinatorStreamMessage> bootstrappedStream = coordinatorStreamConsumer.getBootstrappedStream(SetChangelogMapping.TYPE);
    HashMap<TaskName, Integer> changelogMapping = new HashMap<TaskName, Integer>();
    for (CoordinatorStreamMessage coordinatorStreamMessage : bootstrappedStream) {
      SetChangelogMapping changelogMapEntry = new SetChangelogMapping(coordinatorStreamMessage);
      changelogMapping.put(new TaskName(changelogMapEntry.getTaskName()), changelogMapEntry.getPartition());
      log.debug("TaskName: {} is mapped to {}", changelogMapEntry.getTaskName(), changelogMapEntry.getPartition());
    }
    return changelogMapping;
  }

  /**
   * Write the taskName to partition mapping that is being maintained by this ChangelogManager
   * @param changelogEntries The entries that needs to be written to the coordinator stream, the map takes the taskName
   *                       and it's corresponding changelog partition.
   */
  public void writeChangeLogPartitionMapping(Map<TaskName, Integer> changelogEntries) {
    log.debug("Updating changelog information with: ");
    for (Map.Entry<TaskName, Integer> entry : changelogEntries.entrySet()) {
      log.debug("TaskName: {} to Partition: {}", entry.getKey().getTaskName(), entry.getValue());
      SetChangelogMapping changelogMapping = new SetChangelogMapping(source,
          entry.getKey().getTaskName(),
          entry.getValue());
      coordinatorStreamProducer.send(changelogMapping);
    }
  }

}
