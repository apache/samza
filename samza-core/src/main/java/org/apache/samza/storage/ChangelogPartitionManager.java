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
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.apache.samza.coordinator.stream.AbstractCoordinatorStreamManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Changelog manager is used to persist and read the changelog information from the coordinator stream.
 */
public class ChangelogPartitionManager extends AbstractCoordinatorStreamManager {

  private static final Logger log = LoggerFactory.getLogger(ChangelogPartitionManager.class);
  private boolean isCoordinatorConsumerRegistered = false;

  public ChangelogPartitionManager(CoordinatorStreamSystemProducer coordinatorStreamProducer,
      CoordinatorStreamSystemConsumer coordinatorStreamConsumer,
      String source) {
    super(coordinatorStreamProducer, coordinatorStreamConsumer, source);
  }

  /**
   * Registers this manager to write changelog mapping for a particular task.
   * @param taskName The taskname to be registered for changelog mapping.
   */
  public void register(TaskName taskName) {
    log.debug("Adding taskName {} to {}", taskName, this);
    if (!isCoordinatorConsumerRegistered) {
      registerCoordinatorStreamConsumer();
      isCoordinatorConsumerRegistered = true;
    }
    registerCoordinatorStreamProducer(taskName.getTaskName());
  }

  /**
   * Read the taskName to partition mapping that is being maintained by this ChangelogManager
   * @return TaskName to change log partition mapping, or an empty map if there were no messages.
   */
  public Map<TaskName, Integer> readChangeLogPartitionMapping() {
    log.debug("Reading changelog partition information");
    final HashMap<TaskName, Integer> changelogMapping = new HashMap<TaskName, Integer>();
    for (CoordinatorStreamMessage coordinatorStreamMessage : getBootstrappedStream(SetChangelogMapping.TYPE)) {
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
      send(new SetChangelogMapping(getSource(), entry.getKey().getTaskName(), entry.getValue()));
    }
  }

  /**
   * Create and validate changelog streams
   * @param jobModel JobModel with changelog information
   */
  public void createChangeLogStreams(JobModel jobModel) {
    createChangeLogStreams(getSource(), jobModel);
  }

  /**
   * Create and validate changelog streams
   * @param source Source name that creates the changelog stream
   * @param jobModel JobModel with changelog information
   */
  public static void createChangeLogStreams(String source, JobModel jobModel) {
    // Get changelog store config
    JavaStorageConfig storageConfig = new JavaStorageConfig(jobModel.getConfig());
    Map<String, SystemStream> storeNameSystemStreamMapping =
        storageConfig.getStoreNames()
            .stream()
            .filter(name -> StringUtils.isNotBlank(storageConfig.getChangelogStream(name)))
            .collect(Collectors.toMap(name -> name,
                name -> Util.getSystemStreamFromNames(storageConfig.getChangelogStream(name))));

    // Get SystemAdmin for changelog store's system and attempt to create the stream
    JavaSystemConfig systemConfig = new JavaSystemConfig(jobModel.getConfig());
    Map<String, SystemAdmin> systemAdminMapping = systemConfig.getSystemAdmins();
    storeNameSystemStreamMapping.forEach((storeName, systemStream) -> {
        SystemAdmin systemAdmin = systemAdminMapping.get(systemStream.getSystem());
        if (systemAdmin == null) {
          throw new SamzaException(String.format("Error creating changelog from %s. Changelog on store %s uses system %s, which is missing from the configuration.",
              source, storeName, systemStream.getSystem()));
        }

        StreamSpec changelogSpec = StreamSpec.createChangeLogStreamSpec(systemStream.getStream(), systemStream.getSystem(),
            jobModel.maxChangeLogStreamPartitions);
        if (systemAdmin.createStream(changelogSpec)) {
          log.info(String.format("%s created changelog stream %s.", source, systemStream.getStream()));
        } else {
          log.info(String.format("changelog stream %s already exists.", systemStream.getStream()));
        }
        systemAdmin.validateStream(changelogSpec);

        if (storageConfig.getAccessLogEnabled(storeName)) {
          String accesslogStream = storageConfig.getAccessLogStream(systemStream.getStream());
          StreamSpec accesslogSpec = new StreamSpec(accesslogStream, accesslogStream, systemStream.getSystem(),
              jobModel.maxChangeLogStreamPartitions);
          systemAdmin.createStream(accesslogSpec);
          systemAdmin.validateStream(accesslogSpec);
        }
      });
  }

  public static ChangelogPartitionManager fromConfig(Config config, String source, MetricsRegistryMap registryMap) {
    CoordinatorStreamSystemConsumer consumer = new CoordinatorStreamSystemConsumer(config, registryMap);
    CoordinatorStreamSystemProducer producer = new CoordinatorStreamSystemProducer(config, registryMap);
    log.info("Registering coordinator system stream consumer.");
    consumer.register();
    log.debug("Starting coordinator system stream consumer.");
    consumer.start();
    log.debug("Bootstrapping coordinator system stream consumer.");
    consumer.bootstrap();
    log.info("Registering coordinator system stream producer.");
    producer.register(source);
    log.debug("Starting coordinator system stream producer");
    producer.start();
    return new ChangelogPartitionManager(producer, consumer, source);
  }
}
