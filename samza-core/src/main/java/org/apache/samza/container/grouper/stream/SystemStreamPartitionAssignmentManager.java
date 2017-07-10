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
package org.apache.samza.container.grouper.stream;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.AbstractCoordinatorStreamManager;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.SetSSPTaskMapping;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SystemStreamPartition assignment Manager is used to persist and read the ssp-to-task
 * assignment information from the coordinator stream
 * */
public class SystemStreamPartitionAssignmentManager extends AbstractCoordinatorStreamManager {
  private static final Logger log = LoggerFactory.getLogger(SystemStreamPartitionAssignmentManager.class);
  private boolean registered = false;

  /**
   * Default constructor that creates a read-write manager
   *
   * @param coordinatorStreamProducer producer to the coordinator stream
   * @param coordinatorStreamConsumer consumer for the coordinator stream
   */
  public SystemStreamPartitionAssignmentManager(CoordinatorStreamSystemProducer coordinatorStreamProducer,
                                                CoordinatorStreamSystemConsumer coordinatorStreamConsumer) {
    super(coordinatorStreamProducer, coordinatorStreamConsumer, "SamzaSystemStreamPartitionAssignmentManager");
    register(null);
  }

  @Override
  public void register(TaskName taskName) {
    if (!registered) {
      // taskName will not be used. This producer is global scope.
      registerCoordinatorStreamProducer(getSource());
      // We don't register the consumer because we don't manage the consumer's
      // lifecycle. Also, we don't need to set any properties on the consumer.
      registered = true;
    }
  }

  /**
   * Method to allow read ssp-to-task assignment information from coordinator stream.
   *
   * @return the map of SystemStreamPartition to taskName
   */
  public Map<SystemStreamPartition, String> readSystemStreamPartitionToTaskAssignment() {
    log.debug("Reading SystemStreamPartition assignment information");
    final Map<SystemStreamPartition, String> systemStreamPartitionToTask = new HashMap<>();
    for (CoordinatorStreamMessage message: getBootstrappedStream(SetSSPTaskMapping.TYPE)) {
      SetSSPTaskMapping setSSPTaskMapEntry = new SetSSPTaskMapping(message);
      systemStreamPartitionToTask.put(Util.stringToSsp(setSSPTaskMapEntry.getKey()), setSSPTaskMapEntry.getTaskAssignment());
      log.debug("SystemStreamPartition: {} is mapped to task {}", setSSPTaskMapEntry.getKey(), setSSPTaskMapEntry.getTaskAssignment());
    }

    return systemStreamPartitionToTask;
  }

  /**
   * Write the SystemStreamPartition to task mapping that is being maintained by this SystemStreamPartitionAssignmentManager
   * @param systemStreamPartitionToTask The entries that needs to be written to the coordinator stream, the map takes the systemStreamPartition and it's corresponding taskName.
   */
  public void writeSystemStreamPartitionToTaskAssignment(Map<SystemStreamPartition, String> systemStreamPartitionToTask) {
    for (Map.Entry<SystemStreamPartition, String> entry : systemStreamPartitionToTask.entrySet()) {
      send(new SetSSPTaskMapping(getSource(), Util.sspToString(entry.getKey()), entry.getValue()));
    }
  }

}
