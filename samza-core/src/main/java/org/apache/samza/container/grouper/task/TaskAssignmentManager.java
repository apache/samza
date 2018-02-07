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
import org.apache.samza.coordinator.stream.CoordinatorStreamManager;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.Delete;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task assignment Manager is used to persist and read the task-to-container
 * assignment information from the coordinator stream
 * */
public class TaskAssignmentManager {
  private static final Logger log = LoggerFactory.getLogger(TaskAssignmentManager.class);
  private final Map<String, String> taskNameToContainerId = new HashMap<>();
  private final CoordinatorStreamManager coordinatorStreamManager;
  private static final String SOURCE = "SamzaTaskAssignmentManager";

  /**
   * Default constructor that creates a read-write manager
   *
   * @param coordinatorStreamManager coordinator stream manager.
   */
  public TaskAssignmentManager(CoordinatorStreamManager coordinatorStreamManager) {
    this.coordinatorStreamManager = coordinatorStreamManager;
    coordinatorStreamManager.registerCoordinatorStreamProducer(SOURCE);
  }

  /**
   * Method to allow read container task information from coordinator stream. This method is used
   * in {@link org.apache.samza.coordinator.JobModelManager}.
   *
   * @return the map of taskName: containerId
   */
  public Map<String, String> readTaskAssignment() {
    taskNameToContainerId.clear();
    for (CoordinatorStreamMessage message: coordinatorStreamManager.getBootstrappedStream(SetTaskContainerMapping.TYPE)) {
      if (message.isDelete()) {
        taskNameToContainerId.remove(message.getKey());
        log.debug("Got TaskContainerMapping delete message: {}", message);
      } else {
        SetTaskContainerMapping mapping = new SetTaskContainerMapping(message);
        taskNameToContainerId.put(mapping.getKey(), mapping.getTaskAssignment());
        log.debug("Got TaskContainerMapping message: {}", mapping);
      }
    }

    for (Map.Entry<String, String> entry : taskNameToContainerId.entrySet()) {
      log.debug("Assignment for task \"{}\": {}", entry.getKey(), entry.getValue());
    }

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
      log.info("Task \"{}\" moved from container {} to container {}", new Object[]{taskName, existingContainerId, containerId});
    } else {
      log.debug("Task \"{}\" assigned to container {}", taskName, containerId);
    }

    if (containerId == null) {
      coordinatorStreamManager.send(new Delete(SOURCE, taskName, SetTaskContainerMapping.TYPE));
      taskNameToContainerId.remove(taskName);
    } else {
      coordinatorStreamManager.send(new SetTaskContainerMapping(SOURCE, taskName, String.valueOf(containerId)));
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
      writeTaskContainerMapping(taskName, null);
    }
  }
}
