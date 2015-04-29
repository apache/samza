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

package org.apache.samza.checkpoint;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage.SetCheckpoint;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The CheckpointManager is used to persist and restore checkpoint information. The CheckpointManager uses
 * CoordinatorStream underneath to do this.
 */
public class CheckpointManager {

  private static final Logger log = LoggerFactory.getLogger(CheckpointManager.class);
  private final CoordinatorStreamSystemProducer coordinatorStreamProducer;
  private final CoordinatorStreamSystemConsumer coordinatorStreamConsumer;
  private final Map<TaskName, Checkpoint> taskNamesToOffsets;
  private final HashSet<TaskName> taskNames;
  private String source;

  public CheckpointManager(CoordinatorStreamSystemProducer coordinatorStreamProducer,
      CoordinatorStreamSystemConsumer coordinatorStreamConsumer) {
    this.coordinatorStreamConsumer = coordinatorStreamConsumer;
    this.coordinatorStreamProducer = coordinatorStreamProducer;
    taskNamesToOffsets = new HashMap<TaskName, Checkpoint>();
    taskNames = new HashSet<TaskName>();
    this.source = "Unknown";
  }

  public CheckpointManager(CoordinatorStreamSystemProducer coordinatorStreamProducer,
      CoordinatorStreamSystemConsumer coordinatorStreamConsumer,
      String source) {
    this(coordinatorStreamProducer, coordinatorStreamConsumer);
    this.source = source;
  }

  public void start() {
    coordinatorStreamProducer.start();
    coordinatorStreamConsumer.start();
  }

  /**
   * Registers this manager to write checkpoints of a specific Samza stream partition.
   * @param taskName Specific Samza taskName of which to write checkpoints for.
   */
  public void register(TaskName taskName) {
    log.debug("Adding taskName {} to {}", taskName, this);
    taskNames.add(taskName);
    coordinatorStreamConsumer.register();
    coordinatorStreamProducer.register(taskName.getTaskName());
  }

  /**
   * Writes a checkpoint based on the current state of a Samza stream partition.
   * @param taskName Specific Samza taskName of which to write a checkpoint of.
   * @param checkpoint Reference to a Checkpoint object to store offset data in.
   */
  public void writeCheckpoint(TaskName taskName, Checkpoint checkpoint) {
    log.debug("Writing checkpoint for Task: {} with offsets: {}", taskName.getTaskName(), checkpoint.getOffsets());
    SetCheckpoint checkPointMessage = new SetCheckpoint(source, taskName.getTaskName(), checkpoint);
    coordinatorStreamProducer.send(checkPointMessage);
  }

  /**
   * Returns the last recorded checkpoint for a specified taskName.
   * @param taskName Specific Samza taskName for which to get the last checkpoint of.
   * @return A Checkpoint object with the recorded offset data of the specified partition.
   */
  public Checkpoint readLastCheckpoint(TaskName taskName) {
    // Bootstrap each time to make sure that we are caught up with the stream, the bootstrap will just catch up on consecutive calls
    log.debug("Reading checkpoint for Task: {}", taskName.getTaskName());
    Set<CoordinatorStreamMessage> bootstrappedStream = coordinatorStreamConsumer.getBootstrappedStream(SetCheckpoint.TYPE);
    for (CoordinatorStreamMessage coordinatorStreamMessage : bootstrappedStream) {
      SetCheckpoint setCheckpoint = new SetCheckpoint(coordinatorStreamMessage);
      TaskName taskNameInCheckpoint = new TaskName(setCheckpoint.getKey());
      if(taskNames.contains(taskNameInCheckpoint)) {
        taskNamesToOffsets.put(taskNameInCheckpoint, setCheckpoint.getCheckpoint());
        log.debug("Adding checkpoint {} for taskName {}", taskNameInCheckpoint, taskName);
      }
    }
    return taskNamesToOffsets.get(taskName);
  }

  public void stop() {
    coordinatorStreamConsumer.stop();
    coordinatorStreamProducer.stop();
  }
}
