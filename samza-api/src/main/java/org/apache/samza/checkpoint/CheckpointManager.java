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

import org.apache.samza.Partition;

/**
 * CheckpointManagers read and write {@link org.apache.samza.checkpoint.Checkpoint} to some
 * implementation-specific location.
 */
public interface CheckpointManager {
  public void start();

  /**
   * Registers this manager to write checkpoints of a specific Samza stream partition.
   * @param partition Specific Samza stream partition of which to write checkpoints for.
   */
  public void register(Partition partition);

  /**
   * Writes a checkpoint based on the current state of a Samza stream partition.
   * @param partition Specific Samza stream partition of which to write a checkpoint of.
   * @param checkpoint Reference to a Checkpoint object to store offset data in.
   */
  public void writeCheckpoint(Partition partition, Checkpoint checkpoint);

  /**
   * Returns the last recorded checkpoint for a specified Samza stream partition.
   * @param partition Specific Samza stream partition for which to get the last checkpoint of.
   * @return A Checkpoint object with the recorded offset data of the specified partition.
   */
  public Checkpoint readLastCheckpoint(Partition partition);

  public void stop();
}
