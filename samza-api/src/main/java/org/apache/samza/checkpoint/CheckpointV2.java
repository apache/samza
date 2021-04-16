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

import com.google.common.collect.ImmutableMap;
import java.util.Objects;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Map;

/**
 * A checkpoint is a mapping of all the streams a job is consuming and the most recent current offset for each.
 * It is used to restore a {@link org.apache.samza.task.StreamTask}, either as part of a job restart or as part
 * of restarting a failed container within a running job.
 */

public class CheckpointV2 implements Checkpoint {
  public static final short CHECKPOINT_VERSION = 2;

  private final CheckpointId checkpointId;
  private final Map<SystemStreamPartition, String> inputOffsets;
  private final Map<String, Map<String, String>> stateCheckpointMarkers;

  /**
   * Constructs the checkpoint with separated input and state offsets
   *
   * @param checkpointId {@link CheckpointId} associated with this checkpoint
   * @param inputOffsets Map of Samza system stream partition to offset of the checkpoint
   * @param stateCheckpoints Map of state backend factory name to map of local state store names
   *                         to state checkpoints
   */
  public CheckpointV2(CheckpointId checkpointId,
      Map<SystemStreamPartition, String> inputOffsets,
      Map<String, Map<String, String>> stateCheckpoints) {
    this.checkpointId = checkpointId;
    this.inputOffsets = ImmutableMap.copyOf(inputOffsets);
    this.stateCheckpointMarkers = ImmutableMap.copyOf(stateCheckpoints);
  }

  public short getVersion() {
    return CHECKPOINT_VERSION;
  }

  /**
   * Gets the checkpoint id for the checkpoint
   * @return The timestamp based checkpoint identifier associated with the checkpoint
   */
  public CheckpointId getCheckpointId() {
    return checkpointId;
  }

  /**
   * Gets a unmodifiable view of the current input {@link SystemStreamPartition} offsets.
   * @return An unmodifiable map of input {@link SystemStreamPartition}s to their recorded offsets.
   */
  @Override
  public Map<SystemStreamPartition, String> getOffsets() {
    return inputOffsets;
  }

  /**
   * Gets the state checkpoint markers for all stores for each configured state backend.
   *
   * Note: We don't add this method to the {@link Checkpoint} interface since it is difficult
   * to implement it for {@link CheckpointV1} without changing the underlying serialization format -
   * the changelog SSP offsets are serialized in the same way as input offsets, and at
   * deserialization time we don't have enough information (e.g. configs) to decide whether a
   * particular entry is for an input SSP or a changelog SSP.
   *
   * @return Map of state backend factory name to map of local state store names to state checkpoint markers
   */
  public Map<String, Map<String, String>> getStateCheckpointMarkers() {
    return stateCheckpointMarkers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CheckpointV2)) return false;

    CheckpointV2 that = (CheckpointV2) o;

    return checkpointId.equals(that.checkpointId) &&
        Objects.equals(inputOffsets, that.inputOffsets) &&
        Objects.equals(stateCheckpointMarkers, that.stateCheckpointMarkers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(checkpointId, inputOffsets, stateCheckpointMarkers);
  }

  @Override
  public String toString() {
    return "CheckpointV2 [CHECKPOINT_VERSION=" + CHECKPOINT_VERSION + ", checkpointId=" + checkpointId +
        ", inputOffsets=" + inputOffsets + ", stateCheckpointMarkers=" + stateCheckpointMarkers + "]";
  }
}