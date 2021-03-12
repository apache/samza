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

package org.apache.samza.serializers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.system.SystemStreamPartition;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * The {@link Serde} for {@link CheckpointV2} which includes {@link CheckpointId}s and state checkpoint markers
 * in addition to the input {@link SystemStreamPartition} offsets.
 *
 * {@link CheckpointId} is serde'd using {@link CheckpointId#toString()} and {@link CheckpointId#fromString(String)}.
 *
 * The overall payload is serde'd as JSON using {@link JsonSerdeV2}. Since the Samza classes cannot be directly
 * serialized by Jackson, we use {@link JsonCheckpoint} as an intermediate POJO to help with serde.
 *
 * The serialized JSON looks as follows:
 * <pre>
 * {
 *   "checkpointId" : "1614147487244-33577",
 *   "inputOffsets" : {
 *     "SystemStreamPartition [test-system, test-stream, 777]" : {
 *       "system" : "SystemName",
 *       "stream" : "StreamName"
 *       "partition" : "777",
 *       "offset" : "1",
 *     }
 *   },
 *   "stateCheckpointMarkers" : {
 *     "org.apache.samza.kafka.KafkaChangelogStateBackendFactory" : {
 *       "store1": "changelogSystem;changelogTopic1;1;50"
 *       "store2": "changelogSystem;changelogTopic2;1;51"
 *     },
 *     "factory2": {...}
 *   }
 * }
 * </pre>
 */
public class CheckpointV2Serde implements Serde<CheckpointV2> {
  private static final Serde<JsonCheckpoint> JSON_SERDE = new JsonSerdeV2<>(JsonCheckpoint.class);
  private static final String SYSTEM = "system";
  private static final String STREAM = "stream";
  private static final String PARTITION = "partition";
  private static final String OFFSET = "offset";

  public CheckpointV2Serde() { }

  @Override
  public CheckpointV2 fromBytes(byte[] bytes) {
    try {
      JsonCheckpoint jsonCheckpoint = JSON_SERDE.fromBytes(bytes);
      Map<SystemStreamPartition, String> sspOffsets = new HashMap<>();

      jsonCheckpoint.getInputOffsets().forEach((sspName, sspInfo) -> {
        String system = sspInfo.get(SYSTEM);
        checkNotNull(system, String.format(
            "System must be present in JSON-encoded SystemStreamPartition, input offsets map: %s, ", sspInfo));
        String stream = sspInfo.get(STREAM);
        checkNotNull(stream, String.format(
            "Stream must be present in JSON-encoded SystemStreamPartition, input offsets map: %s, ", sspInfo));
        String partition = sspInfo.get(PARTITION);
        checkNotNull(partition, String.format(
            "Partition must be present in JSON-encoded SystemStreamPartition, input offsets map: %s, ", sspInfo));
        String offset = sspInfo.get(OFFSET);
        checkNotNull(offset, String.format(
            "Offset must be present in JSON-encoded SystemStreamPartition, input offsets map: %s, ", sspInfo));
        sspOffsets.put(new SystemStreamPartition(system, stream, new Partition(Integer.parseInt(partition))), offset);
      });


      return new CheckpointV2(CheckpointId.fromString(jsonCheckpoint.getCheckpointId()), sspOffsets,
          jsonCheckpoint.getStateCheckpointMarkers());
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception while deserializing checkpoint: %s", Arrays.toString(bytes)), e);
    }
  }

  @Override
  public byte[] toBytes(CheckpointV2 checkpoint) {
    try {
      String checkpointId = checkpoint.getCheckpointId().toString();
      Map<String, Map<String, String>> inputOffsets = new HashMap<>();

      // TODO HIGH dchen change format to write specific serdes similar to Samza Object Mapper
      // Serialize input offsets as maps keyed by the SSP.toString() to the another map of the constituent SSP
      // components and offset. Jackson can't automatically serialize the SSP since it's not a POJO and this avoids
      // having to wrap it another class while maintaining readability.
      // {SSP.toString() -> {"system": system, "stream": stream, "partition": partition, "offset": offset)}
      checkpoint.getOffsets().forEach((ssp, offset) -> {
        Map<String, String> sspOffsetsMap = new HashMap<>();
        sspOffsetsMap.put(SYSTEM, ssp.getSystem());
        sspOffsetsMap.put(STREAM, ssp.getStream());
        sspOffsetsMap.put(PARTITION, Integer.toString(ssp.getPartition().getPartitionId()));
        sspOffsetsMap.put(OFFSET, offset);

        inputOffsets.put(ssp.toString(), sspOffsetsMap);
      });

      Map<String, Map<String, String>> stateCheckpointMarkers = checkpoint.getStateCheckpointMarkers();
      JsonCheckpoint jsonCheckpoint = new JsonCheckpoint(checkpointId, inputOffsets, stateCheckpointMarkers);
      return JSON_SERDE.toBytes(jsonCheckpoint);
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception while serializing checkpoint: %s", checkpoint.toString()), e);
    }
  }
}
