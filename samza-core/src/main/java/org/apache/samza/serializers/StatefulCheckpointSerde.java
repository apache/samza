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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.StateCheckpointMarker;
import org.apache.samza.checkpoint.StateCheckpointMarkerSerde;
import org.apache.samza.system.SystemStreamPartition;

import static com.google.common.base.Preconditions.*;


/**
 * JSON Serde for the {@link Checkpoint} using the {@link JsonSerdeV2} by converting the Checkpoint to a {@link JsonCheckpoint}
 * The following will be the representation of the data format:
 * <code>
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
 *     "store1" : [ StateCheckpointMarker...]
 *     "store2": [...]
 *   }
 * }
 * </code>
 *
 */
public class StatefulCheckpointSerde implements Serde<Checkpoint> {
  private static final StateCheckpointMarkerSerde STATE_CHECKPOINT_MARKER_SERDE = new StateCheckpointMarkerSerde();
  private final Serde<JsonCheckpoint> jsonCheckpointSerde;

  public StatefulCheckpointSerde() {
    this.jsonCheckpointSerde = new JsonSerdeV2<>(JsonCheckpoint.class);
  }

  @Override
  public Checkpoint fromBytes(byte[] bytes) {
    try {
      JsonCheckpoint jsonCheckpoint = jsonCheckpointSerde.fromBytes(bytes);
      Map<SystemStreamPartition, String> sspOffsets = new HashMap<>();
      Map<String, List<StateCheckpointMarker>> stateCheckpoints = new HashMap<>();

      jsonCheckpoint.getInputOffsets().forEach((sspName, m) -> {
        String system = m.get("system");
        checkNotNull(system, "System must be present in JSON-encoded SystemStreamPartition");
        String stream = m.get("stream");
        checkNotNull(stream, "Stream must be present in JSON-encoded SystemStreamPartition");
        String partition = m.get("partition");
        checkNotNull(partition, "Partition must be present in JSON-encoded SystemStreamPartition");
        String offset = m.get("offset");
        checkNotNull(stream, "Offset must be present in JSON-encoded SystemStreamPartition");
        sspOffsets.put(new SystemStreamPartition(system, stream, new Partition(Integer.parseInt(partition))), offset);
      });

      jsonCheckpoint.getStateCheckpointMarkers().forEach((storeName, scmlist) -> {
        List<StateCheckpointMarker> stateCheckpointMarkersList = new ArrayList<>();
        checkArgument(!scmlist.isEmpty(), "StateCheckpointMarker must be present in Stateful checkpoint");
        scmlist.forEach((scm) -> {
          stateCheckpointMarkersList.add(STATE_CHECKPOINT_MARKER_SERDE.deserializePayload(scm));
        });
        stateCheckpoints.put(storeName, stateCheckpointMarkersList);
      });

      return new Checkpoint(CheckpointId.fromString(jsonCheckpoint.getCheckpointId()), sspOffsets, stateCheckpoints);
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception while deserializing checkpoint: "), e);
    }
  }

  @Override
  public byte[] toBytes(Checkpoint checkpoint) {
    try {
      String checkpointId = checkpoint.getCheckpointId().toString();
      Map<String, Map<String, String>> inputOffsets = new HashMap<>();
      Map<String, List<String>> stateCheckpointMarkers = new HashMap<>();

      // Create input offsets map similar to CheckpointSerde
      // (ssp -> (system, stream, partition, offset))
      checkpoint.getInputOffsets().forEach((ssp, offset) -> {
        Map<String, String> sspOffsetsMap = new HashMap<>();
        sspOffsetsMap.put("system", ssp.getSystem());
        sspOffsetsMap.put("stream", ssp.getStream());
        sspOffsetsMap.put("partition", Integer.toString(ssp.getPartition().getPartitionId()));
        sspOffsetsMap.put("offset", offset);

        inputOffsets.put(ssp.toString(), sspOffsetsMap);
      });

      // Create mapping for state checkpoint markers
      // (storeName -> (StateCheckpointMarkerFactory -> StateCheckpointMarker))
      checkpoint.getStateCheckpointMarkers().forEach((storeName, stateCheckpointMarkerList) -> {
        List<String> stateCheckpointMarkerByFactory = new ArrayList<>();
        stateCheckpointMarkerList.forEach(stateCheckpointMarker -> {
          // Serialize the StateCheckpointMarker according to StateBackendFactory
          stateCheckpointMarkerByFactory.add(STATE_CHECKPOINT_MARKER_SERDE.serialize(stateCheckpointMarker));
        });
        stateCheckpointMarkers.put(storeName, stateCheckpointMarkerByFactory);
      });

      return jsonCheckpointSerde.toBytes(new JsonCheckpoint(checkpointId, inputOffsets, stateCheckpointMarkers));
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception while serializing checkpoint: %s", checkpoint.toString()), e);
    }
  }
}
