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

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestCheckpointV2Serde {

  @Test
  public void testCheckpointV2Serde() {
    CheckpointV2Serde serde = new CheckpointV2Serde();
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    SystemStreamPartition systemStreamPartition = new SystemStreamPartition("test-system", "test-stream", new Partition(777));
    offsets.put(systemStreamPartition, "1");

    // State Checkpoint marker
    Map<String, Map<String, String>> factoryStateCheckpointMarkersMap = new HashMap<>();
    Map<String, String> stateCheckpointMarkersMap = new HashMap<>();
    stateCheckpointMarkersMap.put("store1", "marker1");
    stateCheckpointMarkersMap.put("store2", "marker2");

    Map<String, String> stateCheckpointMarkersMap2 = new HashMap<>();
    stateCheckpointMarkersMap2.put("store1", "marker3");
    stateCheckpointMarkersMap2.put("store2", "marker4");

    factoryStateCheckpointMarkersMap.put("factory1", stateCheckpointMarkersMap);
    factoryStateCheckpointMarkersMap.put("factory2", stateCheckpointMarkersMap2);

    CheckpointId checkpointId = CheckpointId.create();

    CheckpointV2 checkpoint = new CheckpointV2(checkpointId, offsets, factoryStateCheckpointMarkersMap);
    CheckpointV2 deserializedCheckpoint = serde.fromBytes(serde.toBytes(checkpoint));

    // Validate input checkpoints
    assertEquals(checkpointId, deserializedCheckpoint.getCheckpointId());
    assertEquals("1", deserializedCheckpoint.getOffsets().get(systemStreamPartition));
    assertEquals(1, deserializedCheckpoint.getOffsets().size());

    // Validate state checkpoints
    assertEquals(2, deserializedCheckpoint.getStateCheckpointMarkers().size());
    assertTrue(deserializedCheckpoint.getStateCheckpointMarkers().containsKey("factory1"));
    assertEquals(stateCheckpointMarkersMap, deserializedCheckpoint.getStateCheckpointMarkers().get("factory1"));
    assertTrue(deserializedCheckpoint.getStateCheckpointMarkers().containsKey("factory2"));
    assertEquals(stateCheckpointMarkersMap2, deserializedCheckpoint.getStateCheckpointMarkers().get("factory2"));
  }

  @Test
  public void testCheckpointV2SerdeStatelessJob() {
    CheckpointV2Serde serde = new CheckpointV2Serde();
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    SystemStreamPartition systemStreamPartition = new SystemStreamPartition("test-system", "test-stream", new Partition(777));
    offsets.put(systemStreamPartition, "1");

    // State Checkpoint marker
    CheckpointId checkpointId = CheckpointId.create();


    CheckpointV2 checkpoint = new CheckpointV2(checkpointId, offsets,  new HashMap<>());
    CheckpointV2 deserializedCheckpoint = serde.fromBytes(serde.toBytes(checkpoint));

    // Validate input checkpoints
    assertEquals(checkpointId, deserializedCheckpoint.getCheckpointId());
    assertEquals("1", deserializedCheckpoint.getOffsets().get(systemStreamPartition));
    assertEquals(1, deserializedCheckpoint.getOffsets().size());

    // No state checkpoints, but a map is still created
    assertEquals(0, deserializedCheckpoint.getStateCheckpointMarkers().size());
  }
}
