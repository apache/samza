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
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.MockStateCheckpointMarker;
import org.apache.samza.checkpoint.StateCheckpointMarker;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestStateCheckpointSerde {

  @Test
  public void TestStatefulCheckpointSerde() {
    StatefulCheckpointSerde serde = new StatefulCheckpointSerde();
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    SystemStreamPartition systemStreamPartition = new SystemStreamPartition("test-system", "test-stream", new Partition(777));
    offsets.put(systemStreamPartition, "1");

    // State Checkpoint marker
    Map<String, List<StateCheckpointMarker>> stateCheckpointMarkersMap = new HashMap<>();
    List<StateCheckpointMarker> stateCheckpointMarkersList = new ArrayList<>();
    SystemStreamPartition changelogSSP = new SystemStreamPartition("changelog-system", "changelog-stream", new Partition(1));
    StateCheckpointMarker scm1 = new MockStateCheckpointMarker(changelogSSP, "2");
    stateCheckpointMarkersList.add(scm1);
    SystemStreamPartition changelogSSP2 = new SystemStreamPartition("changelog-system", "changelog-stream", new Partition(2));
    StateCheckpointMarker scm2 = new MockStateCheckpointMarker(changelogSSP2, "2");
    stateCheckpointMarkersList.add(scm2);

    stateCheckpointMarkersMap.put("store1", stateCheckpointMarkersList);

    CheckpointId checkpointId = CheckpointId.create();

    Checkpoint checkpoint = new Checkpoint(checkpointId, offsets, stateCheckpointMarkersMap);
    Checkpoint deserializedCheckpoint = serde.fromBytes(serde.toBytes(checkpoint));

    // Validate input checkpoints
    assertEquals(checkpointId, deserializedCheckpoint.getCheckpointId());
    assertEquals("1", deserializedCheckpoint.getInputOffsets().get(systemStreamPartition));
    assertEquals(1, deserializedCheckpoint.getInputOffsets().size());

    // Validate state checkpoints
    assertEquals(1, deserializedCheckpoint.getStateCheckpointMarkers().size());
    assertEquals(stateCheckpointMarkersList, deserializedCheckpoint.getStateCheckpointMarkers().get("store1"));

    assertEquals(MockStateCheckpointMarker.MOCK_FACTORY_NAME,
        deserializedCheckpoint.getStateCheckpointMarkers().get("store1").get(0).getFactoryName());
    assertEquals(scm1, deserializedCheckpoint.getStateCheckpointMarkers().get("store1").get(0));
    assertEquals(scm2, deserializedCheckpoint.getStateCheckpointMarkers().get("store1").get(1));
  }

  @Test
  public void TestStatefulCheckpointSerdeStatelessJob() {
    StatefulCheckpointSerde serde = new StatefulCheckpointSerde();
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    SystemStreamPartition systemStreamPartition = new SystemStreamPartition("test-system", "test-stream", new Partition(777));
    offsets.put(systemStreamPartition, "1");

    // State Checkpoint marker
    CheckpointId checkpointId = CheckpointId.create();


    Checkpoint checkpoint = new Checkpoint(checkpointId, offsets,  new HashMap<>());
    Checkpoint deserializedCheckpoint = serde.fromBytes(serde.toBytes(checkpoint));

    // Validate input checkpoints
    assertEquals(checkpointId, deserializedCheckpoint.getCheckpointId());
    assertEquals("1", deserializedCheckpoint.getInputOffsets().get(systemStreamPartition));
    assertEquals(1, deserializedCheckpoint.getInputOffsets().size());

    // No state checkpoints, but a map is still created
    assertEquals(0, deserializedCheckpoint.getStateCheckpointMarkers().size());
  }
}
