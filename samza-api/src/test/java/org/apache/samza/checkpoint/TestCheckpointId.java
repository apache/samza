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

import org.junit.Test;

import static org.apache.samza.checkpoint.CheckpointId.*;
import static org.junit.Assert.assertEquals;


public class TestCheckpointId {
  @Test
  public void testSerializationDeserialization() {
    CheckpointId checkpointId = CheckpointId.create();
    CheckpointId deserializedCheckpointId = CheckpointId.deserialize(checkpointId.serialize());

    assertEquals(checkpointId.getMillis(), deserializedCheckpointId.getMillis());
    assertEquals(checkpointId.getNanoId(), deserializedCheckpointId.getNanoId());
    assertEquals(checkpointId, deserializedCheckpointId);
  }

  @Test
  public void testSerializationFormatForBackwardsCompatibility() {
    CheckpointId checkpointId = CheckpointId.create();
    String serializedCheckpointId = checkpointId.serialize();

    // WARNING: This format is written to persisted remotes stores and local files, making a change in the format
    // would be backwards incompatible
    String legacySerializedFormat = serializeLegacy(checkpointId);
    assertEquals(checkpointId, CheckpointId.deserialize(legacySerializedFormat));
  }

  public String serializeLegacy(CheckpointId id) {
    return String.format("%s%s%s", id.getMillis(), SEPARATOR, id.getNanoId());
  }
}
