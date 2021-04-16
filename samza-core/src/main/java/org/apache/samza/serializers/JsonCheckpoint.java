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

import java.util.Map;

/**
 * Used for Json serialization of the {@link org.apache.samza.checkpoint.Checkpoint} class by the
 * {@link CheckpointV2Serde}
 * This cannot be an internal class as required by Jackson Object mapper
 */
public class JsonCheckpoint {
  private String checkpointId;
  private Map<String, Map<String, String>> inputOffsets;
  // Map<StorageBackendFactoryName, Map<StoreName, StateCheckpointMarker>>
  private Map<String, Map<String, String>> stateCheckpointMarkers;

  // Default constructor required for Jackson ObjectMapper
  public JsonCheckpoint() {}

  public JsonCheckpoint(String checkpointId,
      Map<String, Map<String, String>> inputOffsets,
      Map<String, Map<String, String>> stateCheckpointMakers) {
    this.checkpointId = checkpointId;
    this.inputOffsets = inputOffsets;
    this.stateCheckpointMarkers = stateCheckpointMakers;
  }

  public String getCheckpointId() {
    return checkpointId;
  }

  public Map<String, Map<String, String>> getInputOffsets() {
    return inputOffsets;
  }

  public Map<String, Map<String, String>> getStateCheckpointMarkers() {
    return stateCheckpointMarkers;
  }
}
