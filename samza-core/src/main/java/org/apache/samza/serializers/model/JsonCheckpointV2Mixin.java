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

package org.apache.samza.serializers.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.system.SystemStreamPartition;

@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonCheckpointV2Mixin {
  @JsonCreator
  public JsonCheckpointV2Mixin(
      @JsonProperty("checkpoint-id") CheckpointId checkpointId,
      @JsonProperty("input-offsets") Map<SystemStreamPartition, String> inputOffsets,
      @JsonProperty("state-checkpoint-markers") Map<String, Map<String, String>> stateCheckpointMarkers) {
  }

  @JsonProperty("checkpoint-id")
  abstract CheckpointId getCheckpointId();

  @JsonProperty("input-offsets")
  abstract Map<SystemStreamPartition, String> getOffsets();

  @JsonProperty("state-checkpoint-markers")
  abstract Map<String, Map<String, String>> getStateCheckpointMarkers();
}
