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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.serializers.model.SamzaObjectMapper;


/**
 * The {@link Serde} for {@link CheckpointV2} which includes {@link org.apache.samza.checkpoint.CheckpointId}s,
 * state checkpoint markers and the input {@link org.apache.samza.system.SystemStreamPartition} offsets.
 *
 * The overall payload is serde'd as JSON using {@link SamzaObjectMapper}. Since the Samza classes cannot be directly
 * serialized by Jackson using {@link org.apache.samza.serializers.model.JsonCheckpointV2Mixin}.
 */
public class CheckpointV2Serde implements Serde<CheckpointV2> {
  private static final ObjectMapper OBJECT_MAPPER = SamzaObjectMapper.getObjectMapper();

  public CheckpointV2Serde() { }

  @Override
  public CheckpointV2 fromBytes(byte[] bytes) {
    try {
      return OBJECT_MAPPER.readValue(bytes, CheckpointV2.class);
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception while deserializing checkpoint: %s", new String(bytes)), e);
    }
  }

  @Override
  public byte[] toBytes(CheckpointV2 checkpoint) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(checkpoint);
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception while serializing checkpoint: %s", checkpoint.toString()), e);
    }
  }
}
