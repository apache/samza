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
package org.apache.samza.startpoint;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemStreamPartition;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


/**
 * Holds the {@link Startpoint} fan outs for each {@link SystemStreamPartition}. Each StartpointFanOutPerTask maps to a
 * {@link org.apache.samza.container.TaskName}
 */
class StartpointFanOutPerTask {
  // TODO: Remove the @JsonSerialize and @JsonDeserialize annotations and use the SimpleModule#addKeySerializer and
  //  SimpleModule#addKeyDeserializer methods in StartpointObjectMapper after upgrading jackson version.
  //  Those methods do not work on nested maps with the current version (1.9.13) of jackson.

  @JsonSerialize
  @JsonDeserialize
  private final Instant timestamp;

  @JsonDeserialize(keyUsing = SamzaObjectMapper.SystemStreamPartitionKeyDeserializer.class)
  @JsonSerialize(keyUsing = SamzaObjectMapper.SystemStreamPartitionKeySerializer.class)
  private final Map<SystemStreamPartition, Startpoint> fanOuts;

  // required for Jackson deserialization
  StartpointFanOutPerTask() {
    this(Instant.now());
  }

  StartpointFanOutPerTask(Instant timestamp) {
    this.timestamp = timestamp;
    this.fanOuts = new HashMap<>();
  }

  // Unused in code, but useful for auditing when the fan out is serialized into the store
  Instant getTimestamp() {
    return timestamp;
  }

  Map<SystemStreamPartition, Startpoint> getFanOuts() {
    return fanOuts;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StartpointFanOutPerTask that = (StartpointFanOutPerTask) o;
    return Objects.equal(timestamp, that.timestamp) && Objects.equal(fanOuts, that.fanOuts);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(timestamp, fanOuts);
  }
}
