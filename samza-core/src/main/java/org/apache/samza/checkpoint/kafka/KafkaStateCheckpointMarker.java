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

package org.apache.samza.checkpoint.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.samza.SamzaException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.storage.KafkaChangelogStateBackendFactory;
import org.apache.samza.system.SystemStreamPartition;
import scala.Option;


/**
 * Used as the serialization format for the state checkpoints of {@link org.apache.samza.checkpoint.CheckpointV2}
 * for a store using {@link org.apache.samza.storage.KafkaTransactionalStateTaskBackupManager} or
 * {@link org.apache.samza.storage.KafkaNonTransactionalStateTaskBackupManager} for tracking the latest committed
 * store changelog offsets.
 *
 * Kafka state checkpoint marker has the format: [system, stream, partition, offset], separated by a semi-colon.
 */
@InterfaceStability.Unstable
public class KafkaStateCheckpointMarker {
  public static final String KAFKA_STATE_BACKEND_FACTORY_NAME = KafkaChangelogStateBackendFactory.class.getName();
  public static final short MARKER_VERSION = 1;
  private static final ObjectMapper MAPPER = SamzaObjectMapper.getObjectMapper();

  // Required for Jackson Serde
  private final short version;
  private final SystemStreamPartition changelogSSP;
  private final String changelogOffset;

  public KafkaStateCheckpointMarker(SystemStreamPartition changelogSSP, String changelogOffset) {
    this(MARKER_VERSION, changelogSSP, changelogOffset);
  }

  public KafkaStateCheckpointMarker(short version, SystemStreamPartition changelogSSP, String changelogOffset) {
    this.version = version;
    this.changelogSSP = changelogSSP;
    this.changelogOffset = changelogOffset;
  }

  public static KafkaStateCheckpointMarker deserialize(String stateCheckpointMarker) {
    try {
      return MAPPER.readValue(stateCheckpointMarker, KafkaStateCheckpointMarker.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Could not deserialize KafkaStateCheckpointMarker: " + stateCheckpointMarker);
    }
  }

  public short getVersion() {
    return version;
  }

  public SystemStreamPartition getChangelogSSP() {
    return changelogSSP;
  }

  public String getChangelogOffset() {
    return changelogOffset;
  }

  /**
   * Builds a map of store changelog SSPs to their offset for Kafka changelog backed stores from the provided
   * map of state backend factory name to map of store name to serialized state checkpoint markers.
   *
   * @param stateBackendToStoreSCMs Map of state backend factory name to map of store name to serialized
   *                                state checkpoint markers
   * @return Map of store changelog SSPss to their optional offset, or an empty map if there is no mapping for
   * {@link #KAFKA_STATE_BACKEND_FACTORY_NAME} in the input map. Optional offset may be empty if the
   * changelog SSP was empty.
   */
  public static Map<SystemStreamPartition, Option<String>> scmsToSSPOffsetMap(
      Map<String, Map<String, String>> stateBackendToStoreSCMs) {
    Map<SystemStreamPartition, Option<String>> sspToOffsetOptions = new HashMap<>();
    if (stateBackendToStoreSCMs.containsKey(KAFKA_STATE_BACKEND_FACTORY_NAME)) {
      Map<String, String> storeToKafkaSCMs = stateBackendToStoreSCMs.get(KAFKA_STATE_BACKEND_FACTORY_NAME);
      storeToKafkaSCMs.forEach((key, value) -> {
        KafkaStateCheckpointMarker stateMarker = KafkaStateCheckpointMarker.deserialize(value);
        Option<String> offsetOption = Option.apply(stateMarker.getChangelogOffset());
        sspToOffsetOptions.put(new SystemStreamPartition(stateMarker.getChangelogSSP()), offsetOption);
      });
    }
    return sspToOffsetOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KafkaStateCheckpointMarker that = (KafkaStateCheckpointMarker) o;
    return Objects.equals(changelogSSP, that.changelogSSP) &&
        Objects.equals(changelogOffset, that.changelogOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(changelogSSP, changelogOffset);
  }

  /**
   * It is used for serde'ing {@link KafkaStateCheckpointMarker}s, in conjunction with {@link #deserialize(String)}.
   * @return the String representation of this {@link KafkaStateCheckpointMarker}
   */
  public static String serialize(KafkaStateCheckpointMarker marker) {
    try {
      return MAPPER.writeValueAsString(marker);
    } catch (JsonProcessingException e) {
      throw new SamzaException(String.format("Error serializing KafkaCheckpointMarker %s", marker), e);
    }
  }

  @Override
  public String toString() {
    String separator = ",";
    return String.format("%s%s%s%s%s%s%s",
        changelogSSP.getSystem(), separator, changelogSSP.getStream(), separator,
        changelogSSP.getPartition().getPartitionId(), separator, changelogOffset);
  }
}
