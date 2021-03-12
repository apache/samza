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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.Partition;
import org.apache.samza.annotation.InterfaceStability;
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
  public static final String SEPARATOR = ";";

  private final SystemStreamPartition changelogSSP;
  private final String changelogOffset;

  public KafkaStateCheckpointMarker(SystemStreamPartition changelogSSP, String changelogOffset) {
    this.changelogSSP = changelogSSP;
    this.changelogOffset = changelogOffset;
  }

  public static KafkaStateCheckpointMarker fromString(String stateCheckpointMarker) {
    if (StringUtils.isBlank(stateCheckpointMarker)) {
      throw new IllegalArgumentException("Invalid KafkaStateCheckpointMarker format: " + stateCheckpointMarker);
    }
    String[] payload = stateCheckpointMarker.split(KafkaStateCheckpointMarker.SEPARATOR);
    if (payload.length != 4) {
      throw new IllegalArgumentException("Invalid KafkaStateCheckpointMarker parts count: " + stateCheckpointMarker);
    }

    String system = payload[0];
    String stream = payload[1];
    Partition partition = new Partition(Integer.parseInt(payload[2]));
    String offset = null;
    if (!"null".equals(payload[3])) {
      offset = payload[3];
    }

    return new KafkaStateCheckpointMarker(new SystemStreamPartition(system, stream, partition), offset);
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
        KafkaStateCheckpointMarker stateMarker = KafkaStateCheckpointMarker.fromString(value);
        Option<String> offsetOption = Option.apply(stateMarker.getChangelogOffset());
        sspToOffsetOptions.put(new SystemStreamPartition(stateMarker.getChangelogSSP()), offsetOption);
      });
    }
    return sspToOffsetOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KafkaStateCheckpointMarker that = (KafkaStateCheckpointMarker) o;
    return Objects.equals(changelogSSP, that.changelogSSP) &&
        Objects.equals(changelogOffset, that.changelogOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(changelogSSP, changelogOffset);
  }

  /**
   * WARNING: Do not change the toString() representation. It is used for serde'ing {@link KafkaStateCheckpointMarker}s,
   * in conjunction with {@link #fromString(String)}.
   * @return the String representation of this {@link KafkaStateCheckpointMarker}
   */
  @Override
  public String toString() {
    return String.format("%s%s%s%s%s%s%s",
        changelogSSP.getSystem(), SEPARATOR, changelogSSP.getStream(), SEPARATOR,
        changelogSSP.getPartition().getPartitionId(), SEPARATOR, changelogOffset);
  }
}
