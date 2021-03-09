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
 * Kafka state checkpoints offsets has the format: [system, stream, partition, offsets], separated by a semi-colon.
 */
@InterfaceStability.Unstable
public class KafkaStateCheckpointMarker {
  public static final String SEPARATOR = ";";
  public static final String KAFKA_BACKEND_FACTORY_NAME = KafkaChangelogStateBackendFactory.class.getName();

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
   * Builds the SSP to kafka offset mapping from map of backend factory name to (map of store name to serialized
   * StateCheckpointMarkers.
   * @param factoryStateBackendStateMarkersMap Map of factory name to (map of store name to serialized
   *                                           StateCheckpointMarkers)
   * @return Map of ssp to option of Kafka offset or an empty map if there is KafkaStateBackendFactory name found
   * in the input map
   */
  public static Map<SystemStreamPartition, Option<String>> scmsToSSPOffsetMap(
      Map<String, Map<String, String>> factoryStateBackendStateMarkersMap) {
    return scmToSSPOffsetMap(factoryStateBackendStateMarkersMap
        .getOrDefault(KAFKA_BACKEND_FACTORY_NAME, null));
  }

  /**
   * Builds a SSP to Kafka offset mapping from map of store name to KafkaStateCheckpointMarkers
   * @param storeToKafkaStateMarker storeName to serialized KafkaStateCheckpointMarker
   * @return Map of SSP to Optional offset
   */
  public static Map<SystemStreamPartition, Option<String>> scmToSSPOffsetMap(Map<String, String> storeToKafkaStateMarker) {
    Map<SystemStreamPartition, Option<String>> sspToOffset = new HashMap<>();
    if (storeToKafkaStateMarker != null) {
      storeToKafkaStateMarker.forEach((key, value) -> {
        KafkaStateCheckpointMarker stateMarker = KafkaStateCheckpointMarker.fromString(value);
        Option<String> offsetOption = Option.<String>apply(stateMarker.getChangelogOffset());
        sspToOffset.put(new SystemStreamPartition(stateMarker.getChangelogSSP()), offsetOption);
      });
    }
    return sspToOffset;
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
