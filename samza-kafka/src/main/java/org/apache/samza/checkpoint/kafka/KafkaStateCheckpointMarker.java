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
import org.apache.samza.checkpoint.StateCheckpointMarker;
import org.apache.samza.storage.KafkaChangelogStateBackendFactory;
import org.apache.samza.system.SystemStreamPartition;
import scala.Option;


@InterfaceStability.Unstable
public class KafkaStateCheckpointMarker implements StateCheckpointMarker {
  private static final short PROTO_VERSION = 1;
  private static final String FACTORY_NAME = KafkaChangelogStateBackendFactory.class.getName();
  private static final String SEPARATOR = ";";

  // One offset per SSP
  private final SystemStreamPartition ssp;
  private final String changelogOffset;

  public KafkaStateCheckpointMarker(SystemStreamPartition ssp, String changelogOffset) {
    this.ssp = ssp;
    this.changelogOffset = changelogOffset;
  }

  public static short getProtocolVersion() {
    return PROTO_VERSION;
  }

  public SystemStreamPartition getSsp() {
    return ssp;
  }

  public String getChangelogOffset() {
    return changelogOffset;
  }

  @Override
  public String getFactoryName() {
    return FACTORY_NAME;
  }

  /**
   * Builds a SSP to Kafka offset mapping from map of store name to KafkaStateCheckpointMarkers
   */
  public static Map<SystemStreamPartition, Option<String>> stateCheckpointMarkerToSSPmap(Map<String, StateCheckpointMarker> markers) {
    Map<SystemStreamPartition, Option<String>> sspMap = new HashMap<>();
    if (markers != null) {
      markers.forEach((key, value) -> {
        KafkaStateCheckpointMarker kafkaStateCheckpoint = (KafkaStateCheckpointMarker) value;
        Option<String> offsetOption = Option.<String>apply(kafkaStateCheckpoint.getChangelogOffset());
        sspMap.put(new SystemStreamPartition(kafkaStateCheckpoint.getSsp()), offsetOption);
      });
    }
    return sspMap;
  }

  public static KafkaStateCheckpointMarker fromString(String message) {
    if (StringUtils.isBlank(message)) {
      throw new IllegalArgumentException("Invalid KafkaStateCheckpointMarker format: " + message);
    }
    String[] payload = message.split(KafkaStateCheckpointMarker.SEPARATOR);
    if (payload.length != 5) {
      throw new IllegalArgumentException("Invalid KafkaStateCheckpointMarker argument count: " + message);
    }
    if (Short.parseShort(payload[0]) != PROTO_VERSION) {
      throw new IllegalArgumentException("Invalid KafkaStateCheckpointMarker protocol version: " + message);
    }
    Partition partition = new Partition(Integer.parseInt(payload[3]));
    String offset = null;
    if (!"null".equals(payload[4])) {
      offset = payload[4];
    }

    return new KafkaStateCheckpointMarker(new SystemStreamPartition(payload[1], payload[2], partition), offset);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KafkaStateCheckpointMarker that = (KafkaStateCheckpointMarker) o;
    return Objects.equals(ssp, that.ssp) &&
        Objects.equals(changelogOffset, that.changelogOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(PROTO_VERSION, ssp, changelogOffset);
  }

  @Override
  public String toString() {
    return String.format("%s%s%s%s%s%s%s%s%s", PROTO_VERSION, SEPARATOR,
        ssp.getSystem(), SEPARATOR, ssp.getStream(), SEPARATOR, ssp.getPartition().getPartitionId(), SEPARATOR, changelogOffset);
  }
}
