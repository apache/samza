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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.checkpoint.StateCheckpointMarker;
import org.apache.samza.storage.KafkaChangelogStateBackendFactory;
import org.apache.samza.system.SystemStreamPartition;
import scala.Option;


@InterfaceStability.Unstable
public class KafkaStateCheckpointMarker implements StateCheckpointMarker {
  public static final short SCHEMA_VERSION = 1;
  public static final String SEPARATOR = ";";
  public static final String FACTORY_NAME = KafkaChangelogStateBackendFactory.class.getName();

  // One offset per SSP
  private final SystemStreamPartition ssp;
  private final String changelogOffset;

  public KafkaStateCheckpointMarker(SystemStreamPartition ssp, String changelogOffset) {
    this.ssp = ssp;
    this.changelogOffset = changelogOffset;
  }

  public static short getSchemaVersion() {
    return SCHEMA_VERSION;
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
  public static Map<SystemStreamPartition, Option<String>> stateCheckpointMarkerToSSPMap(Map<String, StateCheckpointMarker> markers) {
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

  /**
   * Builds the SSP to kafka offset mapping from map of store name to list of StateCheckpointMarkers
   * containing a KafkaStateCheckpointMarker
   * @param markers Map of store name to list of StateCheckpointMarkers containing a KafkaStateCheckpointMarker
   * @return Map of ssp to option of Kafka offset
   */
  public static Map<SystemStreamPartition, Option<String>> stateCheckpointMarkerListToSSPMap(Map<String, List<StateCheckpointMarker>> markers) {
    Map<String, StateCheckpointMarker> scmMap = new HashMap<>();
    markers.forEach((storeName, scmList) -> {
      StateCheckpointMarker kafkaMarker = null;
      for (StateCheckpointMarker scm : scmList) {
        if (FACTORY_NAME.equals(scm.getFactoryName())) {
          kafkaMarker = scm;
          break; // only one KafkaStateCheckpointMarker per list
        }
      }
      scmMap.put(storeName, kafkaMarker);
    });
    return stateCheckpointMarkerToSSPMap(scmMap);
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
    return Objects.hash(SCHEMA_VERSION, ssp, changelogOffset);
  }

  @Override
  public String toString() {
    return String.format("%s%s%s%s%s%s%s%s%s", SCHEMA_VERSION, SEPARATOR,
        ssp.getSystem(), SEPARATOR, ssp.getStream(), SEPARATOR, ssp.getPartition().getPartitionId(), SEPARATOR, changelogOffset);
  }
}
