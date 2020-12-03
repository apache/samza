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
import org.apache.samza.checkpoint.StateCheckpointMarker;
import org.apache.samza.storage.KafkaChangelogStateBackendFactory;
import org.apache.samza.storage.StateBackendFactory;
import org.apache.samza.system.SystemStreamPartition;


public class KafkaStateCheckpointMarker implements StateCheckpointMarker {
  private static final short PROTO_VERSION = 1;
  private static final String FACTORY_NAME = KafkaChangelogStateBackendFactory.class.getName();

  // One offset per SSP
  private final SystemStreamPartition ssp;
  private final String changelogOffset;

  KafkaStateCheckpointMarker(SystemStreamPartition ssp, String changelogOffset) {
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

  @Override
  public StateBackendFactory getFactory() {
    return new KafkaChangelogStateBackendFactory();
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

  public static Map<SystemStreamPartition, String> stateCheckpointMarkerToSSPmap(Map<String, StateCheckpointMarker> markers) {
    Map<SystemStreamPartition, String> sspMap = new HashMap<>();
    if (markers != null) {
      markers.forEach((key, value) -> {
        KafkaStateCheckpointMarker kafkaStateCheckpoint = (KafkaStateCheckpointMarker) value;
        sspMap.put(new SystemStreamPartition(kafkaStateCheckpoint.getSsp()), kafkaStateCheckpoint.getChangelogOffset());
      });
    }
    return sspMap;
  }
}
