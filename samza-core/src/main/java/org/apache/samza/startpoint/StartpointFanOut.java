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

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Holds the {@link Startpoint} fan outs for each {@link SystemStreamPartition}. Each StartpointFanOut maps to a
 * {@link org.apache.samza.container.TaskName}
 */
class StartpointFanOut {
  private final Instant timestamp;
  private final Map<SystemStreamPartition, Startpoint> fanOuts;

  StartpointFanOut(Instant timestamp, Map<SystemStreamPartition, Startpoint> fanOuts) {
    this.timestamp = timestamp;
    this.fanOuts = fanOuts;
  }

  // Unused in code, but useful for auditing when the fan out is serialized into the store
  Instant getTimestamp() {
    return timestamp;
  }

  Map<SystemStreamPartition, Startpoint> getFanOuts() {
    return fanOuts;
  }

  static class StartpointFanOutSerde implements Serde<StartpointFanOut> {
    private static final String TIMESTAMP = "timestamp";
    private static final String FAN_OUTS = "fanOuts";
    private static final String SYSTEM = "system";
    private static final String STREAM = "stream";
    private static final String PARTITION = "partition";

    private final ObjectMapper mapper = new ObjectMapper();
    private final StartpointSerde startpointSerde = new StartpointSerde();

    @Override
    public StartpointFanOut fromBytes(byte[] bytes) {
      try {
        LinkedHashMap<String, String> deserialized = mapper.readValue(bytes, LinkedHashMap.class);

        // Deserialize timestamp
        Instant timestamp = Instant.ofEpochMilli(Long.valueOf(deserialized.get(TIMESTAMP)));

        // Deserialize SSP to Startpoint map
        LinkedHashMap<String, String> sspToStartpointSerialized = mapper.readValue(deserialized.get(FAN_OUTS), LinkedHashMap.class);
        HashMap<SystemStreamPartition, Startpoint> sspToStartpointMap = new HashMap<>();
        for (String sspJson : sspToStartpointSerialized.keySet()) {
          // Deserialize SSP
          LinkedHashMap<String, String> sspMap = mapper.readValue(sspJson, LinkedHashMap.class);
          String system = sspMap.get(SYSTEM);
          String stream = sspMap.get(STREAM);
          Partition partition = new Partition(Integer.valueOf(sspMap.get(PARTITION)));
          SystemStreamPartition ssp = new SystemStreamPartition(system, stream, partition);

          // Deserialize Startpoint
          String startpointSerialized = sspToStartpointSerialized.get(sspJson);
          Startpoint startpoint = startpointSerde.fromBytes(startpointSerialized.getBytes());

          sspToStartpointMap.put(ssp, startpoint);
        }

        return new StartpointFanOut(timestamp, sspToStartpointMap);
      } catch (Exception e) {
        throw new SamzaException(String.format("Exception in de-serializing startpoint bytes: %s", Arrays.toString(bytes)), e);
      }
    }

    @Override
    public byte[] toBytes(StartpointFanOut startpointFanOut) {
      try {
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();

        mapBuilder.put(TIMESTAMP, Long.toString(startpointFanOut.getTimestamp().toEpochMilli()));

        ImmutableMap.Builder<String, String> fanOutMapBuilder = ImmutableMap.builder();
        for (SystemStreamPartition ssp : startpointFanOut.getFanOuts().keySet()) {
          Map<String, String> sspMap = ImmutableMap.of(
              SYSTEM, ssp.getSystem(),
              STREAM, ssp.getStream(),
              PARTITION, Integer.toString(ssp.getPartition().getPartitionId()));
          Startpoint startpoint = startpointFanOut.getFanOuts().get(ssp);
          String startpointSerialized = new String(startpointSerde.toBytes(startpoint));
          fanOutMapBuilder.put(mapper.writeValueAsString(sspMap), startpointSerialized);
        }
        mapBuilder.put(FAN_OUTS, mapper.writeValueAsString(fanOutMapBuilder.build()));

        return mapper.writeValueAsBytes(mapBuilder.build());
      } catch (Exception e) {
        throw new SamzaException(String.format("Exception in serializing: %s", startpointFanOut), e);
      }
    }
  }
}
