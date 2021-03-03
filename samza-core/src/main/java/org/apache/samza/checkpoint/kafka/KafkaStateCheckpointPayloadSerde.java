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

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.StateCheckpointPayloadSerde;
import org.apache.samza.system.SystemStreamPartition;


public class KafkaStateCheckpointPayloadSerde implements StateCheckpointPayloadSerde<KafkaStateCheckpointMarker> {

  @Override
  public String serialize(KafkaStateCheckpointMarker payload) {
    SystemStreamPartition ssp = payload.getSsp();
    return String.format("%s%s%s%s%s%s%s%s%s",
        KafkaStateCheckpointMarker.SCHEMA_VERSION, KafkaStateCheckpointMarker.SEPARATOR,
        ssp.getSystem(), KafkaStateCheckpointMarker.SEPARATOR,
        ssp.getStream(), KafkaStateCheckpointMarker.SEPARATOR,
        ssp.getPartition().getPartitionId(), KafkaStateCheckpointMarker.SEPARATOR,
        payload.getChangelogOffset());
  }

  @Override
  public KafkaStateCheckpointMarker deserialize(String data) {
    if (StringUtils.isBlank(data)) {
      throw new IllegalArgumentException("Invalid KafkaStateCheckpointMarker format: " + data);
    }
    String[] payload = data.split(KafkaStateCheckpointMarker.SEPARATOR);
    if (payload.length != 5) {
      throw new IllegalArgumentException("Invalid KafkaStateCheckpointMarker argument count: " + data);
    }
    if (Short.parseShort(payload[0]) != KafkaStateCheckpointMarker.SCHEMA_VERSION) {
      throw new IllegalArgumentException("Invalid KafkaStateCheckpointMarker schema version: " + data);
    }
    Partition partition = new Partition(Integer.parseInt(payload[3]));
    String offset = null;
    if (!"null".equals(payload[4])) {
      offset = payload[4];
    }

    return new KafkaStateCheckpointMarker(new SystemStreamPartition(payload[1], payload[2], partition), offset);
  }
}
