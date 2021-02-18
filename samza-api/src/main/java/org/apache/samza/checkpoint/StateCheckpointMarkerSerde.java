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

package org.apache.samza.checkpoint;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;


public class StateCheckpointMarkerSerde<T extends StateCheckpointMarker> {
  private static final short PROTOCOL_VERSION = 1;
  private static final String SCM_SEPARATOR = ":";
  private static final int PARTS_COUNT = 3;

  public String serialize(T payload, StateCheckpointPayloadSerde<T> serde) {
    StringBuilder builder = new StringBuilder();
    builder.append(PROTOCOL_VERSION);
    builder.append(SCM_SEPARATOR);
    builder.append(serde.getClass().getName());
    builder.append(SCM_SEPARATOR);
    builder.append(serde.serialize(payload));

    return builder.toString();
  }

  public T deserializePayload(String serializedSCM) {
    if (StringUtils.isBlank(serializedSCM)) {
      throw new IllegalArgumentException("Invalid remote store checkpoint message: " + serializedSCM);
    }
    String[] parts = serializedSCM.split(SCM_SEPARATOR, PARTS_COUNT);
    if (parts.length != PARTS_COUNT) {
      throw new IllegalArgumentException("Invalid state checkpoint marker: " + serializedSCM);
    }
    if (PROTOCOL_VERSION != Short.parseShort(parts[0])) {
      throw new SamzaException("StateCheckpointMarker deserialize protocol version does not match serialized version");
    }
    String payloadSerdeClassName = parts[1];
    try {
      StateCheckpointPayloadSerde<T> serde = (StateCheckpointPayloadSerde<T>) Class.forName(payloadSerdeClassName).newInstance();
      return serde.deserialize(parts[2]);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new SamzaException("StateCheckpoint payload serde not found for class name: " + payloadSerdeClassName, e);
    }
  }
}
