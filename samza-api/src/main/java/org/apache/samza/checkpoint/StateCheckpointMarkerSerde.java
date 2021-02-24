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
import org.apache.samza.storage.StateBackendFactory;


public class StateCheckpointMarkerSerde<T extends StateCheckpointMarker> {
  private static final short PROTOCOL_VERSION = 1;
  private static final String SCM_SEPARATOR = ":";
  private static final int PARTS_COUNT = 3; // protocol_version, serde_class, payload

  /**
   * Serializes the {@link StateCheckpointMarker} according to the {@link StateCheckpointPayloadSerde} provided by
   * the {@link StateBackendFactory}
   * @param payload of type StateCheckpointMarker to be serialized into String
   * @return serialized StateCheckpointMarker
   */
  public String serialize(T payload) {
    try {
      StateBackendFactory stateBackendFactory = (StateBackendFactory) Class.forName(payload.getFactoryName()).newInstance();
      return serialize(payload, stateBackendFactory.getStateCheckpointPayloadSerde());
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new SamzaException("State backend factory serde not found for class name: " + payload.getFactoryName(), e);
    }
  }

  /**
   * Deserializes the serialized {@link StateCheckpointMarker} with the {@link StateCheckpointPayloadSerde} provided in
   * the serializedSCM
   * @param serializedSCM serialized version of a {@link StateCheckpointMarker}
   * @return {@link StateCheckpointMarker} object deserialized
   */
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
      StateCheckpointPayloadSerde<T> serde = (StateCheckpointPayloadSerde<T>) Class.forName(payloadSerdeClassName)
          .getDeclaredConstructor().newInstance();
      return serde.deserialize(parts[2]);
    } catch (Exception e) {
      throw new SamzaException("StateCheckpoint payload serde not found for class name: " + payloadSerdeClassName, e);
    }
  }

  private String serialize(T payload, StateCheckpointPayloadSerde<T> serde) {
    return PROTOCOL_VERSION + SCM_SEPARATOR + serde.getClass().getName() + SCM_SEPARATOR + serde.serialize(payload);
  }
}
