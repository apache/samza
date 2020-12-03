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

import org.apache.samza.SamzaException;


public class StateCheckpointMarkerSerde<T extends StateCheckpointMarker> {
  // TODO @dchen implement SCM serialization scheme
  private static final short PROTOCOL_VERSION = 1;

  public byte[] serialize(T payload, StateCheckpointPayloadSerde<T> serde) {
    return new byte[]{};
  }

  public T deserializePayload(byte[] serializedSCM) {
    if (PROTOCOL_VERSION != getProtocolVersion(serializedSCM)) {
      throw new SamzaException("StateCheckpointMarker deserialize protocol version does not match serialized version");
    }

    String payloadSerdeClassName = getPayloadSerdeClassName(serializedSCM);
    try {
      StateCheckpointPayloadSerde<T> serde = (StateCheckpointPayloadSerde<T>) Class.forName(payloadSerdeClassName).newInstance();
      return serde.deserialize(serializedSCM);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new SamzaException("StateCheckpoint payload serde not found for class name: " + payloadSerdeClassName, e);
    }
  }

  private short getProtocolVersion(byte[] data) {
    return 1;
  }

  private String getPayloadSerdeClassName(byte[] data) {
    return "";
  }
}
