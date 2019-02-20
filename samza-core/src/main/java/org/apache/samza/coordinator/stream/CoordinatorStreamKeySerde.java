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
package org.apache.samza.coordinator.stream;

import java.util.Arrays;
import java.util.List;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;

/**
 * Serializer for keys written into coordinator stream.
 */
public class CoordinatorStreamKeySerde implements Serde<String> {

  private static final int KEY_INDEX = 2;

  private final Serde<List<?>> keySerde;
  private final String type;

  public CoordinatorStreamKeySerde(String type) {
    this.type = type;
    this.keySerde = new JsonSerde<>();
  }

  @Override
  public String fromBytes(byte[] bytes) {
    Object[] keyArray = keySerde.fromBytes(bytes).toArray();
    return (String) keyArray[KEY_INDEX];
  }

  @Override
  public byte[] toBytes(String key) {
    Object[] keyArray = new Object[]{CoordinatorStreamMessage.VERSION, type, key};
    return keySerde.toBytes(Arrays.asList(keyArray));
  }
}
