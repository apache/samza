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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.Serde;
import org.codehaus.jackson.map.ObjectMapper;


class StartpointSerde implements Serde<Startpoint> {
  private static final String STARTPOINT_CLASS = "startpointClass";
  private static final String STARTPOINT_OBJ = "startpointObj";

  private final ObjectMapper mapper = new ObjectMapper();
  private final HashMap<String, Class<? extends Startpoint>> registry = new HashMap<>();

  void register(Class<? extends Startpoint> clazz) {
    registry.put(clazz.getCanonicalName(), clazz);
  }

  @Override
  public Startpoint fromBytes(byte[] bytes) {
    try {
      LinkedHashMap<String, String> deserialized = mapper.readValue(bytes, LinkedHashMap.class);
      return mapper.readValue(deserialized.get(STARTPOINT_OBJ), registry.get(deserialized.get(STARTPOINT_CLASS)));
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception in de-serializing startpoint bytes: %s",
          Arrays.toString(bytes)), e);
    }
  }

  @Override
  public byte[] toBytes(Startpoint startpoint) {
    try {
      ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
      mapBuilder.put(STARTPOINT_CLASS, startpoint.getClass().getCanonicalName());
      mapBuilder.put(STARTPOINT_OBJ, mapper.writeValueAsString(startpoint));
      return mapper.writeValueAsBytes(mapBuilder.build());
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception in serializing: %s", startpoint), e);
    }
  }
}
