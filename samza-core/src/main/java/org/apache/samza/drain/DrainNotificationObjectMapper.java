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
package org.apache.samza.drain;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;


/**
 * Wraps a ObjectMapper for serializing and deserializing Drain Notification Messages.
 */
public class DrainNotificationObjectMapper {
  private static ObjectMapper objectMapper = null;

  private DrainNotificationObjectMapper() {
  }

  public static ObjectMapper getObjectMapper() {
    if (objectMapper == null) {
      objectMapper = createObjectMapper();
    }
    return objectMapper;
  }

  private static ObjectMapper createObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("DrainModule");
    module.addSerializer(DrainNotification.class, new DrainNotificationSerializer());
    module.addDeserializer(DrainNotification.class, new DrainNotificationDeserializer());
    objectMapper.registerModule(module);
    objectMapper.registerSubtypes(new NamedType(DrainNotification.class));
    return objectMapper;
  }

  private static class DrainNotificationSerializer extends JsonSerializer<DrainNotification> {
    @Override
    public void serialize(DrainNotification value, JsonGenerator jsonGenerator, SerializerProvider provider)
        throws IOException {
      Map<String, Object> drainMessageMap = new HashMap<>();
      drainMessageMap.put("uuid", value.getUuid().toString());
      drainMessageMap.put("runId", value.getRunId());
      drainMessageMap.put("drainMode", value.getDrainMode().toString());
      jsonGenerator.writeObject(drainMessageMap);
    }
  }

  private static class DrainNotificationDeserializer extends JsonDeserializer<DrainNotification> {
    @Override
    public DrainNotification deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      UUID uuid = UUID.fromString(node.get("uuid").textValue());
      String runId = node.get("runId").textValue();
      DrainMode drainMode = DrainMode.valueOf(node.get("drainMode").textValue().toUpperCase(Locale.ROOT));
      return new DrainNotification(uuid, runId, drainMode);
    }
  }
}
