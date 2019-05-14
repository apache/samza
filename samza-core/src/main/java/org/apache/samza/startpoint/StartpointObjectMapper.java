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

import java.io.IOException;
import java.time.Instant;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;


class StartpointObjectMapper {

  static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("StartpointModule", new Version(1, 0, 0, ""));
    module.addSerializer(Instant.class, new CustomInstantSerializer());
    module.addDeserializer(Instant.class, new CustomInstantDeserializer());

    objectMapper.registerModule(module);
    objectMapper.registerSubtypes(StartpointSpecific.class);
    objectMapper.registerSubtypes(StartpointTimestamp.class);
    objectMapper.registerSubtypes(StartpointUpcoming.class);
    objectMapper.registerSubtypes(StartpointOldest.class);
    return objectMapper;
  }

  private StartpointObjectMapper() { }

  static class CustomInstantSerializer extends JsonSerializer<Instant> {
    @Override
    public void serialize(Instant value, JsonGenerator jsonGenerator, SerializerProvider provider)
        throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(Long.valueOf(value.toEpochMilli()));
    }
  }

  static class CustomInstantDeserializer extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      return Instant.ofEpochMilli(jsonParser.getLongValue());
    }
  }
}
