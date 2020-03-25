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
package org.apache.samza.clustermanager.container.placement;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.samza.container.placement.ContainerPlacementMessage;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.apache.samza.container.placement.ContainerPlacementResponseMessage;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.ObjectCodec;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.jsontype.NamedType;
import org.codehaus.jackson.map.module.SimpleModule;

/**
 * Object mapper for serializing and deserializing Container placement messages
 */
public class ContainerPlacementMessageObjectMapper {

  private static ObjectMapper objectMapper = null;

  private ContainerPlacementMessageObjectMapper() {
  }

  static ObjectMapper getObjectMapper() {
    if (objectMapper == null) {
      objectMapper = createObjectMapper();
    }
    return objectMapper;
  }

  private static ObjectMapper createObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("ContainerPlacementModule", new Version(1, 0, 0, ""));
    module.addSerializer(ContainerPlacementMessage.class,
        new ContainerPlacementMessageObjectMapper.ContainerPlacementMessageSerializer());
    module.addDeserializer(ContainerPlacementMessage.class,
        new ContainerPlacementMessageObjectMapper.ContainerPlacementMessageDeserializer());
    objectMapper.registerModule(module);
    objectMapper.registerSubtypes(new NamedType(ContainerPlacementResponseMessage.class));
    objectMapper.registerSubtypes(new NamedType(ContainerPlacementRequestMessage.class));
    return objectMapper;
  }

  static class ContainerPlacementMessageSerializer extends JsonSerializer<ContainerPlacementMessage> {
    @Override
    public void serialize(ContainerPlacementMessage value, JsonGenerator jsonGenerator, SerializerProvider provider)
        throws IOException {
      Map<String, Object> containerPlacementMessageMap = new HashMap<String, Object>();
      if (value instanceof ContainerPlacementRequestMessage) {
        containerPlacementMessageMap.put("subType", ContainerPlacementRequestMessage.class.getSimpleName());
      } else if (value instanceof ContainerPlacementResponseMessage) {
        containerPlacementMessageMap.put("subType", ContainerPlacementResponseMessage.class.getSimpleName());
        containerPlacementMessageMap.put("responseMessage",
            ((ContainerPlacementResponseMessage) value).getResponseMessage());
      }
      if (value.getRequestExpiry() != null) {
        containerPlacementMessageMap.put("requestExpiry", value.getRequestExpiry().toMillis());
      }
      containerPlacementMessageMap.put("uuid", value.getUuid().toString());
      containerPlacementMessageMap.put("deploymentId", value.getDeploymentId());
      containerPlacementMessageMap.put("processorId", value.getProcessorId());
      containerPlacementMessageMap.put("destinationHost", value.getDestinationHost());
      containerPlacementMessageMap.put("statusCode", value.getStatusCode().name());
      containerPlacementMessageMap.put("timestamp", value.getTimestamp());
      jsonGenerator.writeObject(containerPlacementMessageMap);
    }
  }

  static class ContainerPlacementMessageDeserializer extends JsonDeserializer<ContainerPlacementMessage> {
    @Override
    public ContainerPlacementMessage deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      String subType = node.get("subType").getTextValue();
      String deploymentId = node.get("deploymentId").getTextValue();
      String processorId = node.get("processorId").getTextValue();
      String destinationHost = node.get("destinationHost").getTextValue();
      long timestamp = node.get("timestamp").getLongValue();
      Duration requestExpiry =
          node.get("requestExpiry") == null ? null : Duration.ofMillis(node.get("requestExpiry").getLongValue());
      ContainerPlacementMessage.StatusCode statusCode = null;
      UUID uuid = UUID.fromString(node.get("uuid").getTextValue());
      for (ContainerPlacementMessage.StatusCode code : ContainerPlacementMessage.StatusCode.values()) {
        if (code.name().equals(node.get("statusCode").getTextValue())) {
          statusCode = code;
        }
      }
      ContainerPlacementMessage message = null;
      if (subType.equals(ContainerPlacementRequestMessage.class.getSimpleName())) {
        message = new ContainerPlacementRequestMessage(uuid, deploymentId, processorId, destinationHost, requestExpiry, timestamp);
      } else if (subType.equals(ContainerPlacementResponseMessage.class.getSimpleName())) {
        String responseMessage = node.get("responseMessage").getTextValue();
        message = new ContainerPlacementResponseMessage(uuid, deploymentId, processorId, destinationHost, requestExpiry,
            statusCode, responseMessage, timestamp);
      }
      return message;
    }
  }
}
