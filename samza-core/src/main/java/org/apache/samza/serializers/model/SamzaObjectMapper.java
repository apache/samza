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

package org.apache.samza.serializers.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.ObjectCodec;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.KeyDeserializer;
import org.codehaus.jackson.map.MapperConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.introspect.AnnotatedField;
import org.codehaus.jackson.map.introspect.AnnotatedMethod;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.type.TypeReference;

/**
 * <p>
 * A collection of utility classes and (de)serializers to make Samza's job model
 * work with Jackson. Rather than annotating Samza's job model directly, the
 * Jackson-specific code is isolated so that Samza's core data model does not
 * require a direct dependency on Jackson.
 * </p>
 * 
 * <p>
 * To use Samza's job data model, use the SamzaObjectMapper.getObjectMapper()
 * method.
 * </p>
 */
public class SamzaObjectMapper {
  private static final ObjectMapper OBJECT_MAPPER = getObjectMapper();

  /**
   * @return Returns a new ObjectMapper that's been configured to (de)serialize
   *         Samza's job data model, and simple data types such as TaskName,
   *         Partition, Config, and SystemStreamPartition.
   */
  public static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("SamzaModule", new Version(1, 0, 0, ""));

    // Setup custom serdes for simple data types.
    module.addSerializer(Partition.class, new PartitionSerializer());
    module.addSerializer(SystemStreamPartition.class, new SystemStreamPartitionSerializer());
    module.addKeySerializer(SystemStreamPartition.class, new SystemStreamPartitionKeySerializer());
    module.addSerializer(TaskName.class, new TaskNameSerializer());
    module.addDeserializer(Partition.class, new PartitionDeserializer());
    module.addDeserializer(SystemStreamPartition.class, new SystemStreamPartitionDeserializer());
    module.addKeyDeserializer(SystemStreamPartition.class, new SystemStreamPartitionKeyDeserializer());
    module.addDeserializer(Config.class, new ConfigDeserializer());

    // Setup mixins for data models.
    mapper.getSerializationConfig().addMixInAnnotations(TaskModel.class, JsonTaskModelMixIn.class);
    mapper.getDeserializationConfig().addMixInAnnotations(TaskModel.class, JsonTaskModelMixIn.class);
    mapper.getSerializationConfig().addMixInAnnotations(ContainerModel.class, JsonContainerModelMixIn.class);
    mapper.getDeserializationConfig().addMixInAnnotations(ContainerModel.class, JsonContainerModelMixIn.class);
    mapper.getSerializationConfig().addMixInAnnotations(JobModel.class, JsonJobModelMixIn.class);
    mapper.getDeserializationConfig().addMixInAnnotations(JobModel.class, JsonJobModelMixIn.class);
    mapper.configure(SerializationConfig.Feature.AUTO_DETECT_GETTERS, false);

    // Convert camel case to hyphenated field names, and register the module.
    mapper.setPropertyNamingStrategy(new CamelCaseToDashesStrategy());
    mapper.registerModule(module);

    return mapper;
  }

  public static class ConfigDeserializer extends JsonDeserializer<Config> {
    @Override
    public Config deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      return new MapConfig(OBJECT_MAPPER.<Map<String, String>>readValue(node, new TypeReference<Map<String, String>>() {
      }));
    }
  }

  public static class PartitionSerializer extends JsonSerializer<Partition> {
    @Override
    public void serialize(Partition partition, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(Integer.valueOf(partition.getPartitionId()));
    }
  }

  public static class PartitionDeserializer extends JsonDeserializer<Partition> {
    @Override
    public Partition deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      return new Partition(node.getIntValue());
    }
  }

  public static class TaskNameSerializer extends JsonSerializer<TaskName> {
    @Override
    public void serialize(TaskName taskName, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(taskName.toString());
    }
  }

  public static class TaskNameDeserializer extends JsonDeserializer<TaskName> {
    @Override
    public TaskName deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      return new TaskName(node.getTextValue());
    }
  }

  public static class SystemStreamPartitionKeySerializer extends JsonSerializer<SystemStreamPartition> {
    @Override
    public void serialize(SystemStreamPartition systemStreamPartition, JsonGenerator jgen, SerializerProvider provider) throws IOException {
      String ssp = Util.sspToString(systemStreamPartition);
      jgen.writeFieldName(ssp);
    }
  }

  public static class SystemStreamPartitionKeyDeserializer extends KeyDeserializer {
    @Override
    public Object deserializeKey(String sspString, DeserializationContext ctxt) throws IOException {
      return Util.stringToSsp(sspString);
    }
  }

  public static class SystemStreamPartitionSerializer extends JsonSerializer<SystemStreamPartition> {
    @Override
    public void serialize(SystemStreamPartition systemStreamPartition, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException, JsonProcessingException {
      Map<String, Object> systemStreamPartitionMap = new HashMap<String, Object>();
      systemStreamPartitionMap.put("system", systemStreamPartition.getSystem());
      systemStreamPartitionMap.put("stream", systemStreamPartition.getStream());
      systemStreamPartitionMap.put("partition", systemStreamPartition.getPartition());
      jsonGenerator.writeObject(systemStreamPartitionMap);
    }
  }

  public static class SystemStreamPartitionDeserializer extends JsonDeserializer<SystemStreamPartition> {
    @Override
    public SystemStreamPartition deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      String system = node.get("system").getTextValue();
      String stream = node.get("stream").getTextValue();
      Partition partition = new Partition(node.get("partition").getIntValue());
      return new SystemStreamPartition(system, stream, partition);
    }
  }

  /**
   * A Jackson property naming strategy that converts camel case JSON fields to
   * hyphenated names. For example, myVariableName would be converted to
   * my-variable-name.
   */
  public static class CamelCaseToDashesStrategy extends PropertyNamingStrategy {
    @Override
    public String nameForField(MapperConfig<?> config, AnnotatedField field, String defaultName) {
      return convert(defaultName);
    }

    @Override
    public String nameForGetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
      return convert(defaultName);
    }

    @Override
    public String nameForSetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
      return convert(defaultName);
    }

    public String convert(String defaultName) {
      StringBuilder builder = new StringBuilder();
      char[] arr = defaultName.toCharArray();
      for (int i = 0; i < arr.length; ++i) {
        if (Character.isUpperCase(arr[i])) {
          builder.append("-" + Character.toLowerCase(arr[i]));
        } else {
          builder.append(arr[i]);
        }
      }
      return builder.toString();
    }
  }
}
