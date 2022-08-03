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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.checkpoint.kafka.KafkaStateCheckpointMarker;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.JobCoordinatorMetadata;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.LocalityModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
    mapper.configure(DeserializationFeature.WRAP_EXCEPTIONS, false);
    mapper.configure(SerializationFeature.WRAP_EXCEPTIONS, false);
    SimpleModule module = new SimpleModule("SamzaModule", new Version(1, 0, 0, ""));

    // Setup custom serdes for simple data types.
    module.addSerializer(Partition.class, new PartitionSerializer());
    module.addSerializer(SystemStreamPartition.class, new SystemStreamPartitionSerializer());
    module.addKeySerializer(SystemStreamPartition.class, new SystemStreamPartitionKeySerializer());
    module.addSerializer(TaskName.class, new TaskNameSerializer());
    module.addSerializer(TaskMode.class, new TaskModeSerializer());
    module.addDeserializer(TaskName.class, new TaskNameDeserializer());
    module.addDeserializer(Partition.class, new PartitionDeserializer());
    module.addDeserializer(SystemStreamPartition.class, new SystemStreamPartitionDeserializer());
    module.addKeyDeserializer(SystemStreamPartition.class, new SystemStreamPartitionKeyDeserializer());
    module.addDeserializer(Config.class, new ConfigDeserializer());
    module.addDeserializer(TaskMode.class, new TaskModeDeserializer());
    module.addSerializer(CheckpointId.class, new CheckpointIdSerializer());
    module.addDeserializer(CheckpointId.class, new CheckpointIdDeserializer());

    // Setup mixins for data models.
    mapper.addMixIn(TaskModel.class, JsonTaskModelMixIn.class);
    mapper.addMixIn(ContainerModel.class, JsonContainerModelMixIn.class);
    mapper.addMixIn(JobModel.class, JsonJobModelMixIn.class);
    mapper.addMixIn(CheckpointV2.class, JsonCheckpointV2Mixin.class);
    mapper.addMixIn(KafkaStateCheckpointMarker.class, KafkaStateCheckpointMarkerMixin.class);

    module.addDeserializer(ContainerModel.class, new JsonDeserializer<ContainerModel>() {
      @Override
      public ContainerModel deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec oc = jp.getCodec();
        JsonNode node = oc.readTree(jp);
        /*
         * Before Samza 0.13, "container-id" was used.
         * In Samza 0.13, "processor-id" was added to be the id to use and "container-id" was deprecated. However,
         * "container-id" still needed to be checked for backwards compatibility in case "processor-id" was missing
         * (i.e. from a job model corresponding to a version of the job that was on a pre Samza 0.13 version).
         * In Samza 1.0, "container-id" was further cleaned up from ContainerModel. This logic is still being left here
         * as a fallback for backwards compatibility with pre Samza 0.13. ContainerModel.getProcessorId was changed to
         * ContainerModel.getId in the Java API, but "processor-id" still needs to be used as the JSON key for backwards
         * compatibility with Samza 0.13 and Samza 0.14.
         */
        String id;
        if (node.get(JsonContainerModelMixIn.PROCESSOR_ID_KEY) == null) {
          if (node.get(JsonContainerModelMixIn.CONTAINER_ID_KEY) == null) {
            throw new SamzaException(
                String.format("JobModel was missing %s and %s. This should never happen. JobModel corrupt!",
                    JsonContainerModelMixIn.PROCESSOR_ID_KEY, JsonContainerModelMixIn.CONTAINER_ID_KEY));
          }
          id = String.valueOf(node.get(JsonContainerModelMixIn.CONTAINER_ID_KEY).intValue());
        } else {
          id = node.get(JsonContainerModelMixIn.PROCESSOR_ID_KEY).textValue();
        }
        Map<TaskName, TaskModel> tasksMapping =
            OBJECT_MAPPER.readValue(OBJECT_MAPPER.treeAsTokens(node.get(JsonContainerModelMixIn.TASKS_KEY)),
                new TypeReference<Map<TaskName, TaskModel>>() { });
        return new ContainerModel(id, tasksMapping);
      }
    });

    mapper.addMixIn(LocalityModel.class, JsonLocalityModelMixIn.class);
    mapper.addMixIn(ProcessorLocality.class, JsonProcessorLocalityMixIn.class);

    // Register mixins for job coordinator metadata model
    mapper.addMixIn(JobCoordinatorMetadata.class, JsonJobCoordinatorMetadataMixIn.class);

    // Convert camel case to hyphenated field names, and register the module.
    mapper.setPropertyNamingStrategy(new CamelCaseToDashesStrategy());
    mapper.registerModules(module, new Jdk8Module());

    return mapper;
  }

  public static class ConfigDeserializer extends JsonDeserializer<Config> {
    @Override
    public Config deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      return new MapConfig(OBJECT_MAPPER.readValue(OBJECT_MAPPER.treeAsTokens(node), new TypeReference<Map<String, String>>() {
      }));
    }
  }

  public static class PartitionSerializer extends JsonSerializer<Partition> {
    @Override
    public void serialize(Partition partition, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(partition.getPartitionId());
    }
  }

  public static class PartitionDeserializer extends JsonDeserializer<Partition> {
    @Override
    public Partition deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      return new Partition(node.intValue());
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
      return new TaskName(node.textValue());
    }
  }

  public static class TaskModeSerializer extends JsonSerializer<TaskMode> {
    @Override
    public void serialize(TaskMode taskMode, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(taskMode.toString());
    }
  }

  public static class TaskModeDeserializer extends JsonDeserializer<TaskMode> {
    @Override
    public TaskMode deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      if (node == null || node.textValue().equals("")) {
        return TaskMode.Active;
      } else {
        return TaskMode.valueOf(node.textValue());
      }
    }
  }

  public static class SystemStreamPartitionKeySerializer extends JsonSerializer<SystemStreamPartition> {
    @Override
    public void serialize(SystemStreamPartition ssp, JsonGenerator jgen, SerializerProvider provider) throws IOException {
      String sspString = ssp.getSystem() + "." + ssp.getStream() + "."
          + String.valueOf(ssp.getPartition().getPartitionId()) + "."
          + String.valueOf(ssp.getKeyBucket());
      jgen.writeFieldName(sspString);
    }
  }

  public static class SystemStreamPartitionKeyDeserializer extends KeyDeserializer {
    @Override
    public Object deserializeKey(String sspString, DeserializationContext ctxt) throws IOException {
      String[] parts = sspString.split("\\.");
      if (parts.length < 3 || parts.length > 4) {
        throw new IllegalArgumentException("System stream partition expected in format 'system.stream.partition' "
            + "or 'system.stream.partition.keyBucket");
      }
      if (parts.length == 3) {
        return new SystemStreamPartition(
            new SystemStream(parts[0], parts[1]), new Partition(Integer.parseInt(parts[2])));
      }
      // else parts.length == 4 and the 4th part is the keyBucket
      return new SystemStreamPartition(parts[0], parts[1], new Partition(Integer.parseInt(parts[2])), Integer.parseInt(parts[3]));
    }
  }

  public static class SystemStreamPartitionSerializer extends JsonSerializer<SystemStreamPartition> {
    @Override
    public void serialize(SystemStreamPartition systemStreamPartition, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException, JsonProcessingException {
      Map<String, Object> systemStreamPartitionMap = new HashMap<String, Object>();
      systemStreamPartitionMap.put("system", systemStreamPartition.getSystem());
      systemStreamPartitionMap.put("stream", systemStreamPartition.getStream());
      systemStreamPartitionMap.put("partition", systemStreamPartition.getPartition());
      systemStreamPartitionMap.put("keyBucket", systemStreamPartition.getKeyBucket());
      jsonGenerator.writeObject(systemStreamPartitionMap);
    }
  }

  public static class SystemStreamPartitionDeserializer extends JsonDeserializer<SystemStreamPartition> {
    @Override
    public SystemStreamPartition deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      String system = node.get("system").textValue();
      String stream = node.get("stream").textValue();
      Partition partition = new Partition(node.get("partition").intValue());
      int keyBucket = -1;
      if (node.get("keyBucket") != null) {
        keyBucket = node.get("keyBucket").intValue();
      }
      return new SystemStreamPartition(system, stream, partition, keyBucket);
    }
  }

  public static class CheckpointIdSerializer extends JsonSerializer<CheckpointId> {
    @Override
    public void serialize(CheckpointId checkpointId, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeString(checkpointId.serialize());
    }
  }

  public static class CheckpointIdDeserializer extends JsonDeserializer<CheckpointId> {
    @Override
    public CheckpointId deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      return CheckpointId.deserialize(node.textValue());
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
