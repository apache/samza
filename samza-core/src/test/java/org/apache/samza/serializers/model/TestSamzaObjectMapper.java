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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.LocalityModel;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestSamzaObjectMapper {
  private JobModel jobModel;
  private ObjectMapper samzaObjectMapper;

  @Before
  public void setup() {
    Config config = new MapConfig(ImmutableMap.of("a", "b"));
    TaskName taskName = new TaskName("test");
    Set<SystemStreamPartition> ssps = ImmutableSet.of(new SystemStreamPartition("foo", "bar", new Partition(1)));
    TaskModel taskModel = new TaskModel(taskName, ssps, new Partition(2));
    Map<TaskName, TaskModel> tasks = ImmutableMap.of(taskName, taskModel);
    ContainerModel containerModel = new ContainerModel("1", tasks);
    Map<String, ContainerModel> containerMap = ImmutableMap.of("1", containerModel);
    this.jobModel = new JobModel(config, containerMap);
    this.samzaObjectMapper = SamzaObjectMapper.getObjectMapper();
  }

  @Test
  public void testSerializeJobModel() throws IOException {
    String serializedString = this.samzaObjectMapper.writeValueAsString(this.jobModel);
    // use a plain ObjectMapper to read JSON to make comparison easier
    ObjectNode serializedAsJson = (ObjectNode) new ObjectMapper().readTree(serializedString);
    ObjectNode expectedJson = buildJobModelJson();

    /*
     * Jackson serializes all get* methods even if they aren't regular getters. We only care about certain fields now
     * since those are the only ones that get deserialized.
     */
    assertEquals(expectedJson.get("config"), serializedAsJson.get("config"));
    assertEquals(expectedJson.get("containers"), serializedAsJson.get("containers"));
  }

  @Test
  public void testSerializeTaskModel() throws IOException {
    TaskModel taskModel = new TaskModel(new TaskName("Standby Partition 0"), new HashSet<>(), new Partition(0),
        TaskMode.Standby);
    String serializedString = this.samzaObjectMapper.writeValueAsString(taskModel);
    TaskModel deserializedTaskModel = this.samzaObjectMapper.readValue(serializedString, TaskModel.class);
    assertEquals(taskModel, deserializedTaskModel);

    String sampleSerializedString = "{\"task-name\":\"Partition 0\",\"system-stream-partitions\":[],\"changelog-partition\":0}";
    deserializedTaskModel = this.samzaObjectMapper.readValue(sampleSerializedString, TaskModel.class);
    taskModel = new TaskModel(new TaskName("Partition 0"), new HashSet<>(), new Partition(0), TaskMode.Active);
    assertEquals(taskModel, deserializedTaskModel);
  }

  @Test
  public void testDeserializeJobModel() throws IOException {
    ObjectNode asJson = buildJobModelJson();
    assertEquals(this.jobModel, deserializeFromObjectNode(asJson));
  }

  /**
   * Deserialization should not fail if there are fields which are ignored.
   */
  @Test
  public void testDeserializeWithIgnoredFields() throws IOException {
    ObjectNode jobModelJson = buildJobModelJson();
    // JobModel ignores all unknown fields
    jobModelJson.put("unknown_job_model_key", "unknown_job_model_value");
    ObjectNode taskPartitionMappings = new ObjectMapper().createObjectNode();
    taskPartitionMappings.put("1", (Integer) null);
    // old key that used to be serialized
    jobModelJson.put("task-partition-mappings", taskPartitionMappings);
    ObjectNode allContainerLocality = new ObjectMapper().createObjectNode();
    allContainerLocality.put("1", (Integer) null);
    // currently gets serialized since there is a getAllContainerLocality
    jobModelJson.put("all-container-locality", allContainerLocality);
    assertEquals(this.jobModel, deserializeFromObjectNode(jobModelJson));
  }

  /**
   * Given a {@link ContainerModel} JSON with a processor-id and a container-id, deserialization should properly ignore
   * the container-id.
   */
  @Test
  public void testDeserializeContainerIdAndProcessorId() throws IOException {
    ObjectNode jobModelJson = buildJobModelJson();
    ObjectNode containerModelJson = (ObjectNode) jobModelJson.get("containers").get("1");
    containerModelJson.put("container-id", 123);
    assertEquals(this.jobModel, deserializeFromObjectNode(jobModelJson));
  }

  /**
   * Given a {@link ContainerModel} JSON with an unknown field, deserialization should properly ignore it.
   */
  @Test
  public void testDeserializeUnknownContainerModelField() throws IOException {
    ObjectNode jobModelJson = buildJobModelJson();
    ObjectNode containerModelJson = (ObjectNode) jobModelJson.get("containers").get("1");
    containerModelJson.put("unknown_container_model_key", "unknown_container_model_value");
    assertEquals(this.jobModel, deserializeFromObjectNode(jobModelJson));
  }

  /**
   * Given a {@link ContainerModel} JSON without a processor-id but with a container-id, deserialization should use the
   * container-id to calculate the processor-id.
   */
  @Test
  public void testDeserializeContainerModelOnlyContainerId() throws IOException {
    ObjectNode jobModelJson = buildJobModelJson();
    ObjectNode containerModelJson = (ObjectNode) jobModelJson.get("containers").get("1");
    containerModelJson.remove("processor-id");
    containerModelJson.put("container-id", 1);
    assertEquals(this.jobModel, deserializeFromObjectNode(jobModelJson));
  }

  /**
   * Given a {@link ContainerModel} JSON with an unknown field, deserialization should properly ignore it.
   */
  @Test
  public void testDeserializeUnknownTaskModelField() throws IOException {
    ObjectNode jobModelJson = buildJobModelJson();
    ObjectNode taskModelJson = (ObjectNode) jobModelJson.get("containers").get("1").get("tasks").get("test");
    taskModelJson.put("unknown_task_model_key", "unknown_task_model_value");
    assertEquals(this.jobModel, deserializeFromObjectNode(jobModelJson));
  }

  /**
   * Given a {@link ContainerModel} JSON with neither a processor-id nor a container-id, deserialization should fail.
   */
  @Test(expected = SamzaException.class)
  public void testDeserializeContainerModelMissingProcessorIdAndContainerId() throws IOException {
    ObjectNode jobModelJson = buildJobModelJson();
    ObjectNode containerModelJson = (ObjectNode) jobModelJson.get("containers").get("1");
    containerModelJson.remove("processor-id");
    deserializeFromObjectNode(jobModelJson);
  }

  /**
   * Given a {@link ContainerModel} JSON with only an "id" field, deserialization should fail.
   * This verifies that even though {@link ContainerModel} has a getId method, the "id" field is not used, since
   * "processor-id" is the field that is supposed to be used.
   */
  @Test(expected = SamzaException.class)
  public void testDeserializeContainerModelIdFieldOnly() throws IOException {
    ObjectNode jobModelJson = buildJobModelJson();
    ObjectNode containerModelJson = (ObjectNode) jobModelJson.get("containers").get("1");
    containerModelJson.remove("processor-id");
    containerModelJson.put("id", 1);
    deserializeFromObjectNode(jobModelJson);
  }

  @Test
  public void testSerializeSystemStreamPartition() throws IOException {
    // case 1: keyBucket not explicitly mentioned
    SystemStreamPartition ssp = new SystemStreamPartition("foo", "bar", new Partition(1));
    String serializedString = this.samzaObjectMapper.writeValueAsString(ssp);

    ObjectMapper objectMapper = new ObjectMapper();

    ObjectNode sspJson = objectMapper.createObjectNode();
    sspJson.put("system", "foo");
    sspJson.put("stream", "bar");
    sspJson.put("partition", 1);

    // use a plain ObjectMapper to read JSON to make comparison easier
    ObjectNode serializedAsJson = (ObjectNode) new ObjectMapper().readTree(serializedString);
    ObjectNode expectedJson = sspJson;

    assertEquals(expectedJson.get("system"), serializedAsJson.get("system"));
    assertEquals(expectedJson.get("stream"), serializedAsJson.get("stream"));
    assertEquals(expectedJson.get("partition"), serializedAsJson.get("partition"));

    //Case 2: with non-null keyBucket
    ssp = new SystemStreamPartition("foo", "bar", new Partition(1), 1);
    serializedString = this.samzaObjectMapper.writeValueAsString(ssp);

    sspJson = objectMapper.createObjectNode();
    sspJson.put("system", "foo");
    sspJson.put("stream", "bar");
    sspJson.put("partition", 1);
    sspJson.put("keyBucket", 1);

    // use a plain ObjectMapper to read JSON to make comparison easier
    serializedAsJson = (ObjectNode) new ObjectMapper().readTree(serializedString);
    expectedJson = sspJson;

    assertEquals(expectedJson.get("system"), serializedAsJson.get("system"));
    assertEquals(expectedJson.get("stream"), serializedAsJson.get("stream"));
    assertEquals(expectedJson.get("partition"), serializedAsJson.get("partition"));
    assertEquals(expectedJson.get("keyBucket"), serializedAsJson.get("keyBucket"));
  }

  @Test
  public void testDeserializeSystemStreamPartition() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();

    // case 1: keyBucket not explicitly mentioned
    ObjectNode sspJson = objectMapper.createObjectNode();
    sspJson.put("system", "foo");
    sspJson.put("stream", "bar");
    sspJson.put("partition", 1);

    SystemStreamPartition ssp = new SystemStreamPartition("foo", "bar", new Partition(1));
    String jsonString = new ObjectMapper().writeValueAsString(sspJson);
    SystemStreamPartition deserSSP = this.samzaObjectMapper.readValue(jsonString, SystemStreamPartition.class);

    assertEquals(ssp, deserSSP);

    // case 2: explicitly set key bucket
    sspJson = objectMapper.createObjectNode();
    sspJson.put("system", "foo");
    sspJson.put("stream", "bar");
    sspJson.put("partition", 1);
    sspJson.put("keyBucket", 1);

    ssp = new SystemStreamPartition("foo", "bar", new Partition(1), 1);
    jsonString = new ObjectMapper().writeValueAsString(sspJson);
    deserSSP = this.samzaObjectMapper.readValue(jsonString, SystemStreamPartition.class);

    assertEquals(ssp, deserSSP);
  }

  @Test
  public void testSerDePreElasticSystemStreamPartition() throws IOException {
    ObjectMapper preElasticObjectMapper = getPreEleasticObjectMapper();
    ObjectMapper elasticObjectMapper = SamzaObjectMapper.getObjectMapper();


    //Scenario 1: ssp serialized with preElasticMapper and deserialized by new Mapper with elasticity
    SystemStreamPartition ssp = new SystemStreamPartition("foo", "bar", new Partition(1));
    String serializedString = preElasticObjectMapper.writeValueAsString(ssp);

    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode serializedSSPAsJson = objectMapper.createObjectNode();
    serializedSSPAsJson.put("system", "foo");
    serializedSSPAsJson.put("stream", "bar");
    serializedSSPAsJson.put("partition", 1);

    JsonNode deserSSPAsJson = elasticObjectMapper.readTree(serializedString);

    assertEquals(serializedSSPAsJson.get("system"), deserSSPAsJson.get("system"));
    assertEquals(serializedSSPAsJson.get("stream"), deserSSPAsJson.get("stream"));
    assertEquals(serializedSSPAsJson.get("partition"), deserSSPAsJson.get("partition"));
    assertEquals(serializedSSPAsJson.get("keyBucket"), deserSSPAsJson.get("-1"));

    //Scenario 1: ssp serialized with new elasticMapper and deserialized by old preElastic Mapper
    SystemStreamPartition sspWithKeyBucket = new SystemStreamPartition("foo", "bar", new Partition(1), 1);
    serializedString = elasticObjectMapper.writeValueAsString(sspWithKeyBucket);


    serializedSSPAsJson = objectMapper.createObjectNode();
    serializedSSPAsJson.put("system", "foo");
    serializedSSPAsJson.put("stream", "bar");
    serializedSSPAsJson.put("partition", 1);
    serializedSSPAsJson.put("keyBucket", 1);

    deserSSPAsJson = preElasticObjectMapper.readTree(serializedString);

    assertEquals(serializedSSPAsJson.get("system"), deserSSPAsJson.get("system"));
    assertEquals(serializedSSPAsJson.get("stream"), deserSSPAsJson.get("stream"));
    assertEquals(serializedSSPAsJson.get("partition"), deserSSPAsJson.get("partition"));
  }

  private JobModel deserializeFromObjectNode(ObjectNode jobModelJson) throws IOException {
    // use plain ObjectMapper to get JSON string
    String jsonString = new ObjectMapper().writeValueAsString(jobModelJson);
    return this.samzaObjectMapper.readValue(jsonString, JobModel.class);
  }

  /**
   * Builds {@link ObjectNode} which matches the {@link JobModel} built in setup.
   */
  private static ObjectNode buildJobModelJson() {
    ObjectMapper objectMapper = new ObjectMapper();

    ObjectNode configJson = objectMapper.createObjectNode();
    configJson.put("a", "b");

    ObjectNode containerModel1TaskTestSSPJson = objectMapper.createObjectNode();
    containerModel1TaskTestSSPJson.put("system", "foo");
    containerModel1TaskTestSSPJson.put("stream", "bar");
    containerModel1TaskTestSSPJson.put("partition", 1);
    containerModel1TaskTestSSPJson.put("keyBucket", -1);

    ArrayNode containerModel1TaskTestSSPsJson = objectMapper.createArrayNode();
    containerModel1TaskTestSSPsJson.add(containerModel1TaskTestSSPJson);

    ObjectNode containerModel1TaskTestJson = objectMapper.createObjectNode();
    containerModel1TaskTestJson.put("task-name", "test");
    containerModel1TaskTestJson.put("system-stream-partitions", containerModel1TaskTestSSPsJson);
    containerModel1TaskTestJson.put("changelog-partition", 2);
    containerModel1TaskTestJson.put("task-mode", "Active");

    ObjectNode containerModel1TasksJson = objectMapper.createObjectNode();
    containerModel1TasksJson.put("test", containerModel1TaskTestJson);

    ObjectNode containerModel1Json = objectMapper.createObjectNode();
    // important: needs to be "processor-id" for compatibility between Samza 0.14 and 1.0
    containerModel1Json.put("processor-id", "1");
    containerModel1Json.put("tasks", containerModel1TasksJson);

    ObjectNode containersJson = objectMapper.createObjectNode();
    containersJson.put("1", containerModel1Json);

    ObjectNode jobModelJson = objectMapper.createObjectNode();
    jobModelJson.put("config", configJson);
    jobModelJson.put("containers", containersJson);

    return jobModelJson;
  }

  private static class PreElasticitySystemStreamPartitionSerializer extends JsonSerializer<SystemStreamPartition> {
    @Override
    public void serialize(SystemStreamPartition systemStreamPartition, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException,
                                                                                                                                        JsonProcessingException {
      Map<String, Object> systemStreamPartitionMap = new HashMap<String, Object>();
      systemStreamPartitionMap.put("system", systemStreamPartition.getSystem());
      systemStreamPartitionMap.put("stream", systemStreamPartition.getStream());
      systemStreamPartitionMap.put("partition", systemStreamPartition.getPartition());
      jsonGenerator.writeObject(systemStreamPartitionMap);
    }
  }

  private static class PreElasticitySystemStreamPartitionDeserializer extends JsonDeserializer<SystemStreamPartition> {
    @Override
    public SystemStreamPartition deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      String system = node.get("system").textValue();
      String stream = node.get("stream").textValue();
      Partition partition = new Partition(node.get("partition").intValue());
      return new SystemStreamPartition(system, stream, partition);
    }
  }

  private static final ObjectMapper OBJECT_MAPPER = getPreEleasticObjectMapper();
  public static ObjectMapper getPreEleasticObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.WRAP_EXCEPTIONS, false);
    mapper.configure(SerializationFeature.WRAP_EXCEPTIONS, false);
    SimpleModule module = new SimpleModule("SamzaModule", new Version(1, 0, 0, ""));

    // Setup custom serdes for simple data types.
    module.addSerializer(Partition.class, new SamzaObjectMapper.PartitionSerializer());
    module.addSerializer(SystemStreamPartition.class, new PreElasticitySystemStreamPartitionSerializer());
    module.addKeySerializer(SystemStreamPartition.class, new SamzaObjectMapper.SystemStreamPartitionKeySerializer());
    module.addSerializer(TaskName.class, new SamzaObjectMapper.TaskNameSerializer());
    module.addSerializer(TaskMode.class, new SamzaObjectMapper.TaskModeSerializer());
    module.addDeserializer(TaskName.class, new SamzaObjectMapper.TaskNameDeserializer());
    module.addDeserializer(Partition.class, new SamzaObjectMapper.PartitionDeserializer());
    module.addDeserializer(SystemStreamPartition.class, new PreElasticitySystemStreamPartitionDeserializer());
    module.addKeyDeserializer(SystemStreamPartition.class, new SamzaObjectMapper.SystemStreamPartitionKeyDeserializer());
    module.addDeserializer(Config.class, new SamzaObjectMapper.ConfigDeserializer());
    module.addDeserializer(TaskMode.class, new SamzaObjectMapper.TaskModeDeserializer());
    module.addSerializer(CheckpointId.class, new SamzaObjectMapper.CheckpointIdSerializer());
    module.addDeserializer(CheckpointId.class, new SamzaObjectMapper.CheckpointIdDeserializer());

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
    mapper.setPropertyNamingStrategy(new SamzaObjectMapper.CamelCaseToDashesStrategy());
    mapper.registerModule(module);

    return mapper;
  }

}
