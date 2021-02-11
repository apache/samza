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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
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
}
