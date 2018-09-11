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
package org.apache.samza.execution;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationDescriptorImpl;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.TaskApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.GenericOutputDescriptor;
import org.apache.samza.operators.descriptors.GenericSystemDescriptor;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.impl.store.TimestampedValueSerde;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.IdentityStreamTask;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit test for {@link JobGraphConfigureGenerator}
 */
public class TestJobGraphConfigureGenerator {

  private StreamApplicationDescriptorImpl mockStreamAppDesc;
  private Config mockConfig;
  private JobNode mockJobNode;
  private StreamSpec input1Spec;
  private StreamSpec input2Spec;
  private StreamSpec outputSpec;
  private StreamSpec repartitionSpec;
  private KVSerde<String, Object> defaultSerde;
  private GenericSystemDescriptor inputSystemDescriptor;
  private GenericSystemDescriptor outputSystemDescriptor;
  private GenericSystemDescriptor intermediateSystemDescriptor;
  private GenericInputDescriptor<KV<String, Object>> input1Descriptor;
  private GenericInputDescriptor<KV<String, Object>> input2Descriptor;
  private GenericInputDescriptor<KV<String, Object>> intermediateInputDescriptor;
  private GenericOutputDescriptor<KV<String, Object>> outputDescriptor;
  private GenericOutputDescriptor<KV<String, Object>> intermediateOutputDescriptor;

  @Before
  public void setUp() {
    input1Spec = new StreamSpec("input1", "input1", "input-system");
    input2Spec = new StreamSpec("input2", "input2", "input-system");
    outputSpec = new StreamSpec("output", "output", "output-system");
    repartitionSpec =
        new StreamSpec("jobName-jobId-partition_by-p1", "partition_by-p1", "intermediate-system");

    defaultSerde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>());
    inputSystemDescriptor = new GenericSystemDescriptor("input-system", "mockSystemFactoryClassName");
    outputSystemDescriptor = new GenericSystemDescriptor("output-system", "mockSystemFactoryClassName");
    intermediateSystemDescriptor = new GenericSystemDescriptor("intermediate-system", "mockSystemFactoryClassName");
    input1Descriptor = inputSystemDescriptor.getInputDescriptor("input1", defaultSerde);
    input2Descriptor = inputSystemDescriptor.getInputDescriptor("input2", defaultSerde);
    outputDescriptor = outputSystemDescriptor.getOutputDescriptor("output", defaultSerde);
    intermediateInputDescriptor = intermediateSystemDescriptor.getInputDescriptor("jobName-jobId-partition_by-p1", defaultSerde)
        .withPhysicalName("partition_by-p1");
    intermediateOutputDescriptor = intermediateSystemDescriptor.getOutputDescriptor("jobName-jobId-partition_by-p1", defaultSerde)
        .withPhysicalName("partition_by-p1");

    mockConfig = mock(Config.class);
    when(mockConfig.get(JobConfig.JOB_NAME())).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("jobId");

    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionJoinStreamApplication(), mockConfig);

    mockJobNode = mock(JobNode.class);
    StreamEdge input1Edge = new StreamEdge(input1Spec, false, false, mockConfig);
    StreamEdge input2Edge = new StreamEdge(input2Spec, false, false, mockConfig);
    StreamEdge outputEdge = new StreamEdge(outputSpec, false, false, mockConfig);
    StreamEdge repartitionEdge = new StreamEdge(repartitionSpec, true, false, mockConfig);
    List<StreamEdge> inputEdges = new ArrayList<>();
    inputEdges.add(input1Edge);
    inputEdges.add(input2Edge);
    inputEdges.add(repartitionEdge);
    List<StreamEdge> outputEdges = new ArrayList<>();
    outputEdges.add(outputEdge);
    outputEdges.add(repartitionEdge);
    when(mockJobNode.getInEdges()).thenReturn(inputEdges);
    when(mockJobNode.getOutEdges()).thenReturn(outputEdges);
    when(mockJobNode.getConfig()).thenReturn(mockConfig);
    when(mockJobNode.getJobName()).thenReturn("jobName");
    when(mockJobNode.getJobId()).thenReturn("jobId");
  }

  @Test
  public void testConfigureSerdesWithRepartitionJoinApplication() {
    JobGraphConfigureGenerator configureGenerator = new JobGraphConfigureGenerator(mockStreamAppDesc);
//    Collection<OperatorSpec> reachableOperators = configureGenerator.getReachableOperators(mockJobNode);
//    assertEquals(reachableOperators.toArray(), mockStreamAppDesc.getOperatorSpecGraph().getAllOperatorSpecs().toArray());
//    List<StoreDescriptor> stores = configureGenerator.getStoreDescriptors(reachableOperators);
//    Optional<OperatorSpec> joinOpOptional = reachableOperators.stream().filter(operatorSpec ->
//        operatorSpec.getOpCode() == OperatorSpec.OpCode.JOIN).findFirst();
//    assertEquals(2, stores.size());
//    List<StoreDescriptor> nonJoinStores = stores.stream().filter(store ->
//        !store.getStoreName().contains(joinOpOptional.get().getOpId())).collect(Collectors.toList());
//    assertEquals(0, nonJoinStores.size());

    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "testJobGraphJson");

    Config serializers = jobConfig.subset("serializers.registry.", true);

    // make sure that the serializers deserialize correctly
    SerializableSerde<Serde> serializableSerde = new SerializableSerde<>();
    Map<String, Serde> deserializedSerdes = serializers.entrySet().stream().collect(Collectors.toMap(
        e -> e.getKey().replace(SerializerConfig.SERIALIZED_INSTANCE_SUFFIX(), ""),
        e -> serializableSerde.fromBytes(Base64.getDecoder().decode(e.getValue().getBytes()))
    ));
    assertEquals(5, serializers.size()); // 2 default + 3 specific for join
    validateStreamSerdeConfigures(jobConfig, deserializedSerdes);
    validateJoinStoreSerdeConfigures(jobConfig, deserializedSerdes);
  }

  @Test
  public void testConfigureSerdesForRepartitionWithNoDefaultSystem() {
    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionOnlyStreamApplication(), mockConfig);

    StreamEdge reparStreamEdge = new StreamEdge(repartitionSpec, true, false, mockConfig);
    List<StreamEdge> inputEdges = new ArrayList<>();
    inputEdges.add(new StreamEdge(input1Spec, false, false, mockConfig));
    inputEdges.add(reparStreamEdge);
    List<StreamEdge> outputEdges = new ArrayList<>();
    outputEdges.add(reparStreamEdge);
    when(mockJobNode.getInEdges()).thenReturn(inputEdges);
    when(mockJobNode.getOutEdges()).thenReturn(outputEdges);

    JobGraphConfigureGenerator configureGenerator = new JobGraphConfigureGenerator(mockStreamAppDesc);

//    Collection<OperatorSpec> reachableOperators = configureGenerator.getReachableOperators(mockJobNode);
//    assertEquals(reachableOperators.toArray(), mockStreamAppDesc.getOperatorSpecGraph().getAllOperatorSpecs().toArray());
//    List<StoreDescriptor> stores = configureGenerator.getStoreDescriptors(reachableOperators);
//    assertEquals(0, stores.size());

    MapConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "testJobGraphJson");
    Config serializers = jobConfig.subset("serializers.registry.", true);

    // make sure that the serializers deserialize correctly
    SerializableSerde<Serde> serializableSerde = new SerializableSerde<>();
    Map<String, Serde> deserializedSerdes = serializers.entrySet().stream().collect(Collectors.toMap(
        e -> e.getKey().replace(SerializerConfig.SERIALIZED_INSTANCE_SUFFIX(), ""),
        e -> serializableSerde.fromBytes(Base64.getDecoder().decode(e.getValue().getBytes()))
    ));
    assertEquals(2, serializers.size()); // 2 input stream

    String partitionByKeySerde = jobConfig.get("streams.jobName-jobId-partition_by-p1.samza.key.serde");
    String partitionByMsgSerde = jobConfig.get("streams.jobName-jobId-partition_by-p1.samza.msg.serde");
    assertTrue("Serialized serdes should not contain intermediate stream key serde",
        !deserializedSerdes.containsKey(partitionByKeySerde));
    assertTrue("Serialized serdes should not contain intermediate stream msg serde",
        !deserializedSerdes.containsKey(partitionByMsgSerde));
  }

  @Test
  public void testGenerateJobConfigWithTaskApplication() {
    TaskApplicationDescriptorImpl taskAppDesc = new TaskApplicationDescriptorImpl(getTaskApplication(), mockConfig);
    JobGraphConfigureGenerator configureGenerator = new JobGraphConfigureGenerator(taskAppDesc);
    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "");

    Config serializers = jobConfig.subset("serializers.registry.", true);
    SerializableSerde<Serde> serializableSerde = new SerializableSerde<>();
    Map<String, Serde> deserializedSerdes = serializers.entrySet().stream().collect(Collectors.toMap(
        e -> e.getKey().replace(SerializerConfig.SERIALIZED_INSTANCE_SUFFIX(), ""),
        e -> serializableSerde.fromBytes(Base64.getDecoder().decode(e.getValue().getBytes()))
    ));
    assertEquals(2, serializers.size()); // 2 default
    validateStreamSerdeConfigures(jobConfig, deserializedSerdes);
  }

  @Test
  public void testGenerateJobConfigWithLegacyTaskApplication() {
    Map<String, String> originConfig = new HashMap<>();
    originConfig.put(JobConfig.JOB_NAME(), "jobName1");
    originConfig.put(JobConfig.JOB_ID(), "jobId1");
    TaskApplicationDescriptorImpl taskAppDesc = new TaskApplicationDescriptorImpl(getLegacyTaskApplication(), mockConfig);
    when(mockJobNode.getInEdges()).thenReturn(new ArrayList<>());
    when(mockJobNode.getOutEdges()).thenReturn(new ArrayList<>());
    when(mockJobNode.getJobName()).thenReturn("jobName1");
    when(mockJobNode.getJobId()).thenReturn("jobId1");
    when(mockJobNode.getConfig()).thenReturn(new MapConfig(originConfig));
    JobGraphConfigureGenerator configureGenerator = new JobGraphConfigureGenerator(taskAppDesc);
    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "");
    // jobConfig should be exactly the same as original config
    Map<String, String> generatedConfig = new HashMap<>(jobConfig);
    assertEquals(originConfig, generatedConfig);
  }

  private void validateJoinStoreSerdeConfigures(MapConfig mapConfig, Map<String, Serde> deserializedSerdes) {
    String leftJoinStoreKeySerde = mapConfig.get("stores.jobName-jobId-join-j1-L.key.serde");
    String leftJoinStoreMsgSerde = mapConfig.get("stores.jobName-jobId-join-j1-L.msg.serde");
    assertTrue("Serialized serdes should contain left join store key serde",
        deserializedSerdes.containsKey(leftJoinStoreKeySerde));
    assertTrue("Serialized left join store key serde should be a StringSerde",
        leftJoinStoreKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain left join store msg serde",
        deserializedSerdes.containsKey(leftJoinStoreMsgSerde));
    assertTrue("Serialized left join store msg serde should be a TimestampedValueSerde",
        leftJoinStoreMsgSerde.startsWith(TimestampedValueSerde.class.getSimpleName()));

    String rightJoinStoreKeySerde = mapConfig.get("stores.jobName-jobId-join-j1-R.key.serde");
    String rightJoinStoreMsgSerde = mapConfig.get("stores.jobName-jobId-join-j1-R.msg.serde");
    assertTrue("Serialized serdes should contain right join store key serde",
        deserializedSerdes.containsKey(rightJoinStoreKeySerde));
    assertTrue("Serialized right join store key serde should be a StringSerde",
        rightJoinStoreKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain right join store msg serde",
        deserializedSerdes.containsKey(rightJoinStoreMsgSerde));
    assertTrue("Serialized right join store msg serde should be a TimestampedValueSerde",
        rightJoinStoreMsgSerde.startsWith(TimestampedValueSerde.class.getSimpleName()));
  }

  private void validateStreamSerdeConfigures(Config config, Map<String, Serde> deserializedSerdes) {
    String input1KeySerde = config.get("streams.input1.samza.key.serde");
    String input1MsgSerde = config.get("streams.input1.samza.msg.serde");
    assertTrue("Serialized serdes should contain input1 key serde",
        deserializedSerdes.containsKey(input1KeySerde));
    assertTrue("Serialized input1 key serde should be a StringSerde",
        input1KeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain input1 msg serde",
        deserializedSerdes.containsKey(input1MsgSerde));
    assertTrue("Serialized input1 msg serde should be a JsonSerdeV2",
        input1MsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));

    String input2KeySerde = config.get("streams.input2.samza.key.serde");
    String input2MsgSerde = config.get("streams.input2.samza.msg.serde");
    assertTrue("Serialized serdes should contain input2 key serde",
        deserializedSerdes.containsKey(input2KeySerde));
    assertTrue("Serialized input2 key serde should be a StringSerde",
        input2KeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain input2 msg serde",
        deserializedSerdes.containsKey(input2MsgSerde));
    assertTrue("Serialized input2 msg serde should be a JsonSerdeV2",
        input2MsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));

    String outputKeySerde = config.get("streams.output.samza.key.serde");
    String outputMsgSerde = config.get("streams.output.samza.msg.serde");
    assertTrue("Serialized serdes should contain output key serde",
        deserializedSerdes.containsKey(outputKeySerde));
    assertTrue("Serialized output key serde should be a StringSerde",
        outputKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain output msg serde",
        deserializedSerdes.containsKey(outputMsgSerde));
    assertTrue("Serialized output msg serde should be a JsonSerdeV2",
        outputMsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));

    String partitionByKeySerde = config.get("streams.jobName-jobId-partition_by-p1.samza.key.serde");
    String partitionByMsgSerde = config.get("streams.jobName-jobId-partition_by-p1.samza.msg.serde");
    assertTrue("Serialized serdes should contain intermediate stream key serde",
        deserializedSerdes.containsKey(partitionByKeySerde));
    assertTrue("Serialized intermediate stream key serde should be a StringSerde",
        partitionByKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain intermediate stream msg serde",
        deserializedSerdes.containsKey(partitionByMsgSerde));
    assertTrue(
        "Serialized intermediate stream msg serde should be a JsonSerdeV2",
        partitionByMsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));
  }

  private TaskApplication getTaskApplication() {
    return appDesc -> {
      appDesc.addInputStream(input1Descriptor);
      appDesc.addInputStream(input2Descriptor);
      appDesc.addInputStream(intermediateInputDescriptor);
      appDesc.addOutputStream(intermediateOutputDescriptor);
      appDesc.addOutputStream(outputDescriptor);
      appDesc.setTaskFactory(() -> new IdentityStreamTask());
    };
  }

  private TaskApplication getLegacyTaskApplication() {
    return new LegacyTaskApplication(IdentityStreamTask.class.getName());
  }

  private StreamApplication getRepartitionJoinStreamApplication() {
    return appDesc -> {
        MessageStream<KV<String, Object>> input1 = appDesc.getInputStream(input1Descriptor);
        MessageStream<KV<String, Object>> input2 = appDesc.getInputStream(input2Descriptor);
        OutputStream<KV<String, Object>> output = appDesc.getOutputStream(outputDescriptor);
        JoinFunction<String, Object, Object, KV<String, Object>> mockJoinFn = mock(JoinFunction.class);
        input1
            .partitionBy(KV::getKey, KV::getValue, defaultSerde, "p1")
            .map(kv -> kv.value)
            .join(input2.map(kv -> kv.value), mockJoinFn,
                new StringSerde(), new JsonSerdeV2<>(Object.class), new JsonSerdeV2<>(Object.class),
                Duration.ofHours(1), "j1")
            .sendTo(output);
      };
  }

  private StreamApplication getRepartitionOnlyStreamApplication() {
    return appDesc -> {
      MessageStream<KV<String, Object>> input = appDesc.getInputStream(input1Descriptor);
      input.partitionBy(KV::getKey, KV::getValue, "p1");
    };
  }
}
