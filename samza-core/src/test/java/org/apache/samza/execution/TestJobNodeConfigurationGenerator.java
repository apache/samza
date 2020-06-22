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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.StreamApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.TaskApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.storage.SideInputsProcessor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.table.descriptors.BaseTableDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.table.descriptors.TestLocalTableDescriptor.MockLocalTableDescriptor;
import org.apache.samza.table.descriptors.TestLocalTableDescriptor.MockTableProviderFactory;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.operators.impl.store.TimestampedValueSerde;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link JobNodeConfigurationGenerator}
 */
public class TestJobNodeConfigurationGenerator extends ExecutionPlannerTestBase {

  @Test
  public void testConfigureSerdesWithRepartitionJoinApplication() {
    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionJoinStreamApplication(), mockConfig);
    configureJobNode(mockStreamAppDesc);
    // create the JobGraphConfigureGenerator and generate the jobConfig for the jobNode
    JobNodeConfigurationGenerator configureGenerator = new JobNodeConfigurationGenerator();
    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "testJobGraphJson");

    // Verify the results
    Config expectedJobConfig = getExpectedJobConfig(mockConfig, mockJobNode.getInEdges());
    validateJobConfig(expectedJobConfig, jobConfig);
    // additional, check the computed window.ms for join
    assertEquals("3600000", jobConfig.get(TaskConfig.WINDOW_MS));
    Map<String, Serde> deserializedSerdes = validateAndGetDeserializedSerdes(jobConfig, 5);
    validateStreamConfigures(jobConfig, deserializedSerdes);
    validateJoinStoreConfigures(jobConfig, deserializedSerdes);
  }

  @Test
  public void testConfigureSerdesForRepartitionWithNoDefaultSystem() {
    // set the application to RepartitionOnlyStreamApplication
    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionOnlyStreamApplication(), mockConfig);
    configureJobNode(mockStreamAppDesc);

    // create the JobGraphConfigureGenerator and generate the jobConfig for the jobNode
    JobNodeConfigurationGenerator configureGenerator = new JobNodeConfigurationGenerator();
    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "testJobGraphJson");

    // Verify the results
    Config expectedJobConfig = getExpectedJobConfig(mockConfig, mockJobNode.getInEdges());
    validateJobConfig(expectedJobConfig, jobConfig);

    Map<String, Serde> deserializedSerdes = validateAndGetDeserializedSerdes(jobConfig, 2);
    validateStreamConfigures(jobConfig, null);

    String partitionByKeySerde = jobConfig.get("streams.jobName-jobId-partition_by-p1.samza.key.serde");
    String partitionByMsgSerde = jobConfig.get("streams.jobName-jobId-partition_by-p1.samza.msg.serde");
    assertTrue("Serialized serdes should not contain intermediate stream key serde",
        !deserializedSerdes.containsKey(partitionByKeySerde));
    assertTrue("Serialized serdes should not contain intermediate stream msg serde",
        !deserializedSerdes.containsKey(partitionByMsgSerde));
  }

  @Test
  public void testGenerateJobConfigWithTaskApplication() {
    // set the application to TaskApplication, which still wire up all input/output/intermediate streams
    TaskApplicationDescriptorImpl taskAppDesc = new TaskApplicationDescriptorImpl(getTaskApplication(), mockConfig);
    configureJobNode(taskAppDesc);
    // create the JobGraphConfigureGenerator and generate the jobConfig for the jobNode
    JobNodeConfigurationGenerator configureGenerator = new JobNodeConfigurationGenerator();
    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "testJobGraphJson");

    // Verify the results
    Config expectedJobConfig = getExpectedJobConfig(mockConfig, mockJobNode.getInEdges());
    validateJobConfig(expectedJobConfig, jobConfig);
    Map<String, Serde> deserializedSerdes = validateAndGetDeserializedSerdes(jobConfig, 2);
    validateStreamConfigures(jobConfig, deserializedSerdes);
  }

  @Test
  public void testGenerateJobConfigWithLegacyTaskApplication() {
    TaskApplicationDescriptorImpl taskAppDesc = new TaskApplicationDescriptorImpl(getLegacyTaskApplication(), mockConfig);
    configureJobNode(taskAppDesc);
    Map<String, String> originConfig = new HashMap<>(mockConfig);

    // create the JobGraphConfigureGenerator and generate the jobConfig for the jobNode
    JobNodeConfigurationGenerator configureGenerator = new JobNodeConfigurationGenerator();
    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "");
    // jobConfig should be exactly the same as original config
    Map<String, String> generatedConfig = new HashMap<>(jobConfig);
    assertEquals(originConfig, generatedConfig);
  }

  @Test
  public void testBroadcastStreamApplication() {
    // set the application to BroadcastStreamApplication
    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getBroadcastOnlyStreamApplication(defaultSerde), mockConfig);
    configureJobNode(mockStreamAppDesc);

    // create the JobGraphConfigureGenerator and generate the jobConfig for the jobNode
    JobNodeConfigurationGenerator configureGenerator = new JobNodeConfigurationGenerator();
    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "testJobGraphJson");
    Config expectedJobConfig = getExpectedJobConfig(mockConfig, mockJobNode.getInEdges());
    validateJobConfig(expectedJobConfig, jobConfig);
    Map<String, Serde> deserializedSerdes = validateAndGetDeserializedSerdes(jobConfig, 2);
    validateStreamSerdeConfigure(broadcastInputDesriptor.getStreamId(), jobConfig, deserializedSerdes);
    validateIntermediateStreamConfigure(broadcastInputDesriptor.getStreamId(), broadcastInputDesriptor.getPhysicalName().get(), jobConfig);
  }

  @Test
  public void testStreamApplicationWithTableAndSideInput() {
    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionJoinStreamApplication(), mockConfig);
    // add table to the RepartitionJoinStreamApplication
    GenericInputDescriptor<KV<String, Object>> sideInput1 = inputSystemDescriptor.getInputDescriptor("sideInput1", defaultSerde);
    BaseTableDescriptor mockTableDescriptor = new MockLocalTableDescriptor("testTable", defaultSerde)
        .withSideInputs(Arrays.asList(sideInput1.getStreamId()))
        .withSideInputsProcessor(mock(SideInputsProcessor.class, withSettings().serializable()))
        .withConfig("mock.table.provider.config", "mock.config.value");
    // add side input and terminate at table in the appplication
    mockStreamAppDesc.getInputStream(sideInput1).sendTo(mockStreamAppDesc.getTable(mockTableDescriptor));
    StreamEdge sideInputEdge = new StreamEdge(new StreamSpec(sideInput1.getStreamId(), "sideInput1",
        inputSystemDescriptor.getSystemName()), false, false, mockConfig);
    // need to put the sideInput related stream configuration to the original config
    // TODO: this is confusing since part of the system and stream related configuration is generated outside the JobGraphConfigureGenerator
    // It would be nice if all system and stream related configuration is generated in one place and only intermediate stream
    // configuration is generated by JobGraphConfigureGenerator
    Map<String, String> configs = new HashMap<>(mockConfig);
    configs.putAll(sideInputEdge.generateConfig());
    mockConfig = spy(new MapConfig(configs));
    configureJobNode(mockStreamAppDesc);

    // create the JobGraphConfigureGenerator and generate the jobConfig for the jobNode
    JobNodeConfigurationGenerator configureGenerator = new JobNodeConfigurationGenerator();
    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "testJobGraphJson");
    Config expectedJobConfig = getExpectedJobConfig(mockConfig, mockJobNode.getInEdges());
    validateJobConfig(expectedJobConfig, jobConfig);
    Map<String, Serde> deserializedSerdes = validateAndGetDeserializedSerdes(jobConfig, 5);
    validateTableConfigure(jobConfig, deserializedSerdes, mockTableDescriptor);
  }

  @Test
  public void testTaskApplicationWithTableAndSideInput() {
    // add table to the RepartitionJoinStreamApplication
    GenericInputDescriptor<KV<String, Object>> sideInput1 = inputSystemDescriptor.getInputDescriptor("sideInput1", defaultSerde);
    BaseTableDescriptor mockTableDescriptor = new MockLocalTableDescriptor("testTable", defaultSerde)
        .withSideInputs(Arrays.asList(sideInput1.getStreamId()))
        .withSideInputsProcessor(mock(SideInputsProcessor.class, withSettings().serializable()))
        .withConfig("mock.table.provider.config", "mock.config.value");
    StreamEdge sideInputEdge = new StreamEdge(new StreamSpec(sideInput1.getStreamId(), "sideInput1",
        inputSystemDescriptor.getSystemName()), false, false, mockConfig);
    // need to put the sideInput related stream configuration to the original config
    // TODO: this is confusing since part of the system and stream related configuration is generated outside the JobGraphConfigureGenerator
    // It would be nice if all system and stream related configuration is generated in one place and only intermediate stream
    // configuration is generated by JobGraphConfigureGenerator
    Map<String, String> configs = new HashMap<>(mockConfig);
    configs.putAll(sideInputEdge.generateConfig());
    mockConfig = spy(new MapConfig(configs));

    // set the application to TaskApplication, which still wire up all input/output/intermediate streams
    TaskApplicationDescriptorImpl taskAppDesc = new TaskApplicationDescriptorImpl(getTaskApplication(), mockConfig);
    // add table to the task application
    taskAppDesc.withTable(mockTableDescriptor);
    taskAppDesc.withInputStream(inputSystemDescriptor.getInputDescriptor("sideInput1", defaultSerde));
    configureJobNode(taskAppDesc);

    // create the JobGraphConfigureGenerator and generate the jobConfig for the jobNode
    JobNodeConfigurationGenerator configureGenerator = new JobNodeConfigurationGenerator();
    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "testJobGraphJson");

    // Verify the results
    Config expectedJobConfig = getExpectedJobConfig(mockConfig, mockJobNode.getInEdges());
    validateJobConfig(expectedJobConfig, jobConfig);
    Map<String, Serde> deserializedSerdes = validateAndGetDeserializedSerdes(jobConfig, 2);
    validateStreamConfigures(jobConfig, deserializedSerdes);
    validateTableConfigure(jobConfig, deserializedSerdes, mockTableDescriptor);
  }

  @Test
  public void testConfigRewriter() {
    Map<String, String> configs = new HashMap<>(mockConfig);
    String streamCfgToOverride = String.format("streams.%s.samza.system", intermediateInputDescriptor.getStreamId());
    configs.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, "mock"), MockConfigRewriter.class.getName());
    configs.put(JobConfig.CONFIG_REWRITERS, "mock");
    configs.put(String.format("job.config.rewriter.mock.%s", streamCfgToOverride), "rewritten-system");
    mockConfig = spy(new MapConfig(configs));
    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionJoinStreamApplication(), mockConfig);
    configureJobNode(mockStreamAppDesc);

    JobNodeConfigurationGenerator configureGenerator = new JobNodeConfigurationGenerator();
    JobConfig jobConfig = configureGenerator.generateJobConfig(mockJobNode, "testJobGraphJson");
    Config expectedConfig = getExpectedJobConfig(mockConfig, mockJobNode.getInEdges());
    validateJobConfig(expectedConfig, jobConfig);
    assertEquals("rewritten-system", jobConfig.get(streamCfgToOverride));
  }

  @Test(expected = SamzaException.class)
  public void testJobNameConfigValidation() {
    ImmutableMap<String, String> userConfigs =
        ImmutableMap.of("job.name", "samza-job", "job.id", "1", "app.name", "samza-app");
    ImmutableMap<String, String> generatedConfigs =
        ImmutableMap.of("job.name", "samza-app", "job.id", "1", "app.name", "samza-app");
    JobNodeConfigurationGenerator.validateJobConfigs(userConfigs, generatedConfigs);
  }

  @Test(expected = SamzaException.class)
  public void testJobIdConfigValidation() {
    ImmutableMap<String, String> userConfigs =
        ImmutableMap.of("job.id", "1", "app.id", "this-should-take-precedence", "app.name", "samza-app");
    ImmutableMap<String, String> generatedConfigs =
        ImmutableMap.of("job.name", "samza-app", "job.id", "this-should-take-precedence", "app.name", "samza-app");
    JobNodeConfigurationGenerator.validateJobConfigs(userConfigs, generatedConfigs);
  }

  private void validateTableConfigure(JobConfig jobConfig, Map<String, Serde> deserializedSerdes,
      TableDescriptor tableDescriptor) {
    Config tableConfig = jobConfig.subset(String.format("tables.%s.", tableDescriptor.getTableId()));
    assertEquals(MockTableProviderFactory.class.getName(), tableConfig.get("provider.factory"));
    assertEquals("mock.config.value", jobConfig.get("mock.table.provider.config"));
    validateTableSerdeConfigure(tableDescriptor.getTableId(), jobConfig, deserializedSerdes);
  }

  private Config getExpectedJobConfig(Config originConfig, Map<String, StreamEdge> inputEdges) {
    Map<String, String> configMap = new HashMap<>(originConfig);
    Set<String> inputs = new HashSet<>();
    Set<String> broadcasts = new HashSet<>();
    for (StreamEdge inputEdge : inputEdges.values()) {
      if (inputEdge.isBroadcast()) {
        broadcasts.add(inputEdge.getName() + "#0");
      } else {
        inputs.add(inputEdge.getName());
      }
    }
    if (!inputs.isEmpty()) {
      configMap.put(TaskConfig.INPUT_STREAMS, Joiner.on(',').join(inputs));
    }
    if (!broadcasts.isEmpty()) {
      configMap.put(TaskConfig.BROADCAST_INPUT_STREAMS, Joiner.on(',').join(broadcasts));
    }
    return new MapConfig(configMap);
  }

  private Map<String, Serde> validateAndGetDeserializedSerdes(Config jobConfig, int numSerdes) {
    Config serializers = jobConfig.subset("serializers.registry.", true);
    // make sure that the serializers deserialize correctly
    SerializableSerde<Serde> serializableSerde = new SerializableSerde<>();
    assertEquals(numSerdes, serializers.size());
    return serializers.entrySet().stream().collect(Collectors.toMap(
      e -> e.getKey().replace(SerializerConfig.SERIALIZED_INSTANCE_SUFFIX, ""),
      e -> serializableSerde.fromBytes(Base64.getDecoder().decode(e.getValue().getBytes()))
    ));
  }

  private void validateJobConfig(Config expectedConfig, JobConfig jobConfig) {
    assertEquals(expectedConfig.get(JobConfig.JOB_NAME), jobConfig.getName().get());
    assertEquals(expectedConfig.get(JobConfig.JOB_ID), jobConfig.getJobId());
    assertEquals("testJobGraphJson", jobConfig.get(JobNodeConfigurationGenerator.CONFIG_INTERNAL_EXECUTION_PLAN));
    assertEquals(expectedConfig.get(TaskConfig.INPUT_STREAMS), jobConfig.get(TaskConfig.INPUT_STREAMS));
    assertEquals(expectedConfig.get(TaskConfig.BROADCAST_INPUT_STREAMS), jobConfig.get(TaskConfig.BROADCAST_INPUT_STREAMS));
  }

  private void validateStreamSerdeConfigure(String streamId, Config config, Map<String, Serde> deserializedSerdes) {
    Config streamConfig = config.subset(String.format("streams.%s.samza.", streamId));
    String keySerdeName = streamConfig.get("key.serde");
    String valueSerdeName = streamConfig.get("msg.serde");
    assertTrue(String.format("Serialized serdes should contain %s key serde", streamId), deserializedSerdes.containsKey(keySerdeName));
    assertTrue(String.format("Serialized %s key serde should be a StringSerde", streamId), keySerdeName.startsWith(StringSerde.class.getSimpleName()));
    assertTrue(String.format("Serialized serdes should contain %s msg serde", streamId), deserializedSerdes.containsKey(valueSerdeName));
    assertTrue(String.format("Serialized %s msg serde should be a JsonSerdeV2", streamId), valueSerdeName.startsWith(JsonSerdeV2.class.getSimpleName()));
  }

  private void validateTableSerdeConfigure(String tableId, Config config, Map<String, Serde> deserializedSerdes) {
    Config streamConfig = config.subset(String.format("stores.%s.", tableId));
    String keySerdeName = streamConfig.get("key.serde");
    String valueSerdeName = streamConfig.get("msg.serde");
    assertTrue(String.format("Serialized serdes should contain %s key serde", tableId), deserializedSerdes.containsKey(keySerdeName));
    assertTrue(String.format("Serialized %s key serde should be a StringSerde", tableId), keySerdeName.startsWith(StringSerde.class.getSimpleName()));
    assertTrue(String.format("Serialized serdes should contain %s value serde", tableId), deserializedSerdes.containsKey(valueSerdeName));
    assertTrue(String.format("Serialized %s msg serde should be a JsonSerdeV2", tableId), valueSerdeName.startsWith(JsonSerdeV2.class.getSimpleName()));
  }

  private void validateIntermediateStreamConfigure(String streamId, String physicalName, Config config) {
    Config intStreamConfig = config.subset(String.format("streams.%s.", streamId),  true);
    assertEquals("intermediate-system", intStreamConfig.get("samza.system"));
    assertEquals(String.valueOf(Integer.MAX_VALUE), intStreamConfig.get("samza.priority"));
    assertEquals("true", intStreamConfig.get("samza.delete.committed.messages"));
    assertEquals(physicalName, intStreamConfig.get("samza.physical.name"));
    assertEquals("true", intStreamConfig.get("samza.intermediate"));
  }

  private void validateStreamConfigures(Config config, Map<String, Serde> deserializedSerdes) {

    if (deserializedSerdes != null) {
      validateStreamSerdeConfigure(input1Descriptor.getStreamId(), config, deserializedSerdes);
      validateStreamSerdeConfigure(input2Descriptor.getStreamId(), config, deserializedSerdes);
      validateStreamSerdeConfigure(outputDescriptor.getStreamId(), config, deserializedSerdes);
      validateStreamSerdeConfigure(intermediateInputDescriptor.getStreamId(), config, deserializedSerdes);
    }

    // generated stream config for intermediate stream
    String physicalName = intermediateInputDescriptor.getPhysicalName().isPresent() ?
        intermediateInputDescriptor.getPhysicalName().get() : null;
    validateIntermediateStreamConfigure(intermediateInputDescriptor.getStreamId(), physicalName, config);
  }

  private void validateJoinStoreConfigures(MapConfig mapConfig, Map<String, Serde> deserializedSerdes) {
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

    Config leftJoinStoreConfig = mapConfig.subset("stores.jobName-jobId-join-j1-L.", true);
    validateJoinStoreConfigure(leftJoinStoreConfig, "jobName-jobId-join-j1-L");
    Config rightJoinStoreConfig = mapConfig.subset("stores.jobName-jobId-join-j1-R.", true);
    validateJoinStoreConfigure(rightJoinStoreConfig, "jobName-jobId-join-j1-R");
  }

  private void validateJoinStoreConfigure(Config joinStoreConfig, String changelogName) {
    assertEquals("org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory", joinStoreConfig.get("factory"));
    assertEquals(changelogName, joinStoreConfig.get("changelog"));
    assertEquals("delete", joinStoreConfig.get("changelog.kafka.cleanup.policy"));
    assertEquals("3600000", joinStoreConfig.get("changelog.kafka.retention.ms"));
    assertEquals("3600000", joinStoreConfig.get("rocksdb.ttl.ms"));
  }

  public static class MockConfigRewriter implements ConfigRewriter {
    @Override
    public Config rewrite(String name, Config config) {
      Map<String, String> configMap = new HashMap<>(config);
      configMap.putAll(config.subset(String.format("job.config.rewriter.%s.", name)));
      return new MapConfig(configMap);
    }
  }

}
