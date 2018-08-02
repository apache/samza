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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.GenericOutputDescriptor;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.testUtils.StreamTestUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestExecutionPlanner {

  private static final String DEFAULT_SYSTEM = "test-system";
  private static final int DEFAULT_PARTITIONS = 10;

  private SystemAdmins systemAdmins;
  private StreamManager streamManager;
  private Config config;

  private StreamSpec input1Spec;
  private GenericInputDescriptor<KV<Object, Object>> input1Descriptor;
  private StreamSpec input2Spec;
  private GenericInputDescriptor<KV<Object, Object>> input2Descriptor;
  private StreamSpec input3Spec;
  private GenericInputDescriptor<KV<Object, Object>> input3Descriptor;
  private StreamSpec input4Spec;
  private GenericInputDescriptor<KV<Object, Object>> input4Descriptor;
  private StreamSpec output1Spec;
  private GenericOutputDescriptor<KV<Object, Object>> output1Descriptor;
  private StreamSpec output2Spec;
  private GenericOutputDescriptor<KV<Object, Object>> output2Descriptor;

  static SystemAdmin createSystemAdmin(Map<String, Integer> streamToPartitions) {

    return new SystemAdmin() {
      @Override
      public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
        return null;
      }

      @Override
      public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
        Map<String, SystemStreamMetadata> map = new HashMap<>();
        for (String stream : streamNames) {
          Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> m = new HashMap<>();
          for (int i = 0; i < streamToPartitions.get(stream); i++) {
            m.put(new Partition(i), new SystemStreamMetadata.SystemStreamPartitionMetadata("", "", ""));
          }
          map.put(stream, new SystemStreamMetadata(stream, m));
        }
        return map;
      }

      @Override
      public Integer offsetComparator(String offset1, String offset2) {
        return null;
      }
    };
  }

  private StreamGraphSpec createSimpleGraph() {
    /**
     * a simple graph of partitionBy and map
     *
     * input1 -> partitionBy -> map -> output1
     *
     */
    StreamGraphSpec graphSpec = new StreamGraphSpec(config);
    MessageStream<KV<Object, Object>> input1 = graphSpec.getInputStream(input1Descriptor);
    OutputStream<KV<Object, Object>> output1 = graphSpec.getOutputStream(output1Descriptor);
    input1
        .partitionBy(m -> m.key, m -> m.value, "p1")
        .map(kv -> kv)
        .sendTo(output1);
    return graphSpec;
  }

  private StreamGraphSpec createStreamGraphWithJoin() {

    /**
     * the graph looks like the following. number of partitions in parentheses. quotes indicate expected value.
     *
     *                               input1 (64) -> map -> join -> output1 (8)
     *                                                       |
     *          input2 (16) -> partitionBy ("64") -> filter -|
     *                                                       |
     * input3 (32) -> filter -> partitionBy ("64") -> map -> join -> output2 (16)
     *
     */

    StreamGraphSpec graphSpec = new StreamGraphSpec(config);
    MessageStream<KV<Object, Object>> messageStream1 =
        graphSpec.getInputStream(input1Descriptor)
            .map(m -> m);
    MessageStream<KV<Object, Object>> messageStream2 =
        graphSpec.getInputStream(input2Descriptor)
            .partitionBy(m -> m.key, m -> m.value, "p1")
            .filter(m -> true);
    MessageStream<KV<Object, Object>> messageStream3 =
        graphSpec.getInputStream(input3Descriptor)
            .filter(m -> true)
            .partitionBy(m -> m.key, m -> m.value, "p2")
            .map(m -> m);
    OutputStream<KV<Object, Object>> output1 = graphSpec.getOutputStream(output1Descriptor);
    OutputStream<KV<Object, Object>> output2 = graphSpec.getOutputStream(output2Descriptor);

    messageStream1
        .join(messageStream2,
            (JoinFunction<Object, KV<Object, Object>, KV<Object, Object>, KV<Object, Object>>) mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(2), "j1")
        .sendTo(output1);
    messageStream3
        .join(messageStream2,
            (JoinFunction<Object, KV<Object, Object>, KV<Object, Object>, KV<Object, Object>>) mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
        .sendTo(output2);

    return graphSpec;
  }

  private StreamGraphSpec createStreamGraphWithJoinAndWindow() {

    StreamGraphSpec graphSpec = new StreamGraphSpec(config);
    MessageStream<KV<Object, Object>> messageStream1 =
        graphSpec.getInputStream(input1Descriptor)
            .map(m -> m);
    MessageStream<KV<Object, Object>> messageStream2 =
        graphSpec.getInputStream(input2Descriptor)
            .partitionBy(m -> m.key, m -> m.value, "p1")
            .filter(m -> true);
    MessageStream<KV<Object, Object>> messageStream3 =
        graphSpec.getInputStream(input3Descriptor)
            .filter(m -> true)
            .partitionBy(m -> m.key, m -> m.value, "p2")
            .map(m -> m);
    OutputStream<KV<Object, Object>> output1 = graphSpec.getOutputStream(output1Descriptor);
    OutputStream<KV<Object, Object>> output2 = graphSpec.getOutputStream(output2Descriptor);

    messageStream1.map(m -> m)
        .filter(m->true)
        .window(Windows.keyedTumblingWindow(m -> m, Duration.ofMillis(8), mock(Serde.class), mock(Serde.class)), "w1");

    messageStream2.map(m -> m)
        .filter(m->true)
        .window(Windows.keyedTumblingWindow(m -> m, Duration.ofMillis(16), mock(Serde.class), mock(Serde.class)), "w2");

    messageStream1
        .join(messageStream2,
            (JoinFunction<Object, KV<Object, Object>, KV<Object, Object>, KV<Object, Object>>) mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofMillis(1600), "j1")
        .sendTo(output1);
    messageStream3
        .join(messageStream2,
            (JoinFunction<Object, KV<Object, Object>, KV<Object, Object>, KV<Object, Object>>) mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofMillis(100), "j2")
        .sendTo(output2);
    messageStream3
        .join(messageStream2,
            (JoinFunction<Object, KV<Object, Object>, KV<Object, Object>, KV<Object, Object>>) mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofMillis(252), "j3")
        .sendTo(output2);

    return graphSpec;
  }

  @Before
  public void setup() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_NAME(), "test-app");
    configMap.put(JobConfig.JOB_DEFAULT_SYSTEM(), DEFAULT_SYSTEM);
    StreamTestUtils.addStreamConfigs(configMap, "input1", "system1", "input1");
    StreamTestUtils.addStreamConfigs(configMap, "input2", "system2", "input2");
    StreamTestUtils.addStreamConfigs(configMap, "input3", "system2", "input3");
    StreamTestUtils.addStreamConfigs(configMap, "input4", "system1", "input4");
    StreamTestUtils.addStreamConfigs(configMap, "output1", "system1", "output1");
    StreamTestUtils.addStreamConfigs(configMap, "output2", "system2", "output2");
    config = new MapConfig(configMap);

    input1Spec = new StreamSpec("input1", "input1", "system1");
    input2Spec = new StreamSpec("input2", "input2", "system2");
    input3Spec = new StreamSpec("input3", "input3", "system2");
    input4Spec = new StreamSpec("input4", "input4", "system1");

    output1Spec = new StreamSpec("output1", "output1", "system1");
    output2Spec = new StreamSpec("output2", "output2", "system2");

    KVSerde<Object, Object> kvSerde = new KVSerde<>(new NoOpSerde(), new NoOpSerde());
    input1Descriptor = GenericInputDescriptor.from("input1", "system1", kvSerde);
    input2Descriptor = GenericInputDescriptor.from("input2", "system2", kvSerde);
    input3Descriptor = GenericInputDescriptor.from("input3", "system2", kvSerde);
    input4Descriptor = GenericInputDescriptor.from("input4", "system1", kvSerde);
    output1Descriptor = GenericOutputDescriptor.from("output1", "system1", kvSerde);
    output2Descriptor = GenericOutputDescriptor.from("output2", "system2", kvSerde);

    // set up external partition count
    Map<String, Integer> system1Map = new HashMap<>();
    system1Map.put("input1", 64);
    system1Map.put("output1", 8);
    system1Map.put("input4", ExecutionPlanner.MAX_INFERRED_PARTITIONS * 2);
    Map<String, Integer> system2Map = new HashMap<>();
    system2Map.put("input2", 16);
    system2Map.put("input3", 32);
    system2Map.put("output2", 16);

    SystemAdmin systemAdmin1 = createSystemAdmin(system1Map);
    SystemAdmin systemAdmin2 = createSystemAdmin(system2Map);
    systemAdmins = mock(SystemAdmins.class);
    when(systemAdmins.getSystemAdmin("system1")).thenReturn(systemAdmin1);
    when(systemAdmins.getSystemAdmin("system2")).thenReturn(systemAdmin2);
    streamManager = new StreamManager(systemAdmins);
  }

  @Test
  public void testCreateProcessorGraph() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamGraphSpec graphSpec = createStreamGraphWithJoin();

    JobGraph jobGraph = planner.createJobGraph(graphSpec.getOperatorSpecGraph());
    assertTrue(jobGraph.getSources().size() == 3);
    assertTrue(jobGraph.getSinks().size() == 2);
    assertTrue(jobGraph.getIntermediateStreams().size() == 2); // two streams generated by partitionBy
  }

  @Test
  public void testFetchExistingStreamPartitions() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamGraphSpec graphSpec = createStreamGraphWithJoin();
    JobGraph jobGraph = planner.createJobGraph(graphSpec.getOperatorSpecGraph());

    ExecutionPlanner.updateExistingPartitions(jobGraph, streamManager);
    assertTrue(jobGraph.getOrCreateStreamEdge(input1Spec).getPartitionCount() == 64);
    assertTrue(jobGraph.getOrCreateStreamEdge(input2Spec).getPartitionCount() == 16);
    assertTrue(jobGraph.getOrCreateStreamEdge(input3Spec).getPartitionCount() == 32);
    assertTrue(jobGraph.getOrCreateStreamEdge(output1Spec).getPartitionCount() == 8);
    assertTrue(jobGraph.getOrCreateStreamEdge(output2Spec).getPartitionCount() == 16);

    jobGraph.getIntermediateStreamEdges().forEach(edge -> {
        assertTrue(edge.getPartitionCount() == -1);
      });
  }

  @Test
  public void testCalculateJoinInputPartitions() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamGraphSpec graphSpec = createStreamGraphWithJoin();
    JobGraph jobGraph = planner.createJobGraph(graphSpec.getOperatorSpecGraph());

    ExecutionPlanner.updateExistingPartitions(jobGraph, streamManager);
    ExecutionPlanner.calculateJoinInputPartitions(jobGraph, config);

    // the partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertEquals(64, edge.getPartitionCount());
      });
  }

  @Test
  public void testDefaultPartitions() {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamGraphSpec graphSpec = createSimpleGraph();
    JobGraph jobGraph = planner.createJobGraph(graphSpec.getOperatorSpecGraph());
    planner.calculatePartitions(jobGraph);

    // the partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertTrue(edge.getPartitionCount() == DEFAULT_PARTITIONS);
      });
  }

  @Test
  public void testTriggerIntervalForJoins() throws Exception {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamGraphSpec graphSpec = createStreamGraphWithJoin();
    ExecutionPlan plan = planner.plan(graphSpec.getOperatorSpecGraph());
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    for (JobConfig config : jobConfigs) {
      System.out.println(config);
    }
  }

  @Test
  public void testTriggerIntervalForWindowsAndJoins() throws Exception {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamGraphSpec graphSpec = createStreamGraphWithJoinAndWindow();
    ExecutionPlan plan = planner.plan(graphSpec.getOperatorSpecGraph());
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(1, jobConfigs.size());

    // GCD of 8, 16, 1600 and 252 is 4
    assertEquals("4", jobConfigs.get(0).get(TaskConfig.WINDOW_MS()));
  }

  @Test
  public void testTriggerIntervalWithInvalidWindowMs() throws Exception {
    Map<String, String> map = new HashMap<>(config);
    map.put(TaskConfig.WINDOW_MS(), "-1");
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamGraphSpec graphSpec = createStreamGraphWithJoinAndWindow();
    ExecutionPlan plan = planner.plan(graphSpec.getOperatorSpecGraph());
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(1, jobConfigs.size());

    // GCD of 8, 16, 1600 and 252 is 4
    assertEquals("4", jobConfigs.get(0).get(TaskConfig.WINDOW_MS()));
  }


  @Test
  public void testTriggerIntervalForStatelessOperators() throws Exception {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamGraphSpec graphSpec = createSimpleGraph();
    ExecutionPlan plan = planner.plan(graphSpec.getOperatorSpecGraph());
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(1, jobConfigs.size());
    assertFalse(jobConfigs.get(0).containsKey(TaskConfig.WINDOW_MS()));
  }

  @Test
  public void testTriggerIntervalWhenWindowMsIsConfigured() throws Exception {
    Map<String, String> map = new HashMap<>(config);
    map.put(TaskConfig.WINDOW_MS(), "2000");
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamGraphSpec graphSpec = createSimpleGraph();
    ExecutionPlan plan = planner.plan(graphSpec.getOperatorSpecGraph());
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(1, jobConfigs.size());
    assertEquals("2000", jobConfigs.get(0).get(TaskConfig.WINDOW_MS()));
  }

  @Test
  public void testCalculateIntStreamPartitions() throws Exception {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamGraphSpec graphSpec = createSimpleGraph();
    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec.getOperatorSpecGraph());

    // the partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertEquals(64, edge.getPartitionCount()); // max of input1 and output1
      });
  }

  @Test
  public void testMaxPartition() {
    Collection<StreamEdge> edges = new ArrayList<>();
    StreamEdge edge = new StreamEdge(input1Spec, false, false, config);
    edge.setPartitionCount(2);
    edges.add(edge);
    edge = new StreamEdge(input2Spec, false, false, config);
    edge.setPartitionCount(32);
    edges.add(edge);
    edge = new StreamEdge(input3Spec, false, false, config);
    edge.setPartitionCount(16);
    edges.add(edge);

    assertEquals(32, ExecutionPlanner.maxPartition(edges));

    edges = Collections.emptyList();
    assertEquals(StreamEdge.PARTITIONS_UNKNOWN, ExecutionPlanner.maxPartition(edges));
  }

  @Test
  public void testMaxPartitionLimit() throws Exception {
    int partitionLimit = ExecutionPlanner.MAX_INFERRED_PARTITIONS;

    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamGraphSpec graphSpec = new StreamGraphSpec(config);

    MessageStream<KV<Object, Object>> input1 = graphSpec.getInputStream(input4Descriptor);
    OutputStream<KV<Object, Object>> output1 = graphSpec.getOutputStream(output1Descriptor);
    input1.partitionBy(m -> m.key, m -> m.value, "p1").map(kv -> kv).sendTo(output1);
    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec.getOperatorSpecGraph());

    // the partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertEquals(partitionLimit, edge.getPartitionCount()); // max of input1 and output1
      });
  }
}
