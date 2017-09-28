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

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestExecutionPlanner {

  private static final String DEFAULT_SYSTEM = "test-system";
  private static final int DEFAULT_PARTITIONS = 10;

  private Map<String, SystemAdmin> systemAdmins;
  private StreamManager streamManager;
  private ApplicationRunner runner;
  private Config config;

  private StreamSpec input1;
  private StreamSpec input2;
  private StreamSpec input3;
  private StreamSpec output1;
  private StreamSpec output2;

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

  private StreamGraphImpl createSimpleGraph() {
    /**
     * a simple graph of partitionBy and map
     *
     * input1 -> partitionBy -> map -> output1
     *
     */
    StreamGraphImpl streamGraph = new StreamGraphImpl(runner, config);
    MessageStream<KV<Object, Object>> input1 = streamGraph.getInputStream("input1");
    OutputStream<KV<Object, Object>> output1 = streamGraph.getOutputStream("output1");
    input1
        .partitionBy(m -> m.key, m -> m.value)
        .map(kv -> kv)
        .sendTo(output1);
    return streamGraph;
  }

  private StreamGraphImpl createStreamGraphWithJoin() {

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

    StreamGraphImpl streamGraph = new StreamGraphImpl(runner, config);
    MessageStream<KV<Object, Object>> messageStream1 =
        streamGraph.<KV<Object, Object>>getInputStream("input1")
            .map(m -> m);
    MessageStream<KV<Object, Object>> messageStream2 =
        streamGraph.<KV<Object, Object>>getInputStream("input2")
            .partitionBy(m -> m.key, m -> m.value)
            .filter(m -> true);
    MessageStream<KV<Object, Object>> messageStream3 =
        streamGraph.<KV<Object, Object>>getInputStream("input3")
            .filter(m -> true)
            .partitionBy(m -> m.key, m -> m.value)
            .map(m -> m);
    OutputStream<KV<Object, Object>> output1 = streamGraph.getOutputStream("output1");
    OutputStream<KV<Object, Object>> output2 = streamGraph.getOutputStream("output2");

    messageStream1
        .join(messageStream2, mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(2))
        .sendTo(output1);
    messageStream3
        .join(messageStream2, mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1))
        .sendTo(output2);

    return streamGraph;
  }

  private StreamGraphImpl createStreamGraphWithJoinAndWindow() {

    StreamGraphImpl streamGraph = new StreamGraphImpl(runner, config);
    MessageStream<KV<Object, Object>> messageStream1 =
        streamGraph.<KV<Object, Object>>getInputStream("input1")
            .map(m -> m);
    MessageStream<KV<Object, Object>> messageStream2 =
        streamGraph.<KV<Object, Object>>getInputStream("input2")
            .partitionBy(m -> m.key, m -> m.value)
            .filter(m -> true);
    MessageStream<KV<Object, Object>> messageStream3 =
        streamGraph.<KV<Object, Object>>getInputStream("input3")
            .filter(m -> true)
            .partitionBy(m -> m.key, m -> m.value)
            .map(m -> m);
    OutputStream<KV<Object, Object>> output1 = streamGraph.getOutputStream("output1");
    OutputStream<KV<Object, Object>> output2 = streamGraph.getOutputStream("output2");

    messageStream1.map(m -> m)
        .filter(m->true)
        .window(Windows.keyedTumblingWindow(m -> m, Duration.ofMillis(8)));

    messageStream2.map(m -> m)
        .filter(m->true)
        .window(Windows.keyedTumblingWindow(m -> m, Duration.ofMillis(16)));

    messageStream1
        .join(messageStream2, mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofMillis(1600))
        .sendTo(output1);
    messageStream3
        .join(messageStream2, mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofMillis(100))
        .sendTo(output2);
    messageStream3
        .join(messageStream2, mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofMillis(252))
        .sendTo(output2);

    return streamGraph;
  }

  @Before
  public void setup() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_NAME(), "test-app");
    configMap.put(JobConfig.JOB_DEFAULT_SYSTEM(), DEFAULT_SYSTEM);

    config = new MapConfig(configMap);

    input1 = new StreamSpec("input1", "input1", "system1");
    input2 = new StreamSpec("input2", "input2", "system2");
    input3 = new StreamSpec("input3", "input3", "system2");

    output1 = new StreamSpec("output1", "output1", "system1");
    output2 = new StreamSpec("output2", "output2", "system2");

    // set up external partition count
    Map<String, Integer> system1Map = new HashMap<>();
    system1Map.put("input1", 64);
    system1Map.put("output1", 8);
    Map<String, Integer> system2Map = new HashMap<>();
    system2Map.put("input2", 16);
    system2Map.put("input3", 32);
    system2Map.put("output2", 16);

    SystemAdmin systemAdmin1 = createSystemAdmin(system1Map);
    SystemAdmin systemAdmin2 = createSystemAdmin(system2Map);
    systemAdmins = new HashMap<>();
    systemAdmins.put("system1", systemAdmin1);
    systemAdmins.put("system2", systemAdmin2);
    streamManager = new StreamManager(systemAdmins);

    runner = mock(ApplicationRunner.class);
    when(runner.getStreamSpec("input1")).thenReturn(input1);
    when(runner.getStreamSpec("input2")).thenReturn(input2);
    when(runner.getStreamSpec("input3")).thenReturn(input3);
    when(runner.getStreamSpec("output1")).thenReturn(output1);
    when(runner.getStreamSpec("output2")).thenReturn(output2);

    // intermediate streams used in tests
    when(runner.getStreamSpec("test-app-1-partition_by-1"))
        .thenReturn(new StreamSpec("test-app-1-partition_by-1", "test-app-1-partition_by-1", "default-system"));
    when(runner.getStreamSpec("test-app-1-partition_by-3"))
        .thenReturn(new StreamSpec("test-app-1-partition_by-3", "test-app-1-partition_by-3", "default-system"));
    when(runner.getStreamSpec("test-app-1-partition_by-8"))
        .thenReturn(new StreamSpec("test-app-1-partition_by-8", "test-app-1-partition_by-8", "default-system"));
  }

  @Test
  public void testCreateProcessorGraph() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamGraphImpl streamGraph = createStreamGraphWithJoin();

    JobGraph jobGraph = planner.createJobGraph(streamGraph);
    assertTrue(jobGraph.getSources().size() == 3);
    assertTrue(jobGraph.getSinks().size() == 2);
    assertTrue(jobGraph.getIntermediateStreams().size() == 2); // two streams generated by partitionBy
  }

  @Test
  public void testFetchExistingStreamPartitions() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamGraphImpl streamGraph = createStreamGraphWithJoin();
    JobGraph jobGraph = planner.createJobGraph(streamGraph);

    ExecutionPlanner.updateExistingPartitions(jobGraph, streamManager);
    assertTrue(jobGraph.getOrCreateStreamEdge(input1).getPartitionCount() == 64);
    assertTrue(jobGraph.getOrCreateStreamEdge(input2).getPartitionCount() == 16);
    assertTrue(jobGraph.getOrCreateStreamEdge(input3).getPartitionCount() == 32);
    assertTrue(jobGraph.getOrCreateStreamEdge(output1).getPartitionCount() == 8);
    assertTrue(jobGraph.getOrCreateStreamEdge(output2).getPartitionCount() == 16);

    jobGraph.getIntermediateStreamEdges().forEach(edge -> {
        assertTrue(edge.getPartitionCount() == -1);
      });
  }

  @Test
  public void testCalculateJoinInputPartitions() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamGraphImpl streamGraph = createStreamGraphWithJoin();
    JobGraph jobGraph = planner.createJobGraph(streamGraph);

    ExecutionPlanner.updateExistingPartitions(jobGraph, streamManager);
    ExecutionPlanner.calculateJoinInputPartitions(streamGraph, jobGraph);

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
    StreamGraphImpl streamGraph = createSimpleGraph();
    JobGraph jobGraph = planner.createJobGraph(streamGraph);
    planner.calculatePartitions(streamGraph, jobGraph);

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
    StreamGraphImpl streamGraph = createStreamGraphWithJoin();
    ExecutionPlan plan = planner.plan(streamGraph);
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
    StreamGraphImpl streamGraph = createStreamGraphWithJoinAndWindow();
    ExecutionPlan plan = planner.plan(streamGraph);
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(jobConfigs.size(), 1);

    // GCD of 8, 16, 1600 and 252 is 4
    assertEquals(jobConfigs.get(0).get(TaskConfig.WINDOW_MS()), "4");
  }

  @Test
  public void testTriggerIntervalWithInvalidWindowMs() throws Exception {
    Map<String, String> map = new HashMap<>(config);
    map.put(TaskConfig.WINDOW_MS(), "-1");
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamGraphImpl streamGraph = createStreamGraphWithJoinAndWindow();
    ExecutionPlan plan = planner.plan(streamGraph);
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(jobConfigs.size(), 1);

    // GCD of 8, 16, 1600 and 252 is 4
    assertEquals(jobConfigs.get(0).get(TaskConfig.WINDOW_MS()), "4");
  }


  @Test
  public void testTriggerIntervalForStatelessOperators() throws Exception {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamGraphImpl streamGraph = createSimpleGraph();
    ExecutionPlan plan = planner.plan(streamGraph);
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(jobConfigs.size(), 1);
    assertFalse(jobConfigs.get(0).containsKey(TaskConfig.WINDOW_MS()));
  }

  @Test
  public void testTriggerIntervalWhenWindowMsIsConfigured() throws Exception {
    Map<String, String> map = new HashMap<>(config);
    map.put(TaskConfig.WINDOW_MS(), "2000");
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamGraphImpl streamGraph = createSimpleGraph();
    ExecutionPlan plan = planner.plan(streamGraph);
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(jobConfigs.size(), 1);
    assertEquals(jobConfigs.get(0).get(TaskConfig.WINDOW_MS()), "2000");
  }

  @Test
  public void testCalculateIntStreamPartitions() throws Exception {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamGraphImpl streamGraph = createSimpleGraph();
    JobGraph jobGraph = (JobGraph) planner.plan(streamGraph);

    // the partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertTrue(edge.getPartitionCount() == 64); // max of input1 and output1
      });
  }

  @Test
  public void testMaxPartition() {
    Collection<StreamEdge> edges = new ArrayList<>();
    StreamEdge edge = new StreamEdge(input1, config);
    edge.setPartitionCount(2);
    edges.add(edge);
    edge = new StreamEdge(input2, config);
    edge.setPartitionCount(32);
    edges.add(edge);
    edge = new StreamEdge(input3, config);
    edge.setPartitionCount(16);
    edges.add(edge);

    assertEquals(ExecutionPlanner.maxPartition(edges), 32);

    edges = Collections.emptyList();
    assertEquals(ExecutionPlanner.maxPartition(edges), StreamEdge.PARTITIONS_UNKNOWN);
  }
}
