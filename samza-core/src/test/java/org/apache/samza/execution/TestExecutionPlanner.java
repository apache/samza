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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.runtime.AbstractApplicationRunner;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class TestExecutionPlanner {

  private Config config;

  private static final String DEFAULT_SYSTEM = "test-system";
  private static final int DEFAULT_PARTITIONS = 10;

  private StreamSpec input1;
  private StreamSpec input2;
  private StreamSpec input3;
  private StreamSpec output1;
  private StreamSpec output2;

  private Map<String, SystemAdmin> systemAdmins;

  private ApplicationRunner runner;

  private JoinFunction createJoin() {
    return new JoinFunction() {
      @Override
      public Object apply(Object message, Object otherMessage) {
        return null;
      }

      @Override
      public Object getFirstKey(Object message) {
        return null;
      }

      @Override
      public Object getSecondKey(Object message) {
        return null;
      }
    };
  }

  private SinkFunction createSink() {
    return new SinkFunction() {
      @Override
      public void apply(Object message, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
      }
    };
  }

  private SystemAdmin createSystemAdmin(Map<String, Integer> streamToPartitions) {

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
      public void createChangelogStream(String streamName, int numOfPartitions) {

      }

      @Override
      public void validateChangelogStream(String streamName, int numOfPartitions) {

      }

      @Override
      public void createCoordinatorStream(String streamName) {

      }

      @Override
      public Integer offsetComparator(String offset1, String offset2) {
        return null;
      }
    };
  }

  private StreamGraph createSimpleGraph() {
    /**
     * a simple graph of partitionBy and map
     *
     * input1 -> partitionBy -> map -> output1
     *
     */
    StreamGraph streamGraph = new StreamGraphImpl(runner, config);
    streamGraph.createInStream(input1, null, null).partitionBy(m -> "yes!!!").map(m -> m).sendTo(streamGraph.createOutStream(output1, null, null));
    return streamGraph;
  }

  private StreamGraph createStreamGraphWithJoin() {

    /** the graph looks like the following
     *
     *                        input1 -> map -> join -> output1
     *                                           |
     *          input2 -> partitionBy -> filter -|
     *                                           |
     * input3 -> filter -> partitionBy -> map -> join -> output2
     *
     */

    StreamGraph streamGraph = new StreamGraphImpl(runner, config);
    MessageStream m1 = streamGraph.createInStream(input1, null, null).map(m -> m);
    MessageStream m2 = streamGraph.createInStream(input2, null, null).partitionBy(m -> "haha").filter(m -> true);
    MessageStream m3 = streamGraph.createInStream(input3, null, null).filter(m -> true).partitionBy(m -> "hehe").map(m -> m);

    m1.join(m2, createJoin(), Duration.ofHours(1)).sendTo(streamGraph.createOutStream(output1, null, null));
    m3.join(m2, createJoin(), Duration.ofHours(1)).sendTo(streamGraph.createOutStream(output2, null, null));

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

    runner = new AbstractApplicationRunner(config) {
      @Override
      public void run(StreamApplication streamApp, Config config) {
      }
    };
  }

  @Test
  public void testCreateProcessorGraph() {
    ExecutionPlanner planner = new ExecutionPlanner(config);
    StreamGraph streamGraph = createStreamGraphWithJoin();

    ProcessorGraph processorGraph = planner.createProcessorGraph(streamGraph);
    assertTrue(processorGraph.getSources().size() == 3);
    assertTrue(processorGraph.getSinks().size() == 2);
    assertTrue(processorGraph.getIntermediateStreams().size() == 2); // two streams generated by partitionBy
  }

  @Test
  public void testFetchExistingStreamPartitions() {
    ExecutionPlanner planner = new ExecutionPlanner(config);
    StreamGraph streamGraph = createStreamGraphWithJoin();
    ProcessorGraph processorGraph = planner.createProcessorGraph(streamGraph);

    ExecutionPlanner.updateExistingPartitions(processorGraph, systemAdmins);
    assertTrue(processorGraph.getOrCreateEdge(input1).getPartitionCount() == 64);
    assertTrue(processorGraph.getOrCreateEdge(input2).getPartitionCount() == 16);
    assertTrue(processorGraph.getOrCreateEdge(input3).getPartitionCount() == 32);
    assertTrue(processorGraph.getOrCreateEdge(output1).getPartitionCount() == 8);
    assertTrue(processorGraph.getOrCreateEdge(output2).getPartitionCount() == 16);

    processorGraph.getIntermediateStreams().forEach(edge -> {
        assertTrue(edge.getPartitionCount() == -1);
      });
  }

  @Test
  public void testCalculateJoinInputPartitions() {
    ExecutionPlanner planner = new ExecutionPlanner(config);
    StreamGraph streamGraph = createStreamGraphWithJoin();
    ProcessorGraph processorGraph = planner.createProcessorGraph(streamGraph);

    ExecutionPlanner.updateExistingPartitions(processorGraph, systemAdmins);
    ExecutionPlanner.calculateJoinInputPartitions(streamGraph, processorGraph);

    // the partitions should be the same as input1
    processorGraph.getIntermediateStreams().forEach(edge -> {
        assertTrue(edge.getPartitionCount() == 64);
      });
  }

  @Test
  public void testDefaultPartitions() {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg);
    StreamGraph streamGraph = createSimpleGraph();
    ProcessorGraph processorGraph = planner.createProcessorGraph(streamGraph);
    planner.calculatePartitions(streamGraph, processorGraph, systemAdmins);

    // the partitions should be the same as input1
    processorGraph.getIntermediateStreams().forEach(edge -> {
        assertTrue(edge.getPartitionCount() == DEFAULT_PARTITIONS);
      });
  }

  @Test
  public void testCalculateIntStreamPartitions() {
    ExecutionPlanner planner = new ExecutionPlanner(config);
    StreamGraph streamGraph = createSimpleGraph();
    ProcessorGraph processorGraph = planner.createProcessorGraph(streamGraph);
    planner.calculatePartitions(streamGraph, processorGraph, systemAdmins);

    // the partitions should be the same as input1
    processorGraph.getIntermediateStreams().forEach(edge -> {
        assertTrue(edge.getPartitionCount() == 64); // max of input1 and output1
      });
  }
}
