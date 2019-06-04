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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.TaskApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig$;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericOutputDescriptor;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.SideInputsProcessor;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.TestLocalTableDescriptor;
import org.apache.samza.testUtils.StreamTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestExecutionPlanner {

  private static final String DEFAULT_SYSTEM = "test-system";
  private static final int DEFAULT_PARTITIONS = 10;

  private final Set<SystemDescriptor> systemDescriptors = new HashSet<>();
  private final Map<String, InputDescriptor> inputDescriptors = new HashMap<>();
  private final Map<String, OutputDescriptor> outputDescriptors = new HashMap<>();
  private final Set<TableDescriptor> tableDescriptors = new HashSet<>();

  private SystemAdmins systemAdmins;
  private StreamManager streamManager;
  private Config config;

  private StreamSpec input1Spec;
  private GenericInputDescriptor<KV<Object, Object>> input1Descriptor;
  private StreamSpec input2Spec;
  private GenericInputDescriptor<KV<Object, Object>> input2Descriptor;
  private StreamSpec input3Spec;
  private GenericInputDescriptor<KV<Object, Object>> input3Descriptor;
  private GenericInputDescriptor<KV<Object, Object>> input4Descriptor;
  private StreamSpec output1Spec;
  private GenericOutputDescriptor<KV<Object, Object>> output1Descriptor;
  private StreamSpec output2Spec;
  private GenericOutputDescriptor<KV<Object, Object>> output2Descriptor;
  private GenericSystemDescriptor system1Descriptor;
  private GenericSystemDescriptor system2Descriptor;

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

  private StreamApplicationDescriptorImpl createSimpleGraph() {
    /**
     * a simple graph of partitionBy and map
     *
     * input1 -> partitionBy -> map -> output1
     *
     */
    return new StreamApplicationDescriptorImpl(appDesc-> {
        MessageStream<KV<Object, Object>> input1 = appDesc.getInputStream(input1Descriptor);
        OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);
        input1
            .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1")
            .map(kv -> kv)
            .sendTo(output1);
      }, config);
  }

  private StreamApplicationDescriptorImpl createStreamGraphWithStreamStreamJoin() {

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
    return new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<KV<Object, Object>> messageStream1 =
            appDesc.getInputStream(input1Descriptor)
                .map(m -> m);
        MessageStream<KV<Object, Object>> messageStream2 =
            appDesc.getInputStream(input2Descriptor)
                .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1")
                .filter(m -> true);
        MessageStream<KV<Object, Object>> messageStream3 =
            appDesc.getInputStream(input3Descriptor)
                .filter(m -> true)
                .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p2")
                .map(m -> m);
        OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);
        OutputStream<KV<Object, Object>> output2 = appDesc.getOutputStream(output2Descriptor);

        messageStream1
            .join(messageStream2,
                mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(2), "j1")
            .sendTo(output1);
        messageStream3
            .join(messageStream2,
                mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
            .sendTo(output2);
      }, config);
  }

  private StreamApplicationDescriptorImpl createStreamGraphWithInvalidStreamStreamJoin() {
    /**
     * Creates the following stream-stream join which is invalid due to partition count disagreement
     * between the 2 input streams.
     *
     *   input1 (64) --
     *                 |
     *                join -> output1 (8)
     *                 |
     *   input3 (32) --
     */
    return new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
        MessageStream<KV<Object, Object>> messageStream3 = appDesc.getInputStream(input3Descriptor);
        OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

        messageStream1
            .join(messageStream3,
                mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(2), "j1")
            .sendTo(output1);
      }, config);
  }

  private StreamApplicationDescriptorImpl createStreamGraphWithJoinAndWindow() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor).map(m -> m);
        MessageStream<KV<Object, Object>> messageStream2 =
          appDesc.getInputStream(input2Descriptor)
              .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1")
              .filter(m -> true);
        MessageStream<KV<Object, Object>> messageStream3 =
          appDesc.getInputStream(input3Descriptor)
              .filter(m -> true)
              .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p2")
              .map(m -> m);
        OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);
        OutputStream<KV<Object, Object>> output2 = appDesc.getOutputStream(output2Descriptor);

        messageStream1.map(m -> m)
            .filter(m -> true)
            .window(Windows.keyedTumblingWindow(m -> m, Duration.ofMillis(8), (Serde<KV<Object, Object>>) mock(Serde.class), (Serde<KV<Object, Object>>) mock(Serde.class)), "w1");

        messageStream2.map(m -> m)
            .filter(m -> true)
            .window(Windows.keyedTumblingWindow(m -> m, Duration.ofMillis(16), (Serde<KV<Object, Object>>) mock(Serde.class), (Serde<KV<Object, Object>>) mock(Serde.class)), "w2");

        messageStream1.join(messageStream2, mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofMillis(1600), "j1").sendTo(output1);
        messageStream3.join(messageStream2, mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofMillis(100), "j2").sendTo(output2);
        messageStream3.join(messageStream2, mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofMillis(252), "j3").sendTo(output2);
      }, config);
  }

  private StreamApplicationDescriptorImpl createStreamGraphWithStreamTableJoin() {
    /**
     * Example stream-table join app. Expected partition counts of intermediate streams introduced
     * by partitionBy operations are enclosed in quotes.
     *
     *    input2 (16) -> partitionBy ("32") -> send-to-table t
     *
     *                                      join-table t —————
     *                                       |                |
     *    input1 (64) -> partitionBy ("32") _|                |
     *                                                       join -> output1 (8)
     *                                                        |
     *                                      input3 (32) ——————
     *
     */
    return new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
        MessageStream<KV<Object, Object>> messageStream2 = appDesc.getInputStream(input2Descriptor);
        MessageStream<KV<Object, Object>> messageStream3 = appDesc.getInputStream(input3Descriptor);
        OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

        TableDescriptor tableDescriptor = new TestLocalTableDescriptor.MockLocalTableDescriptor(
            "table-id", new KVSerde(new StringSerde(), new StringSerde()));
        Table table = appDesc.getTable(tableDescriptor);

        messageStream2
            .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1")
            .sendTo(table);

        messageStream1
            .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p2")
            .join(table, mock(StreamTableJoinFunction.class))
            .join(messageStream3,
                  mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
            .sendTo(output1);
      }, config);
  }

  private StreamApplicationDescriptorImpl createStreamGraphWithComplexStreamStreamJoin() {
    /**
     * Example stream-table join app. Expected partition counts of intermediate streams introduced
     * by partitionBy operations are enclosed in quotes.
     *
     *    input1 (64)  ________________________
     *                                         |
     *                                       join ————— output1 (8)
     *                                         |
     *    input2 (16) -> partitionBy ("64")  --|
     *                                         |
     *                                       join ————— output1 (8)
     *                                         |
     *    input3 (32) -> partitionBy ("64")  --|
     *                                         |
     *                                       join ————— output1 (8)
     *                                         |
     *    input4 (512) -> partitionBy ("64") __|
     *
     *
     */
    return new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);

        MessageStream<KV<Object, Object>> messageStream2 =
            appDesc.getInputStream(input2Descriptor)
                .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p2");

        MessageStream<KV<Object, Object>> messageStream3 =
            appDesc.getInputStream(input3Descriptor)
                .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p3");

        MessageStream<KV<Object, Object>> messageStream4 =
            appDesc.getInputStream(input4Descriptor)
                .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p4");

        OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

        messageStream1
            .join(messageStream2,
                mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j1")
            .sendTo(output1);

        messageStream3
            .join(messageStream4,
                mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
            .sendTo(output1);

        messageStream2
            .join(messageStream3,
                mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j3")
            .sendTo(output1);
      }, config);
  }

  private StreamApplicationDescriptorImpl createStreamGraphWithInvalidStreamTableJoin() {
    /**
     * Example stream-table join that is invalid due to disagreement in partition count
     * between the 2 input streams.
     *
     *    input1 (64) -> send-to-table t
     *
     *                   join-table t -> output1 (8)
     *                         |
     *    input2 (16) —————————
     *
     */
    return new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
        MessageStream<KV<Object, Object>> messageStream2 = appDesc.getInputStream(input2Descriptor);
        OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

        TableDescriptor tableDescriptor = new TestLocalTableDescriptor.MockLocalTableDescriptor(
          "table-id", new KVSerde(new StringSerde(), new StringSerde()));
        Table table = appDesc.getTable(tableDescriptor);

        messageStream1.sendTo(table);

        messageStream1
            .join(table, mock(StreamTableJoinFunction.class))
            .join(messageStream2,
                mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
            .sendTo(output1);
      }, config);
  }

  private StreamApplicationDescriptorImpl createStreamGraphWithStreamTableJoinWithSideInputs() {
    /**
     * Example stream-table join where table t is configured with input1 (64) as a side-input stream.
     *
     *                                   join-table t -> output1 (8)
     *                                        |
     *    input2 (16) -> partitionBy ("64") __|
     *
     */
    return new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<KV<Object, Object>> messageStream2 = appDesc.getInputStream(input2Descriptor);
        OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

        TableDescriptor tableDescriptor = new TestLocalTableDescriptor.MockLocalTableDescriptor(
          "table-id", new KVSerde(new StringSerde(), new StringSerde()))
            .withSideInputs(Arrays.asList("input1"))
            .withSideInputsProcessor(mock(SideInputsProcessor.class));
        Table table = appDesc.getTable(tableDescriptor);

        messageStream2
            .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1")
            .join(table, mock(StreamTableJoinFunction.class))
            .sendTo(output1);
      }, config);
  }

  private StreamApplicationDescriptorImpl createStreamGraphWithInvalidStreamTableJoinWithSideInputs() {
    /**
     * Example stream-table join that is invalid due to disagreement in partition count between the
     * stream behind table t and another joined stream. Table t is configured with input2 (16) as
     * side-input stream.
     *
     *                   join-table t -> output1 (8)
     *                         |
     *    input1 (64) —————————
     *
     */
    return new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
        OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

        TableDescriptor tableDescriptor = new TestLocalTableDescriptor.MockLocalTableDescriptor(
          "table-id", new KVSerde(new StringSerde(), new StringSerde()))
            .withSideInputs(Arrays.asList("input2"))
            .withSideInputsProcessor(mock(SideInputsProcessor.class));
        Table table = appDesc.getTable(tableDescriptor);

        messageStream1
            .join(table, mock(StreamTableJoinFunction.class))
            .sendTo(output1);
      }, config);
  }

  private StreamApplicationDescriptorImpl createStreamGraphWithStreamTableJoinAndSendToSameTable() {
    /**
     * A special example of stream-table join where a stream is joined with a table, and the result is
     * sent to the same table. This example is necessary to ensure {@link ExecutionPlanner} does not
     * get stuck traversing the virtual cycle between stream-table-join and send-to-table operator specs
     * indefinitely.
     *
     * The reason such virtual cycle is present is to support computing partitions of intermediate
     * streams participating in stream-table joins. Please, refer to SAMZA SEP-16 for more details.
     */
    return new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);

        TableDescriptor tableDescriptor = new TestLocalTableDescriptor.MockLocalTableDescriptor(
          "table-id", new KVSerde(new StringSerde(), new StringSerde()));
        Table table = appDesc.getTable(tableDescriptor);

        messageStream1
          .join(table, mock(StreamTableJoinFunction.class))
          .sendTo(table);

      }, config);
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

    output1Spec = new StreamSpec("output1", "output1", "system1");
    output2Spec = new StreamSpec("output2", "output2", "system2");

    KVSerde<Object, Object> kvSerde = new KVSerde<>(new NoOpSerde(), new NoOpSerde());
    String mockSystemFactoryClass = "factory.class.name";
    system1Descriptor = new GenericSystemDescriptor("system1", mockSystemFactoryClass);
    system2Descriptor = new GenericSystemDescriptor("system2", mockSystemFactoryClass);
    input1Descriptor = system1Descriptor.getInputDescriptor("input1", kvSerde);
    input2Descriptor = system2Descriptor.getInputDescriptor("input2", kvSerde);
    input3Descriptor = system2Descriptor.getInputDescriptor("input3", kvSerde);
    input4Descriptor = system1Descriptor.getInputDescriptor("input4", kvSerde);
    output1Descriptor = system1Descriptor.getOutputDescriptor("output1", kvSerde);
    output2Descriptor = system2Descriptor.getOutputDescriptor("output2", kvSerde);

    // clean and set up sets and maps of descriptors
    systemDescriptors.clear();
    inputDescriptors.clear();
    outputDescriptors.clear();
    tableDescriptors.clear();
    systemDescriptors.add(system1Descriptor);
    systemDescriptors.add(system2Descriptor);
    inputDescriptors.put(input1Descriptor.getStreamId(), input1Descriptor);
    inputDescriptors.put(input2Descriptor.getStreamId(), input2Descriptor);
    inputDescriptors.put(input3Descriptor.getStreamId(), input3Descriptor);
    inputDescriptors.put(input4Descriptor.getStreamId(), input4Descriptor);
    outputDescriptors.put(output1Descriptor.getStreamId(), output1Descriptor);
    outputDescriptors.put(output2Descriptor.getStreamId(), output2Descriptor);


    // set up external partition count
    Map<String, Integer> system1Map = new HashMap<>();
    system1Map.put("input1", 64);
    system1Map.put("output1", 8);
    system1Map.put("input4", IntermediateStreamManager.MAX_INFERRED_PARTITIONS * 2);
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
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithStreamStreamJoin();

    JobGraph jobGraph = planner.createJobGraph(graphSpec);
    assertTrue(jobGraph.getInputStreams().size() == 3);
    assertTrue(jobGraph.getOutputStreams().size() == 2);
    assertTrue(jobGraph.getIntermediateStreams().size() == 2); // two streams generated by partitionBy
  }

  @Test
  public void testFetchExistingStreamPartitions() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithStreamStreamJoin();
    JobGraph jobGraph = planner.createJobGraph(graphSpec);

    planner.setInputAndOutputStreamPartitionCount(jobGraph);
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
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithStreamStreamJoin();
    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec);

    // Partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertEquals(64, edge.getPartitionCount());
      });
  }

  @Test
  public void testCalculateOrderSensitiveJoinInputPartitions() {
    // This test ensures that the ExecutionPlanner can handle groups of joined stream edges
    // in the correct order. It creates an example stream-stream join application that has
    // the following sets of joined streams (notice the order):
    //
    //    a. e1 (16), e2` (?)
    //    b. e3` (?), e4` (?)
    //    c. e2` (?), e3` (?)
    //
    // If processed in the above order, the ExecutionPlanner will fail to assign the partitions
    // correctly.
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithComplexStreamStreamJoin();
    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec);

    // Partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertEquals(64, edge.getPartitionCount());
      });
  }

  @Test
  public void testCalculateIntStreamPartitions() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createSimpleGraph();

    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec);

    // Partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertEquals(64, edge.getPartitionCount()); // max of input1 and output1
      });
  }

  @Test
  public void testCalculateInStreamPartitionsBehindTables() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithStreamTableJoin();

    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec);

    // Partitions should be the same as input3
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertEquals(32, edge.getPartitionCount());
      });
  }

  @Test
  public void testCalculateInStreamPartitionsBehindTablesWithSideInputs() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithStreamTableJoinWithSideInputs();

    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec);

    // Partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertEquals(64, edge.getPartitionCount());
      });
  }

  @Test
  public void testHandlesVirtualStreamTableJoinCycles() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithStreamTableJoinAndSendToSameTable();

    // Just make sure planning terminates.
    planner.plan(graphSpec);
  }

  @Test
  public void testDefaultPartitions() {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createSimpleGraph();
    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec);

    // Partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertTrue(edge.getPartitionCount() == DEFAULT_PARTITIONS);
      });
  }

  @Test
  public void testBroadcastConfig() {
    Map<String, String> map = new HashMap<>(config);
    map.put(String.format(StreamConfig$.MODULE$.BROADCAST_FOR_STREAM_ID(), "input1"), "true");
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createSimpleGraph();
    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec);

    StreamEdge edge = jobGraph.getStreamEdge("input1");
    Assert.assertTrue(edge.isBroadcast());
    Config jobConfig = jobGraph.getJobConfigs().get(0);
    Assert.assertEquals("system1.input1#[0-63]", jobConfig.get("task.broadcast.inputs"));
  }

  @Test
  public void testMaxPartitionLimit() {
    int partitionLimit = IntermediateStreamManager.MAX_INFERRED_PARTITIONS;

    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamApplicationDescriptorImpl graphSpec = new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<KV<Object, Object>> input1 = appDesc.getInputStream(input4Descriptor);
        OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);
        input1.partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1").map(kv -> kv).sendTo(output1);
      }, config);

    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec);

    // Partitions should be the same as input1
    jobGraph.getIntermediateStreams().forEach(edge -> {
        assertEquals(partitionLimit, edge.getPartitionCount()); // max of input1 and output1
      });
  }

  @Test(expected = SamzaException.class)
  public void testRejectsInvalidStreamStreamJoin() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithInvalidStreamStreamJoin();

    planner.plan(graphSpec);
  }

  @Test(expected = SamzaException.class)
  public void testRejectsInvalidStreamTableJoin() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithInvalidStreamTableJoin();

    planner.plan(graphSpec);
  }

  @Test(expected = SamzaException.class)
  public void testRejectsInvalidStreamTableJoinWithSideInputs() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithInvalidStreamTableJoinWithSideInputs();

    planner.plan(graphSpec);
  }

  @Test
  public void testTriggerIntervalForJoins() {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithStreamStreamJoin();
    ExecutionPlan plan = planner.plan(graphSpec);

    List<JobConfig> jobConfigs = plan.getJobConfigs();
    for (JobConfig config : jobConfigs) {
      System.out.println(config);
    }
  }

  @Test
  public void testTriggerIntervalForWindowsAndJoins() {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithJoinAndWindow();
    ExecutionPlan plan = planner.plan(graphSpec);
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(1, jobConfigs.size());

    // GCD of 8, 16, 1600 and 252 is 4
    assertEquals("4", jobConfigs.get(0).get(TaskConfig.WINDOW_MS()));
  }

  @Test
  public void testTriggerIntervalWithNoWindowMs() {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createStreamGraphWithJoinAndWindow();
    ExecutionPlan plan = planner.plan(graphSpec);
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(1, jobConfigs.size());

    // GCD of 8, 16, 1600 and 252 is 4
    assertEquals("4", jobConfigs.get(0).get(TaskConfig.WINDOW_MS()));
  }

  @Test
  public void testTriggerIntervalForStatelessOperators() {
    Map<String, String> map = new HashMap<>(config);
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createSimpleGraph();
    ExecutionPlan plan = planner.plan(graphSpec);
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(1, jobConfigs.size());
    assertFalse(jobConfigs.get(0).containsKey(TaskConfig.WINDOW_MS()));
  }

  @Test
  public void testTriggerIntervalWhenWindowMsIsConfigured() {
    Map<String, String> map = new HashMap<>(config);
    map.put(TaskConfig.WINDOW_MS(), "2000");
    map.put(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), String.valueOf(DEFAULT_PARTITIONS));
    Config cfg = new MapConfig(map);

    ExecutionPlanner planner = new ExecutionPlanner(cfg, streamManager);
    StreamApplicationDescriptorImpl graphSpec = createSimpleGraph();
    ExecutionPlan plan = planner.plan(graphSpec);
    List<JobConfig> jobConfigs = plan.getJobConfigs();
    assertEquals(1, jobConfigs.size());
    assertEquals("2000", jobConfigs.get(0).get(TaskConfig.WINDOW_MS()));
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

    assertEquals(32, IntermediateStreamManager.maxPartitions(edges));

    edges = Collections.emptyList();
    assertEquals(StreamEdge.PARTITIONS_UNKNOWN, IntermediateStreamManager.maxPartitions(edges));
  }

  @Test
  public void testCreateJobGraphForTaskApplication() {
    TaskApplicationDescriptorImpl taskAppDesc = mock(TaskApplicationDescriptorImpl.class);
    // add interemediate streams
    String intermediateStream1 = "intermediate-stream1";
    String intermediateBroadcast = "intermediate-broadcast1";
    // intermediate stream1, not broadcast
    GenericInputDescriptor<KV<Object, Object>> intermediateInput1 = system1Descriptor.getInputDescriptor(
        intermediateStream1, new KVSerde<>(new NoOpSerde(), new NoOpSerde()));
    GenericOutputDescriptor<KV<Object, Object>> intermediateOutput1 = system1Descriptor.getOutputDescriptor(
        intermediateStream1, new KVSerde<>(new NoOpSerde(), new NoOpSerde()));
    // intermediate stream2, broadcast
    GenericInputDescriptor<KV<Object, Object>> intermediateBroacastInput1 = system1Descriptor.getInputDescriptor(
        intermediateBroadcast, new KVSerde<>(new NoOpSerde<>(), new NoOpSerde<>()));
    GenericOutputDescriptor<KV<Object, Object>> intermediateBroacastOutput1 = system1Descriptor.getOutputDescriptor(
        intermediateBroadcast, new KVSerde<>(new NoOpSerde<>(), new NoOpSerde<>()));
    inputDescriptors.put(intermediateStream1, intermediateInput1);
    outputDescriptors.put(intermediateStream1, intermediateOutput1);
    inputDescriptors.put(intermediateBroadcast, intermediateBroacastInput1);
    outputDescriptors.put(intermediateBroadcast, intermediateBroacastOutput1);
    Set<String> broadcastStreams = new HashSet<>();
    broadcastStreams.add(intermediateBroadcast);

    when(taskAppDesc.getInputDescriptors()).thenReturn(inputDescriptors);
    when(taskAppDesc.getInputStreamIds()).thenReturn(inputDescriptors.keySet());
    when(taskAppDesc.getOutputDescriptors()).thenReturn(outputDescriptors);
    when(taskAppDesc.getOutputStreamIds()).thenReturn(outputDescriptors.keySet());
    when(taskAppDesc.getTableDescriptors()).thenReturn(Collections.emptySet());
    when(taskAppDesc.getSystemDescriptors()).thenReturn(systemDescriptors);
    when(taskAppDesc.getIntermediateBroadcastStreamIds()).thenReturn(broadcastStreams);
    doReturn(MockTaskApplication.class).when(taskAppDesc).getAppClass();

    Map<String, String> systemStreamConfigs = new HashMap<>();
    inputDescriptors.forEach((key, value) -> systemStreamConfigs.putAll(value.toConfig()));
    outputDescriptors.forEach((key, value) -> systemStreamConfigs.putAll(value.toConfig()));
    systemDescriptors.forEach(sd -> systemStreamConfigs.putAll(sd.toConfig()));

    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    JobGraph jobGraph = planner.createJobGraph(taskAppDesc);
    assertEquals(1, jobGraph.getJobNodes().size());
    assertTrue(jobGraph.getInputStreams().stream().map(edge -> edge.getName())
        .filter(streamId -> inputDescriptors.containsKey(streamId)).collect(Collectors.toList()).isEmpty());
    Set<String> intermediateStreams = new HashSet<>(inputDescriptors.keySet());
    jobGraph.getInputStreams().forEach(edge -> {
        if (intermediateStreams.contains(edge.getStreamSpec().getId())) {
          intermediateStreams.remove(edge.getStreamSpec().getId());
        }
      });
    assertEquals(new HashSet<>(Arrays.asList(intermediateStream1, intermediateBroadcast)), intermediateStreams);
  }

  @Test
  public void testCreateJobGraphForLegacyTaskApplication() {
    TaskApplicationDescriptorImpl taskAppDesc = mock(TaskApplicationDescriptorImpl.class);

    when(taskAppDesc.getInputDescriptors()).thenReturn(new HashMap<>());
    when(taskAppDesc.getOutputDescriptors()).thenReturn(new HashMap<>());
    when(taskAppDesc.getTableDescriptors()).thenReturn(new HashSet<>());
    when(taskAppDesc.getSystemDescriptors()).thenReturn(new HashSet<>());
    when(taskAppDesc.getIntermediateBroadcastStreamIds()).thenReturn(new HashSet<>());
    doReturn(LegacyTaskApplication.class).when(taskAppDesc).getAppClass();

    Map<String, String> systemStreamConfigs = new HashMap<>();
    inputDescriptors.forEach((key, value) -> systemStreamConfigs.putAll(value.toConfig()));
    outputDescriptors.forEach((key, value) -> systemStreamConfigs.putAll(value.toConfig()));
    systemDescriptors.forEach(sd -> systemStreamConfigs.putAll(sd.toConfig()));

    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    JobGraph jobGraph = planner.createJobGraph(taskAppDesc);
    assertEquals(1, jobGraph.getJobNodes().size());
    JobNode jobNode = jobGraph.getJobNodes().get(0);
    assertEquals("test-app", jobNode.getJobName());
    assertEquals("test-app-1", jobNode.getJobNameAndId());
    assertEquals(0, jobNode.getInEdges().size());
    assertEquals(0, jobNode.getOutEdges().size());
    assertEquals(0, jobNode.getTables().size());
    assertEquals(config, jobNode.getConfig());
  }

  public static class MockTaskApplication implements SamzaApplication {

    @Override
    public void describe(ApplicationDescriptor appDescriptor) {

    }
  }
}
