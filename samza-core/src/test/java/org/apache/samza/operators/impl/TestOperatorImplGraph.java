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

package org.apache.samza.operators.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskContextImpl;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestOperatorImplGraph {

  @Test
  public void testEmptyChain() {
    StreamGraphImpl streamGraph = new StreamGraphImpl(mock(ApplicationRunner.class), mock(Config.class));
    OperatorImplGraph opGraph =
        new OperatorImplGraph(streamGraph, mock(Config.class), mock(TaskContextImpl.class), mock(Clock.class));
    assertEquals(0, opGraph.getAllInputOperators().size());
  }

  @Test
  public void testLinearChain() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec(eq("input"))).thenReturn(new StreamSpec("input", "input-stream", "input-system"));
    when(mockRunner.getStreamSpec(eq("output"))).thenReturn(mock(StreamSpec.class));
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mock(Config.class));

    MessageStream<Object> inputStream = streamGraph.getInputStream("input", mock(BiFunction.class));
    OutputStream<Object, Object, Object> outputStream =
        streamGraph.getOutputStream("output", mock(Function.class), mock(Function.class));

    inputStream
        .filter(mock(FilterFunction.class))
        .map(mock(MapFunction.class))
        .sendTo(outputStream);

    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    when(mockTaskContext.getTaskName()).thenReturn(new TaskName("task 0"));
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(streamGraph, mock(Config.class), mockTaskContext, mock(Clock.class));

    InputOperatorImpl inputOpImpl = opImplGraph.getInputOperator(new SystemStream("input-system", "input-stream"));
    assertEquals(1, inputOpImpl.registeredOperators.size());

    OperatorImpl filterOpImpl = (StreamOperatorImpl) inputOpImpl.registeredOperators.iterator().next();
    assertEquals(1, filterOpImpl.registeredOperators.size());
    assertEquals(OpCode.FILTER, filterOpImpl.getOperatorSpec().getOpCode());

    OperatorImpl mapOpImpl = (StreamOperatorImpl) filterOpImpl.registeredOperators.iterator().next();
    assertEquals(1, mapOpImpl.registeredOperators.size());
    assertEquals(OpCode.MAP, mapOpImpl.getOperatorSpec().getOpCode());

    OperatorImpl sendToOpImpl = (OutputOperatorImpl) mapOpImpl.registeredOperators.iterator().next();
    assertEquals(0, sendToOpImpl.registeredOperators.size());
    assertEquals(OpCode.SEND_TO, sendToOpImpl.getOperatorSpec().getOpCode());
  }

  @Test
  public void testBroadcastChain() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec(eq("input"))).thenReturn(new StreamSpec("input", "input-stream", "input-system"));
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mock(Config.class));

    MessageStream<Object> inputStream = streamGraph.getInputStream("input", mock(BiFunction.class));
    inputStream.filter(mock(FilterFunction.class));
    inputStream.map(mock(MapFunction.class));

    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(streamGraph, mock(Config.class), mockTaskContext, mock(Clock.class));

    InputOperatorImpl inputOpImpl = opImplGraph.getInputOperator(new SystemStream("input-system", "input-stream"));
    assertEquals(2, inputOpImpl.registeredOperators.size());
    assertTrue(inputOpImpl.registeredOperators.stream()
        .anyMatch(opImpl -> ((OperatorImpl) opImpl).getOperatorSpec().getOpCode() == OpCode.FILTER));
    assertTrue(inputOpImpl.registeredOperators.stream()
        .anyMatch(opImpl -> ((OperatorImpl) opImpl).getOperatorSpec().getOpCode() == OpCode.MAP));
  }

  @Test
  public void testMergeChain() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec(eq("input"))).thenReturn(new StreamSpec("input", "input-stream", "input-system"));
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mock(Config.class));

    MessageStream<Object> inputStream = streamGraph.getInputStream("input", mock(BiFunction.class));
    MessageStream<Object> stream1 = inputStream.filter(mock(FilterFunction.class));
    MessageStream<Object> stream2 = inputStream.map(mock(MapFunction.class));
    MessageStream<Object> mergedStream = stream1.merge(Collections.singleton(stream2));
    MapFunction mockMapFunction = mock(MapFunction.class);
    mergedStream.map(mockMapFunction);

    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(streamGraph, mock(Config.class), mockTaskContext, mock(Clock.class));

    // verify that the DAG after merge is only traversed & initialized once
    verify(mockMapFunction, times(1)).init(any(Config.class), any(TaskContextImpl.class));
  }

  @Test
  public void testJoinChain() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec(eq("input1"))).thenReturn(new StreamSpec("input1", "input-stream1", "input-system"));
    when(mockRunner.getStreamSpec(eq("input2"))).thenReturn(new StreamSpec("input2", "input-stream2", "input-system"));
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mock(Config.class));

    JoinFunction mockJoinFunction = mock(JoinFunction.class);
    MessageStream<Object> inputStream1 = streamGraph.getInputStream("input1", (k, v) -> v);
    MessageStream<Object> inputStream2 = streamGraph.getInputStream("input2", (k, v) -> v);
    inputStream1.join(inputStream2, mockJoinFunction, Duration.ofHours(1));

    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(streamGraph, mock(Config.class), mockTaskContext, mock(Clock.class));

    // verify that join function is initialized once.
    verify(mockJoinFunction, times(1)).init(any(Config.class), any(TaskContextImpl.class));

    InputOperatorImpl inputOpImpl1 = opImplGraph.getInputOperator(new SystemStream("input-system", "input-stream1"));
    InputOperatorImpl inputOpImpl2 = opImplGraph.getInputOperator(new SystemStream("input-system", "input-stream2"));
    PartialJoinOperatorImpl leftPartialJoinOpImpl =
        (PartialJoinOperatorImpl) inputOpImpl1.registeredOperators.iterator().next();
    PartialJoinOperatorImpl rightPartialJoinOpImpl =
        (PartialJoinOperatorImpl) inputOpImpl2.registeredOperators.iterator().next();

    assertEquals(leftPartialJoinOpImpl.getOperatorSpec(), rightPartialJoinOpImpl.getOperatorSpec());
    assertNotSame(leftPartialJoinOpImpl, rightPartialJoinOpImpl);

    Object joinKey = new Object();
    // verify that left partial join operator calls getFirstKey
    Object mockLeftMessage = mock(Object.class);
    when(mockJoinFunction.getFirstKey(eq(mockLeftMessage))).thenReturn(joinKey);
    inputOpImpl1.onMessage(Pair.of("", mockLeftMessage), mock(MessageCollector.class), mock(TaskCoordinator.class));
    verify(mockJoinFunction, times(1)).getFirstKey(mockLeftMessage);

    // verify that right partial join operator calls getSecondKey
    Object mockRightMessage = mock(Object.class);
    when(mockJoinFunction.getSecondKey(eq(mockRightMessage))).thenReturn(joinKey);
    inputOpImpl2.onMessage(Pair.of("", mockRightMessage), mock(MessageCollector.class), mock(TaskCoordinator.class));
    verify(mockJoinFunction, times(1)).getSecondKey(mockRightMessage);

    // verify that the join function apply is called with the correct messages on match
    verify(mockJoinFunction, times(1)).apply(mockLeftMessage, mockRightMessage);
  }

  @Test
  public void testOperatorGraphInitAndClose() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec("input1")).thenReturn(new StreamSpec("input1", "input-stream1", "input-system"));
    when(mockRunner.getStreamSpec("input2")).thenReturn(new StreamSpec("input2", "input-stream2", "input-system"));
    Config mockConfig = mock(Config.class);
    TaskContextImpl mockContext = mock(TaskContextImpl.class);
    when(mockContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mockConfig);

    MessageStream<Object> inputStream1 = streamGraph.getInputStream("input1", (k, v) -> v);
    MessageStream<Object> inputStream2 = streamGraph.getInputStream("input2", (k, v) -> v);

    List<String> initializedOperators = new ArrayList<>();
    List<String> closedOperators = new ArrayList<>();

    inputStream1.map(createMapFunction("1", initializedOperators, closedOperators))
        .map(createMapFunction("2", initializedOperators, closedOperators));

    inputStream2.map(createMapFunction("3", initializedOperators, closedOperators))
        .map(createMapFunction("4", initializedOperators, closedOperators));

    OperatorImplGraph opImplGraph = new OperatorImplGraph(streamGraph, mockConfig, mockContext, SystemClock.instance());

    // Assert that initialization occurs in topological order.
    assertEquals(initializedOperators.get(0), "1");
    assertEquals(initializedOperators.get(1), "2");
    assertEquals(initializedOperators.get(2), "3");
    assertEquals(initializedOperators.get(3), "4");

    // Assert that finalization occurs in reverse topological order.
    opImplGraph.close();
    assertEquals(closedOperators.get(0), "4");
    assertEquals(closedOperators.get(1), "3");
    assertEquals(closedOperators.get(2), "2");
    assertEquals(closedOperators.get(3), "1");
  }

  /**
   * Creates an identity map function that appends to the provided lists when init/close is invoked.
   */
  private MapFunction<Object, Object> createMapFunction(String id,
      List<String> initializedOperators, List<String> finalizedOperators) {
    return new MapFunction<Object, Object>() {
      @Override
      public void init(Config config, TaskContext context) {
        initializedOperators.add(id);
      }

      @Override
      public void close() {
        finalizedOperators.add(id);
      }

      @Override
      public Object apply(Object message) {
        return message;
      }
    };
  }

  @Test
  public void testGetStreamToConsumerTasks() {
    String system = "test-system";
    String stream0 = "test-stream-0";
    String stream1 = "test-stream-1";

    SystemStreamPartition ssp0 = new SystemStreamPartition(system, stream0, new Partition(0));
    SystemStreamPartition ssp1 = new SystemStreamPartition(system, stream0, new Partition(1));
    SystemStreamPartition ssp2 = new SystemStreamPartition(system, stream1, new Partition(0));

    TaskName task0 = new TaskName("Task 0");
    TaskName task1 = new TaskName("Task 1");
    Set<SystemStreamPartition> ssps = new HashSet<>();
    ssps.add(ssp0);
    ssps.add(ssp2);
    TaskModel tm0 = new TaskModel(task0, ssps, new Partition(0));
    ContainerModel cm0 = new ContainerModel("c0", 0, Collections.singletonMap(task0, tm0));
    TaskModel tm1 = new TaskModel(task1, Collections.singleton(ssp1), new Partition(1));
    ContainerModel cm1 = new ContainerModel("c1", 1, Collections.singletonMap(task1, tm1));

    Map<String, ContainerModel> cms = new HashMap<>();
    cms.put(cm0.getProcessorId(), cm0);
    cms.put(cm1.getProcessorId(), cm1);

    JobModel jobModel = new JobModel(new MapConfig(), cms, null);
    Multimap<SystemStream, String> streamToTasks = OperatorImplGraph.getStreamToConsumerTasks(jobModel);
    assertEquals(streamToTasks.get(ssp0.getSystemStream()).size(), 2);
    assertEquals(streamToTasks.get(ssp2.getSystemStream()).size(), 1);
  }

  @Test
  public void testGetOutputToInputStreams() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_NAME(), "test-app");
    configMap.put(JobConfig.JOB_DEFAULT_SYSTEM(), "test-system");
    Config config = new MapConfig(configMap);

    /**
     * the graph looks like the following. number of partitions in parentheses. quotes indicate expected value.
     *
     *                                    input1 -> map -> join -> partitionBy (10) -> output1
     *                                                       |
     *                                     input2 -> filter -|
     *                                                       |
     *           input3 -> filter -> partitionBy -> map -> join -> output2
     *
     */
    StreamSpec input1 = new StreamSpec("input1", "input1", "system1");
    StreamSpec input2 = new StreamSpec("input2", "input2", "system2");
    StreamSpec input3 = new StreamSpec("input3", "input3", "system2");

    StreamSpec output1 = new StreamSpec("output1", "output1", "system1");
    StreamSpec output2 = new StreamSpec("output2", "output2", "system2");

    ApplicationRunner runner = mock(ApplicationRunner.class);
    when(runner.getStreamSpec("input1")).thenReturn(input1);
    when(runner.getStreamSpec("input2")).thenReturn(input2);
    when(runner.getStreamSpec("input3")).thenReturn(input3);
    when(runner.getStreamSpec("output1")).thenReturn(output1);
    when(runner.getStreamSpec("output2")).thenReturn(output2);

    // intermediate streams used in tests
    StreamSpec int1 = new StreamSpec("test-app-1-partition_by-10", "test-app-1-partition_by-10", "default-system");
    StreamSpec int2 = new StreamSpec("test-app-1-partition_by-6", "test-app-1-partition_by-6", "default-system");
    when(runner.getStreamSpec("test-app-1-partition_by-10"))
        .thenReturn(int1);
    when(runner.getStreamSpec("test-app-1-partition_by-6"))
        .thenReturn(int2);

    StreamGraphImpl streamGraph = new StreamGraphImpl(runner, config);
    BiFunction msgBuilder = mock(BiFunction.class);
    MessageStream m1 = streamGraph.getInputStream("input1", msgBuilder).map(m -> m);
    MessageStream m2 = streamGraph.getInputStream("input2", msgBuilder).filter(m -> true);
    MessageStream m3 = streamGraph.getInputStream("input3", msgBuilder).filter(m -> true).partitionBy(m -> "hehe").map(m -> m);
    Function mockFn = mock(Function.class);
    OutputStream<Object, Object, Object> om1 = streamGraph.getOutputStream("output1", mockFn, mockFn);
    OutputStream<Object, Object, Object> om2 = streamGraph.getOutputStream("output2", mockFn, mockFn);

    m1.join(m2, mock(JoinFunction.class), Duration.ofHours(2)).partitionBy(m -> "haha").sendTo(om1);
    m3.join(m2, mock(JoinFunction.class), Duration.ofHours(1)).sendTo(om2);

    Multimap<SystemStream, SystemStream> outputToInput = OperatorImplGraph.getIntermediateToInputStreams(streamGraph);
    Collection<SystemStream> inputs = outputToInput.get(int1.toSystemStream());
    assertEquals(inputs.size(), 2);
    assertTrue(inputs.contains(input1.toSystemStream()));
    assertTrue(inputs.contains(input2.toSystemStream()));

    inputs = outputToInput.get(int2.toSystemStream());
    assertEquals(inputs.size(), 1);
    assertEquals(inputs.iterator().next(), input3.toSystemStream());
  }

  @Test
  public void testGetProducerTaskCountForIntermediateStreams() {
    SystemStream input1 = new SystemStream("system1", "intput1");
    SystemStream input2 = new SystemStream("system2", "intput2");
    SystemStream input3 = new SystemStream("system2", "intput3");

    SystemStream int1 = new SystemStream("system1", "int1");
    SystemStream int2 = new SystemStream("system1", "int2");


    String task0 = "Task 0";
    String task1 = "Task 1";
    String task2 = "Task 2";

    Multimap<SystemStream, String> streamToConsumerTasks = HashMultimap.create();
    streamToConsumerTasks.put(input1, task0);
    streamToConsumerTasks.put(input1, task1);
    streamToConsumerTasks.put(input2, task1);
    streamToConsumerTasks.put(input2, task2);
    streamToConsumerTasks.put(input3, task1);
    streamToConsumerTasks.put(int1, task0);
    streamToConsumerTasks.put(int1, task1);
    streamToConsumerTasks.put(int2, task0);

    Multimap<SystemStream, SystemStream> intermediateToInputStreams = HashMultimap.create();
    intermediateToInputStreams.put(int1, input1);
    intermediateToInputStreams.put(int1, input2);
    intermediateToInputStreams.put(int1, input3);

    intermediateToInputStreams.put(int2, input2);
    intermediateToInputStreams.put(int2, input3);

    Map<SystemStream, Integer> counts = OperatorImplGraph.getProducerTaskCountForIntermediateStreams(
        streamToConsumerTasks, intermediateToInputStreams);
    assertTrue(counts.get(int1) == 3);
    assertTrue(counts.get(int2) == 2);
  }
}
