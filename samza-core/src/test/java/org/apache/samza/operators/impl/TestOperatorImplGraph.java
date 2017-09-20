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

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
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
        new OperatorImplGraph(streamGraph, mock(Config.class), mock(TaskContext.class), mock(Clock.class));
    assertEquals(0, opGraph.getAllInputOperators().size());
  }

  @Test
  public void testLinearChain() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec(eq("input"))).thenReturn(new StreamSpec("input", "input-stream", "input-system"));
    when(mockRunner.getStreamSpec(eq("output"))).thenReturn(mock(StreamSpec.class));
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mock(Config.class));

    MessageStream<Object> inputStream = streamGraph.getInputStream("input");
    OutputStream<Object> outputStream = streamGraph.getOutputStream("output");

    inputStream
        .filter(mock(FilterFunction.class))
        .map(mock(MapFunction.class))
        .sendTo(outputStream);

    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
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
  public void testRepartitionChain() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec(eq("input"))).thenReturn(new StreamSpec("input", "input-stream", "input-system"));
    when(mockRunner.getStreamSpec(eq("output"))).thenReturn(new StreamSpec("output", "output-stream", "output-system"));
    when(mockRunner.getStreamSpec(eq("null-null-partition_by-1")))
        .thenReturn(new StreamSpec("intermediate", "intermediate-stream", "intermediate-system"));
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mock(Config.class));
    MessageStream<Object> inputStream = streamGraph.getInputStream("input");
    OutputStream<KV<Integer, String>> outputStream = streamGraph
        .getOutputStream("output", KVSerde.of(mock(IntegerSerde.class), mock(StringSerde.class)));

    inputStream
        .repartition(Object::hashCode, Object::toString, KVSerde.of(mock(IntegerSerde.class), mock(StringSerde.class)))
        .sendTo(outputStream);

    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(streamGraph, mock(Config.class), mockTaskContext, mock(Clock.class));

    InputOperatorImpl inputOpImpl = opImplGraph.getInputOperator(new SystemStream("input-system", "input-stream"));
    assertEquals(1, inputOpImpl.registeredOperators.size());

    OperatorImpl repartitionOpImpl = (RepartitionOperatorImpl) inputOpImpl.registeredOperators.iterator().next();
    assertEquals(0, repartitionOpImpl.registeredOperators.size()); // is terminal but paired with an input operator
    assertEquals(OpCode.PARTITION_BY, repartitionOpImpl.getOperatorSpec().getOpCode());

    InputOperatorImpl repartitionedInputOpImpl =
        opImplGraph.getInputOperator(new SystemStream("intermediate-system", "intermediate-stream"));
    assertEquals(1, repartitionedInputOpImpl.registeredOperators.size());

    OperatorImpl sendToOpImpl = (OutputOperatorImpl) repartitionedInputOpImpl.registeredOperators.iterator().next();
    assertEquals(0, sendToOpImpl.registeredOperators.size());
    assertEquals(OpCode.SEND_TO, sendToOpImpl.getOperatorSpec().getOpCode());
  }

  @Test
  public void testBroadcastChain() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec(eq("input"))).thenReturn(new StreamSpec("input", "input-stream", "input-system"));
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mock(Config.class));

    MessageStream<Object> inputStream = streamGraph.getInputStream("input");
    inputStream.filter(mock(FilterFunction.class));
    inputStream.map(mock(MapFunction.class));

    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(streamGraph, mock(Config.class), mockTaskContext, mock(Clock.class));

    InputOperatorImpl inputOpImpl = opImplGraph.getInputOperator(new SystemStream("input-system", "input-stream"));
    assertEquals(2, inputOpImpl.registeredOperators.size());
    assertTrue(inputOpImpl.registeredOperators.stream().anyMatch(opImpl ->
        ((OperatorImpl) opImpl).getOperatorSpec().getOpCode() == OpCode.FILTER));
    assertTrue(inputOpImpl.registeredOperators.stream().anyMatch(opImpl ->
        ((OperatorImpl) opImpl).getOperatorSpec().getOpCode() == OpCode.MAP));
  }

  @Test
  public void testMergeChain() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec(eq("input"))).thenReturn(new StreamSpec("input", "input-stream", "input-system"));
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mock(Config.class));

    MessageStream<Object> inputStream = streamGraph.getInputStream("input");
    MessageStream<Object> stream1 = inputStream.filter(mock(FilterFunction.class));
    MessageStream<Object> stream2 = inputStream.map(mock(MapFunction.class));
    MessageStream<Object> mergedStream = stream1.merge(Collections.singleton(stream2));
    MapFunction mockMapFunction = mock(MapFunction.class);
    mergedStream.map(mockMapFunction);

    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(streamGraph, mock(Config.class), mockTaskContext, mock(Clock.class));

    // verify that the DAG after merge is only traversed & initialized once
    verify(mockMapFunction, times(1)).init(any(Config.class), any(TaskContext.class));
  }

  @Test
  public void testJoinChain() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec(eq("input1"))).thenReturn(new StreamSpec("input1", "input-stream1", "input-system"));
    when(mockRunner.getStreamSpec(eq("input2"))).thenReturn(new StreamSpec("input2", "input-stream2", "input-system"));
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mock(Config.class));

    JoinFunction mockJoinFunction = mock(JoinFunction.class);
    MessageStream<Object> inputStream1 = streamGraph.getInputStream("input1", new NoOpSerde<>());
    MessageStream<Object> inputStream2 = streamGraph.getInputStream("input2", new NoOpSerde<>());
    inputStream1.join(inputStream2, mockJoinFunction, Duration.ofHours(1));

    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(streamGraph, mock(Config.class), mockTaskContext, mock(Clock.class));

    // verify that join function is initialized once.
    verify(mockJoinFunction, times(1)).init(any(Config.class), any(TaskContext.class));

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
    inputOpImpl1.onMessage(KV.of("", mockLeftMessage), mock(MessageCollector.class), mock(TaskCoordinator.class));
    verify(mockJoinFunction, times(1)).getFirstKey(mockLeftMessage);

    // verify that right partial join operator calls getSecondKey
    Object mockRightMessage = mock(Object.class);
    when(mockJoinFunction.getSecondKey(eq(mockRightMessage))).thenReturn(joinKey);
    inputOpImpl2.onMessage(KV.of("", mockRightMessage), mock(MessageCollector.class), mock(TaskCoordinator.class));
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
    TaskContext mockContext = mock(TaskContext.class);
    when(mockContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mockConfig);

    MessageStream<Object> inputStream1 = streamGraph.getInputStream("input1");
    MessageStream<Object> inputStream2 = streamGraph.getInputStream("input2");

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
}
