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
package org.apache.samza.operators;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

import org.apache.samza.application.StreamApplicationDescriptorImpl;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.data.TestOutputMessageEnvelope;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.functions.SupplierFunction;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.spec.PartitionByOperatorSpec;
import org.apache.samza.operators.spec.SendToTableOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.TableSpec;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestMessageStreamImpl {

  @Test
  public void testMap() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec mockOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec);

    MapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> mockMapFn = mock(MapFunction.class);
    inputStream.map(mockMapFn);

    ArgumentCaptor<OperatorSpec> registeredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec).registerNextOperatorSpec(registeredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec = registeredOpCaptor.getValue();
    assertTrue(registeredOpSpec instanceof StreamOperatorSpec);

    FlatMapFunction transformFn = ((StreamOperatorSpec) registeredOpSpec).getTransformFn();
    assertNotNull(transformFn);
    assertEquals(OpCode.MAP, registeredOpSpec.getOpCode());

    TestOutputMessageEnvelope mockOutput = mock(TestOutputMessageEnvelope.class);
    when(mockMapFn.apply(anyObject())).thenReturn(mockOutput);
    assertTrue(transformFn.apply(new Object()).contains(mockOutput));
    when(mockMapFn.apply(anyObject())).thenReturn(null);
    assertTrue(transformFn.apply(null).isEmpty());
  }

  @Test
  public void testFlatMap() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec mockOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec);

    inputStream.flatMap(mock(FlatMapFunction.class));

    ArgumentCaptor<OperatorSpec> registeredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec).registerNextOperatorSpec(registeredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec = registeredOpCaptor.getValue();

    assertTrue(registeredOpSpec instanceof StreamOperatorSpec);
    assertNotNull(((StreamOperatorSpec) registeredOpSpec).getTransformFn());
    assertEquals(OpCode.FLAT_MAP, registeredOpSpec.getOpCode());
  }

  @Test
  public void testFlatMapWithRelaxedTypes() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec mockOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<TestInputMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec);

    FlatMapFunction flatMapFunction =
        (FlatMapFunction<TestMessageEnvelope, TestOutputMessageEnvelope>) message -> Collections.emptyList();
    // should compile since TestInputMessageEnvelope extends TestMessageEnvelope
    inputStream.flatMap(flatMapFunction);

    ArgumentCaptor<OperatorSpec> registeredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec).registerNextOperatorSpec(registeredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec = registeredOpCaptor.getValue();

    assertTrue(registeredOpSpec instanceof StreamOperatorSpec);
    assertNotNull(((StreamOperatorSpec) registeredOpSpec).getTransformFn());
    assertEquals(OpCode.FLAT_MAP, registeredOpSpec.getOpCode());
  }

  @Test
  public void testFilter() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec mockOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec);

    FilterFunction<Object> mockFilterFn = mock(FilterFunction.class);
    inputStream.filter(mockFilterFn);

    ArgumentCaptor<OperatorSpec> registeredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec).registerNextOperatorSpec(registeredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec = registeredOpCaptor.getValue();
    assertTrue(registeredOpSpec instanceof StreamOperatorSpec);

    FlatMapFunction transformFn = ((StreamOperatorSpec) registeredOpSpec).getTransformFn();
    assertNotNull(transformFn);
    assertEquals(OpCode.FILTER, registeredOpSpec.getOpCode());

    Object mockInput = new Object();
    when(mockFilterFn.apply(anyObject())).thenReturn(true);
    assertTrue(transformFn.apply(mockInput).contains(mockInput));
    when(mockFilterFn.apply(anyObject())).thenReturn(false);
    assertTrue(transformFn.apply(mockInput).isEmpty());
  }

  @Test
  public void testSink() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec mockOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec);

    inputStream.sink(mock(SinkFunction.class));

    ArgumentCaptor<OperatorSpec> registeredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec).registerNextOperatorSpec(registeredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec = registeredOpCaptor.getValue();

    assertTrue(registeredOpSpec instanceof SinkOperatorSpec);
    assertNotNull(((SinkOperatorSpec) registeredOpSpec).getSinkFn());
    assertEquals(OpCode.SINK, registeredOpSpec.getOpCode());
  }

  @Test
  public void testSendTo() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec mockOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec);
    OutputStreamImpl<TestMessageEnvelope> mockOutputStreamImpl = mock(OutputStreamImpl.class);
    inputStream.sendTo(mockOutputStreamImpl);

    ArgumentCaptor<OperatorSpec> registeredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec).registerNextOperatorSpec(registeredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec = registeredOpCaptor.getValue();

    assertTrue(registeredOpSpec instanceof OutputOperatorSpec);
    assertEquals(OpCode.SEND_TO, registeredOpSpec.getOpCode());
    assertEquals(mockOutputStreamImpl, ((OutputOperatorSpec) registeredOpSpec).getOutputStream());

    // same behavior as above so nothing new to assert. but ensures that this variant compiles.
    MessageStreamImpl<KV<String, TestMessageEnvelope>> keyedInputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec);
    OutputStreamImpl<KV<String, TestMessageEnvelope>> mockKeyedOutputStreamImpl = mock(OutputStreamImpl.class);
    keyedInputStream.sendTo(mockKeyedOutputStreamImpl);

    // can't unit test it, but the following variants should not compile
//    inputStream.sendTo(mockKeyedOutputStreamImpl);
//    keyedInputStream.sendTo(mockOutputStreamImpl);
  }

  @Test
  public void testPartitionBy() throws IOException {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec mockOpSpec = mock(OperatorSpec.class);
    String mockOpName = "mockName";
    when(mockGraph.getNextOpId(anyObject(), anyObject())).thenReturn(mockOpName);
    OutputStreamImpl mockOutputStreamImpl = mock(OutputStreamImpl.class);
    KVSerde mockKVSerde = mock(KVSerde.class);
    IntermediateMessageStreamImpl mockIntermediateStream = mock(IntermediateMessageStreamImpl.class);
    when(mockGraph.getIntermediateStream(eq(mockOpName), eq(mockKVSerde), eq(false)))
        .thenReturn(mockIntermediateStream);
    when(mockIntermediateStream.getOutputStream())
        .thenReturn(mockOutputStreamImpl);
    when(mockIntermediateStream.isKeyed()).thenReturn(true);

    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec);
    MapFunction mockKeyFunction = mock(MapFunction.class);
    MapFunction mockValueFunction = mock(MapFunction.class);
    inputStream.partitionBy(mockKeyFunction, mockValueFunction, mockKVSerde, "p1");

    ArgumentCaptor<OperatorSpec> registeredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec).registerNextOperatorSpec(registeredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec = registeredOpCaptor.getValue();

    assertTrue(registeredOpSpec instanceof PartitionByOperatorSpec);
    assertEquals(OpCode.PARTITION_BY, registeredOpSpec.getOpCode());
    assertEquals(mockOutputStreamImpl, ((PartitionByOperatorSpec) registeredOpSpec).getOutputStream());
    assertEquals(mockKeyFunction, ((PartitionByOperatorSpec) registeredOpSpec).getKeyFunction());
    assertEquals(mockValueFunction, ((PartitionByOperatorSpec) registeredOpSpec).getValueFunction());
  }

  @Test
  public void testRepartitionWithoutSerde() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec mockOpSpec = mock(OperatorSpec.class);
    String mockOpName = "mockName";
    when(mockGraph.getNextOpId(anyObject(), anyObject())).thenReturn(mockOpName);
    OutputStreamImpl mockOutputStreamImpl = mock(OutputStreamImpl.class);
    IntermediateMessageStreamImpl mockIntermediateStream = mock(IntermediateMessageStreamImpl.class);
    when(mockGraph.getIntermediateStream(eq(mockOpName), eq(null), eq(false)))
        .thenReturn(mockIntermediateStream);
    when(mockIntermediateStream.getOutputStream())
        .thenReturn(mockOutputStreamImpl);
    when(mockIntermediateStream.isKeyed()).thenReturn(true);

    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec);
    MapFunction mockKeyFunction = mock(MapFunction.class);
    MapFunction mockValueFunction = mock(MapFunction.class);
    inputStream.partitionBy(mockKeyFunction, mockValueFunction, "p1");

    ArgumentCaptor<OperatorSpec> registeredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec).registerNextOperatorSpec(registeredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec = registeredOpCaptor.getValue();

    assertTrue(registeredOpSpec instanceof PartitionByOperatorSpec);
    assertEquals(OpCode.PARTITION_BY, registeredOpSpec.getOpCode());
    assertEquals(mockOutputStreamImpl, ((PartitionByOperatorSpec) registeredOpSpec).getOutputStream());
    assertEquals(mockKeyFunction, ((PartitionByOperatorSpec) registeredOpSpec).getKeyFunction());
    assertEquals(mockValueFunction, ((PartitionByOperatorSpec) registeredOpSpec).getValueFunction());
  }

  @Test
  public void testWindowWithRelaxedTypes() throws Exception {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec mockOpSpec = mock(OperatorSpec.class);
    MessageStream<TestInputMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec);

    MapFunction<TestMessageEnvelope, String> keyExtractor = m -> m.getKey();
    FoldLeftFunction<TestMessageEnvelope, Integer> aggregator = (m, c) -> c + 1;
    SupplierFunction<Integer> initialValue = () -> 0;

    // should compile since TestMessageEnvelope (input for functions) is base class of TestInputMessageEnvelope (M)
    Window<TestInputMessageEnvelope, String, Integer> window = Windows
        .keyedTumblingWindow(keyExtractor, Duration.ofHours(1), initialValue, aggregator, null, mock(Serde.class));
    MessageStream<WindowPane<String, Integer>> windowedStream = inputStream.window(window, "w1");

    ArgumentCaptor<OperatorSpec> registeredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec).registerNextOperatorSpec(registeredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec = registeredOpCaptor.getValue();

    assertTrue(registeredOpSpec instanceof WindowOperatorSpec);
    assertEquals(OpCode.WINDOW, registeredOpSpec.getOpCode());
    assertEquals(window, ((WindowOperatorSpec) registeredOpSpec).getWindow());
  }

  @Test
  public void testJoin() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec leftInputOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<TestMessageEnvelope> source1 = new MessageStreamImpl<>(mockGraph, leftInputOpSpec);
    OperatorSpec rightInputOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<TestMessageEnvelope> source2 = new MessageStreamImpl<>(mockGraph, rightInputOpSpec);

    JoinFunction<String, TestMessageEnvelope, TestMessageEnvelope, TestOutputMessageEnvelope> mockJoinFn =
        mock(JoinFunction.class);

    Duration joinTtl = Duration.ofMinutes(1);
    source1.join(source2, mockJoinFn,
        mock(Serde.class), mock(Serde.class), mock(Serde.class), joinTtl, "j1");

    ArgumentCaptor<OperatorSpec> leftRegisteredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(leftInputOpSpec).registerNextOperatorSpec(leftRegisteredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> leftRegisteredOpSpec = leftRegisteredOpCaptor.getValue();

    ArgumentCaptor<OperatorSpec> rightRegisteredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(rightInputOpSpec).registerNextOperatorSpec(rightRegisteredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> rightRegisteredOpSpec = rightRegisteredOpCaptor.getValue();

    assertEquals(leftRegisteredOpSpec, rightRegisteredOpSpec);
    assertEquals(OpCode.JOIN, leftRegisteredOpSpec.getOpCode());
    assertTrue(leftRegisteredOpSpec instanceof JoinOperatorSpec);
    assertEquals(mockJoinFn, ((JoinOperatorSpec) leftRegisteredOpSpec).getJoinFn());
    assertEquals(joinTtl.toMillis(), ((JoinOperatorSpec) leftRegisteredOpSpec).getTtlMs());
    assertEquals(leftInputOpSpec, ((JoinOperatorSpec) leftRegisteredOpSpec).getLeftInputOpSpec());
    assertEquals(rightInputOpSpec, ((JoinOperatorSpec) leftRegisteredOpSpec).getRightInputOpSpec());
  }

  @Test
  public void testSendToTable() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec inputOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<TestMessageEnvelope> source = new MessageStreamImpl<>(mockGraph, inputOpSpec);

    TableSpec tableSpec = new TableSpec();
    TableImpl table = new TableImpl(tableSpec);

    source.sendTo(table);

    ArgumentCaptor<OperatorSpec> registeredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(inputOpSpec).registerNextOperatorSpec(registeredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec = registeredOpCaptor.getValue();

    assertTrue(registeredOpSpec instanceof SendToTableOperatorSpec);
    SendToTableOperatorSpec sendToTableOperatorSpec = (SendToTableOperatorSpec) registeredOpSpec;

    assertEquals(OpCode.SEND_TO, sendToTableOperatorSpec.getOpCode());
    assertEquals(tableSpec, sendToTableOperatorSpec.getTableSpec());
  }

  @Test
  public void testStreamTableJoin() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec leftInputOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<KV<String, TestMessageEnvelope>> source1 = new MessageStreamImpl<>(mockGraph, leftInputOpSpec);
    OperatorSpec rightInputOpSpec = mock(OperatorSpec.class);
    MessageStreamImpl<TestMessageEnvelope> source2 = new MessageStreamImpl<>(mockGraph, rightInputOpSpec);

    TableSpec tableSpec = new TableSpec();
    TableImpl table = new TableImpl(tableSpec);

    source2.sendTo(table);

    StreamTableJoinFunction<String, KV<String, TestMessageEnvelope>, KV<String, TestMessageEnvelope>, TestOutputMessageEnvelope>
        mockJoinFn = mock(StreamTableJoinFunction.class);
    source1.join(table, mockJoinFn);

    ArgumentCaptor<OperatorSpec> leftRegisteredOpCaptor = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(leftInputOpSpec).registerNextOperatorSpec(leftRegisteredOpCaptor.capture());
    OperatorSpec<?, TestMessageEnvelope> leftRegisteredOpSpec = leftRegisteredOpCaptor.getValue();

    assertTrue(leftRegisteredOpSpec instanceof StreamTableJoinOperatorSpec);
    StreamTableJoinOperatorSpec joinOpSpec = (StreamTableJoinOperatorSpec) leftRegisteredOpSpec;
    assertEquals(OpCode.JOIN, joinOpSpec.getOpCode());
    assertEquals(mockJoinFn, joinOpSpec.getJoinFn());
    assertEquals(tableSpec, joinOpSpec.getTableSpec());
  }

  @Test
  public void testMerge() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec mockOpSpec1 = mock(OperatorSpec.class);
    MessageStream<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mockOpSpec1);

    // other streams have the same message type T as input stream message type M
    OperatorSpec mockOpSpec2 = mock(OperatorSpec.class);
    OperatorSpec mockOpSpec3 = mock(OperatorSpec.class);
    Collection<MessageStream<TestMessageEnvelope>> otherStreams1 = ImmutableList.of(
        new MessageStreamImpl<>(mockGraph, mockOpSpec2),
        new MessageStreamImpl<>(mockGraph, mockOpSpec3)
    );

    inputStream.merge(otherStreams1);

    ArgumentCaptor<OperatorSpec> registeredOpCaptor1 = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec1).registerNextOperatorSpec(registeredOpCaptor1.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec1 = registeredOpCaptor1.getValue();
    assertTrue(registeredOpSpec1 instanceof StreamOperatorSpec);
    FlatMapFunction transformFn = ((StreamOperatorSpec) registeredOpSpec1).getTransformFn();

    ArgumentCaptor<OperatorSpec> registeredOpCaptor2 = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec2).registerNextOperatorSpec(registeredOpCaptor2.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec2 = registeredOpCaptor2.getValue();

    ArgumentCaptor<OperatorSpec> registeredOpCaptor3 = ArgumentCaptor.forClass(OperatorSpec.class);
    verify(mockOpSpec3).registerNextOperatorSpec(registeredOpCaptor3.capture());
    OperatorSpec<?, TestMessageEnvelope> registeredOpSpec3 = registeredOpCaptor3.getValue();

    assertEquals(registeredOpSpec1, registeredOpSpec2);
    assertEquals(registeredOpSpec2, registeredOpSpec3);
    assertEquals(OpCode.MERGE, registeredOpSpec1.getOpCode());

    assertNotNull(transformFn);
    TestMessageEnvelope mockInput = mock(TestMessageEnvelope.class);
    assertTrue(transformFn.apply(mockInput).contains(mockInput));
    assertEquals(1, transformFn.apply(mockInput).size());
  }

  @Test
  public void testMergeWithRelaxedTypes() {
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    MessageStream<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph, mock(OperatorSpec.class));

    // other streams have the same message type T as input stream message type M
    Collection<MessageStream<TestMessageEnvelope>> otherStreams1 = ImmutableList.of(
        new MessageStreamImpl<>(mockGraph, mock(OperatorSpec.class)),
        new MessageStreamImpl<>(mockGraph, mock(OperatorSpec.class))
    );

    // other streams have the same message type T that extends as input stream message type M
    Collection<MessageStream<TestInputMessageEnvelope>> otherStreams2 = ImmutableList.of(
        new MessageStreamImpl<TestInputMessageEnvelope>(mockGraph, mock(OperatorSpec.class)),
        new MessageStreamImpl<TestInputMessageEnvelope>(mockGraph, mock(OperatorSpec.class))
    );

    // other streams have a mix of message types such that T extends input stream message type M
    Collection<MessageStream<TestMessageEnvelope>> otherStreams3 = ImmutableList.of(
        new MessageStreamImpl<TestMessageEnvelope>(mockGraph, mock(OperatorSpec.class)),
        // unchecked cast required for the next stream
        (MessageStream) new MessageStreamImpl<TestInputMessageEnvelope>(mockGraph, mock(OperatorSpec.class))
    );

    // not supported:
    // other streams have a mix of message types such that T extends input stream message type M
    Collection<MessageStream<? extends TestMessageEnvelope>> otherStreams4 = ImmutableList.of(
        new MessageStreamImpl<TestMessageEnvelope>(mockGraph, mock(OperatorSpec.class)),
        new MessageStreamImpl<TestInputMessageEnvelope>(mockGraph, mock(OperatorSpec.class))
    );

    // check if all type combinations compile
    inputStream.merge(otherStreams1);
    inputStream.merge(otherStreams2);
    inputStream.merge(otherStreams3);
    inputStream.merge(otherStreams4);
  }

  @Test
  public <T> void testMergeWithNestedTypes() {
    class MessageEnvelope<TM> { }
    MessageStream<MessageEnvelope<T>> ms1 = mock(MessageStreamImpl.class);
    MessageStream<MessageEnvelope<T>> ms2 = mock(MessageStreamImpl.class);
    MessageStream<MessageEnvelope<T>> ms3 = mock(MessageStreamImpl.class);
    Collection<MessageStream<MessageEnvelope<T>>> otherStreams = ImmutableList.of(ms2, ms3);

    // should compile
    ms1.merge(otherStreams);
  }

  @Test
  public void testMergeAll() {
    MessageStream<TestMessageEnvelope> input1 = mock(MessageStreamImpl.class);
    MessageStream<TestMessageEnvelope> input2 = mock(MessageStreamImpl.class);
    MessageStream<TestMessageEnvelope> input3 = mock(MessageStreamImpl.class);

    MessageStream.mergeAll(ImmutableList.of(input1, input2, input3));

    ArgumentCaptor<Collection> otherStreamsCaptor = ArgumentCaptor.forClass(Collection.class);
    verify(input1, times(1)).merge(otherStreamsCaptor.capture());
    assertEquals(2, otherStreamsCaptor.getValue().size());
    assertTrue(otherStreamsCaptor.getValue().contains(input2));
    assertTrue(otherStreamsCaptor.getValue().contains(input3));
  }

  @Test
  public void testMergeAllWithRelaxedTypes() {
    MessageStreamImpl<TestInputMessageEnvelope> input1 = mock(MessageStreamImpl.class);
    MessageStreamImpl<TestMessageEnvelope> input2 = mock(MessageStreamImpl.class);
    Collection<MessageStream<? extends TestMessageEnvelope>> streams = ImmutableList.of(input1, input2);

    // should compile
    MessageStream.mergeAll(streams);
    ArgumentCaptor<Collection> otherStreamsCaptor = ArgumentCaptor.forClass(Collection.class);
    verify(input1, times(1)).merge(otherStreamsCaptor.capture());
    assertEquals(1, otherStreamsCaptor.getValue().size());
    assertTrue(otherStreamsCaptor.getValue().contains(input2));
  }

  @Test
  public <T> void testMergeAllWithNestedTypes() {
    class MessageEnvelope<TM> { }
    MessageStream<MessageEnvelope<T>> input1 = mock(MessageStreamImpl.class);
    MessageStream<MessageEnvelope<T>> input2 = mock(MessageStreamImpl.class);
    MessageStream<MessageEnvelope<T>> input3 = mock(MessageStreamImpl.class);

    // should compile
    MessageStream.mergeAll(ImmutableList.of(input1, input2, input3));

    ArgumentCaptor<Collection> otherStreamsCaptor = ArgumentCaptor.forClass(Collection.class);
    verify(input1, times(1)).merge(otherStreamsCaptor.capture());
    assertEquals(2, otherStreamsCaptor.getValue().size());
    assertTrue(otherStreamsCaptor.getValue().contains(input2));
    assertTrue(otherStreamsCaptor.getValue().contains(input3));
  }

  class TestInputMessageEnvelope extends TestMessageEnvelope {
    public TestInputMessageEnvelope(String key, Object value) {
      super(key, value);
    }
  }
}
