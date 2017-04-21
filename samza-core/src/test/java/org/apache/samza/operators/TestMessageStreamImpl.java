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

import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.data.*;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestMessageStreamImpl {

  private StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);

  @Test
  public void testMap() {
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph);
    MapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> xMap = (TestMessageEnvelope m)  ->
        new TestOutputMessageEnvelope(m.getKey(), m.getMessage().getValue().length() + 1);
    MessageStream<TestOutputMessageEnvelope> outputStream = inputStream.map(xMap);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestOutputMessageEnvelope> mapOp = subs.iterator().next();
    assertTrue(mapOp instanceof StreamOperatorSpec);
    assertEquals(mapOp.getNextStream(), outputStream);
    // assert that the transformation function is what we defined above
    TestMessageEnvelope xTestMsg = mock(TestMessageEnvelope.class);
    MessageType mockInnerTestMessage = mock(MessageType.class);
    when(xTestMsg.getKey()).thenReturn("test-msg-key");
    when(xTestMsg.getMessage()).thenReturn(mockInnerTestMessage);
    when(mockInnerTestMessage.getValue()).thenReturn("123456789");

    Collection<TestOutputMessageEnvelope> cOutputMsg = ((StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) mapOp).getTransformFn().apply(xTestMsg);
    assertEquals(cOutputMsg.size(), 1);
    TestOutputMessageEnvelope outputMessage = cOutputMsg.iterator().next();
    assertEquals(outputMessage.getKey(), xTestMsg.getKey());
    assertEquals(outputMessage.getMessage(), Integer.valueOf(xTestMsg.getMessage().getValue().length() + 1));
  }

  @Test
  public void testFlatMap() {
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph);
    List<TestOutputMessageEnvelope> flatOuts = new ArrayList<TestOutputMessageEnvelope>() { {
        this.add(mock(TestOutputMessageEnvelope.class));
        this.add(mock(TestOutputMessageEnvelope.class));
        this.add(mock(TestOutputMessageEnvelope.class));
      } };
    final List<TestMessageEnvelope> inputMsgs = new ArrayList<>();
    FlatMapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> xFlatMap = (TestMessageEnvelope message) -> {
      inputMsgs.add(message);
      return flatOuts;
    };
    MessageStream<TestOutputMessageEnvelope> outputStream = inputStream.flatMap(xFlatMap);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestOutputMessageEnvelope> flatMapOp = subs.iterator().next();
    assertTrue(flatMapOp instanceof StreamOperatorSpec);
    assertEquals(flatMapOp.getNextStream(), outputStream);
    assertEquals(((StreamOperatorSpec) flatMapOp).getTransformFn(), xFlatMap);

    TestMessageEnvelope mockInput  = mock(TestMessageEnvelope.class);
    // assert that the transformation function is what we defined above
    List<TestOutputMessageEnvelope> result = (List<TestOutputMessageEnvelope>)
        ((StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) flatMapOp).getTransformFn().apply(mockInput);
    assertEquals(flatOuts, result);
    assertEquals(inputMsgs.size(), 1);
    assertEquals(inputMsgs.get(0), mockInput);
  }

  @Test
  public void testFlatMapWithRelaxedTypes() {
    MessageStreamImpl<TestInputMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph);
    List<TestExtOutputMessageEnvelope> flatOuts = new ArrayList<TestExtOutputMessageEnvelope>() { {
        this.add(new TestExtOutputMessageEnvelope("output-key-1", 1, "output-id-001"));
        this.add(new TestExtOutputMessageEnvelope("output-key-2", 2, "output-id-002"));
        this.add(new TestExtOutputMessageEnvelope("output-key-3", 3, "output-id-003"));
      } };

    class MyFlatMapFunction implements FlatMapFunction<TestMessageEnvelope, TestExtOutputMessageEnvelope> {
      public final List<TestMessageEnvelope> inputMsgs = new ArrayList<>();

      @Override
      public Collection<TestExtOutputMessageEnvelope> apply(TestMessageEnvelope message) {
        inputMsgs.add(message);
        return flatOuts;
      }

      @Override
      public void init(Config config, TaskContext context) {
        inputMsgs.clear();
      }
    }

    MyFlatMapFunction xFlatMap = new MyFlatMapFunction();

    MessageStream<TestOutputMessageEnvelope> outputStream = inputStream.flatMap(xFlatMap);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestOutputMessageEnvelope> flatMapOp = subs.iterator().next();
    assertTrue(flatMapOp instanceof StreamOperatorSpec);
    assertEquals(flatMapOp.getNextStream(), outputStream);
    assertEquals(((StreamOperatorSpec) flatMapOp).getTransformFn(), xFlatMap);

    TestMessageEnvelope mockInput  = mock(TestMessageEnvelope.class);
    // assert that the transformation function is what we defined above
    List<TestOutputMessageEnvelope> result = (List<TestOutputMessageEnvelope>)
        ((StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) flatMapOp).getTransformFn().apply(mockInput);
    assertEquals(flatOuts, result);
    assertEquals(xFlatMap.inputMsgs.size(), 1);
    assertEquals(xFlatMap.inputMsgs.get(0), mockInput);
  }

  @Test
  public void testFilter() {
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph);
    FilterFunction<TestMessageEnvelope> xFilter = (TestMessageEnvelope m) -> m.getMessage().getEventTime() > 123456L;
    MessageStream<TestMessageEnvelope> outputStream = inputStream.filter(xFilter);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessageEnvelope> filterOp = subs.iterator().next();
    assertTrue(filterOp instanceof StreamOperatorSpec);
    assertEquals(filterOp.getNextStream(), outputStream);
    // assert that the transformation function is what we defined above
    FlatMapFunction<TestMessageEnvelope, TestMessageEnvelope> txfmFn = ((StreamOperatorSpec<TestMessageEnvelope, TestMessageEnvelope>) filterOp).getTransformFn();
    TestMessageEnvelope mockMsg = mock(TestMessageEnvelope.class);
    MessageType mockInnerTestMessage = mock(MessageType.class);
    when(mockMsg.getMessage()).thenReturn(mockInnerTestMessage);
    when(mockInnerTestMessage.getEventTime()).thenReturn(11111L);
    Collection<TestMessageEnvelope> output = txfmFn.apply(mockMsg);
    assertTrue(output.isEmpty());
    when(mockMsg.getMessage()).thenReturn(mockInnerTestMessage);
    when(mockInnerTestMessage.getEventTime()).thenReturn(999999L);
    output = txfmFn.apply(mockMsg);
    assertEquals(output.size(), 1);
    assertEquals(output.iterator().next(), mockMsg);
  }

  @Test
  public void testSink() {
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(mockGraph);
    SystemStream testStream = new SystemStream("test-sys", "test-stream");
    SinkFunction<TestMessageEnvelope> xSink = (TestMessageEnvelope m, MessageCollector mc, TaskCoordinator tc) -> {
      mc.send(new OutgoingMessageEnvelope(testStream, m.getMessage()));
      tc.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    };
    inputStream.sink(xSink);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessageEnvelope> sinkOp = subs.iterator().next();
    assertTrue(sinkOp instanceof SinkOperatorSpec);
    assertEquals(((SinkOperatorSpec) sinkOp).getSinkFn(), xSink);

    TestMessageEnvelope mockTest1 = mock(TestMessageEnvelope.class);
    MessageType mockMsgBody = mock(MessageType.class);
    when(mockTest1.getMessage()).thenReturn(mockMsgBody);
    final List<OutgoingMessageEnvelope> outMsgs = new ArrayList<>();
    MessageCollector mockCollector = mock(MessageCollector.class);
    doAnswer(invocation -> {
        outMsgs.add((OutgoingMessageEnvelope) invocation.getArguments()[0]);
        return null;
      }).when(mockCollector).send(any());
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    ((SinkOperatorSpec) sinkOp).getSinkFn().apply(mockTest1, mockCollector, mockCoordinator);
    assertEquals(1, outMsgs.size());
    assertEquals(testStream, outMsgs.get(0).getSystemStream());
    assertEquals(mockMsgBody, outMsgs.get(0).getMessage());
  }

  @Test
  public void testJoin() {
    MessageStreamImpl<TestMessageEnvelope> source1 = new MessageStreamImpl<>(mockGraph);
    MessageStreamImpl<TestMessageEnvelope> source2 = new MessageStreamImpl<>(mockGraph);
    JoinFunction<String, TestMessageEnvelope, TestMessageEnvelope, TestOutputMessageEnvelope> joiner =
      new JoinFunction<String, TestMessageEnvelope, TestMessageEnvelope, TestOutputMessageEnvelope>() {
        @Override
        public TestOutputMessageEnvelope apply(TestMessageEnvelope m1, TestMessageEnvelope m2) {
          return new TestOutputMessageEnvelope(m1.getKey(), m1.getMessage().getValue().length() + m2.getMessage().getValue().length());
        }

        @Override
        public String getFirstKey(TestMessageEnvelope message) {
          return message.getKey();
        }

        @Override
        public String getSecondKey(TestMessageEnvelope message) {
          return message.getKey();
        }
      };

    MessageStream<TestOutputMessageEnvelope> joinOutput = source1.join(source2, joiner, Duration.ofMinutes(1));
    Collection<OperatorSpec> subs = source1.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessageEnvelope> joinOp1 = subs.iterator().next();
    assertTrue(joinOp1 instanceof PartialJoinOperatorSpec);
    assertEquals(((PartialJoinOperatorSpec) joinOp1).getNextStream(), joinOutput);
    subs = source2.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessageEnvelope> joinOp2 = subs.iterator().next();
    assertTrue(joinOp2 instanceof PartialJoinOperatorSpec);
    assertEquals(((PartialJoinOperatorSpec) joinOp2).getNextStream(), joinOutput);
    TestMessageEnvelope joinMsg1 = new TestMessageEnvelope("test-join-1", "join-msg-001", 11111L);
    TestMessageEnvelope joinMsg2 = new TestMessageEnvelope("test-join-2", "join-msg-002", 22222L);
    TestOutputMessageEnvelope xOut = (TestOutputMessageEnvelope) ((PartialJoinOperatorSpec) joinOp1).getThisPartialJoinFn().apply(joinMsg1, joinMsg2);
    assertEquals(xOut.getKey(), "test-join-1");
    assertEquals(xOut.getMessage(), Integer.valueOf(24));
    xOut = (TestOutputMessageEnvelope) ((PartialJoinOperatorSpec) joinOp2).getThisPartialJoinFn().apply(joinMsg2, joinMsg1);
    assertEquals(xOut.getKey(), "test-join-1");
    assertEquals(xOut.getMessage(), Integer.valueOf(24));
  }

  @Test
  public void testMerge() {
    MessageStream<TestMessageEnvelope> merge1 = new MessageStreamImpl<>(mockGraph);
    Collection<MessageStream<? extends TestMessageEnvelope>> others = new ArrayList<MessageStream<? extends TestMessageEnvelope>>() { {
        this.add(new MessageStreamImpl<>(mockGraph));
        this.add(new MessageStreamImpl<>(mockGraph));
      } };
    MessageStream<TestMessageEnvelope> mergeOutput = merge1.merge(others);
    validateMergeOperator(merge1, mergeOutput);

    others.forEach(merge -> validateMergeOperator((MessageStream<TestMessageEnvelope>) merge, mergeOutput));
  }

  private void validateMergeOperator(MessageStream<TestMessageEnvelope> mergeSource, MessageStream<TestMessageEnvelope> mergeOutput) {
    Collection<OperatorSpec> subs = ((MessageStreamImpl<TestMessageEnvelope>) mergeSource).getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessageEnvelope> mergeOp = subs.iterator().next();
    assertTrue(mergeOp instanceof StreamOperatorSpec);
    assertEquals(((StreamOperatorSpec) mergeOp).getNextStream(), mergeOutput);
    TestMessageEnvelope mockMsg = mock(TestMessageEnvelope.class);
    Collection<TestMessageEnvelope> outputs = ((StreamOperatorSpec<TestMessageEnvelope, TestMessageEnvelope>) mergeOp).getTransformFn().apply(
        mockMsg);
    assertEquals(outputs.size(), 1);
    assertEquals(outputs.iterator().next(), mockMsg);
  }

  @Test
  public void testPartitionBy() {
    Map<String, String> map = new HashMap<>();
    map.put(JobConfig.JOB_DEFAULT_SYSTEM(), "testsystem");
    Config config = new MapConfig(map);
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);
    StreamGraphImpl streamGraph = new StreamGraphImpl(runner, config);
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(streamGraph);
    Function<TestMessageEnvelope, String> keyExtractorFunc = m -> "222";
    inputStream.partitionBy(keyExtractorFunc);
    assertTrue(streamGraph.getInputStreams().size() == 1);
    assertTrue(streamGraph.getOutputStreams().size() == 1);

    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessageEnvelope> partitionByOp = subs.iterator().next();
    assertTrue(partitionByOp instanceof SinkOperatorSpec);
    assertNull(partitionByOp.getNextStream());

    ((SinkOperatorSpec) partitionByOp).getSinkFn().apply(new TestMessageEnvelope("111", "test", 1000),
        envelope -> assertTrue(envelope.getPartitionKey().equals("222")), null);
  }
}
