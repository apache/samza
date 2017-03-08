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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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
    TestMessageEnvelope.MessageType mockInnerTestMessage = mock(TestMessageEnvelope.MessageType.class);
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
    Set<TestOutputMessageEnvelope> flatOuts = new HashSet<TestOutputMessageEnvelope>() { {
        this.add(mock(TestOutputMessageEnvelope.class));
        this.add(mock(TestOutputMessageEnvelope.class));
        this.add(mock(TestOutputMessageEnvelope.class));
      } };
    FlatMapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> xFlatMap = (TestMessageEnvelope message) -> flatOuts;
    MessageStream<TestOutputMessageEnvelope> outputStream = inputStream.flatMap(xFlatMap);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestOutputMessageEnvelope> flatMapOp = subs.iterator().next();
    assertTrue(flatMapOp instanceof StreamOperatorSpec);
    assertEquals(flatMapOp.getNextStream(), outputStream);
    // assert that the transformation function is what we defined above
    assertEquals(((StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) flatMapOp).getTransformFn(), xFlatMap);
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
    TestMessageEnvelope.MessageType mockInnerTestMessage = mock(TestMessageEnvelope.MessageType.class);
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
    SinkFunction<TestMessageEnvelope> xSink = (TestMessageEnvelope m, MessageCollector mc, TaskCoordinator tc) -> {
      mc.send(new OutgoingMessageEnvelope(new SystemStream("test-sys", "test-stream"), m.getMessage()));
      tc.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    };
    inputStream.sink(xSink);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessageEnvelope> sinkOp = subs.iterator().next();
    assertTrue(sinkOp instanceof SinkOperatorSpec);
    assertEquals(((SinkOperatorSpec) sinkOp).getSinkFn(), xSink);
    assertNull(((SinkOperatorSpec) sinkOp).getNextStream());
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

    MessageStream<TestOutputMessageEnvelope> joinOutput = source1.join(source2, joiner);
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
    TestOutputMessageEnvelope xOut = (TestOutputMessageEnvelope) ((PartialJoinOperatorSpec) joinOp1).getTransformFn().apply(joinMsg1, joinMsg2);
    assertEquals(xOut.getKey(), "test-join-1");
    assertEquals(xOut.getMessage(), Integer.valueOf(24));
    xOut = (TestOutputMessageEnvelope) ((PartialJoinOperatorSpec) joinOp2).getTransformFn().apply(joinMsg2, joinMsg1);
    assertEquals(xOut.getKey(), "test-join-1");
    assertEquals(xOut.getMessage(), Integer.valueOf(24));
  }

  @Test
  public void testMerge() {
    MessageStream<TestMessageEnvelope> merge1 = new MessageStreamImpl<>(mockGraph);
    Collection<MessageStream<TestMessageEnvelope>> others = new ArrayList<MessageStream<TestMessageEnvelope>>() { {
        this.add(new MessageStreamImpl<>(mockGraph));
        this.add(new MessageStreamImpl<>(mockGraph));
      } };
    MessageStream<TestMessageEnvelope> mergeOutput = merge1.merge(others);
    validateMergeOperator(merge1, mergeOutput);

    others.forEach(merge -> validateMergeOperator(merge, mergeOutput));
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
    ApplicationRunner runner = ApplicationRunner.fromConfig(new MapConfig(map));
    StreamGraphImpl streamGraph = new StreamGraphImpl(runner);
    MessageStreamImpl<TestMessageEnvelope> inputStream = new MessageStreamImpl<>(streamGraph);
    Function<TestMessageEnvelope, String> keyExtractorFunc = m -> "222";
    inputStream.partitionBy(keyExtractorFunc);
    assertTrue(streamGraph.getInStreams().size() == 1);
    assertTrue(streamGraph.getOutStreams().size() == 1);

    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessageEnvelope> partitionByOp = subs.iterator().next();
    assertTrue(partitionByOp instanceof SinkOperatorSpec);
    assertNull(partitionByOp.getNextStream());

    ((SinkOperatorSpec) partitionByOp).getSinkFn().apply(new TestMessageEnvelope("111", "test", 1000), new MessageCollector() {
      @Override
      public void send(OutgoingMessageEnvelope envelope) {
        assertTrue(envelope.getPartitionKey().equals("222"));
      }
    }, null);
  }
}
