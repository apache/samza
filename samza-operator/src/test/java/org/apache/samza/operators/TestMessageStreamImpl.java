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

import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.windows.SessionWindow;
import org.apache.samza.operators.windows.WindowFn;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestMessageStreamImpl {

  @Test
  public void testMap() {
    MessageStreamImpl<TestMessage> inputStream = new MessageStreamImpl<>();
    MapFunction<TestMessage, TestOutputMessage> xMap = m -> new TestOutputMessage(m.getKey(), m.getMessage().length() + 1, m.getReceivedTimeNs() + 2);
    MessageStream<TestOutputMessage> outputStream = inputStream.map(xMap);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestOutputMessage> mapOp = subs.iterator().next();
    assertTrue(mapOp instanceof StreamOperatorSpec);
    assertEquals(mapOp.getOutputStream(), outputStream);
    // assert that the transformation function is what we defined above
    TestMessage xTestMsg = mock(TestMessage.class);
    when(xTestMsg.getKey()).thenReturn("test-msg-key");
    when(xTestMsg.getMessage()).thenReturn("123456789");
    when(xTestMsg.getReceivedTimeNs()).thenReturn(12345L);
    Collection<TestOutputMessage> cOutputMsg = ((StreamOperatorSpec<TestMessage, TestOutputMessage>) mapOp).getTransformFn().apply(xTestMsg);
    assertEquals(cOutputMsg.size(), 1);
    TestOutputMessage outputMessage = cOutputMsg.iterator().next();
    assertEquals(outputMessage.getKey(), xTestMsg.getKey());
    assertEquals(outputMessage.getMessage(), Integer.valueOf(xTestMsg.getMessage().length() + 1));
    assertEquals(outputMessage.getReceivedTimeNs(), xTestMsg.getReceivedTimeNs() + 2);
  }

  @Test
  public void testFlatMap() {
    MessageStreamImpl<TestMessage> inputStream = new MessageStreamImpl<>();
    Set<TestOutputMessage> flatOuts = new HashSet<TestOutputMessage>() { {
        this.add(mock(TestOutputMessage.class));
        this.add(mock(TestOutputMessage.class));
        this.add(mock(TestOutputMessage.class));
      } };
    FlatMapFunction<TestMessage, TestOutputMessage> xFlatMap = m -> flatOuts;
    MessageStream<TestOutputMessage> outputStream = inputStream.flatMap(xFlatMap);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestOutputMessage> flatMapOp = subs.iterator().next();
    assertTrue(flatMapOp instanceof StreamOperatorSpec);
    assertEquals(flatMapOp.getOutputStream(), outputStream);
    // assert that the transformation function is what we defined above
    assertEquals(((StreamOperatorSpec<TestMessage, TestOutputMessage>) flatMapOp).getTransformFn(), xFlatMap);
  }

  @Test
  public void testFilter() {
    MessageStreamImpl<TestMessage> inputStream = new MessageStreamImpl<>();
    FilterFunction<TestMessage> xFilter = m -> m.getReceivedTimeNs() > 123456L;
    MessageStream<TestMessage> outputStream = inputStream.filter(xFilter);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessage> filterOp = subs.iterator().next();
    assertTrue(filterOp instanceof StreamOperatorSpec);
    assertEquals(filterOp.getOutputStream(), outputStream);
    // assert that the transformation function is what we defined above
    FlatMapFunction<TestMessage, TestMessage> txfmFn = ((StreamOperatorSpec<TestMessage, TestMessage>) filterOp).getTransformFn();
    TestMessage mockMsg = mock(TestMessage.class);
    when(mockMsg.getReceivedTimeNs()).thenReturn(11111L);
    Collection<TestMessage> output = txfmFn.apply(mockMsg);
    assertTrue(output.isEmpty());
    when(mockMsg.getReceivedTimeNs()).thenReturn(999999L);
    output = txfmFn.apply(mockMsg);
    assertEquals(output.size(), 1);
    assertEquals(output.iterator().next(), mockMsg);
  }

  @Test
  public void testSink() {
    MessageStreamImpl<TestMessage> inputStream = new MessageStreamImpl<>();
    SinkFunction<TestMessage> xSink = (m, mc, tc) -> {
      mc.send(new OutgoingMessageEnvelope(new SystemStream("test-sys", "test-stream"), m.getMessage()));
      tc.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    };
    inputStream.sink(xSink);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessage> sinkOp = subs.iterator().next();
    assertTrue(sinkOp instanceof SinkOperatorSpec);
    assertEquals(((SinkOperatorSpec) sinkOp).getSinkFn(), xSink);
    assertNull(((SinkOperatorSpec) sinkOp).getOutputStream());
  }

  @Test
  public void testWindow() {
    MessageStreamImpl<TestMessage> inputStream = new MessageStreamImpl<>();
    SessionWindow<TestMessage, String, Integer> window = mock(SessionWindow.class);
    doReturn(mock(WindowFn.class)).when(window).getInternalWindowFn();
    MessageStream<WindowOutput<String, Integer>> outStream = inputStream.window(window);
    Collection<OperatorSpec> subs = inputStream.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessage> wndOp = subs.iterator().next();
    assertTrue(wndOp instanceof WindowOperatorSpec);
    assertEquals(((WindowOperatorSpec) wndOp).getOutputStream(), outStream);
  }

  @Test
  public void testJoin() {
    MessageStreamImpl<TestMessage> source1 = new MessageStreamImpl<>();
    MessageStreamImpl<TestMessage> source2 = new MessageStreamImpl<>();
    JoinFunction<TestMessage, TestMessage, TestOutputMessage> joiner = (m1, m2) -> new TestOutputMessage(m1.getKey(), m1.getMessage().length() + m2.getMessage().length(), m1.getReceivedTimeNs());
    MessageStream<TestOutputMessage> joinOutput = source1.join(source2, joiner);
    Collection<OperatorSpec> subs = source1.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessage> joinOp1 = subs.iterator().next();
    assertTrue(joinOp1 instanceof PartialJoinOperatorSpec);
    assertEquals(((PartialJoinOperatorSpec) joinOp1).getOutputStream(), joinOutput);
    subs = source2.getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessage> joinOp2 = subs.iterator().next();
    assertTrue(joinOp2 instanceof PartialJoinOperatorSpec);
    assertEquals(((PartialJoinOperatorSpec) joinOp2).getOutputStream(), joinOutput);
    TestMessage joinMsg1 = new TestMessage("test-join-1", "join-msg-001", 11111L);
    TestMessage joinMsg2 = new TestMessage("test-join-2", "join-msg-002", 22222L);
    TestOutputMessage xOut = (TestOutputMessage) ((PartialJoinOperatorSpec) joinOp1).getTransformFn().apply(joinMsg1, joinMsg2);
    assertEquals(xOut.getKey(), "test-join-1");
    assertEquals(xOut.getMessage(), Integer.valueOf(24));
    assertEquals(xOut.getReceivedTimeNs(), 11111L);
    xOut = (TestOutputMessage) ((PartialJoinOperatorSpec) joinOp2).getTransformFn().apply(joinMsg2, joinMsg1);
    assertEquals(xOut.getKey(), "test-join-1");
    assertEquals(xOut.getMessage(), Integer.valueOf(24));
    assertEquals(xOut.getReceivedTimeNs(), 11111L);
  }

  @Test
  public void testMerge() {
    MessageStream<TestMessage> merge1 = new MessageStreamImpl<>();
    Collection<MessageStream<TestMessage>> others = new ArrayList<MessageStream<TestMessage>>() { {
        this.add(new MessageStreamImpl<>());
        this.add(new MessageStreamImpl<>());
      } };
    MessageStream<TestMessage> mergeOutput = merge1.merge(others);
    validateMergeOperator(merge1, mergeOutput);

    others.forEach(merge -> validateMergeOperator(merge, mergeOutput));
  }

  private void validateMergeOperator(MessageStream<TestMessage> mergeSource, MessageStream<TestMessage> mergeOutput) {
    Collection<OperatorSpec> subs = ((MessageStreamImpl<TestMessage>) mergeSource).getRegisteredOperatorSpecs();
    assertEquals(subs.size(), 1);
    OperatorSpec<TestMessage> mergeOp = subs.iterator().next();
    assertTrue(mergeOp instanceof StreamOperatorSpec);
    assertEquals(((StreamOperatorSpec) mergeOp).getOutputStream(), mergeOutput);
    TestMessage mockMsg = mock(TestMessage.class);
    Collection<TestMessage> outputs = ((StreamOperatorSpec<TestMessage, TestMessage>) mergeOp).getTransformFn().apply(mockMsg);
    assertEquals(outputs.size(), 1);
    assertEquals(outputs.iterator().next(), mockMsg);
  }
}
