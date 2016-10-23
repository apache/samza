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

import org.apache.samza.operators.internal.Operators.*;
import org.apache.samza.operators.internal.WindowOutput;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestMessageStream {

  @Test public void testMap() {
    MessageStream<MockMessage> inputStream = new MessageStream<>();
    Function<MockMessage, MockOutputMessage> xMap = m -> new MockOutputMessage(m.getKey(), m.getMessage().length() + 1, m.getTimestamp() + 2);
    MessageStream<MockOutputMessage> outputStream = inputStream.map(xMap);
    Collection<Operator> subs = inputStream.getSubscribers();
    assertEquals(subs.size(), 1);
    Operator<MockOutputMessage> mapOp = subs.iterator().next();
    assertTrue(mapOp instanceof StreamOperator);
    assertEquals(mapOp.getOutputStream(), outputStream);
    // assert that the transformation function is what we defined above
    MockMessage xTestMsg = mock(MockMessage.class);
    when(xTestMsg.getKey()).thenReturn("test-msg-key");
    when(xTestMsg.getMessage()).thenReturn("123456789");
    when(xTestMsg.getTimestamp()).thenReturn(12345L);
    Collection<MockOutputMessage> cOutputMsg = ((StreamOperator<MockMessage, MockOutputMessage>) mapOp).getFunction().apply(xTestMsg);
    assertEquals(cOutputMsg.size(), 1);
    MockOutputMessage outputMessage = cOutputMsg.iterator().next();
    assertEquals(outputMessage.getKey(), xTestMsg.getKey());
    assertEquals(outputMessage.getMessage(), Integer.valueOf(xTestMsg.getMessage().length() + 1));
    assertEquals(outputMessage.getTimestamp(), xTestMsg.getTimestamp() + 2);
  }

  @Test public void testFlatMap() {
    MessageStream<MockMessage> inputStream = new MessageStream<>();
    Set<MockOutputMessage> flatOuts = new HashSet<MockOutputMessage>() {{
      this.add(mock(MockOutputMessage.class));
      this.add(mock(MockOutputMessage.class));
      this.add(mock(MockOutputMessage.class));
    }};
    Function<MockMessage, Collection<MockOutputMessage>> xFlatMap = m -> flatOuts;
    MessageStream<MockOutputMessage> outputStream = inputStream.flatMap(xFlatMap);
    Collection<Operator> subs = inputStream.getSubscribers();
    assertEquals(subs.size(), 1);
    Operator<MockOutputMessage> flatMapOp = subs.iterator().next();
    assertTrue(flatMapOp instanceof StreamOperator);
    assertEquals(flatMapOp.getOutputStream(), outputStream);
    // assert that the transformation function is what we defined above
    assertEquals(((StreamOperator<MockMessage, MockOutputMessage>) flatMapOp).getFunction(), xFlatMap);
  }

  @Test public void testFilter() {
    MessageStream<MockMessage> inputStream = new MessageStream<>();
    Function<MockMessage, Boolean> xFilter = m -> m.getTimestamp() > 123456L;
    MessageStream<MockMessage> outputStream = inputStream.filter(xFilter);
    Collection<Operator> subs = inputStream.getSubscribers();
    assertEquals(subs.size(), 1);
    Operator<MockMessage> filterOp = subs.iterator().next();
    assertTrue(filterOp instanceof StreamOperator);
    assertEquals(filterOp.getOutputStream(), outputStream);
    // assert that the transformation function is what we defined above
    Function<MockMessage, Collection<MockMessage>> txfmFn = ((StreamOperator<MockMessage, MockMessage>) filterOp).getFunction();
    MockMessage mockMsg = mock(MockMessage.class);
    when(mockMsg.getTimestamp()).thenReturn(11111L);
    Collection<MockMessage> output = txfmFn.apply(mockMsg);
    assertTrue(output.isEmpty());
    when(mockMsg.getTimestamp()).thenReturn(999999L);
    output = txfmFn.apply(mockMsg);
    assertEquals(output.size(), 1);
    assertEquals(output.iterator().next(), mockMsg);
  }

  @Test public void testSink() {
    MessageStream<MockMessage> inputStream = new MessageStream<>();
    MessageStream.VoidFunction3<MockMessage, MessageCollector, TaskCoordinator> xSink = (m, mc, tc) -> {
      mc.send(new OutgoingMessageEnvelope(new SystemStream("test-sys", "test-stream"), m.getMessage()));
      tc.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    };
    inputStream.sink(xSink);
    Collection<Operator> subs = inputStream.getSubscribers();
    assertEquals(subs.size(), 1);
    Operator<MockMessage> sinkOp = subs.iterator().next();
    assertTrue(sinkOp instanceof SinkOperator);
    assertEquals(((SinkOperator) sinkOp).getFunction(), xSink);
    assertNull(((SinkOperator) sinkOp).getOutputStream());
  }

  @Test public void testWindow() {
    MessageStream<MockMessage> inputStream = new MessageStream<>();
    Windows.SessionWindow<MockMessage, String, Integer> window = mock(Windows.SessionWindow.class);
    MessageStream<WindowOutput<String, Integer>> outStream = inputStream.window(window);
    Collection<Operator> subs = inputStream.getSubscribers();
    assertEquals(subs.size(), 1);
    Operator<MockMessage> wndOp = subs.iterator().next();
    assertTrue(wndOp instanceof WindowOperator);
    assertEquals(((WindowOperator) wndOp).getOutputStream(), outStream);
  }

  @Test public void testJoin() {
    MessageStream<MockMessage> source1 = new MessageStream<>();
    MessageStream<MockMessage> source2 = new MessageStream<>();
    BiFunction<MockMessage, MockMessage, MockOutputMessage> joiner = (m1, m2) -> new MockOutputMessage(m1.getKey(), m1.getMessage().length() + m2.getMessage().length(), m1.getTimestamp());
    MessageStream<MockOutputMessage> joinOutput = source1.join(source2, joiner);
    Collection<Operator> subs = source1.getSubscribers();
    assertEquals(subs.size(), 1);
    Operator<MockMessage> joinOp1 = subs.iterator().next();
    assertTrue(joinOp1 instanceof PartialJoinOperator);
    assertEquals(((PartialJoinOperator) joinOp1).getOutputStream(), joinOutput);
    subs = source2.getSubscribers();
    assertEquals(subs.size(), 1);
    Operator<MockMessage> joinOp2 = subs.iterator().next();
    assertTrue(joinOp2 instanceof PartialJoinOperator);
    assertEquals(((PartialJoinOperator) joinOp2).getOutputStream(), joinOutput);
    MockMessage joinMsg1 = new MockMessage("test-join-1", "join-msg-001", 11111L);
    MockMessage joinMsg2 = new MockMessage("test-join-2", "join-msg-002", 22222L);
    MockOutputMessage xOut = (MockOutputMessage) ((PartialJoinOperator) joinOp1).getFunction().apply(joinMsg1, joinMsg2);
    assertEquals(xOut.getKey(), "test-join-1");
    assertEquals(xOut.getMessage(), Integer.valueOf(24));
    assertEquals(xOut.getTimestamp(), 11111L);
    xOut = (MockOutputMessage) ((PartialJoinOperator) joinOp2).getFunction().apply(joinMsg2, joinMsg1);
    assertEquals(xOut.getKey(), "test-join-1");
    assertEquals(xOut.getMessage(), Integer.valueOf(24));
    assertEquals(xOut.getTimestamp(), 11111L);
  }

  @Test public void testMerge() {
    MessageStream<MockMessage> merge1 = new MessageStream<>();
    Collection<MessageStream<MockMessage>> others = new ArrayList<MessageStream<MockMessage>>(){{
      this.add(new MessageStream<>());
      this.add(new MessageStream<>());
    }};
    MessageStream<MockMessage> mergeOutput = merge1.merge(others);
    validateMergeOperator(merge1, mergeOutput);

    others.forEach(merge -> validateMergeOperator(merge, mergeOutput));
  }

  private void validateMergeOperator(MessageStream<MockMessage> mergeSource, MessageStream<MockMessage> mergeOutput) {
    Collection<Operator> subs = mergeSource.getSubscribers();
    assertEquals(subs.size(), 1);
    Operator<MockMessage> mergeOp = subs.iterator().next();
    assertTrue(mergeOp instanceof StreamOperator);
    assertEquals(((StreamOperator) mergeOp).getOutputStream(), mergeOutput);
    MockMessage mockMsg = mock(MockMessage.class);
    Collection<MockMessage> outputs = ((StreamOperator<MockMessage, MockMessage>) mergeOp).getFunction().apply(mockMsg);
    assertEquals(outputs.size(), 1);
    assertEquals(outputs.iterator().next(), mockMsg);
  }
}
