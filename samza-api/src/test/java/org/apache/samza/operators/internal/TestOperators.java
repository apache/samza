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
package org.apache.samza.operators.internal;

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.WindowState;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.internal.Operators;
import org.apache.samza.operators.internal.Trigger;
import org.apache.samza.operators.internal.WindowFn;
import org.apache.samza.operators.internal.WindowOutput;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestOperators {

  private class TestMessage implements Message<String, Object> {
    private final long timestamp;
    private final String key;
    private final Object msg;


    TestMessage(String key, Object msg, long timestamp) {
      this.timestamp = timestamp;
      this.key = key;
      this.msg = msg;
    }

    @Override public Object getMessage() {
      return this.msg;
    }

    @Override public String getKey() {
      return this.key;
    }

    @Override public long getTimestamp() {
      return this.timestamp;
    }
  }

  @Test public void testGetStreamOperator() {
    Function<Message, Collection<TestMessage>> transformFn = m -> new ArrayList<TestMessage>() {{
      this.add(new TestMessage(m.getKey().toString(), m.getMessage(), 12345L));
    }};
    Operators.StreamOperator<Message, TestMessage> strmOp = Operators.getStreamOperator(transformFn);
    assertEquals(strmOp.getFunction(), transformFn);
    assertTrue(strmOp.getOutputStream() instanceof MessageStream);
  }

  @Test public void testGetSinkOperator() {
    MessageStream.VoidFunction3<TestMessage, MessageCollector, TaskCoordinator> sinkFn = (m, c, t) -> {};
    Operators.SinkOperator<TestMessage> sinkOp = Operators.getSinkOperator(sinkFn);
    assertEquals(sinkOp.getFunction(), sinkFn);
    assertTrue(sinkOp.getOutputStream() == null);
  }

  @Test public void testGetWindowOperator() {
    WindowFn<TestMessage, String, WindowState<Integer>, WindowOutput<String, Integer>> windowFn = mock(WindowFn.class);
    BiFunction<TestMessage, Entry<String, WindowState<Integer>>, WindowOutput<String, Integer>> xFunction = (m, e) -> null;
    Operators.StoreFunctions<TestMessage, String, WindowState<Integer>> storeFns = mock(Operators.StoreFunctions.class);
    Trigger<TestMessage, WindowState<Integer>> trigger = mock(Trigger.class);
    MessageStream<TestMessage> mockInput = mock(MessageStream.class);
    when(windowFn.getTransformFunc()).thenReturn(xFunction);
    when(windowFn.getStoreFuncs()).thenReturn(storeFns);
    when(windowFn.getTrigger()).thenReturn(trigger);
    when(mockInput.toString()).thenReturn("mockStream1");

    Operators.WindowOperator<TestMessage, String, WindowState<Integer>, WindowOutput<String, Integer>> windowOp = Operators.getWindowOperator(windowFn);
    assertEquals(windowOp.getFunction(), xFunction);
    assertEquals(windowOp.getStoreFunctions(), storeFns);
    assertEquals(windowOp.getTrigger(), trigger);
    assertEquals(windowOp.getStoreName(mockInput), String.format("input-mockStream1-wndop-%s", windowOp.toString()));
  }

  @Test public void testGetPartialJoinOperator() {
    BiFunction<Message<Object, ?>, Message<Object, ?>, TestMessage> merger =
        (m1, m2) -> new TestMessage(m1.getKey().toString(), m2.getMessage(),
            Math.max(m1.getTimestamp(), m2.getTimestamp()));
    MessageStream<TestMessage> joinOutput = new MessageStream<>();
    Operators.PartialJoinOperator<Message<Object, ?>, Object, Message<Object, ?>, TestMessage> partialJoin =
        Operators.getPartialJoinOperator(merger, joinOutput);

    assertEquals(partialJoin.getOutputStream(), joinOutput);
    Message<Object, Object> m = mock(Message.class);
    Message<Object, Object> s = mock(Message.class);
    assertEquals(partialJoin.getFunction(), merger);
    assertEquals(partialJoin.getSelfStoreFunctions().getStoreKeyFinder().apply(m), m.getKey());
    assertEquals(partialJoin.getSelfStoreFunctions().getStateUpdater().apply(m, s), m);
    assertEquals(partialJoin.getJoinStoreFunctions().getStoreKeyFinder().apply(m), m.getKey());
    assertNull(partialJoin.getJoinStoreFunctions().getStateUpdater());
  }

  @Test public void testGetMergeOperator() {
    MessageStream<TestMessage> output = new MessageStream<>();
    Operators.StreamOperator<TestMessage, TestMessage> mergeOp = Operators.getMergeOperator(output);
    Function<TestMessage, Collection<TestMessage>> mergeFn = t -> new ArrayList<TestMessage>() {{
      this.add(t);
    }};
    TestMessage t = mock(TestMessage.class);
    assertEquals(mergeOp.getFunction().apply(t), mergeFn.apply(t));
    assertEquals(mergeOp.getOutputStream(), output);
  }
}
