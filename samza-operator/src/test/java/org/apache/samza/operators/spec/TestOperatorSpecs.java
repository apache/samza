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
package org.apache.samza.operators.spec;

import org.apache.samza.operators.TestMessage;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.windows.StoreFunctions;
import org.apache.samza.operators.windows.Trigger;
import org.apache.samza.operators.windows.WindowFn;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.operators.windows.WindowState;
import org.apache.samza.storage.kv.Entry;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestOperatorSpecs {
  @Test
  public void testGetStreamOperator() {
    FlatMapFunction<Message, TestMessage> transformFn = m -> new ArrayList<TestMessage>() { {
        this.add(new TestMessage(m.getKey().toString(), m.getMessage().toString(), 12345L));
      } };
    StreamOperatorSpec<Message, TestMessage> strmOp = OperatorSpecs.createStreamOperator(transformFn);
    assertEquals(strmOp.getTransformFn(), transformFn);
    assertTrue(strmOp.getOutputStream() instanceof MessageStreamImpl);
  }

  @Test
  public void testGetSinkOperator() {
    SinkFunction<TestMessage> sinkFn = (m, c, t) -> { };
    SinkOperatorSpec<TestMessage> sinkOp = OperatorSpecs.createSinkOperator(sinkFn);
    assertEquals(sinkOp.getSinkFn(), sinkFn);
    assertTrue(sinkOp.getOutputStream() == null);
  }

  @Test
  public void testGetWindowOperator() {
    WindowFn<TestMessage, String, WindowState<Integer>, WindowOutput<String, Integer>> windowFn = mock(WindowFn.class);
    BiFunction<TestMessage, Entry<String, WindowState<Integer>>, WindowOutput<String, Integer>> xFunction = (m, e) -> null;
    StoreFunctions<TestMessage, String, WindowState<Integer>> storeFns = mock(StoreFunctions.class);
    Trigger<TestMessage, WindowState<Integer>> trigger = mock(Trigger.class);
    MessageStreamImpl<TestMessage> mockInput = mock(MessageStreamImpl.class);
    when(windowFn.getTransformFn()).thenReturn(xFunction);
    when(windowFn.getStoreFns()).thenReturn(storeFns);
    when(windowFn.getTrigger()).thenReturn(trigger);
    when(mockInput.toString()).thenReturn("mockStream1");

    WindowOperatorSpec<TestMessage, String, WindowState<Integer>, WindowOutput<String, Integer>> windowOp = OperatorSpecs
        .createWindowOperator(windowFn);
    assertEquals(windowOp.getTransformFn(), xFunction);
    assertEquals(windowOp.getStoreFns(), storeFns);
    assertEquals(windowOp.getTrigger(), trigger);
    assertEquals(windowOp.getStoreName(mockInput), String.format("input-mockStream1-wndop-%s", windowOp.toString()));
  }

  @Test
  public void testGetPartialJoinOperator() {
    BiFunction<Message<Object, ?>, Message<Object, ?>, TestMessage> merger =
        (m1, m2) -> new TestMessage(m1.getKey().toString(), m2.getMessage().toString(), System.nanoTime());
    MessageStreamImpl<TestMessage> joinOutput = new MessageStreamImpl<>();
    PartialJoinOperatorSpec<Message<Object, ?>, Object, Message<Object, ?>, TestMessage> partialJoin =
        OperatorSpecs.createPartialJoinOperator(merger, joinOutput);

    assertEquals(partialJoin.getOutputStream(), joinOutput);
    Message<Object, Object> m = mock(Message.class);
    Message<Object, Object> s = mock(Message.class);
    assertEquals(partialJoin.getTransformFn(), merger);
    assertEquals(partialJoin.getSelfStoreFns().getStoreKeyFn().apply(m), m.getKey());
    assertEquals(partialJoin.getSelfStoreFns().getStateUpdaterFn().apply(m, s), m);
    assertEquals(partialJoin.getJoinStoreFns().getStoreKeyFn().apply(m), m.getKey());
    assertNull(partialJoin.getJoinStoreFns().getStateUpdaterFn());
  }

  @Test
  public void testGetMergeOperator() {
    MessageStreamImpl<TestMessage> output = new MessageStreamImpl<>();
    StreamOperatorSpec<TestMessage, TestMessage> mergeOp = OperatorSpecs.createMergeOperator(output);
    Function<TestMessage, Collection<TestMessage>> mergeFn = t -> new ArrayList<TestMessage>() { {
        this.add(t);
      } };
    TestMessage t = mock(TestMessage.class);
    assertEquals(mergeOp.getTransformFn().apply(t), mergeFn.apply(t));
    assertEquals(mergeOp.getOutputStream(), output);
  }
}
