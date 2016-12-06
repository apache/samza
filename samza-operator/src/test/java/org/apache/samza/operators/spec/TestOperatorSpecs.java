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

import org.apache.samza.operators.TestMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.windows.Trigger;
import org.apache.samza.operators.windows.WindowFn;
import org.apache.samza.operators.windows.WindowOutput;
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
    FlatMapFunction<MessageEnvelope, TestMessageEnvelope> transformFn = m -> new ArrayList<TestMessageEnvelope>() { {
        this.add(new TestMessageEnvelope(m.getKey().toString(), m.getMessage().toString(), 12345L));
      } };
    StreamOperatorSpec<MessageEnvelope, TestMessageEnvelope> strmOp = OperatorSpecs.createStreamOperator(transformFn);
    assertEquals(strmOp.getTransformFn(), transformFn);
    assertTrue(strmOp.getOutputStream() instanceof MessageStreamImpl);
  }

  @Test
  public void testGetSinkOperator() {
    SinkFunction<TestMessageEnvelope> sinkFn = (m, c, t) -> { };
    SinkOperatorSpec<TestMessageEnvelope> sinkOp = OperatorSpecs.createSinkOperator(sinkFn);
    assertEquals(sinkOp.getSinkFn(), sinkFn);
    assertTrue(sinkOp.getOutputStream() == null);
  }

  @Test
  public void testGetWindowOperator() {
    WindowFn<TestMessageEnvelope, String, WindowState<Integer>, WindowOutput<String, Integer>> windowFn = mock(WindowFn.class);
    BiFunction<TestMessageEnvelope, Entry<String, WindowState<Integer>>, WindowOutput<String, Integer>> xFunction = (m, e) -> null;
    StoreFunctions<TestMessageEnvelope, String, WindowState<Integer>> storeFns = mock(StoreFunctions.class);
    Trigger<TestMessageEnvelope, WindowState<Integer>> trigger = mock(Trigger.class);
    MessageStreamImpl<TestMessageEnvelope> mockInput = mock(MessageStreamImpl.class);
    when(windowFn.getTransformFn()).thenReturn(xFunction);
    when(windowFn.getStoreFns()).thenReturn(storeFns);
    when(windowFn.getTrigger()).thenReturn(trigger);
    when(mockInput.toString()).thenReturn("mockStream1");

    WindowOperatorSpec<TestMessageEnvelope, String, WindowState<Integer>, WindowOutput<String, Integer>> windowOp = OperatorSpecs
        .createWindowOperator(windowFn);
    assertEquals(windowOp.getTransformFn(), xFunction);
    assertEquals(windowOp.getStoreFns(), storeFns);
    assertEquals(windowOp.getTrigger(), trigger);
    assertEquals(windowOp.getStoreName(mockInput), String.format("input-mockStream1-wndop-%s", windowOp.toString()));
  }

  @Test
  public void testGetPartialJoinOperator() {
    BiFunction<MessageEnvelope<Object, ?>, MessageEnvelope<Object, ?>, TestMessageEnvelope> merger =
        (m1, m2) -> new TestMessageEnvelope(m1.getKey().toString(), m2.getMessage().toString(), System.nanoTime());
    MessageStreamImpl<TestMessageEnvelope> joinOutput = new MessageStreamImpl<>();
    PartialJoinOperatorSpec<MessageEnvelope<Object, ?>, Object, MessageEnvelope<Object, ?>, TestMessageEnvelope> partialJoin =
        OperatorSpecs.createPartialJoinOperator(merger, joinOutput);

    assertEquals(partialJoin.getOutputStream(), joinOutput);
    MessageEnvelope<Object, Object> m = mock(MessageEnvelope.class);
    MessageEnvelope<Object, Object> s = mock(MessageEnvelope.class);
    assertEquals(partialJoin.getTransformFn(), merger);
    assertEquals(partialJoin.getSelfStoreFns().getStoreKeyFn().apply(m), m.getKey());
    assertEquals(partialJoin.getSelfStoreFns().getStateUpdaterFn().apply(m, s), m);
    assertEquals(partialJoin.getJoinStoreFns().getStoreKeyFn().apply(m), m.getKey());
    assertNull(partialJoin.getJoinStoreFns().getStateUpdaterFn());
  }

  @Test
  public void testGetMergeOperator() {
    MessageStreamImpl<TestMessageEnvelope> output = new MessageStreamImpl<>();
    StreamOperatorSpec<TestMessageEnvelope, TestMessageEnvelope> mergeOp = OperatorSpecs.createMergeOperator(output);
    Function<TestMessageEnvelope, Collection<TestMessageEnvelope>> mergeFn = t -> new ArrayList<TestMessageEnvelope>() { {
        this.add(t);
      } };
    TestMessageEnvelope t = mock(TestMessageEnvelope.class);
    assertEquals(mergeOp.getTransformFn().apply(t), mergeFn.apply(t));
    assertEquals(mergeOp.getOutputStream(), output);
  }
}
