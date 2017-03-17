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

import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.TestMessageEnvelope;
import org.apache.samza.operators.TestMessageStreamImplUtil;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowType;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


public class TestOperatorSpecs {
  @Test
  public void testGetStreamOperator() {
    FlatMapFunction<MessageEnvelope, TestMessageEnvelope> transformFn = m -> new ArrayList<TestMessageEnvelope>() { {
          this.add(new TestMessageEnvelope(m.getKey().toString(), m.getMessage().toString(), 12345L));
        } };
    MessageStreamImpl<TestMessageEnvelope> mockOutput = mock(MessageStreamImpl.class);
    StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);
    StreamOperatorSpec<MessageEnvelope, TestMessageEnvelope> strmOp = OperatorSpecs.createStreamOperatorSpec(transformFn, mockGraph, mockOutput);
    assertEquals(strmOp.getTransformFn(), transformFn);
    assertEquals(strmOp.getNextStream(), mockOutput);
  }

  @Test
  public void testGetSinkOperator() {
    SinkFunction<TestMessageEnvelope> sinkFn = (TestMessageEnvelope message, MessageCollector messageCollector,
          TaskCoordinator taskCoordinator) -> { };
    StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);
    SinkOperatorSpec<TestMessageEnvelope> sinkOp = OperatorSpecs.createSinkOperatorSpec(sinkFn, mockGraph);
    assertEquals(sinkOp.getSinkFn(), sinkFn);
    assertTrue(sinkOp.getNextStream() == null);
  }

  @Test
  public void testGetWindowOperator() throws Exception {
    Function<TestMessageEnvelope, String> keyExtractor = m -> "globalkey";
    FoldLeftFunction<TestMessageEnvelope, Integer> aggregator = (m, c) -> c + 1;
    Supplier<Integer> initialValue = () -> 0;
    //instantiate a window using reflection
    WindowInternal window = new WindowInternal(null, initialValue, aggregator, keyExtractor, null, WindowType.TUMBLING);

    StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);
    MessageStreamImpl<WindowPane<String, Integer>> mockWndOut = mock(MessageStreamImpl.class);
    WindowOperatorSpec spec = OperatorSpecs.<TestMessageEnvelope, String, Integer>createWindowOperatorSpec(window, mockGraph, mockWndOut);
    assertEquals(spec.getWindow(), window);
    assertEquals(spec.getWindow().getKeyExtractor(), keyExtractor);
    assertEquals(spec.getWindow().getFoldLeftFunction(), aggregator);
  }

  @Test
  public void testGetPartialJoinOperator() {
    PartialJoinFunction<Object, MessageEnvelope<Object, ?>, MessageEnvelope<Object, ?>, TestMessageEnvelope> merger =
      new PartialJoinFunction<Object, MessageEnvelope<Object, ?>, MessageEnvelope<Object, ?>, TestMessageEnvelope>() {
        @Override
        public TestMessageEnvelope apply(MessageEnvelope<Object, ?> m1, MessageEnvelope<Object, ?> m2) {
          return new TestMessageEnvelope(m1.getKey().toString(), m2.getMessage().toString(), System.nanoTime());
        }

        @Override
        public Object getKey(MessageEnvelope<Object, ?> message) {
          return message.getKey();
        }

        @Override
        public Object getOtherKey(MessageEnvelope<Object, ?> message) {
          return message.getKey();
        }
      };

    StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);
    MessageStreamImpl<TestMessageEnvelope> joinOutput = TestMessageStreamImplUtil.<TestMessageEnvelope>getMessageStreamImpl(mockGraph);
    PartialJoinOperatorSpec<MessageEnvelope<Object, ?>, Object, MessageEnvelope<Object, ?>, TestMessageEnvelope> partialJoin =
        OperatorSpecs.createPartialJoinOperatorSpec(merger, mockGraph, joinOutput);

    assertEquals(partialJoin.getNextStream(), joinOutput);
    MessageEnvelope<Object, Object> m = mock(MessageEnvelope.class);
    MessageEnvelope<Object, Object> s = mock(MessageEnvelope.class);
    assertEquals(partialJoin.getTransformFn(), merger);
  }

  @Test
  public void testGetMergeOperator() {
    StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);
    MessageStreamImpl<TestMessageEnvelope> output = TestMessageStreamImplUtil.<TestMessageEnvelope>getMessageStreamImpl(mockGraph);
    StreamOperatorSpec<TestMessageEnvelope, TestMessageEnvelope> mergeOp = OperatorSpecs.createMergeOperatorSpec(mockGraph, output);
    Function<TestMessageEnvelope, Collection<TestMessageEnvelope>> mergeFn = t -> new ArrayList<TestMessageEnvelope>() { {
        this.add(t);
      } };
    TestMessageEnvelope t = mock(TestMessageEnvelope.class);
    assertEquals(mergeOp.getTransformFn().apply(t), mergeFn.apply(t));
    assertEquals(mergeOp.getNextStream(), output);
  }
}
