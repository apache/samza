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
import org.apache.samza.operators.TestMessageStreamImplUtil;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.data.TestOutputMessageEnvelope;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.stream.OutputStreamImpl;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.internal.WindowType;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;


public class TestOperatorSpecs {
  @Test
  public void testCreateStreamOperator() {
    FlatMapFunction<?, TestMessageEnvelope> transformFn = m -> new ArrayList<TestMessageEnvelope>() { {
          this.add(new TestMessageEnvelope(m.toString(), m.toString(), 12345L));
        } };
    MessageStreamImpl<TestMessageEnvelope> mockOutput = mock(MessageStreamImpl.class);
    StreamOperatorSpec<?, TestMessageEnvelope> streamOp =
        OperatorSpecs.createStreamOperatorSpec(transformFn, mockOutput, 1);
    assertEquals(streamOp.getTransformFn(), transformFn);
    assertEquals(streamOp.getNextStream(), mockOutput);
  }

  @Test
  public void testCreateSinkOperator() {
    SinkFunction<TestMessageEnvelope> sinkFn = (TestMessageEnvelope message, MessageCollector messageCollector,
          TaskCoordinator taskCoordinator) -> { };
    SinkOperatorSpec<TestMessageEnvelope> sinkOp = OperatorSpecs.createSinkOperatorSpec(sinkFn, 1);
    assertEquals(sinkOp.getSinkFn(), sinkFn);
    assertEquals(sinkOp.getOpCode(), OperatorSpec.OpCode.SINK);
    assertEquals(sinkOp.getNextStream(), null);
  }

  @Test
  public void testCreateSendToOperator() {
    OutputStreamImpl<Object, Object, TestMessageEnvelope> mockOutput = mock(OutputStreamImpl.class);
    SinkOperatorSpec<TestMessageEnvelope> sinkOp = OperatorSpecs.createSendToOperatorSpec(mockOutput, 1);
    assertNotNull(sinkOp.getSinkFn());
    assertEquals(sinkOp.getOpCode(), OperatorSpec.OpCode.SEND_TO);
    assertEquals(sinkOp.getNextStream(), null);
  }


  @Test
  public void testCreatePartitionByOperator() {
    OutputStreamImpl<Object, Object, TestMessageEnvelope> mockOutput = mock(OutputStreamImpl.class);
    SinkOperatorSpec<TestMessageEnvelope> sinkOp = OperatorSpecs.createPartitionByOperatorSpec(mockOutput, 1);
    assertNotNull(sinkOp.getSinkFn());
    assertEquals(sinkOp.getOpCode(), OperatorSpec.OpCode.PARTITION_BY);
    assertEquals(sinkOp.getNextStream(), null);
  }

  @Test
  public void testCreateWindowOperator() throws Exception {
    Function<TestMessageEnvelope, String> keyExtractor = m -> "globalkey";
    FoldLeftFunction<TestMessageEnvelope, Integer> aggregator = (m, c) -> c + 1;
    Supplier<Integer> initialValue = () -> 0;
    //instantiate a window using reflection
    WindowInternal window = new WindowInternal(null, initialValue, aggregator, keyExtractor, null, WindowType.TUMBLING);

    MessageStreamImpl<WindowPane<String, Integer>> mockWndOut = mock(MessageStreamImpl.class);
    WindowOperatorSpec spec =
        OperatorSpecs.<TestMessageEnvelope, String, Integer>createWindowOperatorSpec(window, mockWndOut, 1);
    assertEquals(spec.getWindow(), window);
    assertEquals(spec.getWindow().getKeyExtractor(), keyExtractor);
    assertEquals(spec.getWindow().getFoldLeftFunction(), aggregator);
  }

  @Test
  public void testCreatePartialJoinOperator() {
    PartialJoinFunction<String, TestMessageEnvelope, TestMessageEnvelope, TestOutputMessageEnvelope> thisPartialJoinFn
        = mock(PartialJoinFunction.class);
    PartialJoinFunction<String, TestMessageEnvelope, TestMessageEnvelope, TestOutputMessageEnvelope> otherPartialJoinFn
        = mock(PartialJoinFunction.class);
    StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);
    MessageStreamImpl<TestOutputMessageEnvelope> joinOutput = TestMessageStreamImplUtil.getMessageStreamImpl(mockGraph);

    PartialJoinOperatorSpec<String, TestMessageEnvelope, TestMessageEnvelope, TestOutputMessageEnvelope> partialJoinSpec
        = OperatorSpecs.createPartialJoinOperatorSpec(thisPartialJoinFn, otherPartialJoinFn, 1000 * 60, joinOutput, 1);

    assertEquals(partialJoinSpec.getNextStream(), joinOutput);
    assertEquals(partialJoinSpec.getThisPartialJoinFn(), thisPartialJoinFn);
    assertEquals(partialJoinSpec.getOtherPartialJoinFn(), otherPartialJoinFn);
  }

  @Test
  public void testCreateMergeOperator() {
    StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);
    MessageStreamImpl<TestMessageEnvelope> output = TestMessageStreamImplUtil.<TestMessageEnvelope>getMessageStreamImpl(mockGraph);
    StreamOperatorSpec<TestMessageEnvelope, TestMessageEnvelope> mergeOp =
        OperatorSpecs.createMergeOperatorSpec(output, 1);
    Function<TestMessageEnvelope, Collection<TestMessageEnvelope>> mergeFn =
        t -> new ArrayList<TestMessageEnvelope>() { {
            this.add(t);
          } };
    TestMessageEnvelope t = mock(TestMessageEnvelope.class);
    assertEquals(mergeOp.getTransformFn().apply(t), mergeFn.apply(t));
    assertEquals(mergeOp.getNextStream(), output);
  }
}
