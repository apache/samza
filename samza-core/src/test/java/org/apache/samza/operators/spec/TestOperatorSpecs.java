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
import org.apache.samza.operators.data.MessageType;
import org.apache.samza.operators.data.TestInputMessageEnvelope;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.data.TestOutputMessageEnvelope;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.stream.OutputStreamInternalImpl;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.internal.WindowType;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestOperatorSpecs {
  @Test
  public void testCreateStreamOperator() {
    FlatMapFunction<Object, TestMessageEnvelope> transformFn = m -> new ArrayList<TestMessageEnvelope>() { {
          this.add(new TestMessageEnvelope(m.toString(), m.toString(), 12345L));
        } };
    MessageStreamImpl<TestMessageEnvelope> mockOutput = mock(MessageStreamImpl.class);
    StreamOperatorSpec<Object, TestMessageEnvelope> streamOp =
        OperatorSpecs.createStreamOperatorSpec(transformFn, mockOutput, 1);
    assertEquals(streamOp.getTransformFn(), transformFn);

    Object mockInput = mock(Object.class);
    when(mockInput.toString()).thenReturn("test-string-1");
    List<TestMessageEnvelope> outputs = (List<TestMessageEnvelope>) streamOp.getTransformFn().apply(mockInput);
    assertEquals(outputs.size(), 1);
    assertEquals(outputs.get(0).getKey(), "test-string-1");
    assertEquals(outputs.get(0).getMessage().getValue(), "test-string-1");
    assertEquals(outputs.get(0).getMessage().getEventTime(), 12345L);
    assertEquals(streamOp.getNextStream(), mockOutput);
  }

  @Test
  public void testCreateSinkOperator() {
    SystemStream testStream = new SystemStream("test-sys", "test-stream");
    SinkFunction<TestMessageEnvelope> sinkFn = (TestMessageEnvelope message, MessageCollector messageCollector,
          TaskCoordinator taskCoordinator) -> {
      messageCollector.send(new OutgoingMessageEnvelope(testStream, message.getKey(), message.getMessage()));
    };
    SinkOperatorSpec<TestMessageEnvelope> sinkOp = OperatorSpecs.createSinkOperatorSpec(sinkFn, 1);
    assertEquals(sinkOp.getSinkFn(), sinkFn);

    TestMessageEnvelope mockInput = mock(TestMessageEnvelope.class);
    when(mockInput.getKey()).thenReturn("my-test-msg-key");
    MessageType mockMsgBody = mock(MessageType.class);
    when(mockInput.getMessage()).thenReturn(mockMsgBody);
    final List<OutgoingMessageEnvelope> outputMsgs = new ArrayList<>();
    MessageCollector mockCollector = mock(MessageCollector.class);
    doAnswer(invocation -> {
        outputMsgs.add((OutgoingMessageEnvelope) invocation.getArguments()[0]);
        return null;
      }).when(mockCollector).send(any());
    sinkOp.getSinkFn().apply(mockInput, mockCollector, null);
    assertEquals(1, outputMsgs.size());
    assertEquals(outputMsgs.get(0).getKey(), "my-test-msg-key");
    assertEquals(outputMsgs.get(0).getMessage(), mockMsgBody);
    assertEquals(sinkOp.getOpCode(), OperatorSpec.OpCode.SINK);
    assertEquals(sinkOp.getNextStream(), null);
  }

  @Test
  public void testCreateSendToOperator() {
    OutputStreamInternalImpl<Object, Object, TestMessageEnvelope> mockOutput = mock(OutputStreamInternalImpl.class);
    SinkOperatorSpec<TestMessageEnvelope> sinkOp = OperatorSpecs.createSendToOperatorSpec(mockOutput, 1);
    assertNotNull(sinkOp.getSinkFn());
    assertEquals(sinkOp.getOpCode(), OperatorSpec.OpCode.SEND_TO);
    assertEquals(sinkOp.getNextStream(), null);
  }


  @Test
  public void testCreatePartitionByOperator() {
    OutputStreamInternalImpl<Object, Object, TestMessageEnvelope> mockOutput = mock(OutputStreamInternalImpl.class);
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
  public void testCreateWindowOperatorWithRelaxedTypes() throws Exception {
    Function<TestMessageEnvelope, String> keyExtractor = m -> m.getKey();
    FoldLeftFunction<TestMessageEnvelope, Integer> aggregator = (m, c) -> c + 1;
    Supplier<Integer> initialValue = () -> 0;
    //instantiate a window using reflection
    WindowInternal<TestInputMessageEnvelope, String, Integer> window = new WindowInternal(null, initialValue, aggregator, keyExtractor, null, WindowType.TUMBLING);

    MessageStreamImpl<WindowPane<String, Integer>> mockWndOut = mock(MessageStreamImpl.class);
    WindowOperatorSpec spec =
        OperatorSpecs.createWindowOperatorSpec(window, mockWndOut, 1);
    assertEquals(spec.getWindow(), window);
    assertEquals(spec.getWindow().getKeyExtractor(), keyExtractor);
    assertEquals(spec.getWindow().getFoldLeftFunction(), aggregator);

    // make sure that the functions with relaxed types work as expected
    TestInputMessageEnvelope inputMsg = new TestInputMessageEnvelope("test-input-key1", "test-value-1", 23456L, "input-id-1");
    assertEquals("test-input-key1", spec.getWindow().getKeyExtractor().apply(inputMsg));
    assertEquals(1, spec.getWindow().getFoldLeftFunction().apply(inputMsg, 0));
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
