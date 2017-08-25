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

//import junit.framework.Assert;
//import org.apache.samza.config.Config;
//import org.apache.samza.config.JobConfig;
//import org.apache.samza.operators.data.TestMessageEnvelope;
//import org.apache.samza.operators.spec.InputOperatorSpec;
//import org.apache.samza.operators.spec.OperatorSpec.OpCode;
//import org.apache.samza.operators.spec.OutputStreamImpl;
//import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
//import org.apache.samza.runtime.ApplicationRunner;
//import org.apache.samza.system.StreamSpec;
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.function.BiFunction;
//import java.util.function.Function;
//
//import static org.junit.Assert.assertEquals;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;

public class TestStreamGraphImpl {

//  @Test
//  public void testGetInputStream() {
//    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
//    StreamSpec mockStreamSpec = mock(StreamSpec.class);
//    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);
//    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
//    BiFunction<String, String, TestMessageEnvelope> mockMsgBuilder = mock(BiFunction.class);
//
//    MessageStream<TestMessageEnvelope> inputStream = graph.getInputStream("test-stream-1", mockMsgBuilder);
//
//    InputOperatorSpec<String, String, TestMessageEnvelope> inputOpSpec =
//        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
//    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
//    assertEquals(graph.getInputOperators().get(mockStreamSpec), inputOpSpec);
//    assertEquals(mockMsgBuilder, inputOpSpec.getMsgBuilder());
//    assertEquals(mockStreamSpec, inputOpSpec.getStreamSpec());
//  }
//
//  @Test
//  public void testGetInputStreamWithRelaxedTypes() {
//    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
//    StreamSpec mockStreamSpec = mock(StreamSpec.class);
//    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);
//    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
//    BiFunction<String, String, TestMessageEnvelope> mockMsgBuilder = mock(BiFunction.class);
//
//    MessageStream<TestMessageEnvelope> inputStream = graph.getInputStream("test-stream-1", mockMsgBuilder);
//
//    InputOperatorSpec<String, String, TestMessageEnvelope> inputOpSpec =
//        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
//    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
//    assertEquals(graph.getInputOperators().get(mockStreamSpec), inputOpSpec);
//    assertEquals(mockMsgBuilder, inputOpSpec.getMsgBuilder());
//    assertEquals(mockStreamSpec, inputOpSpec.getStreamSpec());
//  }
//
//  @Test
//  public void testMultipleGetInputStreams() {
//    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
//    StreamSpec mockStreamSpec1 = mock(StreamSpec.class);
//    StreamSpec mockStreamSpec2 = mock(StreamSpec.class);
//    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec1);
//    when(mockRunner.getStreamSpec("test-stream-2")).thenReturn(mockStreamSpec2);
//
//    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
//    MessageStream<Object> inputStream1 = graph.getInputStream("test-stream-1", mock(BiFunction.class));
//    MessageStream<Object> inputStream2 = graph.getInputStream("test-stream-2", mock(BiFunction.class));
//
//    InputOperatorSpec<String, String, TestMessageEnvelope> inputOpSpec1 =
//        (InputOperatorSpec) ((MessageStreamImpl<Object>) inputStream1).getOperatorSpec();
//    InputOperatorSpec<String, String, TestMessageEnvelope> inputOpSpec2 =
//        (InputOperatorSpec) ((MessageStreamImpl<Object>) inputStream2).getOperatorSpec();
//
//    assertEquals(graph.getInputOperators().size(), 2);
//    assertEquals(graph.getInputOperators().get(mockStreamSpec1), inputOpSpec1);
//    assertEquals(graph.getInputOperators().get(mockStreamSpec2), inputOpSpec2);
//  }
//
//  @Test(expected = IllegalStateException.class)
//  public void testGetSameInputStreamTwice() {
//    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
//    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mock(StreamSpec.class));
//
//    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
//    graph.getInputStream("test-stream-1", mock(BiFunction.class));
//    graph.getInputStream("test-stream-1", mock(BiFunction.class)); // should throw exception
//  }
//
//  @Test
//  public void testGetOutputStream() {
//    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
//    StreamSpec mockStreamSpec = mock(StreamSpec.class);
//    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);
//
//
//    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
//    Function<TestMessageEnvelope, String> mockKeyExtractor = mock(Function.class);
//    Function<TestMessageEnvelope, String> mockMsgExtractor = mock(Function.class);
//
//    OutputStream<String, String, TestMessageEnvelope> outputStream =
//        graph.getOutputStream("test-stream-1", mockKeyExtractor, mockMsgExtractor);
//
//    OutputStreamImpl<String, String, TestMessageEnvelope> outputOpSpec = (OutputStreamImpl) outputStream;
//    assertEquals(graph.getOutputStreams().get(mockStreamSpec), outputOpSpec);
//    assertEquals(mockKeyExtractor, outputOpSpec.getKeyExtractor());
//    assertEquals(mockMsgExtractor, outputOpSpec.getMsgExtractor());
//    assertEquals(mockStreamSpec, outputOpSpec.getStreamSpec());
//  }
//
//  @Test(expected = IllegalStateException.class)
//  public void testGetSameOutputStreamTwice() {
//    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
//    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mock(StreamSpec.class));
//
//    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
//    graph.getOutputStream("test-stream-1", mock(Function.class), mock(Function.class));
//    graph.getOutputStream("test-stream-1", mock(Function.class), mock(Function.class)); // should throw exception
//  }
//
//  @Test
//  public void testGetIntermediateStream() {
//    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
//    Config mockConfig = mock(Config.class);
//    StreamSpec mockStreamSpec = mock(StreamSpec.class);
//    when(mockConfig.get(JobConfig.JOB_NAME())).thenReturn("myJob");
//    when(mockConfig.get(JobConfig.JOB_ID(), "1")).thenReturn("i001");
//    when(mockRunner.getStreamSpec("myJob-i001-test-stream-1")).thenReturn(mockStreamSpec);
//
//    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);
//    Function<TestMessageEnvelope, String> mockKeyExtractor = mock(Function.class);
//    Function<TestMessageEnvelope, String> mockMsgExtractor = mock(Function.class);
//    BiFunction<String, String, TestMessageEnvelope> mockMsgBuilder = mock(BiFunction.class);
//
//    IntermediateMessageStreamImpl<?, ?, TestMessageEnvelope> intermediateStreamImpl =
//        graph.getIntermediateStream("test-stream-1", mockKeyExtractor, mockMsgExtractor, mockMsgBuilder);
//
//    assertEquals(graph.getInputOperators().get(mockStreamSpec), intermediateStreamImpl.getOperatorSpec());
//    assertEquals(graph.getOutputStreams().get(mockStreamSpec), intermediateStreamImpl.getOutputStream());
//    assertEquals(mockStreamSpec, intermediateStreamImpl.getStreamSpec());
//    assertEquals(mockKeyExtractor, intermediateStreamImpl.getOutputStream().getKeyExtractor());
//    assertEquals(mockMsgExtractor, intermediateStreamImpl.getOutputStream().getMsgExtractor());
//    assertEquals(mockMsgBuilder, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getMsgBuilder());
//  }
//
//  @Test(expected = IllegalStateException.class)
//  public void testGetSameIntermediateStreamTwice() {
//    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
//    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mock(StreamSpec.class));
//
//    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
//    graph.getIntermediateStream("test-stream-1", mock(Function.class), mock(Function.class), mock(BiFunction.class));
//    graph.getIntermediateStream("test-stream-1", mock(Function.class), mock(Function.class), mock(BiFunction.class));
//  }
//
//  @Test
//  public void testGetNextOpIdIncrementsId() {
//    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
//    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
//    assertEquals(graph.getNextOpId(), 0);
//    assertEquals(graph.getNextOpId(), 1);
//  }
//
//  @Test
//  public void testGetInputStreamPreservesInsertionOrder() {
//    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
//    Config mockConfig = mock(Config.class);
//
//    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);
//
//    StreamSpec testStreamSpec1 = new StreamSpec("test-stream-1", "physical-stream-1", "test-system");
//    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(testStreamSpec1);
//
//    StreamSpec testStreamSpec2 = new StreamSpec("test-stream-2", "physical-stream-2", "test-system");
//    when(mockRunner.getStreamSpec("test-stream-2")).thenReturn(testStreamSpec2);
//
//    StreamSpec testStreamSpec3 = new StreamSpec("test-stream-3", "physical-stream-3", "test-system");
//    when(mockRunner.getStreamSpec("test-stream-3")).thenReturn(testStreamSpec3);
//
//    graph.getInputStream("test-stream-1", (k, v) -> v);
//    graph.getInputStream("test-stream-2", (k, v) -> v);
//    graph.getInputStream("test-stream-3", (k, v) -> v);
//
//    List<InputOperatorSpec> inputSpecs = new ArrayList<>(graph.getInputOperators().values());
//    Assert.assertEquals(inputSpecs.size(), 3);
//    Assert.assertEquals(inputSpecs.get(0).getStreamSpec(), testStreamSpec1);
//    Assert.assertEquals(inputSpecs.get(1).getStreamSpec(), testStreamSpec2);
//    Assert.assertEquals(inputSpecs.get(2).getStreamSpec(), testStreamSpec3);
//  }
}
