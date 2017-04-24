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

import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.data.MessageType;
import org.apache.samza.operators.data.TestInputMessageEnvelope;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.stream.InputStreamInternalImpl;
import org.apache.samza.operators.stream.IntermediateStreamInternalImpl;
import org.apache.samza.operators.stream.OutputStreamInternalImpl;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.TaskContext;
import org.junit.Test;

import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStreamGraphImpl {

  @Test
  public void testGetInputStream() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    StreamSpec testStreamSpec = new StreamSpec("test-stream-1", "physical-stream-1", "test-system");
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(testStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);
    BiFunction<String, MessageType, TestInputMessageEnvelope> xMsgBuilder =
        (k, v) -> new TestInputMessageEnvelope(k, v.getValue(), v.getEventTime(), "input-id-1");
    MessageStream<TestMessageEnvelope> mInputStream = graph.getInputStream("test-stream-1", xMsgBuilder);
    assertEquals(graph.getInputStreams().get(testStreamSpec), mInputStream);
    assertTrue(mInputStream instanceof InputStreamInternalImpl);
    assertEquals(((InputStreamInternalImpl) mInputStream).getMsgBuilder(), xMsgBuilder);

    String key = "test-input-key";
    MessageType msgBody = new MessageType("test-msg-value", 333333L);
    TestMessageEnvelope xInputMsg = ((InputStreamInternalImpl<String, MessageType, TestMessageEnvelope>) mInputStream).
        getMsgBuilder().apply(key, msgBody);
    assertEquals(xInputMsg.getKey(), key);
    assertEquals(xInputMsg.getMessage().getValue(), msgBody.getValue());
    assertEquals(xInputMsg.getMessage().getEventTime(), msgBody.getEventTime());
    assertEquals(((TestInputMessageEnvelope) xInputMsg).getInputId(), "input-id-1");
  }

  @Test(expected = IllegalStateException.class)
  public void testMultipleGetInputStream() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);

    StreamSpec testStreamSpec1 = new StreamSpec("test-stream-1", "physical-stream-1", "test-system");
    StreamSpec testStreamSpec2 = new StreamSpec("test-stream-2", "physical-stream-2", "test-system");
    StreamSpec nonExistentStreamSpec = new StreamSpec("non-existent-stream", "physical-stream-1", "test-system");

    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(testStreamSpec1);
    when(mockRunner.getStreamSpec("test-stream-2")).thenReturn(testStreamSpec2);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);
    BiFunction<String, MessageType, TestInputMessageEnvelope> xMsgBuilder =
        (k, v) -> new TestInputMessageEnvelope(k, v.getValue(), v.getEventTime(), "input-id-1");

    //create 2 streams for the corresponding streamIds
    MessageStream<TestInputMessageEnvelope> inputStream1 = graph.getInputStream("test-stream-1", xMsgBuilder);
    MessageStream<TestInputMessageEnvelope> inputStream2 = graph.getInputStream("test-stream-2", xMsgBuilder);

    //assert that the streamGraph contains only the above 2 streams
    assertEquals(graph.getInputStreams().get(testStreamSpec1), inputStream1);
    assertEquals(graph.getInputStreams().get(testStreamSpec2), inputStream2);
    assertEquals(graph.getInputStreams().get(nonExistentStreamSpec), null);
    assertEquals(graph.getInputStreams().size(), 2);

    //should throw IllegalStateException
    graph.getInputStream("test-stream-1", xMsgBuilder);
  }

  @Test
  public void testGetOutputStream() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    StreamSpec testStreamSpec = new StreamSpec("test-stream-1", "physical-stream-1", "test-system");
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(testStreamSpec);

    class MyMessageType extends MessageType {
      public final String outputId;

      public MyMessageType(String value, long eventTime, String outputId) {
        super(value, eventTime);
        this.outputId = outputId;
      }
    }

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);
    Function<TestMessageEnvelope, String> xKeyExtractor = x -> x.getKey();
    Function<TestMessageEnvelope, MyMessageType> xMsgExtractor =
        x -> new MyMessageType(x.getMessage().getValue(), x.getMessage().getEventTime(), "test-output-id-1");

    OutputStream<String, MyMessageType, TestInputMessageEnvelope> mOutputStream =
        graph.getOutputStream("test-stream-1", xKeyExtractor, xMsgExtractor);
    assertEquals(graph.getOutputStreams().get(testStreamSpec), mOutputStream);
    assertTrue(mOutputStream instanceof OutputStreamInternalImpl);
    assertEquals(((OutputStreamInternalImpl) mOutputStream).getKeyExtractor(), xKeyExtractor);
    assertEquals(((OutputStreamInternalImpl) mOutputStream).getMsgExtractor(), xMsgExtractor);

    TestInputMessageEnvelope xInputMsg = new TestInputMessageEnvelope("test-key-1", "test-msg-1", 33333L, "input-id-1");
    assertEquals(((OutputStreamInternalImpl<String, MyMessageType, TestInputMessageEnvelope>) mOutputStream).
        getKeyExtractor().apply(xInputMsg), "test-key-1");
    assertEquals(((OutputStreamInternalImpl<String, MyMessageType, TestInputMessageEnvelope>) mOutputStream).
        getMsgExtractor().apply(xInputMsg).getValue(), "test-msg-1");
    assertEquals(((OutputStreamInternalImpl<String, MyMessageType, TestInputMessageEnvelope>) mOutputStream).
        getMsgExtractor().apply(xInputMsg).getEventTime(), 33333L);
    assertEquals(((OutputStreamInternalImpl<String, MyMessageType, TestInputMessageEnvelope>) mOutputStream).
        getMsgExtractor().apply(xInputMsg).outputId, "test-output-id-1");
  }

  @Test
  public void testWithContextManager() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);

    // ensure that default is noop
    TaskContext mockContext = mock(TaskContext.class);
    assertEquals(graph.getContextManager().initTaskContext(mockConfig, mockContext), mockContext);

    ContextManager testContextManager = new ContextManager() {
      @Override
      public TaskContext initTaskContext(Config config, TaskContext context) {
        return null;
      }

      @Override
      public void finalizeTaskContext() {

      }
    };

    graph.withContextManager(testContextManager);
    assertEquals(graph.getContextManager(), testContextManager);
  }

  @Test
  public void testGetIntermediateStream() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    StreamSpec testStreamSpec = new StreamSpec("myJob-i001-test-stream-1", "physical-stream-1", "test-system");
    when(mockRunner.getStreamSpec("myJob-i001-test-stream-1")).thenReturn(testStreamSpec);
    when(mockConfig.get(JobConfig.JOB_NAME())).thenReturn("myJob");
    when(mockConfig.get(JobConfig.JOB_ID(), "1")).thenReturn("i001");

    class MyMessageType extends MessageType {
      public final String outputId;

      public MyMessageType(String value, long eventTime, String outputId) {
        super(value, eventTime);
        this.outputId = outputId;
      }
    }

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);
    Function<TestMessageEnvelope, String> xKeyExtractor = x -> x.getKey();
    Function<TestMessageEnvelope, MyMessageType> xMsgExtractor =
        x -> new MyMessageType(x.getMessage().getValue(), x.getMessage().getEventTime(), "test-output-id-1");
    BiFunction<String, MessageType, TestInputMessageEnvelope> xMsgBuilder =
        (k, v) -> new TestInputMessageEnvelope(k, v.getValue(), v.getEventTime(), "input-id-1");

    MessageStream<TestMessageEnvelope> mIntermediateStream =
        graph.getIntermediateStream("test-stream-1", xKeyExtractor, xMsgExtractor, xMsgBuilder);
    assertEquals(graph.getOutputStreams().get(testStreamSpec), mIntermediateStream);
    assertTrue(mIntermediateStream instanceof IntermediateStreamInternalImpl);
    assertEquals(((IntermediateStreamInternalImpl) mIntermediateStream).getKeyExtractor(), xKeyExtractor);
    assertEquals(((IntermediateStreamInternalImpl) mIntermediateStream).getMsgExtractor(), xMsgExtractor);
    assertEquals(((IntermediateStreamInternalImpl) mIntermediateStream).getMsgBuilder(), xMsgBuilder);

    TestMessageEnvelope xInputMsg = new TestMessageEnvelope("test-key-1", "test-msg-1", 33333L);
    assertEquals(((IntermediateStreamInternalImpl<String, MessageType, TestMessageEnvelope>) mIntermediateStream).
        getKeyExtractor().apply(xInputMsg), "test-key-1");
    assertEquals(((IntermediateStreamInternalImpl<String, MessageType, TestMessageEnvelope>) mIntermediateStream).
        getMsgExtractor().apply(xInputMsg).getValue(), "test-msg-1");
    assertEquals(((IntermediateStreamInternalImpl<String, MessageType, TestMessageEnvelope>) mIntermediateStream).
        getMsgExtractor().apply(xInputMsg).getEventTime(), 33333L);
    assertEquals(((IntermediateStreamInternalImpl<String, MessageType, TestMessageEnvelope>) mIntermediateStream).
        getMsgBuilder().apply("test-key-1", new MyMessageType("test-msg-1", 33333L, "test-output-id-1")).getKey(), "test-key-1");
    assertEquals(((IntermediateStreamInternalImpl<String, MessageType, TestMessageEnvelope>) mIntermediateStream).
        getMsgBuilder().apply("test-key-1", new MyMessageType("test-msg-1", 33333L, "test-output-id-1")).getMessage().getValue(), "test-msg-1");
    assertEquals(((IntermediateStreamInternalImpl<String, MessageType, TestMessageEnvelope>) mIntermediateStream).
        getMsgBuilder().apply("test-key-1", new MyMessageType("test-msg-1", 33333L, "test-output-id-1")).getMessage().getEventTime(), 33333L);
  }

  @Test
  public void testGetNextOpId() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);
    assertEquals(graph.getNextOpId(), 0);
    assertEquals(graph.getNextOpId(), 1);
  }

}
