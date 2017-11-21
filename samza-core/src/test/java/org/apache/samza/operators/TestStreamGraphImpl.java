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
 * KIND, either express or implied.  See the License for THE
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.table.TableSpec;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import junit.framework.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStreamGraphImpl {

  @Test
  public void testGetInputStreamWithValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    MessageStream<TestMessageEnvelope> inputStream = graph.getInputStream("test-stream-1", mockValueSerde);

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graph.getInputOperators().get(mockStreamSpec), inputOpSpec);
    assertEquals(mockStreamSpec, inputOpSpec.getStreamSpec());
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test
  public void testGetInputStreamWithKeyValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));

    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    MessageStream<TestMessageEnvelope> inputStream = graph.getInputStream("test-stream-1", mockKVSerde);

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graph.getInputOperators().get(mockStreamSpec), inputOpSpec);
    assertEquals(mockStreamSpec, inputOpSpec.getStreamSpec());
    assertEquals(mockKeySerde, inputOpSpec.getKeySerde());
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test(expected = NullPointerException.class)
  public void testGetInputStreamWithNullSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));

    graph.getInputStream("test-stream-1", null);
  }

  @Test
  public void testGetInputStreamWithDefaultValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    graph.setDefaultSerde(mockValueSerde);
    MessageStream<TestMessageEnvelope> inputStream = graph.getInputStream("test-stream-1");

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graph.getInputOperators().get(mockStreamSpec), inputOpSpec);
    assertEquals(mockStreamSpec, inputOpSpec.getStreamSpec());
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test
  public void testGetInputStreamWithDefaultKeyValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));

    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    graph.setDefaultSerde(mockKVSerde);
    MessageStream<TestMessageEnvelope> inputStream = graph.getInputStream("test-stream-1");

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graph.getInputOperators().get(mockStreamSpec), inputOpSpec);
    assertEquals(mockStreamSpec, inputOpSpec.getStreamSpec());
    assertEquals(mockKeySerde, inputOpSpec.getKeySerde());
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test
  public void testGetInputStreamWithDefaultDefaultSerde() {
    // default default serde == user hasn't provided a default serde
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));

    MessageStream<TestMessageEnvelope> inputStream = graph.getInputStream("test-stream-1");

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graph.getInputOperators().get(mockStreamSpec), inputOpSpec);
    assertEquals(mockStreamSpec, inputOpSpec.getStreamSpec());
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertTrue(inputOpSpec.getValueSerde() instanceof NoOpSerde);
  }

  @Test
  public void testGetInputStreamWithRelaxedTypes() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));

    MessageStream<TestMessageEnvelope> inputStream = graph.getInputStream("test-stream-1");

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graph.getInputOperators().get(mockStreamSpec), inputOpSpec);
    assertEquals(mockStreamSpec, inputOpSpec.getStreamSpec());
  }

  @Test
  public void testMultipleGetInputStreams() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec1 = mock(StreamSpec.class);
    StreamSpec mockStreamSpec2 = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec1);
    when(mockRunner.getStreamSpec("test-stream-2")).thenReturn(mockStreamSpec2);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
    MessageStream<Object> inputStream1 = graph.getInputStream("test-stream-1");
    MessageStream<Object> inputStream2 = graph.getInputStream("test-stream-2");

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec1 =
        (InputOperatorSpec) ((MessageStreamImpl<Object>) inputStream1).getOperatorSpec();
    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec2 =
        (InputOperatorSpec) ((MessageStreamImpl<Object>) inputStream2).getOperatorSpec();

    assertEquals(graph.getInputOperators().size(), 2);
    assertEquals(graph.getInputOperators().get(mockStreamSpec1), inputOpSpec1);
    assertEquals(graph.getInputOperators().get(mockStreamSpec2), inputOpSpec2);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameInputStreamTwice() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mock(StreamSpec.class));

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
    graph.getInputStream("test-stream-1");
    // should throw exception
    graph.getInputStream("test-stream-1");
  }

  @Test
  public void testGetOutputStreamWithValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    OutputStream<TestMessageEnvelope> outputStream =
        graph.getOutputStream("test-stream-1", mockValueSerde);

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graph.getOutputStreams().get(mockStreamSpec), outputStreamImpl);
    assertEquals(mockStreamSpec, outputStreamImpl.getStreamSpec());
    assertTrue(outputStreamImpl.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test
  public void testGetOutputStreamWithKeyValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    graph.setDefaultSerde(mockKVSerde);
    OutputStream<TestMessageEnvelope> outputStream = graph.getOutputStream("test-stream-1", mockKVSerde);

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graph.getOutputStreams().get(mockStreamSpec), outputStreamImpl);
    assertEquals(mockStreamSpec, outputStreamImpl.getStreamSpec());
    assertEquals(mockKeySerde, outputStreamImpl.getKeySerde());
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test(expected = NullPointerException.class)
  public void testGetOutputStreamWithNullSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));

    graph.getOutputStream("test-stream-1", null);
  }

  @Test
  public void testGetOutputStreamWithDefaultValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);

    Serde mockValueSerde = mock(Serde.class);
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
    graph.setDefaultSerde(mockValueSerde);
    OutputStream<TestMessageEnvelope> outputStream = graph.getOutputStream("test-stream-1");

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graph.getOutputStreams().get(mockStreamSpec), outputStreamImpl);
    assertEquals(mockStreamSpec, outputStreamImpl.getStreamSpec());
    assertTrue(outputStreamImpl.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test
  public void testGetOutputStreamWithDefaultKeyValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    graph.setDefaultSerde(mockKVSerde);

    OutputStream<TestMessageEnvelope> outputStream = graph.getOutputStream("test-stream-1");

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graph.getOutputStreams().get(mockStreamSpec), outputStreamImpl);
    assertEquals(mockStreamSpec, outputStreamImpl.getStreamSpec());
    assertEquals(mockKeySerde, outputStreamImpl.getKeySerde());
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test
  public void testGetOutputStreamWithDefaultDefaultSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mockStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));

    OutputStream<TestMessageEnvelope> outputStream = graph.getOutputStream("test-stream-1");

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graph.getOutputStreams().get(mockStreamSpec), outputStreamImpl);
    assertEquals(mockStreamSpec, outputStreamImpl.getStreamSpec());
    assertTrue(outputStreamImpl.getKeySerde() instanceof NoOpSerde);
    assertTrue(outputStreamImpl.getValueSerde() instanceof NoOpSerde);
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSerdeAfterGettingStreams() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mock(StreamSpec.class));

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
    graph.getInputStream("test-stream-1");
    graph.setDefaultSerde(mock(Serde.class)); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSerdeAfterGettingOutputStream() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mock(StreamSpec.class));

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
    graph.getOutputStream("test-stream-1");
    graph.setDefaultSerde(mock(Serde.class)); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSerdeAfterGettingIntermediateStream() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mock(StreamSpec.class));

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
    graph.getIntermediateStream("test-stream-1", null);
    graph.setDefaultSerde(mock(Serde.class)); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameOutputStreamTwice() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mock(StreamSpec.class));

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
    graph.getOutputStream("test-stream-1");
    graph.getOutputStream("test-stream-1"); // should throw exception
  }

  @Test
  public void testGetIntermediateStreamWithValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    String mockStreamName = "mockStreamName";
    when(mockRunner.getStreamSpec(mockStreamName)).thenReturn(mockStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);

    Serde mockValueSerde = mock(Serde.class);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graph.getIntermediateStream(mockStreamName, mockValueSerde);

    assertEquals(graph.getInputOperators().get(mockStreamSpec), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graph.getOutputStreams().get(mockStreamSpec), intermediateStreamImpl.getOutputStream());
    assertEquals(mockStreamSpec, intermediateStreamImpl.getStreamSpec());
    assertTrue(intermediateStreamImpl.getOutputStream().getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde());
    assertTrue(((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test
  public void testGetIntermediateStreamWithKeyValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    String mockStreamName = "mockStreamName";
    when(mockRunner.getStreamSpec(mockStreamName)).thenReturn(mockStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);

    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graph.getIntermediateStream(mockStreamName, mockKVSerde);

    assertEquals(graph.getInputOperators().get(mockStreamSpec), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graph.getOutputStreams().get(mockStreamSpec), intermediateStreamImpl.getOutputStream());
    assertEquals(mockStreamSpec, intermediateStreamImpl.getStreamSpec());
    assertEquals(mockKeySerde, intermediateStreamImpl.getOutputStream().getKeySerde());
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde());
    assertEquals(mockKeySerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde());
    assertEquals(mockValueSerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test
  public void testGetIntermediateStreamWithDefaultValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    String mockStreamName = "mockStreamName";
    when(mockRunner.getStreamSpec(mockStreamName)).thenReturn(mockStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);

    Serde mockValueSerde = mock(Serde.class);
    graph.setDefaultSerde(mockValueSerde);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graph.getIntermediateStream(mockStreamName, null);

    assertEquals(graph.getInputOperators().get(mockStreamSpec), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graph.getOutputStreams().get(mockStreamSpec), intermediateStreamImpl.getOutputStream());
    assertEquals(mockStreamSpec, intermediateStreamImpl.getStreamSpec());
    assertTrue(intermediateStreamImpl.getOutputStream().getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde());
    assertTrue(((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test
  public void testGetIntermediateStreamWithDefaultKeyValueSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    String mockStreamName = "mockStreamName";
    when(mockRunner.getStreamSpec(mockStreamName)).thenReturn(mockStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);

    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    graph.setDefaultSerde(mockKVSerde);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graph.getIntermediateStream(mockStreamName, null);

    assertEquals(graph.getInputOperators().get(mockStreamSpec), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graph.getOutputStreams().get(mockStreamSpec), intermediateStreamImpl.getOutputStream());
    assertEquals(mockStreamSpec, intermediateStreamImpl.getStreamSpec());
    assertEquals(mockKeySerde, intermediateStreamImpl.getOutputStream().getKeySerde());
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde());
    assertEquals(mockKeySerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde());
    assertEquals(mockValueSerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test
  public void testGetIntermediateStreamWithDefaultDefaultSerde() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    String mockStreamName = "mockStreamName";
    when(mockRunner.getStreamSpec(mockStreamName)).thenReturn(mockStreamSpec);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graph.getIntermediateStream(mockStreamName, null);

    assertEquals(graph.getInputOperators().get(mockStreamSpec), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graph.getOutputStreams().get(mockStreamSpec), intermediateStreamImpl.getOutputStream());
    assertEquals(mockStreamSpec, intermediateStreamImpl.getStreamSpec());
    assertTrue(intermediateStreamImpl.getOutputStream().getKeySerde() instanceof NoOpSerde);
    assertTrue(intermediateStreamImpl.getOutputStream().getValueSerde() instanceof NoOpSerde);
    assertTrue(((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde() instanceof NoOpSerde);
    assertTrue(((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde() instanceof NoOpSerde);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameIntermediateStreamTwice() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(mock(StreamSpec.class));

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mock(Config.class));
    graph.getIntermediateStream("test-stream-1", mock(Serde.class));
    graph.getIntermediateStream("test-stream-1", mock(Serde.class));
  }

  @Test
  public void testGetNextOpIdIncrementsId() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);
    assertEquals("jobName-1234-merge-0", graph.getNextOpId(OpCode.MERGE, null));
    assertEquals("jobName-1234-join-customName", graph.getNextOpId(OpCode.JOIN, "customName"));
    assertEquals("jobName-1234-map-2", graph.getNextOpId(OpCode.MAP, null));
  }

  @Test(expected = SamzaException.class)
  public void testGetNextOpIdRejectsDuplicates() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);
    assertEquals("jobName-1234-join-customName", graph.getNextOpId(OpCode.JOIN, "customName"));
    graph.getNextOpId(OpCode.JOIN, "customName"); // should throw
  }

  @Test
  public void testUserDefinedIdValidation() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);

    // null and empty userDefinedIDs should fall back to autogenerated IDs.
    try {
      graph.getNextOpId(OpCode.FILTER, null);
      graph.getNextOpId(OpCode.FILTER, "");
      graph.getNextOpId(OpCode.FILTER, " ");
      graph.getNextOpId(OpCode.FILTER, "\t");
    } catch (SamzaException e) {
      Assert.fail("Received an error with a null or empty operator ID instead of defaulting to auto-generated ID.");
    }

    List<String> validOpIds = ImmutableList.of("op.id", "op_id", "op-id", "1000", "op_1", "OP_ID");
    for (String validOpId: validOpIds) {
      try {
        graph.getNextOpId(OpCode.FILTER, validOpId);
      } catch (Exception e) {
        Assert.fail("Received an exception with a valid operator ID: " + validOpId);
      }
    }

    List<String> invalidOpIds = ImmutableList.of("op id", "op#id");
    for (String invalidOpId: invalidOpIds) {
      try {
        graph.getNextOpId(OpCode.FILTER, invalidOpId);
        Assert.fail("Did not receive an exception with an invalid operator ID: " + invalidOpId);
      } catch (SamzaException e) { }
    }
  }

  @Test
  public void testGetInputStreamPreservesInsertionOrder() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);

    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);

    StreamSpec testStreamSpec1 = new StreamSpec("test-stream-1", "physical-stream-1", "test-system");
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(testStreamSpec1);

    StreamSpec testStreamSpec2 = new StreamSpec("test-stream-2", "physical-stream-2", "test-system");
    when(mockRunner.getStreamSpec("test-stream-2")).thenReturn(testStreamSpec2);

    StreamSpec testStreamSpec3 = new StreamSpec("test-stream-3", "physical-stream-3", "test-system");
    when(mockRunner.getStreamSpec("test-stream-3")).thenReturn(testStreamSpec3);

    graph.getInputStream("test-stream-1");
    graph.getInputStream("test-stream-2");
    graph.getInputStream("test-stream-3");

    List<InputOperatorSpec> inputSpecs = new ArrayList<>(graph.getInputOperators().values());
    Assert.assertEquals(inputSpecs.size(), 3);
    Assert.assertEquals(inputSpecs.get(0).getStreamSpec(), testStreamSpec1);
    Assert.assertEquals(inputSpecs.get(1).getStreamSpec(), testStreamSpec2);
    Assert.assertEquals(inputSpecs.get(2).getStreamSpec(), testStreamSpec3);
  }

  @Test
  public void testGetTable() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    Config mockConfig = mock(Config.class);
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);

    TableDescriptor mockTableDescriptor = mock(TableDescriptor.class);
    when(mockTableDescriptor.getTableSpec()).thenReturn(
        new TableSpec("t1", KVSerde.of(new NoOpSerde(), new NoOpSerde()), "", new HashMap<>()));
    Assert.assertNotNull(graph.getTable(mockTableDescriptor));
  }
}
