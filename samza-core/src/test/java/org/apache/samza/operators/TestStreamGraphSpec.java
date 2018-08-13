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

import com.google.common.collect.ImmutableList;
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
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.TableSpec;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStreamGraphSpec {

  @Test
  public void testGetInputStreamWithValueSerde() {
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    String streamId = "test-stream-1";
    Serde mockValueSerde = mock(Serde.class);
    MessageStream<TestMessageEnvelope> inputStream = graphSpec.getInputStream(streamId, mockValueSerde);

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graphSpec.getInputOperators().get(streamId), inputOpSpec);
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test
  public void testGetInputStreamWithKeyValueSerde() {
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    String streamId = "test-stream-1";
    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    MessageStream<TestMessageEnvelope> inputStream = graphSpec.getInputStream(streamId, mockKVSerde);

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graphSpec.getInputOperators().get(streamId), inputOpSpec);
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(mockKeySerde, inputOpSpec.getKeySerde());
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test(expected = NullPointerException.class)
  public void testGetInputStreamWithNullSerde() {
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    graphSpec.getInputStream("test-stream-1", null);
  }

  @Test
  public void testGetInputStreamWithDefaultValueSerde() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    graphSpec.setDefaultSerde(mockValueSerde);
    MessageStream<TestMessageEnvelope> inputStream = graphSpec.getInputStream(streamId);

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graphSpec.getInputOperators().get(streamId), inputOpSpec);
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test
  public void testGetInputStreamWithDefaultKeyValueSerde() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    graphSpec.setDefaultSerde(mockKVSerde);
    MessageStream<TestMessageEnvelope> inputStream = graphSpec.getInputStream(streamId);

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graphSpec.getInputOperators().get(streamId), inputOpSpec);
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(mockKeySerde, inputOpSpec.getKeySerde());
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test
  public void testGetInputStreamWithDefaultDefaultSerde() {
    String streamId = "test-stream-1";

    // default default serde == user hasn't provided a default serde
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    MessageStream<TestMessageEnvelope> inputStream = graphSpec.getInputStream(streamId);

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graphSpec.getInputOperators().get(streamId), inputOpSpec);
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertTrue(inputOpSpec.getValueSerde() instanceof NoOpSerde);
  }

  @Test
  public void testGetInputStreamWithRelaxedTypes() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    MessageStream<TestMessageEnvelope> inputStream = graphSpec.getInputStream(streamId);

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec =
        (InputOperatorSpec) ((MessageStreamImpl<TestMessageEnvelope>) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graphSpec.getInputOperators().get(streamId), inputOpSpec);
    assertEquals(streamId, inputOpSpec.getStreamId());
  }

  @Test
  public void testMultipleGetInputStreams() {
    String streamId1 = "test-stream-1";
    String streamId2 = "test-stream-2";

    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    MessageStream<Object> inputStream1 = graphSpec.getInputStream(streamId1);
    MessageStream<Object> inputStream2 = graphSpec.getInputStream(streamId2);

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec1 =
        (InputOperatorSpec) ((MessageStreamImpl<Object>) inputStream1).getOperatorSpec();
    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec2 =
        (InputOperatorSpec) ((MessageStreamImpl<Object>) inputStream2).getOperatorSpec();

    assertEquals(graphSpec.getInputOperators().size(), 2);
    assertEquals(graphSpec.getInputOperators().get(streamId1), inputOpSpec1);
    assertEquals(graphSpec.getInputOperators().get(streamId2), inputOpSpec2);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameInputStreamTwice() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    graphSpec.getInputStream(streamId);
    // should throw exception
    graphSpec.getInputStream(streamId);
  }

  @Test
  public void testGetOutputStreamWithValueSerde() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    OutputStream<TestMessageEnvelope> outputStream =
        graphSpec.getOutputStream(streamId, mockValueSerde);

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graphSpec.getOutputStreams().get(streamId), outputStreamImpl);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertTrue(outputStreamImpl.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test
  public void testGetOutputStreamWithKeyValueSerde() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    graphSpec.setDefaultSerde(mockKVSerde);
    OutputStream<TestMessageEnvelope> outputStream = graphSpec.getOutputStream(streamId, mockKVSerde);

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graphSpec.getOutputStreams().get(streamId), outputStreamImpl);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertEquals(mockKeySerde, outputStreamImpl.getKeySerde());
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test(expected = NullPointerException.class)
  public void testGetOutputStreamWithNullSerde() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    graphSpec.getOutputStream(streamId, null);
  }

  @Test
  public void testGetOutputStreamWithDefaultValueSerde() {
    String streamId = "test-stream-1";

    Serde mockValueSerde = mock(Serde.class);
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    graphSpec.setDefaultSerde(mockValueSerde);
    OutputStream<TestMessageEnvelope> outputStream = graphSpec.getOutputStream(streamId);

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graphSpec.getOutputStreams().get(streamId), outputStreamImpl);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertTrue(outputStreamImpl.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test
  public void testGetOutputStreamWithDefaultKeyValueSerde() {
    String streamId = "test-stream-1";

    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    graphSpec.setDefaultSerde(mockKVSerde);

    OutputStream<TestMessageEnvelope> outputStream = graphSpec.getOutputStream(streamId);

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graphSpec.getOutputStreams().get(streamId), outputStreamImpl);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertEquals(mockKeySerde, outputStreamImpl.getKeySerde());
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test
  public void testGetOutputStreamWithDefaultDefaultSerde() {
    String streamId = "test-stream-1";

    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    OutputStream<TestMessageEnvelope> outputStream = graphSpec.getOutputStream(streamId);

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graphSpec.getOutputStreams().get(streamId), outputStreamImpl);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertTrue(outputStreamImpl.getKeySerde() instanceof NoOpSerde);
    assertTrue(outputStreamImpl.getValueSerde() instanceof NoOpSerde);
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSerdeAfterGettingStreams() {
    String streamId = "test-stream-1";

    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    graphSpec.getInputStream(streamId);
    graphSpec.setDefaultSerde(mock(Serde.class)); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSerdeAfterGettingOutputStream() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    graphSpec.getOutputStream(streamId);
    graphSpec.setDefaultSerde(mock(Serde.class)); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSerdeAfterGettingIntermediateStream() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    graphSpec.getIntermediateStream(streamId, null);
    graphSpec.setDefaultSerde(mock(Serde.class)); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameOutputStreamTwice() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    graphSpec.getOutputStream(streamId);
    graphSpec.getOutputStream(streamId); // should throw exception
  }

  @Test
  public void testGetIntermediateStreamWithValueSerde() {
    String streamId = "stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graphSpec.getIntermediateStream(streamId, mockValueSerde);

    assertEquals(graphSpec.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graphSpec.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertTrue(intermediateStreamImpl.getOutputStream().getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde());
    assertTrue(((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test
  public void testGetIntermediateStreamWithKeyValueSerde() {
    String streamId = "streamId";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graphSpec.getIntermediateStream(streamId, mockKVSerde);

    assertEquals(graphSpec.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graphSpec.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertEquals(mockKeySerde, intermediateStreamImpl.getOutputStream().getKeySerde());
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde());
    assertEquals(mockKeySerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde());
    assertEquals(mockValueSerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test
  public void testGetIntermediateStreamWithDefaultValueSerde() {
    String streamId = "streamId";
    StreamGraphSpec graph = new StreamGraphSpec(mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    graph.setDefaultSerde(mockValueSerde);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graph.getIntermediateStream(streamId, null);

    assertEquals(graph.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graph.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertTrue(intermediateStreamImpl.getOutputStream().getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde());
    assertTrue(((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test
  public void testGetIntermediateStreamWithDefaultKeyValueSerde() {
    Config mockConfig = mock(Config.class);
    String streamId = "streamId";
    
    StreamGraphSpec graphSpec = new StreamGraphSpec(mockConfig);

    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    graphSpec.setDefaultSerde(mockKVSerde);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graphSpec.getIntermediateStream(streamId, null);

    assertEquals(graphSpec.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graphSpec.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertEquals(mockKeySerde, intermediateStreamImpl.getOutputStream().getKeySerde());
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde());
    assertEquals(mockKeySerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde());
    assertEquals(mockValueSerde, ((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test
  public void testGetIntermediateStreamWithDefaultDefaultSerde() {
    Config mockConfig = mock(Config.class);
    String streamId = "streamId";
    
    StreamGraphSpec graphSpec = new StreamGraphSpec(mockConfig);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graphSpec.getIntermediateStream(streamId, null);

    assertEquals(graphSpec.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graphSpec.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertTrue(intermediateStreamImpl.getOutputStream().getKeySerde() instanceof NoOpSerde);
    assertTrue(intermediateStreamImpl.getOutputStream().getValueSerde() instanceof NoOpSerde);
    assertTrue(((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde() instanceof NoOpSerde);
    assertTrue(((InputOperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde() instanceof NoOpSerde);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameIntermediateStreamTwice() {
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    graphSpec.getIntermediateStream("test-stream-1", mock(Serde.class));
    graphSpec.getIntermediateStream("test-stream-1", mock(Serde.class));
  }

  @Test
  public void testGetNextOpIdIncrementsId() {
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamGraphSpec graphSpec = new StreamGraphSpec(mockConfig);
    assertEquals("jobName-1234-merge-0", graphSpec.getNextOpId(OpCode.MERGE, null));
    assertEquals("jobName-1234-join-customName", graphSpec.getNextOpId(OpCode.JOIN, "customName"));
    assertEquals("jobName-1234-map-2", graphSpec.getNextOpId(OpCode.MAP, null));
  }

  @Test(expected = SamzaException.class)
  public void testGetNextOpIdRejectsDuplicates() {
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamGraphSpec graphSpec = new StreamGraphSpec(mockConfig);
    assertEquals("jobName-1234-join-customName", graphSpec.getNextOpId(OpCode.JOIN, "customName"));
    graphSpec.getNextOpId(OpCode.JOIN, "customName"); // should throw
  }

  @Test
  public void testUserDefinedIdValidation() {
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamGraphSpec graphSpec = new StreamGraphSpec(mockConfig);

    // null and empty userDefinedIDs should fall back to autogenerated IDs.
    try {
      graphSpec.getNextOpId(OpCode.FILTER, null);
      graphSpec.getNextOpId(OpCode.FILTER, "");
      graphSpec.getNextOpId(OpCode.FILTER, " ");
      graphSpec.getNextOpId(OpCode.FILTER, "\t");
    } catch (SamzaException e) {
      fail("Received an error with a null or empty operator ID instead of defaulting to auto-generated ID.");
    }

    List<String> validOpIds = ImmutableList.of("op.id", "op_id", "op-id", "1000", "op_1", "OP_ID");
    for (String validOpId: validOpIds) {
      try {
        graphSpec.getNextOpId(OpCode.FILTER, validOpId);
      } catch (Exception e) {
        fail("Received an exception with a valid operator ID: " + validOpId);
      }
    }

    List<String> invalidOpIds = ImmutableList.of("op id", "op#id");
    for (String invalidOpId: invalidOpIds) {
      try {
        graphSpec.getNextOpId(OpCode.FILTER, invalidOpId);
        fail("Did not receive an exception with an invalid operator ID: " + invalidOpId);
      } catch (SamzaException e) { }
    }
  }

  @Test
  public void testGetInputStreamPreservesInsertionOrder() {
    Config mockConfig = mock(Config.class);

    StreamGraphSpec graphSpec = new StreamGraphSpec(mockConfig);

    String testStreamId1 = "test-stream-1";
    String testStreamId2 = "test-stream-2";
    String testStreamId3 = "test-stream-3";
    
    graphSpec.getInputStream("test-stream-1");
    graphSpec.getInputStream("test-stream-2");
    graphSpec.getInputStream("test-stream-3");

    List<InputOperatorSpec> inputSpecs = new ArrayList<>(graphSpec.getInputOperators().values());
    assertEquals(inputSpecs.size(), 3);
    assertEquals(inputSpecs.get(0).getStreamId(), testStreamId1);
    assertEquals(inputSpecs.get(1).getStreamId(), testStreamId2);
    assertEquals(inputSpecs.get(2).getStreamId(), testStreamId3);
  }

  @Test
  public void testGetTable() {
    Config mockConfig = mock(Config.class);
    StreamGraphSpec graphSpec = new StreamGraphSpec(mockConfig);

    BaseTableDescriptor mockTableDescriptor = mock(BaseTableDescriptor.class);
    when(mockTableDescriptor.getTableSpec()).thenReturn(
        new TableSpec("t1", KVSerde.of(new NoOpSerde(), new NoOpSerde()), "", new HashMap<>()));
    assertNotNull(graphSpec.getTable(mockTableDescriptor));
  }
}
