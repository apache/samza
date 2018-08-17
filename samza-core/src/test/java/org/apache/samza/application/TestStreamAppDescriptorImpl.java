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
package org.apache.samza.application;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.TableSpec;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link StreamAppDescriptorImpl}
 */
public class TestStreamAppDescriptorImpl {

  @Test
  public void testConstructor() {
    StreamApplication mockApp = mock(StreamApplication.class);
    Config mockConfig = mock(Config.class);
    StreamAppDescriptorImpl appDesc = new StreamAppDescriptorImpl(mockApp, mockConfig);
    verify(mockApp, times(1)).describe(appDesc);
    assertEquals(mockConfig, appDesc.config);
  }

  @Test
  public void testGetInputStreamWithValueSerde() {

    String streamId = "test-stream-1";
    Serde mockValueSerde = mock(Serde.class);
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(streamId, mockValueSerde);
      }, mock(Config.class));

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec = graphSpec.getInputOperators().get(streamId);
    assertEquals(OperatorSpec.OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test
  public void testGetInputStreamWithKeyValueSerde() {

    String streamId = "test-stream-1";
    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(streamId, mockKVSerde);
      }, mock(Config.class));

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec = graphSpec.getInputOperators().get(streamId);
    assertEquals(OperatorSpec.OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(mockKeySerde, inputOpSpec.getKeySerde());
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test(expected = NullPointerException.class)
  public void testGetInputStreamWithNullSerde() {
    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream("test-stream-1", null);
      }, mock(Config.class));
  }

  @Test
  public void testGetInputStreamWithDefaultValueSerde() {
    String streamId = "test-stream-1";

    Serde mockValueSerde = mock(Serde.class);
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.setDefaultSerde(mockValueSerde);
        appDesc.getInputStream(streamId);
      }, mock(Config.class));

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec = graphSpec.getInputOperators().get(streamId);
    assertEquals(OperatorSpec.OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test
  public void testGetInputStreamWithDefaultKeyValueSerde() {
    String streamId = "test-stream-1";

    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.setDefaultSerde(mockKVSerde);
        appDesc.getInputStream(streamId);
      }, mock(Config.class));

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec = graphSpec.getInputOperators().get(streamId);
    assertEquals(OperatorSpec.OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(mockKeySerde, inputOpSpec.getKeySerde());
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test
  public void testGetInputStreamWithDefaultDefaultSerde() {
    String streamId = "test-stream-1";

    // default default serde == user hasn't provided a default serde
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(streamId);
      }, mock(Config.class));

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec = graphSpec.getInputOperators().get(streamId);
    assertEquals(OperatorSpec.OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertTrue(inputOpSpec.getValueSerde() instanceof NoOpSerde);
  }

  @Test
  public void testGetInputStreamWithRelaxedTypes() {
    String streamId = "test-stream-1";
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(streamId);
      }, mock(Config.class));

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec = graphSpec.getInputOperators().get(streamId);
    assertEquals(OperatorSpec.OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(streamId, inputOpSpec.getStreamId());
  }

  @Test
  public void testMultipleGetInputStreams() {
    String streamId1 = "test-stream-1";
    String streamId2 = "test-stream-2";

    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(streamId1);
        appDesc.getInputStream(streamId2);
      }, mock(Config.class));

    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec1 = graphSpec.getInputOperators().get(streamId1);
    InputOperatorSpec<String, TestMessageEnvelope> inputOpSpec2 = graphSpec.getInputOperators().get(streamId2);

    assertEquals(graphSpec.getInputOperators().size(), 2);
    assertEquals(streamId1, inputOpSpec1.getStreamId());
    assertEquals(streamId2, inputOpSpec2.getStreamId());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameInputStreamTwice() {
    String streamId = "test-stream-1";
    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(streamId);
        // should throw exception
        appDesc.getInputStream(streamId);
      }, mock(Config.class));
  }

  @Test
  public void testGetOutputStreamWithValueSerde() {
    String streamId = "test-stream-1";
    Serde mockValueSerde = mock(Serde.class);
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getOutputStream(streamId, mockValueSerde);
      }, mock(Config.class));

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = graphSpec.getOutputStreams().get(streamId);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertTrue(outputStreamImpl.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test
  public void testGetOutputStreamWithKeyValueSerde() {
    String streamId = "test-stream-1";
    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.setDefaultSerde(mockKVSerde);
        appDesc.getOutputStream(streamId, mockKVSerde);
      }, mock(Config.class));

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = graphSpec.getOutputStreams().get(streamId);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertEquals(mockKeySerde, outputStreamImpl.getKeySerde());
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test(expected = NullPointerException.class)
  public void testGetOutputStreamWithNullSerde() {
    String streamId = "test-stream-1";
    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getOutputStream(streamId, null);
      }, mock(Config.class));
  }

  @Test
  public void testGetOutputStreamWithDefaultValueSerde() {
    String streamId = "test-stream-1";

    Serde mockValueSerde = mock(Serde.class);
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.setDefaultSerde(mockValueSerde);
        appDesc.getOutputStream(streamId);
      }, mock(Config.class));

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = graphSpec.getOutputStreams().get(streamId);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertTrue(outputStreamImpl.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test
  public void testGetOutputStreamWithDefaultKeyValueSerde() {
    String streamId = "test-stream-1";

    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.setDefaultSerde(mockKVSerde);
        appDesc.getOutputStream(streamId);
      }, mock(Config.class));

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = graphSpec.getOutputStreams().get(streamId);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertEquals(mockKeySerde, outputStreamImpl.getKeySerde());
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test
  public void testGetOutputStreamWithDefaultDefaultSerde() {
    String streamId = "test-stream-1";

    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getOutputStream(streamId);
      }, mock(Config.class));


    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = graphSpec.getOutputStreams().get(streamId);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertTrue(outputStreamImpl.getKeySerde() instanceof NoOpSerde);
    assertTrue(outputStreamImpl.getValueSerde() instanceof NoOpSerde);
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSerdeAfterGettingStreams() {
    String streamId = "test-stream-1";

    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(streamId);
        appDesc.setDefaultSerde(mock(Serde.class)); // should throw exception
      }, mock(Config.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSerdeAfterGettingOutputStream() {
    String streamId = "test-stream-1";
    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getOutputStream(streamId);
        appDesc.setDefaultSerde(mock(Serde.class)); // should throw exception
      }, mock(Config.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSerdeAfterGettingIntermediateStream() {
    String streamId = "test-stream-1";
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> { }, mock(Config.class));
    graphSpec.getIntermediateStream(streamId, null);
    graphSpec.setDefaultSerde(mock(Serde.class)); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameOutputStreamTwice() {
    String streamId = "test-stream-1";
    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getOutputStream(streamId);
        appDesc.getOutputStream(streamId); // should throw exception
      }, mock(Config.class));
  }

  @Test
  public void testGetIntermediateStreamWithValueSerde() {
    String streamId = "stream-1";
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> { }, mock(Config.class));

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
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> { }, mock(Config.class));

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
    StreamAppDescriptorImpl graph = new StreamAppDescriptorImpl(appDesc -> { }, mock(Config.class));

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

    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> { }, mockConfig);

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

    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> { }, mockConfig);
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
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> { }, mock(Config.class));
    graphSpec.getIntermediateStream("test-stream-1", mock(Serde.class));
    graphSpec.getIntermediateStream("test-stream-1", mock(Serde.class));
  }

  @Test
  public void testGetNextOpIdIncrementsId() {
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> { }, mockConfig);
    assertEquals("jobName-1234-merge-0", graphSpec.getNextOpId(OperatorSpec.OpCode.MERGE, null));
    assertEquals("jobName-1234-join-customName", graphSpec.getNextOpId(OperatorSpec.OpCode.JOIN, "customName"));
    assertEquals("jobName-1234-map-2", graphSpec.getNextOpId(OperatorSpec.OpCode.MAP, null));
  }

  @Test(expected = SamzaException.class)
  public void testGetNextOpIdRejectsDuplicates() {
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> { }, mockConfig);
    assertEquals("jobName-1234-join-customName", graphSpec.getNextOpId(OperatorSpec.OpCode.JOIN, "customName"));
    graphSpec.getNextOpId(OperatorSpec.OpCode.JOIN, "customName"); // should throw
  }

  @Test
  public void testUserDefinedIdValidation() {
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> { }, mockConfig);

    // null and empty userDefinedIDs should fall back to autogenerated IDs.
    try {
      graphSpec.getNextOpId(OperatorSpec.OpCode.FILTER, null);
      graphSpec.getNextOpId(OperatorSpec.OpCode.FILTER, "");
      graphSpec.getNextOpId(OperatorSpec.OpCode.FILTER, " ");
      graphSpec.getNextOpId(OperatorSpec.OpCode.FILTER, "\t");
    } catch (SamzaException e) {
      fail("Received an error with a null or empty operator ID instead of defaulting to auto-generated ID.");
    }

    List<String> validOpIds = ImmutableList.of("op.id", "op_id", "op-id", "1000", "op_1", "OP_ID");
    for (String validOpId: validOpIds) {
      try {
        graphSpec.getNextOpId(OperatorSpec.OpCode.FILTER, validOpId);
      } catch (Exception e) {
        fail("Received an exception with a valid operator ID: " + validOpId);
      }
    }

    List<String> invalidOpIds = ImmutableList.of("op id", "op#id");
    for (String invalidOpId: invalidOpIds) {
      try {
        graphSpec.getNextOpId(OperatorSpec.OpCode.FILTER, invalidOpId);
        fail("Did not receive an exception with an invalid operator ID: " + invalidOpId);
      } catch (SamzaException e) { }
    }
  }

  @Test
  public void testGetInputStreamPreservesInsertionOrder() {
    Config mockConfig = mock(Config.class);

    String testStreamId1 = "test-stream-1";
    String testStreamId2 = "test-stream-2";
    String testStreamId3 = "test-stream-3";

    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(testStreamId1);
        appDesc.getInputStream(testStreamId2);
        appDesc.getInputStream(testStreamId3);
      }, mockConfig);

    List<InputOperatorSpec> inputSpecs = new ArrayList<>(graphSpec.getInputOperators().values());
    assertEquals(inputSpecs.size(), 3);
    assertEquals(inputSpecs.get(0).getStreamId(), testStreamId1);
    assertEquals(inputSpecs.get(1).getStreamId(), testStreamId2);
    assertEquals(inputSpecs.get(2).getStreamId(), testStreamId3);
  }

  @Test
  public void testGetTable() throws Exception {
    Config mockConfig = mock(Config.class);

    BaseTableDescriptor mockTableDescriptor = mock(BaseTableDescriptor.class);
    TableSpec testTableSpec = new TableSpec("t1", KVSerde.of(new NoOpSerde(), new NoOpSerde()), "", new HashMap<>());
    when(mockTableDescriptor.getTableSpec()).thenReturn(testTableSpec);
    StreamAppDescriptorImpl graphSpec = new StreamAppDescriptorImpl(appDesc -> {
      appDesc.getTable(mockTableDescriptor);
    }, mockConfig);
    assertNotNull(graphSpec.getTables().get(testTableSpec));
  }

  @Test
  public void testContextManager() {
    ContextManager cntxMan = mock(ContextManager.class);
    StreamApplication testApp = appDesc -> appDesc.withContextManager(cntxMan);
    StreamAppDescriptorImpl appSpec = new StreamAppDescriptorImpl(testApp, mock(Config.class));
    assertEquals(appSpec.getContextManager(), cntxMan);
  }

  @Test
  public void testProcessorLifecycleListenerFactory() {
    ProcessorLifecycleListenerFactory mockFactory = mock(ProcessorLifecycleListenerFactory.class);
    StreamApplication testApp = appSpec -> appSpec.withProcessorLifecycleListenerFactory(mockFactory);
    StreamAppDescriptorImpl appDesc = new StreamAppDescriptorImpl(testApp, mock(Config.class));
    assertEquals(appDesc.getProcessorLifecycleListenerFactory(), mockFactory);
  }
}
