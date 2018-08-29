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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.GenericOutputDescriptor;
import org.apache.samza.operators.descriptors.GenericSystemDescriptor;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.system.ExpandingInputDescriptorProvider;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.operators.descriptors.base.system.TransformingInputDescriptorProvider;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.operators.functions.StreamExpander;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.TableSpec;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link StreamAppDescriptorImpl}
 */
public class TestStreamAppDescriptorImpl {

  @Test
  public void testConstructor() {
    StreamApplication mockApp = mock(StreamApplication.class);
    Config mockConfig = mock(Config.class);
    StreamAppDescriptorImpl appDesc = new StreamAppDescriptorImpl(mockApp, mockConfig);
    verify(mockApp).describe(appDesc);
    assertEquals(mockConfig, appDesc.config);
  }

  @Test
  public void testGetInputStreamWithValueSerde() {

    String streamId = "test-stream-1";
    Serde mockValueSerde = mock(Serde.class);
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd = sd.getInputDescriptor(streamId, mockValueSerde);
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(isd);
      }, mock(Config.class));

    InputOperatorSpec inputOpSpec = streamAppDesc.getInputOperators().get(streamId);
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(isd, streamAppDesc.getInputDescriptors().get(streamId));
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
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd = sd.getInputDescriptor(streamId, mockKVSerde);
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(isd);
      }, mock(Config.class));

    InputOperatorSpec inputOpSpec = streamAppDesc.getInputOperators().get(streamId);
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(isd, streamAppDesc.getInputDescriptors().get(streamId));
    assertEquals(mockKeySerde, inputOpSpec.getKeySerde());
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetInputStreamWithNullSerde() {
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd = sd.getInputDescriptor("mockStreamId", null);
    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(isd);
      }, mock(Config.class));
  }

  @Test
  public void testGetInputStreamWithTransformFunction() {
    String streamId = "test-stream-1";
    Serde mockValueSerde = mock(Serde.class);
    InputTransformer transformer = ime -> ime;
    MockTransformingSystemDescriptor sd = new MockTransformingSystemDescriptor("mockSystem", transformer);
    MockInputDescriptor isd = sd.getInputDescriptor(streamId, mockValueSerde);
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(isd);
      }, mock(Config.class));

    InputOperatorSpec inputOpSpec = streamAppDesc.getInputOperators().get(streamId);
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(isd, streamAppDesc.getInputDescriptors().get(streamId));
    assertEquals(transformer, inputOpSpec.getTransformer());
  }

  @Test
  public void testGetInputStreamWithExpandingSystem() {
    String streamId = "test-stream-1";
    String expandedStreamId = "expanded-stream";
    AtomicInteger expandCallCount = new AtomicInteger();
    StreamExpander expander = (sg, isd) -> {
      expandCallCount.incrementAndGet();
      InputDescriptor expandedISD =
          new GenericSystemDescriptor("expanded-system", "mockFactoryClass")
              .getInputDescriptor(expandedStreamId, new IntegerSerde());

      return sg.getInputStream(expandedISD);
    };
    MockExpandingSystemDescriptor sd = new MockExpandingSystemDescriptor("mock-system", expander);
    MockInputDescriptor isd = sd.getInputDescriptor(streamId, new IntegerSerde());
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(isd);
      }, mock(Config.class));

    InputOperatorSpec inputOpSpec = streamAppDesc.getInputOperators().get(expandedStreamId);
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(1, expandCallCount.get());
    assertFalse(streamAppDesc.getInputOperators().containsKey(streamId));
    assertFalse(streamAppDesc.getInputDescriptors().containsKey(streamId));
    assertTrue(streamAppDesc.getInputDescriptors().containsKey(expandedStreamId));
    assertEquals(expandedStreamId, inputOpSpec.getStreamId());
  }

  @Test
  public void testGetInputStreamWithRelaxedTypes() {
    String streamId = "test-stream-1";
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd = sd.getInputDescriptor(streamId, mock(Serde.class));
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(isd);
      }, mock(Config.class));

    InputOperatorSpec inputOpSpec = streamAppDesc.getInputOperators().get(streamId);
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(isd, streamAppDesc.getInputDescriptors().get(streamId));
  }

  @Test
  public void testMultipleGetInputStreams() {
    String streamId1 = "test-stream-1";
    String streamId2 = "test-stream-2";
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd1 = sd.getInputDescriptor(streamId1, mock(Serde.class));
    GenericInputDescriptor isd2 = sd.getInputDescriptor(streamId2, mock(Serde.class));

    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(isd1);
        appDesc.getInputStream(isd2);
      }, mock(Config.class));

    InputOperatorSpec inputOpSpec1 = streamAppDesc.getInputOperators().get(streamId1);
    InputOperatorSpec inputOpSpec2 = streamAppDesc.getInputOperators().get(streamId2);

    assertEquals(2, streamAppDesc.getInputOperators().size());
    assertEquals(streamId1, inputOpSpec1.getStreamId());
    assertEquals(streamId2, inputOpSpec2.getStreamId());
    assertEquals(2, streamAppDesc.getInputDescriptors().size());
    assertEquals(isd1, streamAppDesc.getInputDescriptors().get(streamId1));
    assertEquals(isd2, streamAppDesc.getInputDescriptors().get(streamId2));
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameInputStreamTwice() {
    String streamId = "test-stream-1";
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd1 = sd.getInputDescriptor(streamId, mock(Serde.class));
    GenericInputDescriptor isd2 = sd.getInputDescriptor(streamId, mock(Serde.class));
    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(isd1);
        // should throw exception
        appDesc.getInputStream(isd2);
      }, mock(Config.class));
  }

  @Test
  public void testMultipleSystemDescriptorForSameSystemName() {
    GenericSystemDescriptor sd1 = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericSystemDescriptor sd2 = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd1 = sd1.getInputDescriptor("test-stream-1", mock(Serde.class));
    GenericInputDescriptor isd2 = sd2.getInputDescriptor("test-stream-2", mock(Serde.class));
    GenericOutputDescriptor osd1 = sd2.getOutputDescriptor("test-stream-3", mock(Serde.class));

    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(isd1);
        try {
          appDesc.getInputStream(isd2);
          fail("Adding input stream with the same system name but different SystemDescriptor should have failed");
        } catch (IllegalStateException e) { }

        try {
          appDesc.getOutputStream(osd1);
          fail("adding output stream with the same system name but different SystemDescriptor should have failed");
        } catch (IllegalStateException e) { }
      }, mock(Config.class));

    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.setDefaultSystem(sd2);
        try {
          appDesc.getInputStream(isd1);
          fail("Adding input stream with the same system name as the default system but different SystemDescriptor should have failed");
        } catch (IllegalStateException e) { }
      }, mock(Config.class));
  }

  @Test
  public void testGetOutputStreamWithKeyValueSerde() {
    String streamId = "test-stream-1";
    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericOutputDescriptor osd = sd.getOutputDescriptor(streamId, mockKVSerde);

    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getOutputStream(osd);
      }, mock(Config.class));

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = streamAppDesc.getOutputStreams().get(streamId);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertEquals(osd, streamAppDesc.getOutputDescriptors().get(streamId));
    assertEquals(mockKeySerde, outputStreamImpl.getKeySerde());
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetOutputStreamWithNullSerde() {
    String streamId = "test-stream-1";
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericOutputDescriptor osd = sd.getOutputDescriptor(streamId, null);
    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getOutputStream(osd);
      }, mock(Config.class));
  }

  @Test
  public void testGetOutputStreamWithValueSerde() {
    String streamId = "test-stream-1";
    Serde mockValueSerde = mock(Serde.class);
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericOutputDescriptor osd = sd.getOutputDescriptor(streamId, mockValueSerde);

    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getOutputStream(osd);
      }, mock(Config.class));

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = streamAppDesc.getOutputStreams().get(streamId);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertEquals(osd, streamAppDesc.getOutputDescriptors().get(streamId));
    assertTrue(outputStreamImpl.getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde());
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSystemDescriptorAfterGettingInputStream() {
    String streamId = "test-stream-1";
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd = sd.getInputDescriptor(streamId, mock(Serde.class));

    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(isd);
        appDesc.setDefaultSystem(sd); // should throw exception
      }, mock(Config.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSystemDescriptorAfterGettingOutputStream() {
    String streamId = "test-stream-1";
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericOutputDescriptor osd = sd.getOutputDescriptor(streamId, mock(Serde.class));
    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getOutputStream(osd);
        appDesc.setDefaultSystem(sd); // should throw exception
      }, mock(Config.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSystemDescriptorAfterGettingIntermediateStream() {
    String streamId = "test-stream-1";
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> { }, mock(Config.class));
    streamAppDesc.getIntermediateStream(streamId, mock(Serde.class), false);
    streamAppDesc.setDefaultSystem(mock(SystemDescriptor.class)); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameOutputStreamTwice() {
    String streamId = "test-stream-1";
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericOutputDescriptor osd1 = sd.getOutputDescriptor(streamId, mock(Serde.class));
    GenericOutputDescriptor osd2 = sd.getOutputDescriptor(streamId, mock(Serde.class));
    new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getOutputStream(osd1);
        appDesc.getOutputStream(osd2); // should throw exception
      }, mock(Config.class));
  }

  @Test
  public void testGetIntermediateStreamWithValueSerde() {
    String streamId = "stream-1";
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> { }, mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        streamAppDesc.getIntermediateStream(streamId, mockValueSerde, false);

    assertEquals(streamAppDesc.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(streamAppDesc.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertTrue(intermediateStreamImpl.getOutputStream().getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde());
    assertTrue(((InputOperatorSpec) (OperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde() instanceof NoOpSerde);
    assertEquals(mockValueSerde, ((InputOperatorSpec) (OperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test
  public void testGetIntermediateStreamWithKeyValueSerde() {
    String streamId = "streamId";
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> { }, mock(Config.class));

    KVSerde mockKVSerde = mock(KVSerde.class);
    Serde mockKeySerde = mock(Serde.class);
    Serde mockValueSerde = mock(Serde.class);
    doReturn(mockKeySerde).when(mockKVSerde).getKeySerde();
    doReturn(mockValueSerde).when(mockKVSerde).getValueSerde();
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        streamAppDesc.getIntermediateStream(streamId, mockKVSerde, false);

    assertEquals(streamAppDesc.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(streamAppDesc.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertEquals(mockKeySerde, intermediateStreamImpl.getOutputStream().getKeySerde());
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde());
    assertEquals(mockKeySerde, ((InputOperatorSpec) (OperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde());
    assertEquals(mockValueSerde, ((InputOperatorSpec) (OperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test
  public void testGetIntermediateStreamWithDefaultSystemDescriptor() {
    Config mockConfig = mock(Config.class);
    String streamId = "streamId";

    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> { }, mockConfig);
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mock-system", "mock-system-factory");
    streamAppDesc.setDefaultSystem(sd);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        streamAppDesc.getIntermediateStream(streamId, mock(Serde.class), false);

    assertEquals(streamAppDesc.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(streamAppDesc.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
  }

  @Test
  public void testGetIntermediateStreamWithNoSerde() {
    Config mockConfig = mock(Config.class);
    String streamId = "streamId";

    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> { }, mockConfig);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        streamAppDesc.getIntermediateStream(streamId, null, false);

    assertEquals(streamAppDesc.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(streamAppDesc.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertNull(intermediateStreamImpl.getOutputStream().getKeySerde());
    assertNull(intermediateStreamImpl.getOutputStream().getValueSerde());
    assertNull(((InputOperatorSpec) (OperatorSpec)  intermediateStreamImpl.getOperatorSpec()).getKeySerde());
    assertNull(((InputOperatorSpec) (OperatorSpec)  intermediateStreamImpl.getOperatorSpec()).getValueSerde());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameIntermediateStreamTwice() {
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> { }, mock(Config.class));
    streamAppDesc.getIntermediateStream("test-stream-1", mock(Serde.class), false);
    // should throw exception
    streamAppDesc.getIntermediateStream("test-stream-1", mock(Serde.class), false);
  }

  @Test
  public void testGetNextOpIdIncrementsId() {
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> { }, mockConfig);
    assertEquals("jobName-1234-merge-0", streamAppDesc.getNextOpId(OpCode.MERGE, null));
    assertEquals("jobName-1234-join-customName", streamAppDesc.getNextOpId(OpCode.JOIN, "customName"));
    assertEquals("jobName-1234-map-2", streamAppDesc.getNextOpId(OpCode.MAP, null));
  }

  @Test(expected = SamzaException.class)
  public void testGetNextOpIdRejectsDuplicates() {
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> { }, mockConfig);
    assertEquals("jobName-1234-join-customName", streamAppDesc.getNextOpId(OpCode.JOIN, "customName"));
    streamAppDesc.getNextOpId(OpCode.JOIN, "customName"); // should throw
  }

  @Test
  public void testOpIdValidation() {
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(eq(JobConfig.JOB_NAME()))).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("1234");

    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> { }, mockConfig);

    // null and empty userDefinedIDs should fall back to autogenerated IDs.
    try {
      streamAppDesc.getNextOpId(OpCode.FILTER, null);
      streamAppDesc.getNextOpId(OpCode.FILTER, "");
      streamAppDesc.getNextOpId(OpCode.FILTER, " ");
      streamAppDesc.getNextOpId(OpCode.FILTER, "\t");
    } catch (SamzaException e) {
      fail("Received an error with a null or empty operator ID instead of defaulting to auto-generated ID.");
    }

    List<String> validOpIds = ImmutableList.of("op_id", "op-id", "1000", "op_1", "OP_ID");
    for (String validOpId: validOpIds) {
      try {
        streamAppDesc.getNextOpId(OpCode.FILTER, validOpId);
      } catch (Exception e) {
        fail("Received an exception with a valid operator ID: " + validOpId);
      }
    }

    List<String> invalidOpIds = ImmutableList.of("op id", "op#id");
    for (String invalidOpId: invalidOpIds) {
      try {
        streamAppDesc.getNextOpId(OpCode.FILTER, invalidOpId);
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

    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getInputStream(sd.getInputDescriptor(testStreamId1, mock(Serde.class)));
        appDesc.getInputStream(sd.getInputDescriptor(testStreamId2, mock(Serde.class)));
        appDesc.getInputStream(sd.getInputDescriptor(testStreamId3, mock(Serde.class)));
      }, mockConfig);

    List<InputOperatorSpec> inputSpecs = new ArrayList<>(streamAppDesc.getInputOperators().values());
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
    when(mockTableDescriptor.getTableId()).thenReturn(testTableSpec.getId());
    StreamAppDescriptorImpl streamAppDesc = new StreamAppDescriptorImpl(appDesc -> {
        appDesc.getTable(mockTableDescriptor);
      }, mockConfig);
    assertNotNull(streamAppDesc.getTables().get(testTableSpec));
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

  @Test(expected = IllegalStateException.class)
  public void testGetTableWithBadId() {
    Config mockConfig = mock(Config.class);
    new StreamAppDescriptorImpl(appDesc -> {
        BaseTableDescriptor mockTableDescriptor = mock(BaseTableDescriptor.class);
        when(mockTableDescriptor.getTableId()).thenReturn("my.table");
        appDesc.getTable(mockTableDescriptor);
      }, mockConfig);
  }

  class MockExpandingSystemDescriptor extends SystemDescriptor<MockExpandingSystemDescriptor> implements ExpandingInputDescriptorProvider<Integer> {
    public MockExpandingSystemDescriptor(String systemName, StreamExpander expander) {
      super(systemName, "factory.class", null, expander);
    }

    @Override
    public MockInputDescriptor<Integer> getInputDescriptor(String streamId, Serde serde) {
      return new MockInputDescriptor<>(streamId, this, serde);
    }
  }

  class MockTransformingSystemDescriptor extends SystemDescriptor<MockTransformingSystemDescriptor> implements TransformingInputDescriptorProvider<Integer> {
    public MockTransformingSystemDescriptor(String systemName, InputTransformer transformer) {
      super(systemName, "factory.class", transformer, null);
    }

    @Override
    public MockInputDescriptor<Integer> getInputDescriptor(String streamId, Serde serde) {
      return new MockInputDescriptor<>(streamId, this, serde);
    }
  }

  public class MockInputDescriptor<StreamMessageType> extends InputDescriptor<StreamMessageType, MockInputDescriptor<StreamMessageType>> {
    MockInputDescriptor(String streamId, SystemDescriptor systemDescriptor, Serde serde) {
      super(streamId, serde, systemDescriptor, null);
    }
  }
}
