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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.GenericOutputDescriptor;
import org.apache.samza.operators.descriptors.GenericSystemDescriptor;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.operators.descriptors.base.system.ExpandingSystemDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.operators.functions.StreamExpander;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.TableSpec;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestStreamGraphSpec {

  @Test
  public void testGetInputStreamWithValueSerde() {
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    String streamId = "test-stream-1";
    Serde mockValueSerde = mock(Serde.class);
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd = sd.getInputDescriptor(streamId, mockValueSerde);
    MessageStream<TestMessageEnvelope> inputStream = graphSpec.getInputStream(isd);

    InputOperatorSpec inputOpSpec = (InputOperatorSpec) ((MessageStreamImpl) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graphSpec.getInputOperators().get(streamId), inputOpSpec);
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(isd, graphSpec.getInputDescriptors().get(streamId));
    assertTrue(inputOpSpec.getKeySerde().get() instanceof NoOpSerde);
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde().get());
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
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd = sd.getInputDescriptor(streamId, mockKVSerde);
    MessageStream<TestMessageEnvelope> inputStream = graphSpec.getInputStream(isd);

    InputOperatorSpec inputOpSpec = (InputOperatorSpec) ((MessageStreamImpl) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graphSpec.getInputOperators().get(streamId), inputOpSpec);
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(isd, graphSpec.getInputDescriptors().get(streamId));
    assertEquals(mockKeySerde, inputOpSpec.getKeySerde().get());
    assertEquals(mockValueSerde, inputOpSpec.getValueSerde().get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetInputStreamWithNullSerde() {
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd = sd.getInputDescriptor("mockStreamId", null);
    graphSpec.getInputStream(isd);
  }

  @Test
  public void testGetInputStreamWithTransformFunction() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockFactory");
    InputTransformer transformer = ime -> ime;
    GenericInputDescriptor isd = sd.getInputDescriptor(streamId, transformer, mockValueSerde);
    MessageStream inputStream = graphSpec.getInputStream(isd);

    InputOperatorSpec inputOpSpec = (InputOperatorSpec) ((MessageStreamImpl) inputStream).getOperatorSpec();
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(graphSpec.getInputOperators().get(streamId), inputOpSpec);
    assertEquals(streamId, inputOpSpec.getStreamId());
    assertEquals(isd, graphSpec.getInputDescriptors().get(streamId));
    assertEquals(transformer, inputOpSpec.getTransformer().get());
  }

  @Test
  public void testGetInputStreamWithExpandingSystem() {
    String streamId = "test-stream-1";
    String expandedStreamId = "expanded-stream";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    AtomicInteger expandCallCount = new AtomicInteger();
    StreamExpander expander = (sg, isd) -> {
      expandCallCount.incrementAndGet();
      InputDescriptor expandedISD =
          new GenericSystemDescriptor("expanded-system", "mockFactoryClass")
              .getInputDescriptor(expandedStreamId, new IntegerSerde());

      return sg.getInputStream(expandedISD);
    };
    MockExpandingSystemDescriptor sd = new MockExpandingSystemDescriptor("mock-system", expander);
    MockExpandingInputDescriptor isd = sd.getInputDescriptor(streamId, new IntegerSerde());
    MessageStream inputStream = graphSpec.getInputStream(isd);
    InputOperatorSpec inputOpSpec = (InputOperatorSpec) ((MessageStreamImpl) inputStream).getOperatorSpec();
    assertEquals(1, expandCallCount.get());
    assertEquals(OpCode.INPUT, inputOpSpec.getOpCode());
    assertEquals(inputOpSpec, graphSpec.getInputOperators().get(expandedStreamId));
    assertFalse(graphSpec.getInputOperators().containsKey(streamId));
    assertFalse(graphSpec.getInputDescriptors().containsKey(streamId));
    assertTrue(graphSpec.getInputDescriptors().containsKey(expandedStreamId));
    assertEquals(expandedStreamId, inputOpSpec.getStreamId());
  }

  @Test
  public void testMultipleGetInputStreams() {
    String streamId1 = "test-stream-1";
    String streamId2 = "test-stream-2";

    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd1 = sd.getInputDescriptor(streamId1, mock(Serde.class));
    GenericInputDescriptor isd2 = sd.getInputDescriptor(streamId2, mock(Serde.class));
    MessageStream<Object> inputStream1 = graphSpec.getInputStream(isd1);
    MessageStream<Object> inputStream2 = graphSpec.getInputStream(isd2);

    InputOperatorSpec inputOpSpec1 =
        (InputOperatorSpec) ((MessageStreamImpl<Object>) inputStream1).getOperatorSpec();
    InputOperatorSpec inputOpSpec2 =
        (InputOperatorSpec) ((MessageStreamImpl<Object>) inputStream2).getOperatorSpec();

    assertEquals(graphSpec.getInputOperators().size(), 2);
    assertEquals(graphSpec.getInputOperators().get(streamId1), inputOpSpec1);
    assertEquals(graphSpec.getInputOperators().get(streamId2), inputOpSpec2);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameInputStreamTwice() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericInputDescriptor isd1 = sd.getInputDescriptor(streamId, mock(Serde.class));
    GenericInputDescriptor isd2 = sd.getInputDescriptor(streamId, mock(Serde.class));
    graphSpec.getInputStream(isd1);
    // should throw exception
    graphSpec.getInputStream(isd2);
  }

  @Test
  public void testGetOutputStreamWithValueSerde() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericOutputDescriptor osd = sd.getOutputDescriptor(streamId, mockValueSerde);
    OutputStream<TestMessageEnvelope> outputStream = graphSpec.getOutputStream(osd);

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graphSpec.getOutputStreams().get(streamId), outputStreamImpl);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertEquals(osd, graphSpec.getOutputDescriptors().get(streamId));
    assertTrue(outputStreamImpl.getKeySerde().get() instanceof NoOpSerde);
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde().get());
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
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericOutputDescriptor osd = sd.getOutputDescriptor(streamId, mockKVSerde);
    OutputStream<TestMessageEnvelope> outputStream = graphSpec.getOutputStream(osd);

    OutputStreamImpl<TestMessageEnvelope> outputStreamImpl = (OutputStreamImpl) outputStream;
    assertEquals(graphSpec.getOutputStreams().get(streamId), outputStreamImpl);
    assertEquals(streamId, outputStreamImpl.getStreamId());
    assertEquals(osd, graphSpec.getOutputDescriptors().get(streamId));
    assertEquals(mockKeySerde, outputStreamImpl.getKeySerde().get());
    assertEquals(mockValueSerde, outputStreamImpl.getValueSerde().get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetOutputStreamWithNullSerde() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericOutputDescriptor osd = sd.getOutputDescriptor(streamId, null);
    graphSpec.getOutputStream(osd);
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSystemDescriptorAfterGettingInputStream() {
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    GenericInputDescriptor id = new GenericSystemDescriptor("system", "factory.class.name")
        .getInputDescriptor("input-stream", mock(Serde.class));
    graphSpec.getInputStream(id);
    graphSpec.setDefaultSystem(new GenericSystemDescriptor("mockSystem", "mockFactory")); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSystemDescriptorAfterGettingOutputStream() {
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    GenericOutputDescriptor od = new GenericSystemDescriptor("system", "factory.class.name")
        .getOutputDescriptor("output-stream", mock(Serde.class));
    graphSpec.getOutputStream(od);
    graphSpec.setDefaultSystem(new GenericSystemDescriptor("mockSystem", "mockFactory")); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testSetDefaultSerdeAfterGettingIntermediateStream() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    graphSpec.getIntermediateStream(streamId, mock(Serde.class), false);
    graphSpec.setDefaultSystem(new GenericSystemDescriptor("mockSystem", "mockFactory")); // should throw exception
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameOutputStreamTwice() {
    String streamId = "test-stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    GenericOutputDescriptor osd1 = sd.getOutputDescriptor(streamId, mock(Serde.class));
    GenericOutputDescriptor osd2 = sd.getOutputDescriptor(streamId, mock(Serde.class));
    graphSpec.getOutputStream(osd1);
    graphSpec.getOutputStream(osd2); // should throw exception
  }

  @Test
  public void testGetIntermediateStreamWithValueSerde() {
    String streamId = "stream-1";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    Serde mockValueSerde = mock(Serde.class);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graphSpec.getIntermediateStream(streamId, mockValueSerde, false);

    assertEquals(graphSpec.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graphSpec.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertTrue(intermediateStreamImpl.getOutputStream().getKeySerde().get() instanceof NoOpSerde);
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde().get());
    assertTrue(((InputOperatorSpec) (OperatorSpec) intermediateStreamImpl.getOperatorSpec()).getKeySerde().get() instanceof NoOpSerde);
    assertEquals(mockValueSerde, ((InputOperatorSpec) (OperatorSpec) intermediateStreamImpl.getOperatorSpec()).getValueSerde().get());
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
        graphSpec.getIntermediateStream(streamId, mockKVSerde, false);

    assertEquals(graphSpec.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graphSpec.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertEquals(mockKeySerde, intermediateStreamImpl.getOutputStream().getKeySerde().get());
    assertEquals(mockValueSerde, intermediateStreamImpl.getOutputStream().getValueSerde().get());
    assertEquals(mockKeySerde, ((InputOperatorSpec) (OperatorSpec)  intermediateStreamImpl.getOperatorSpec()).getKeySerde().get());
    assertEquals(mockValueSerde, ((InputOperatorSpec) (OperatorSpec)  intermediateStreamImpl.getOperatorSpec()).getValueSerde().get());
  }

  @Test
  public void testGetIntermediateStreamWithNoSerde() {
    Config mockConfig = mock(Config.class);
    String streamId = "streamId";

    StreamGraphSpec graphSpec = new StreamGraphSpec(mockConfig);
    IntermediateMessageStreamImpl<TestMessageEnvelope> intermediateStreamImpl =
        graphSpec.getIntermediateStream(streamId, null, false);

    assertEquals(graphSpec.getInputOperators().get(streamId), intermediateStreamImpl.getOperatorSpec());
    assertEquals(graphSpec.getOutputStreams().get(streamId), intermediateStreamImpl.getOutputStream());
    assertEquals(streamId, intermediateStreamImpl.getStreamId());
    assertFalse(intermediateStreamImpl.getOutputStream().getKeySerde().isPresent());
    assertFalse(intermediateStreamImpl.getOutputStream().getValueSerde().isPresent());
    assertFalse(((InputOperatorSpec) (OperatorSpec)  intermediateStreamImpl.getOperatorSpec()).getKeySerde().isPresent());
    assertFalse(((InputOperatorSpec) (OperatorSpec)  intermediateStreamImpl.getOperatorSpec()).getValueSerde().isPresent());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSameIntermediateStreamTwice() {
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    graphSpec.getIntermediateStream("test-stream-1", mock(Serde.class), false);
    graphSpec.getIntermediateStream("test-stream-1", mock(Serde.class), false);
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
  public void testIdValidation() {
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
    GenericSystemDescriptor sd = new GenericSystemDescriptor("mockSystem", "mockSystemFactoryClass");
    graphSpec.getInputStream(sd.getInputDescriptor(testStreamId1, mock(Serde.class)));
    graphSpec.getInputStream(sd.getInputDescriptor(testStreamId2, mock(Serde.class)));
    graphSpec.getInputStream(sd.getInputDescriptor(testStreamId3, mock(Serde.class)));

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

  class MockExpandingSystemDescriptor extends ExpandingSystemDescriptor<Integer, MockExpandingSystemDescriptor> {
    public MockExpandingSystemDescriptor(String systemName, StreamExpander expander) {
      super(systemName, "factory.class", null, expander);
    }

    @Override
    public MockExpandingInputDescriptor<Integer> getInputDescriptor(String streamId, Serde serde) {
      return new MockExpandingInputDescriptor<>(streamId, this, serde);
    }

    @Override
    public MockExpandingInputDescriptor<Integer> getInputDescriptor(String streamId, InputTransformer transformer, Serde serde) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <StreamMessageType> OutputDescriptor<StreamMessageType, ? extends OutputDescriptor> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde) {
      throw new UnsupportedOperationException();
    }
  }

  public class MockExpandingInputDescriptor<StreamMessageType> extends InputDescriptor<StreamMessageType, org.apache.samza.operators.descriptors.expanding.MockExpandingInputDescriptor<StreamMessageType>> {
    MockExpandingInputDescriptor(String streamId, SystemDescriptor systemDescriptor, Serde serde) {
      super(streamId, systemDescriptor.getSystemName(), serde, systemDescriptor, null);
    }
  }
}
