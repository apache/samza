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
package org.apache.samza.operators.impl;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.system.StreamSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link SerializedStreamGraph}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(OperatorSpec.class)
public class TestSerializedStreamGraph {

  private StreamGraphImpl mockGraph;
  private ContextManager mockContextManager;
  private Map<StreamSpec, InputOperatorSpec> inputOpSpecMap;
  private Map<StreamSpec, OutputStreamImpl> outputStrmMap;
  private Collection<OperatorSpec> allOpSpecs;

  @Before
  public void setUp() {
    this.mockGraph = mock(StreamGraphImpl.class);
    this.mockContextManager = mock(ContextManager.class);

    StreamSpec mockInput1 = mock(StreamSpec.class);
    InputOperatorSpec testInput = new InputOperatorSpec(mockInput1, null, null, true, "test-input-1");
    TestOperatorSpec testOp2 = new TestOperatorSpec(OperatorSpec.OpCode.FILTER, "test-filter-2");
    StreamSpec mockOutput1 = mock(StreamSpec.class);
    OutputStreamImpl outputStream1 = new OutputStreamImpl(mockOutput1, null, null, true);
    OutputOperatorSpec outputSpec = OperatorSpecs.createSendToOperatorSpec(outputStream1, "test-output-3");

    this.inputOpSpecMap = new HashMap<>();
    inputOpSpecMap.put(mockInput1, testInput);
    this.outputStrmMap = new HashMap<>();
    outputStrmMap.put(mockOutput1, outputStream1);
    this.allOpSpecs = new HashSet<>();
    allOpSpecs.add(testInput);
    allOpSpecs.add(testOp2);
    allOpSpecs.add(outputSpec);
    when(mockGraph.getContextManager()).thenReturn(mockContextManager);
    when(mockGraph.getInputOperators()).thenReturn(inputOpSpecMap);
    when(mockGraph.getOutputStreams()).thenReturn(outputStrmMap);
    when(mockGraph.getAllOperatorSpecs()).thenReturn(allOpSpecs);
  }

  @After
  public void tearDown() {
    this.mockGraph = null;
    this.mockContextManager = null;
    this.inputOpSpecMap = null;
    this.outputStrmMap = null;
    this.allOpSpecs = null;
  }

  @Test
  public void testConstructor() {
    SerializedStreamGraph serializedGraph = new SerializedStreamGraph(mockGraph);
    assertEquals(serializedGraph.getContextManager(), mockContextManager);
    assertEquals(serializedGraph.getInputOperators(), inputOpSpecMap);
    assertEquals(serializedGraph.getOutputStreams(), outputStrmMap);
  }

  @Test
  public void testGetOpSpec() {
    SerializedStreamGraph serializedStreamGraph = new SerializedStreamGraph(mockGraph);
    // get all deserialized operator spec
    this.allOpSpecs.stream().forEach(op -> {
        try {
          OperatorSpec deserializedOp = serializedStreamGraph.getOpSpec(op.getOpId());
          assertTrue(deserializedOp != op);
          assertTrue(deserializedOp.getOpCode() == op.getOpCode());
          assertEquals(deserializedOp.getOpId(), op.getOpId());
        } catch (IOException | ClassNotFoundException e) {
          throw new RuntimeException(String.format("Test failed in serialize/deserialize operator %s.", op.getOpId()), e);
        }
      });
  }

  @Test
  public void testGetOpSpecWithSerializationError() throws IOException, ClassNotFoundException {
    OperatorSpec mockFailedOpSpec = PowerMockito.mock(OperatorSpec.class);
    when(mockFailedOpSpec.getOpId()).thenReturn("test-failed-op-4");
    this.allOpSpecs.add(mockFailedOpSpec);
    when(this.mockGraph.getAllOperatorSpecs()).thenReturn(this.allOpSpecs);

    SerializedStreamGraph serializedStreamGraph = new SerializedStreamGraph(mockGraph);
    // get deserialized operator spec failed
    try {
      serializedStreamGraph.getOpSpec("test-failed-op-4");
      fail("Should have failed with serialization exception");
    } catch (SamzaException nse) {
      // expected, continue
      assertTrue(nse.getCause() instanceof NotSerializableException);
    }
  }

  @Test
  public void testGetOpSpecWithDeserializationError() throws IOException, ClassNotFoundException {
    TestDeserializeOperatorSpec spyTestOp = PowerMockito.spy(new TestDeserializeOperatorSpec(OperatorSpec.OpCode.MAP, "test-failed-op-4"));
    doThrow(new IOException()).when(spyTestOp).readObject(any());
    this.allOpSpecs.add(spyTestOp);
    when(this.mockGraph.getAllOperatorSpecs()).thenReturn(this.allOpSpecs);

    SerializedStreamGraph serializedStreamGraph = new SerializedStreamGraph(mockGraph);
    // get deserialized operator spec failed
    try {
      serializedStreamGraph.getOpSpec("test-failed-op-4");
      fail("Should have failed with deserialization exception");
    } catch (SamzaException nse) {
      // expected, continue
      assertTrue(nse.getCause() instanceof IOException);
    }
  }

  private static class TestOperatorSpec extends OperatorSpec {

    public TestOperatorSpec(OpCode opCode, String opId) {
      super(opCode, opId);
    }

    @Override
    public WatermarkFunction getWatermarkFn() {
      return null;
    }

    @Override
    public TimerFunction getTimerFn() {
      return null;
    }
  }

  private static class TestDeserializeOperatorSpec extends TestOperatorSpec {

    public TestDeserializeOperatorSpec(OpCode opCode, String opId) {
      super(opCode, opId);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
      ois.readObject();
    }
  }

}
