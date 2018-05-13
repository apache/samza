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

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpecTestUtils;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.serializers.NoOpSerde;
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
 * Unit tests for {@link OperatorSpecGraph}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(OperatorSpec.class)
public class TestOperatorSpecGraph {

  private OperatorSpecGraphBuilder mockGraph;
  private Map<StreamSpec, InputOperatorSpec> inputOpSpecMap;
  private Map<StreamSpec, OutputStreamImpl> outputStrmMap;
  private Set<OperatorSpec> allOpSpecs;

  @Before
  public void setUp() {
    this.mockGraph = mock(OperatorSpecGraphBuilder.class);

    /**
     * Setup two linear transformation pipelines:
     * 1) input1 --> filter --> sendTo
     * 2) input2 --> map --> sink
     */
    StreamSpec testInputSpec = new StreamSpec("test-input-1", "test-input-1", "kafka");
    InputOperatorSpec testInput = new InputOperatorSpec(testInputSpec, new NoOpSerde(), new NoOpSerde(), true, "test-input-1");
    StreamOperatorSpec filterOp = OperatorSpecs.createFilterOperatorSpec(m -> true, "test-filter-2");
    StreamSpec testOutputSpec = new StreamSpec("test-output-1", "test-output-1", "kafka");
    OutputStreamImpl outputStream1 = new OutputStreamImpl(testOutputSpec, null, null, true);
    OutputOperatorSpec outputSpec = OperatorSpecs.createSendToOperatorSpec(outputStream1, "test-output-3");
    testInput.registerNextOperatorSpec(filterOp);
    filterOp.registerNextOperatorSpec(outputSpec);
    StreamSpec testInputSpec2 = new StreamSpec("test-input-2", "test-input-2", "kafka");
    InputOperatorSpec testInput2 = new InputOperatorSpec(testInputSpec2, new NoOpSerde(), new NoOpSerde(), true, "test-input-4");
    StreamOperatorSpec testMap = OperatorSpecs.createMapOperatorSpec(m -> m, "test-map-5");
    SinkOperatorSpec testSink = OperatorSpecs.createSinkOperatorSpec((m, mc, tc) -> { }, "test-sink-6");
    testInput2.registerNextOperatorSpec(testMap);
    testMap.registerNextOperatorSpec(testSink);

    this.inputOpSpecMap = new LinkedHashMap<>();
    inputOpSpecMap.put(testInputSpec, testInput);
    inputOpSpecMap.put(testInputSpec2, testInput2);
    this.outputStrmMap = new LinkedHashMap<>();
    outputStrmMap.put(testOutputSpec, outputStream1);
    when(mockGraph.getInputOperators()).thenReturn(Collections.unmodifiableMap(inputOpSpecMap));
    when(mockGraph.getOutputStreams()).thenReturn(Collections.unmodifiableMap(outputStrmMap));
    this.allOpSpecs = new HashSet<OperatorSpec>() { {
        this.add(testInput);
        this.add(filterOp);
        this.add(outputSpec);
        this.add(testInput2);
        this.add(testMap);
        this.add(testSink);
      } };
  }

  @After
  public void tearDown() {
    this.mockGraph = null;
    this.inputOpSpecMap = null;
    this.outputStrmMap = null;
    this.allOpSpecs = null;
  }

  @Test
  public void testConstructor() {
    OperatorSpecGraph specGraph = new OperatorSpecGraph(mockGraph);
    assertEquals(specGraph.getInputOperators(), inputOpSpecMap);
    assertEquals(specGraph.getOutputStreams(), outputStrmMap);
    assertTrue(specGraph.getTables().isEmpty());
    assertTrue(!specGraph.hasWindowOrJoins());
    assertEquals(specGraph.getAllOperatorSpecs(), this.allOpSpecs);
  }

  @Test
  public void testClone() {
    OperatorSpecGraph operatorSpecGraph = new OperatorSpecGraph(mockGraph);
    OperatorSpecGraph clonedSpecGraph = operatorSpecGraph.clone();
    OperatorSpecTestUtils.assertClonedGraph(operatorSpecGraph, clonedSpecGraph);
  }

  @Test(expected = NotSerializableException.class)
  public void testCloneWithSerializationError() throws Throwable {
    OperatorSpec mockFailedOpSpec = PowerMockito.mock(OperatorSpec.class);
    when(mockFailedOpSpec.getOpId()).thenReturn("test-failed-op-4");
    allOpSpecs.add(mockFailedOpSpec);
    inputOpSpecMap.values().stream().findFirst().get().registerNextOperatorSpec(mockFailedOpSpec);

    //failed with serialization error
    try {
      new OperatorSpecGraph(mockGraph);
      fail("Should have failed with serialization error");
    } catch (SamzaException se) {
      throw se.getCause();
    }
  }

  @Test(expected = IOException.class)
  public void testCloneWithDeserializationError() throws Throwable {
    TestDeserializeOperatorSpec testOp = new TestDeserializeOperatorSpec(OperatorSpec.OpCode.MAP, "test-failed-op-4");
    this.allOpSpecs.add(testOp);
    inputOpSpecMap.values().stream().findFirst().get().registerNextOperatorSpec(testOp);

    OperatorSpecGraph operatorSpecGraph = new OperatorSpecGraph(mockGraph);
    //failed with serialization error
    try {
      operatorSpecGraph.clone();
      fail("Should have failed with serialization error");
    } catch (SamzaException se) {
      throw se.getCause();
    }
  }

  private static class TestDeserializeOperatorSpec extends OperatorSpec {

    public TestDeserializeOperatorSpec(OpCode opCode, String opId) {
      super(opCode, opId);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
      throw new IOException("Raise IOException to cause deserialization failure");
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

}
