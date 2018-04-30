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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.table.TableSpec;
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

  private StreamGraphImpl mockGraph;
  private Map<StreamSpec, InputOperatorSpec> inputOpSpecMap;
  private Map<StreamSpec, OutputStreamImpl> outputStrmMap;
  private Set<OperatorSpec> allOpSpecs;

  @Before
  public void setUp() {
    this.mockGraph = mock(StreamGraphImpl.class);

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
    assertClonedGraph(operatorSpecGraph, clonedSpecGraph);
  }

  private void assertClonedGraph(OperatorSpecGraph originalGraph, OperatorSpecGraph clonedGraph) {
    assertClonedInputs(originalGraph.getInputOperators(), clonedGraph.getInputOperators());
    assertClonedOutputs(originalGraph.getOutputStreams(), clonedGraph.getOutputStreams());
    assertClonedTables(originalGraph.getTables(), clonedGraph.getTables());
    assertAllOperators(originalGraph.getAllOperatorSpecs(), clonedGraph.getAllOperatorSpecs());
  }

  private void assertAllOperators(Collection<OperatorSpec> originalOpSpecs, Collection<OperatorSpec> clonedOpSpecs) {
    assertEquals(originalOpSpecs.size(), clonedOpSpecs.size());
    List<OperatorSpec> originalList = new ArrayList<>(originalOpSpecs);
    List<OperatorSpec> clonedList = new ArrayList<>(clonedOpSpecs);
    Collections.sort(originalList, Comparator.comparing(OperatorSpec::getOpId));
    Collections.sort(clonedList, Comparator.comparing(OperatorSpec::getOpId));
    Iterator<OperatorSpec> oIter = originalList.iterator();
    Iterator<OperatorSpec> nIter = clonedList.iterator();
    oIter.forEachRemaining(opSpec -> assertClonedOpSpec(opSpec, nIter.next()));
  }

  private void assertClonedOpSpec(OperatorSpec oOpSpec, OperatorSpec nOpSpec) {
    assertNotEquals(oOpSpec, nOpSpec);
    assertEquals(oOpSpec.getOpId(), nOpSpec.getOpId());
    assertEquals(oOpSpec.getOpCode(), nOpSpec.getOpCode());
    assertTimerFnsNotEqual(oOpSpec.getTimerFn(), nOpSpec.getTimerFn());
    assertWatermarkFnNotEqual(nOpSpec.getWatermarkFn(), nOpSpec.getWatermarkFn());
    assertAllOperators(oOpSpec.getRegisteredOperatorSpecs(), nOpSpec.getRegisteredOperatorSpecs());
  }

  private void assertWatermarkFnNotEqual(WatermarkFunction watermarkFn, WatermarkFunction watermarkFn1) {
    if (watermarkFn == watermarkFn1 && watermarkFn == null) {
      return;
    }
    assertNotEquals(watermarkFn, watermarkFn1);
  }

  private void assertTimerFnsNotEqual(TimerFunction timerFn, TimerFunction timerFn1) {
    if (timerFn == timerFn1 && timerFn == null) {
      return;
    }
    assertNotEquals(timerFn, timerFn1);
  }

  private void assertClonedTables(Map<TableSpec, TableImpl> originalTables, Map<TableSpec, TableImpl> clonedTables) {
    assertEquals(originalTables.size(), clonedTables.size());
    assertEquals(originalTables.keySet(), clonedTables.keySet());
    Iterator<TableImpl> oIter = originalTables.values().iterator();
    Iterator<TableImpl> nIter = clonedTables.values().iterator();
    oIter.forEachRemaining(oTable -> assertClonedTableImpl(oTable, nIter.next()));
  }

  private void assertClonedTableImpl(TableImpl oTable, TableImpl nTable) {
    assertNotEquals(oTable, nTable);
    assertEquals(oTable.getTableSpec(), nTable.getTableSpec());
  }

  private void assertClonedOutputs(Map<StreamSpec, OutputStreamImpl> originalOutputs,
      Map<StreamSpec, OutputStreamImpl> clonedOutputs) {
    assertEquals(originalOutputs.size(), clonedOutputs.size());
    assertEquals(originalOutputs.keySet(), clonedOutputs.keySet());
    Iterator<OutputStreamImpl> oIter = originalOutputs.values().iterator();
    Iterator<OutputStreamImpl> nIter = clonedOutputs.values().iterator();
    oIter.forEachRemaining(oOutput -> assertClonedOutputImpl(oOutput, nIter.next()));
  }

  private void assertClonedOutputImpl(OutputStreamImpl oOutput, OutputStreamImpl nOutput) {
    assertNotEquals(oOutput, nOutput);
    assertEquals(oOutput.isKeyed(), nOutput.isKeyed());
    assertEquals(oOutput.getSystemStream(), nOutput.getSystemStream());
    assertEquals(oOutput.getStreamSpec(), nOutput.getStreamSpec());
  }

  private void assertClonedInputs(Map<StreamSpec, InputOperatorSpec> originalInputs,
      Map<StreamSpec, InputOperatorSpec> clonedInputs) {
    assertEquals(originalInputs.size(), clonedInputs.size());
    assertEquals(originalInputs.keySet(), clonedInputs.keySet());
    Iterator<InputOperatorSpec> oIter = originalInputs.values().iterator();
    Iterator<InputOperatorSpec> nIter = clonedInputs.values().iterator();
    oIter.forEachRemaining(inputOp -> assertClonedInputOp(inputOp, nIter.next()));
  }

  private void assertClonedInputOp(InputOperatorSpec originalInput, InputOperatorSpec clonedInput) {
    assertNotEquals(originalInput, clonedInput);
    assertEquals(originalInput.getOpId(), clonedInput.getOpId());
    assertEquals(originalInput.getOpCode(), clonedInput.getOpCode());
    assertEquals(originalInput.getStreamSpec(), clonedInput.getStreamSpec());
    assertEquals(originalInput.isKeyed(), clonedInput.isKeyed());
  }

  @Test(expected = NotSerializableException.class)
  public void testCloneWithSerializationError() throws Throwable {
    OperatorSpec mockFailedOpSpec = PowerMockito.mock(OperatorSpec.class);
    when(mockFailedOpSpec.getOpId()).thenReturn("test-failed-op-4");
    allOpSpecs.add(mockFailedOpSpec);
    inputOpSpecMap.values().stream().findFirst().get().registerNextOperatorSpec(mockFailedOpSpec);

    OperatorSpecGraph operatorSpecGraph = new OperatorSpecGraph(mockGraph);
    //failed with serialization error
    try {
      operatorSpecGraph.clone();
      fail("Should have failed with serialization error");
    } catch (SamzaException se) {
      throw se.getCause();
    }
  }

  @Test(expected = IOException.class)
  public void testCloneWithDeserializationError() throws Throwable {
    TestDeserializeOperatorSpec spyTestOp = PowerMockito.spy(new TestDeserializeOperatorSpec(OperatorSpec.OpCode.MAP, "test-failed-op-4"));
    doThrow(new IOException()).when(spyTestOp).readObject(any());
    this.allOpSpecs.add(spyTestOp);
    inputOpSpecMap.values().stream().findFirst().get().registerNextOperatorSpec(spyTestOp);

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
      ois.readObject();
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
