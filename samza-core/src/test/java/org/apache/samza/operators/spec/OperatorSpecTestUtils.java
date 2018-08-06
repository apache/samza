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

package org.apache.samza.operators.spec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.table.TableSpec;

import static org.junit.Assert.*;


/**
 * Test util methods for {@link OperatorSpec} classes
 */
public class OperatorSpecTestUtils {
  private final static SerializableSerde<OperatorSpec> SPEC_SERDE = new SerializableSerde<>();

  static OperatorSpec copyOpSpec(OperatorSpec original) {
    return SPEC_SERDE.fromBytes(SPEC_SERDE.toBytes(original));
  }

  enum TestEnum {
    One, Two, Three
  }

  public static void assertClonedGraph(OperatorSpecGraph originalGraph, OperatorSpecGraph clonedGraph) {
    assertClonedInputs(originalGraph.getInputOperators(), clonedGraph.getInputOperators());
    assertClonedOutputs(originalGraph.getOutputStreams(), clonedGraph.getOutputStreams());
    assertClonedTables(originalGraph.getTables(), clonedGraph.getTables());
    assertAllOperators(originalGraph.getAllOperatorSpecs(), clonedGraph.getAllOperatorSpecs());
  }

  private static void assertAllOperators(Collection<OperatorSpec> originalOpSpecs, Collection<OperatorSpec> clonedOpSpecs) {
    assertEquals(originalOpSpecs.size(), clonedOpSpecs.size());
    List<OperatorSpec> originalList = new ArrayList<>(originalOpSpecs);
    List<OperatorSpec> clonedList = new ArrayList<>(clonedOpSpecs);
    Collections.sort(originalList, Comparator.comparing(OperatorSpec::getOpId));
    Collections.sort(clonedList, Comparator.comparing(OperatorSpec::getOpId));
    Iterator<OperatorSpec> oIter = originalList.iterator();
    Iterator<OperatorSpec> nIter = clonedList.iterator();
    oIter.forEachRemaining(opSpec -> assertClonedOpSpec(opSpec, nIter.next()));
  }

  private static void assertClonedOpSpec(OperatorSpec oOpSpec, OperatorSpec nOpSpec) {
    assertNotEquals(oOpSpec, nOpSpec);
    assertEquals(oOpSpec.getOpId(), nOpSpec.getOpId());
    assertEquals(oOpSpec.getOpCode(), nOpSpec.getOpCode());
    assertTimerFnsNotEqual(oOpSpec.getTimerFn(), nOpSpec.getTimerFn());
    assertWatermarkFnNotEqual(nOpSpec.getWatermarkFn(), nOpSpec.getWatermarkFn());
    assertAllOperators(oOpSpec.getRegisteredOperatorSpecs(), nOpSpec.getRegisteredOperatorSpecs());
  }

  private static void assertWatermarkFnNotEqual(WatermarkFunction watermarkFn, WatermarkFunction watermarkFn1) {
    if (watermarkFn == watermarkFn1 && watermarkFn == null) {
      return;
    }
    assertNotEquals(watermarkFn, watermarkFn1);
  }

  private static void assertTimerFnsNotEqual(TimerFunction timerFn, TimerFunction timerFn1) {
    if (timerFn == timerFn1 && timerFn == null) {
      return;
    }
    assertNotEquals(timerFn, timerFn1);
  }

  private static void assertClonedTables(Map<TableSpec, TableImpl> originalTables, Map<TableSpec, TableImpl> clonedTables) {
    assertEquals(originalTables.size(), clonedTables.size());
    assertEquals(originalTables.keySet(), clonedTables.keySet());
    Iterator<TableImpl> oIter = originalTables.values().iterator();
    Iterator<TableImpl> nIter = clonedTables.values().iterator();
    oIter.forEachRemaining(oTable -> assertClonedTableImpl(oTable, nIter.next()));
  }

  private static void assertClonedTableImpl(TableImpl oTable, TableImpl nTable) {
    assertNotEquals(oTable, nTable);
    assertEquals(oTable.getTableSpec(), nTable.getTableSpec());
  }

  private static void assertClonedOutputs(Map<String, OutputStreamImpl> originalOutputs,
      Map<String, OutputStreamImpl> clonedOutputs) {
    assertEquals(originalOutputs.size(), clonedOutputs.size());
    assertEquals(originalOutputs.keySet(), clonedOutputs.keySet());
    Iterator<OutputStreamImpl> oIter = originalOutputs.values().iterator();
    Iterator<OutputStreamImpl> nIter = clonedOutputs.values().iterator();
    oIter.forEachRemaining(oOutput -> assertClonedOutputImpl(oOutput, nIter.next()));
  }

  private static void assertClonedOutputImpl(OutputStreamImpl oOutput, OutputStreamImpl nOutput) {
    assertNotEquals(oOutput, nOutput);
    assertEquals(oOutput.isKeyed(), nOutput.isKeyed());
    assertEquals(oOutput.getStreamId(), nOutput.getStreamId());
  }

  private static void assertClonedInputs(Map<String, InputOperatorSpec> originalInputs,
      Map<String, InputOperatorSpec> clonedInputs) {
    assertEquals(originalInputs.size(), clonedInputs.size());
    assertEquals(originalInputs.keySet(), clonedInputs.keySet());
    Iterator<InputOperatorSpec> oIter = originalInputs.values().iterator();
    Iterator<InputOperatorSpec> nIter = clonedInputs.values().iterator();
    oIter.forEachRemaining(inputOp -> assertClonedInputOp(inputOp, nIter.next()));
  }

  private static void assertClonedInputOp(InputOperatorSpec originalInput, InputOperatorSpec clonedInput) {
    assertNotEquals(originalInput, clonedInput);
    assertEquals(originalInput.getOpId(), clonedInput.getOpId());
    assertEquals(originalInput.getOpCode(), clonedInput.getOpCode());
    assertEquals(originalInput.getStreamId(), clonedInput.getStreamId());
    assertEquals(originalInput.isKeyed(), clonedInput.isKeyed());
  }

}
