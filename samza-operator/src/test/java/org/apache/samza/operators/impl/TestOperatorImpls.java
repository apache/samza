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
package org.apache.samza.operators.impl;

import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.TestMessageEnvelope;
import org.apache.samza.operators.TestOutputMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.task.TaskContext;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestOperatorImpls {
  Field nextOperatorsField = null;

  @Before
  public void prep() throws NoSuchFieldException {
    nextOperatorsField = OperatorImpl.class.getDeclaredField("nextOperators");
    nextOperatorsField.setAccessible(true);
  }
  
  @Test
  public void testCreateOperator() throws NoSuchFieldException, IllegalAccessException {
    // get window operator
    WindowOperatorSpec mockWnd = mock(WindowOperatorSpec.class);
    OperatorImpl<TestMessageEnvelope, ? extends MessageEnvelope> opImpl = OperatorImpls.createOperatorImpl(mockWnd);
    assertTrue(opImpl instanceof WindowOperatorImpl);

    // get simple operator
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> mockSimpleOp = mock(StreamOperatorSpec.class);
    FlatMapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> mockTxfmFn = mock(FlatMapFunction.class);
    when(mockSimpleOp.getTransformFn()).thenReturn(mockTxfmFn);
    opImpl = OperatorImpls.createOperatorImpl(mockSimpleOp);
    assertTrue(opImpl instanceof StreamOperatorImpl);
    Field txfmFnField = StreamOperatorImpl.class.getDeclaredField("transformFn");
    txfmFnField.setAccessible(true);
    assertEquals(mockTxfmFn, txfmFnField.get(opImpl));

    // get sink operator
    SinkFunction<TestMessageEnvelope> sinkFn = (m, mc, tc) -> { };
    SinkOperatorSpec<TestMessageEnvelope> sinkOp = mock(SinkOperatorSpec.class);
    when(sinkOp.getSinkFn()).thenReturn(sinkFn);
    opImpl = OperatorImpls.createOperatorImpl(sinkOp);
    assertTrue(opImpl instanceof SinkOperatorImpl);
    Field sinkFnField = SinkOperatorImpl.class.getDeclaredField("sinkFn");
    sinkFnField.setAccessible(true);
    assertEquals(sinkFn, sinkFnField.get(opImpl));

    // get join operator
    PartialJoinOperatorSpec<TestMessageEnvelope, String, TestMessageEnvelope, TestOutputMessageEnvelope> joinOp = mock(PartialJoinOperatorSpec.class);
    TestOutputMessageEnvelope mockOutput = mock(TestOutputMessageEnvelope.class);
    BiFunction<TestMessageEnvelope, TestMessageEnvelope, TestOutputMessageEnvelope> joinFn = (m1, m2) -> mockOutput;
    when(joinOp.getTransformFn()).thenReturn(joinFn);
    opImpl = OperatorImpls.createOperatorImpl(joinOp);
    assertTrue(opImpl instanceof PartialJoinOperatorImpl);
  }

  @Test
  public void testEmptyChain() {
    // test creation of empty chain
    MessageStreamImpl<TestMessageEnvelope> testStream = new MessageStreamImpl<>();
    TaskContext mockContext = mock(TaskContext.class);
    RootOperatorImpl operatorChain = OperatorImpls.createOperatorImpls(testStream, mockContext);
    assertTrue(operatorChain != null);
  }

  @Test
  public void testLinearChain() throws IllegalAccessException {
    // test creation of linear chain
    MessageStreamImpl<TestMessageEnvelope> testInput = new MessageStreamImpl<>();
    TaskContext mockContext = mock(TaskContext.class);
    testInput.map(m -> m).window(Windows.tumblingWindow(Duration.ofMillis(1000)));
    RootOperatorImpl operatorChain = OperatorImpls.createOperatorImpls(testInput, mockContext);
    Set<OperatorImpl> subsSet = (Set<OperatorImpl>) nextOperatorsField.get(operatorChain);
    assertEquals(subsSet.size(), 1);
    OperatorImpl<TestMessageEnvelope, TestMessageEnvelope> firstOpImpl = subsSet.iterator().next();
    Set<OperatorImpl> subsOps = (Set<OperatorImpl>) nextOperatorsField.get(firstOpImpl);
    assertEquals(subsOps.size(), 1);
    OperatorImpl wndOpImpl = subsOps.iterator().next();
    subsOps = (Set<OperatorImpl>) nextOperatorsField.get(wndOpImpl);
    assertEquals(subsOps.size(), 0);
  }

  @Test
  public void testBroadcastChain() throws IllegalAccessException {
    // test creation of broadcast chain
    MessageStreamImpl<TestMessageEnvelope> testInput = new MessageStreamImpl<>();
    TaskContext mockContext = mock(TaskContext.class);
    testInput.filter(m -> m.getMessage().getEventTime() > 123456L).flatMap(m -> new ArrayList() { { this.add(m); this.add(m); } });
    testInput.filter(m -> m.getMessage().getEventTime() < 123456L).map(m -> m);
    RootOperatorImpl operatorChain = OperatorImpls.createOperatorImpls(testInput, mockContext);
    Set<OperatorImpl> subsSet = (Set<OperatorImpl>) nextOperatorsField.get(operatorChain);
    assertEquals(subsSet.size(), 2);
    Iterator<OperatorImpl> iter = subsSet.iterator();
    // check the first branch w/ flatMap
    OperatorImpl<TestMessageEnvelope, TestMessageEnvelope> opImpl = iter.next();
    Set<OperatorImpl> subsOps = (Set<OperatorImpl>) nextOperatorsField.get(opImpl);
    assertEquals(subsOps.size(), 1);
    OperatorImpl flatMapImpl = subsOps.iterator().next();
    subsOps = (Set<OperatorImpl>) nextOperatorsField.get(flatMapImpl);
    assertEquals(subsOps.size(), 0);
    // check the second branch w/ map
    opImpl = iter.next();
    subsOps = (Set<OperatorImpl>) nextOperatorsField.get(opImpl);
    assertEquals(subsOps.size(), 1);
    OperatorImpl mapImpl = subsOps.iterator().next();
    subsOps = (Set<OperatorImpl>) nextOperatorsField.get(mapImpl);
    assertEquals(subsOps.size(), 0);
  }

  @Test
  public void testJoinChain() throws IllegalAccessException {
    // test creation of join chain
    MessageStreamImpl<TestMessageEnvelope> input1 = new MessageStreamImpl<>();
    MessageStreamImpl<TestMessageEnvelope> input2 = new MessageStreamImpl<>();
    TaskContext mockContext = mock(TaskContext.class);
    input1
        .join(input2, (m1, m2) ->
            new TestOutputMessageEnvelope(m1.getKey(), m1.getMessage().getValue().length() + m2.getMessage().getValue().length()))
        .map(m -> m);
    // now, we create chained operators from each input sources
    RootOperatorImpl chain1 = OperatorImpls.createOperatorImpls(input1, mockContext);
    RootOperatorImpl chain2 = OperatorImpls.createOperatorImpls(input2, mockContext);
    // check that those two chains will merge at map operator
    // first branch of the join
    Set<OperatorImpl> subsSet = (Set<OperatorImpl>) nextOperatorsField.get(chain1);
    assertEquals(subsSet.size(), 1);
    OperatorImpl<TestMessageEnvelope, TestOutputMessageEnvelope> joinOp1 = subsSet.iterator().next();
    Set<OperatorImpl> subsOps = (Set<OperatorImpl>) nextOperatorsField.get(joinOp1);
    assertEquals(subsOps.size(), 1);
    // the map operator consumes the common join output, where two branches merge
    OperatorImpl mapImpl = subsOps.iterator().next();
    // second branch of the join
    subsSet = (Set<OperatorImpl>) nextOperatorsField.get(chain2);
    assertEquals(subsSet.size(), 1);
    OperatorImpl<TestMessageEnvelope, TestOutputMessageEnvelope> joinOp2 = subsSet.iterator().next();
    assertNotSame(joinOp1, joinOp2);
    subsOps = (Set<OperatorImpl>) nextOperatorsField.get(joinOp2);
    assertEquals(subsOps.size(), 1);
    // make sure that the map operator is the same
    assertEquals(mapImpl, subsOps.iterator().next());
  }
}
