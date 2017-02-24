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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.TestMessageEnvelope;
import org.apache.samza.operators.TestMessageStreamImplUtil;
import org.apache.samza.operators.TestOutputMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.internal.WindowType;
import org.apache.samza.task.TaskContext;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestOperatorImpls {
  Field nextOperatorsField = null;
  Method createOpMethod = null;
  Method createOpsMethod = null;

  @Before
  public void prep() throws NoSuchFieldException, NoSuchMethodException {
    nextOperatorsField = OperatorImpl.class.getDeclaredField("nextOperators");
    nextOperatorsField.setAccessible(true);

    createOpMethod = OperatorGraph.class.getDeclaredMethod("createOperatorImpl", MessageStreamImpl.class,
        OperatorSpec.class, Config.class, TaskContext.class);
    createOpMethod.setAccessible(true);

    createOpsMethod = OperatorGraph.class.getDeclaredMethod("createOperatorImpls", MessageStreamImpl.class, Config.class, TaskContext.class);
    createOpsMethod.setAccessible(true);
  }

  @Test
  public void testCreateOperator() throws NoSuchFieldException, IllegalAccessException, InvocationTargetException {
    // get window operator
    WindowOperatorSpec mockWnd = mock(WindowOperatorSpec.class);
    WindowInternal<TestMessageEnvelope, String, Integer> windowInternal = new WindowInternal<>(null, null, null, null, WindowType.TUMBLING);
    when(mockWnd.getWindow()).thenReturn(windowInternal);
    MessageStreamImpl<TestMessageEnvelope> mockStream = mock(MessageStreamImpl.class);
    Config mockConfig = mock(Config.class);
    TaskContext mockContext = mock(TaskContext.class);

    OperatorGraph opGraph = new OperatorGraph();
    OperatorImpl<TestMessageEnvelope, ? extends MessageEnvelope> opImpl = (OperatorImpl<TestMessageEnvelope, ? extends MessageEnvelope>)
        createOpMethod.invoke(opGraph, mockStream, mockWnd, mockConfig, mockContext);
    assertTrue(opImpl instanceof WindowOperatorImpl);
    Field wndInternalField = WindowOperatorImpl.class.getDeclaredField("window");
    wndInternalField.setAccessible(true);
    WindowInternal wndInternal = (WindowInternal) wndInternalField.get(opImpl);
    assertEquals(wndInternal, windowInternal);

    // get simple operator
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> mockSimpleOp = mock(StreamOperatorSpec.class);
    FlatMapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> mockTxfmFn = mock(FlatMapFunction.class);
    when(mockSimpleOp.getTransformFn()).thenReturn(mockTxfmFn);
    opImpl = (OperatorImpl<TestMessageEnvelope, ? extends MessageEnvelope>) createOpMethod.invoke(opGraph, mockStream, mockSimpleOp, mockConfig, mockContext);
    assertTrue(opImpl instanceof StreamOperatorImpl);
    Field txfmFnField = StreamOperatorImpl.class.getDeclaredField("transformFn");
    txfmFnField.setAccessible(true);
    assertEquals(mockTxfmFn, txfmFnField.get(opImpl));

    // get sink operator
    SinkFunction<TestMessageEnvelope> sinkFn = (m, mc, tc) -> { };
    SinkOperatorSpec<TestMessageEnvelope> sinkOp = mock(SinkOperatorSpec.class);
    when(sinkOp.getSinkFn()).thenReturn(sinkFn);
    opImpl = (OperatorImpl<TestMessageEnvelope, ? extends MessageEnvelope>) createOpMethod.invoke(opGraph, mockStream, sinkOp, mockConfig, mockContext);
    assertTrue(opImpl instanceof SinkOperatorImpl);
    Field sinkFnField = SinkOperatorImpl.class.getDeclaredField("sinkFn");
    sinkFnField.setAccessible(true);
    assertEquals(sinkFn, sinkFnField.get(opImpl));

    // get join operator
    PartialJoinOperatorSpec<TestMessageEnvelope, String, TestMessageEnvelope, TestOutputMessageEnvelope> joinOp = mock(PartialJoinOperatorSpec.class);
    TestOutputMessageEnvelope mockOutput = mock(TestOutputMessageEnvelope.class);
    PartialJoinFunction<String, TestMessageEnvelope, TestMessageEnvelope, TestOutputMessageEnvelope> joinFn = mock(PartialJoinFunction.class);
    when(joinOp.getTransformFn()).thenReturn(joinFn);
    opImpl = (OperatorImpl<TestMessageEnvelope, ? extends MessageEnvelope>) createOpMethod.invoke(opGraph, mockStream, joinOp, mockConfig, mockContext);
    assertTrue(opImpl instanceof PartialJoinOperatorImpl);
  }

  @Test
  public void testEmptyChain() throws InvocationTargetException, IllegalAccessException {
    // test creation of empty chain
    MessageStreamImpl<TestMessageEnvelope> testStream = mock(MessageStreamImpl.class);
    TaskContext mockContext = mock(TaskContext.class);
    Config mockConfig = mock(Config.class);
    OperatorGraph opGraph = new OperatorGraph();
    RootOperatorImpl operatorChain = (RootOperatorImpl) createOpsMethod.invoke(opGraph, testStream, mockConfig, mockContext);
    assertTrue(operatorChain != null);
  }

  @Test
  public void testLinearChain() throws IllegalAccessException, InvocationTargetException {
    // test creation of linear chain
    StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);
    MessageStreamImpl<TestMessageEnvelope> testInput = TestMessageStreamImplUtil.<TestMessageEnvelope>getMessageStreamImpl(mockGraph);
    TaskContext mockContext = mock(TaskContext.class);
    Config mockConfig = mock(Config.class);
    testInput.map(m -> m).window(Windows.keyedSessionWindow(TestMessageEnvelope::getKey, Duration.ofMinutes(10)));
    OperatorGraph opGraph = new OperatorGraph();
    RootOperatorImpl operatorChain = (RootOperatorImpl) createOpsMethod.invoke(opGraph, testInput, mockConfig, mockContext);
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
  public void testBroadcastChain() throws IllegalAccessException, InvocationTargetException {
    // test creation of broadcast chain
    StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);
    MessageStreamImpl<TestMessageEnvelope> testInput = TestMessageStreamImplUtil.<TestMessageEnvelope>getMessageStreamImpl(mockGraph);
    TaskContext mockContext = mock(TaskContext.class);
    Config mockConfig = mock(Config.class);
    testInput.filter(m -> m.getMessage().getEventTime() > 123456L).flatMap(m -> new ArrayList() { { this.add(m); this.add(m); } });
    testInput.filter(m -> m.getMessage().getEventTime() < 123456L).map(m -> m);
    OperatorGraph opGraph = new OperatorGraph();
    RootOperatorImpl operatorChain = (RootOperatorImpl) createOpsMethod.invoke(opGraph, testInput, mockConfig, mockContext);
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
  public void testJoinChain() throws IllegalAccessException, InvocationTargetException {
    // test creation of join chain
    StreamGraphImpl mockGraph = mock(StreamGraphImpl.class);
    MessageStreamImpl<TestMessageEnvelope> input1 = TestMessageStreamImplUtil.getMessageStreamImpl(mockGraph);
    MessageStreamImpl<TestMessageEnvelope> input2 = TestMessageStreamImplUtil.getMessageStreamImpl(mockGraph);
    TaskContext mockContext = mock(TaskContext.class);
    Config mockConfig = mock(Config.class);
    input1
        .join(input2,
            new JoinFunction<String, TestMessageEnvelope, TestMessageEnvelope, TestOutputMessageEnvelope>() {
              @Override
              public TestOutputMessageEnvelope apply(TestMessageEnvelope m1, TestMessageEnvelope m2) {
                return new TestOutputMessageEnvelope(m1.getKey(), m1.getMessage().getValue().length() + m2.getMessage().getValue().length());
              }

              @Override
              public String getFirstKey(TestMessageEnvelope message) {
                return message.getKey();
              }

              @Override
              public String getSecondKey(TestMessageEnvelope message) {
                return message.getKey();
              }
            })
        .map(m -> m);
    OperatorGraph opGraph = new OperatorGraph();
    // now, we create chained operators from each input sources
    RootOperatorImpl chain1 = (RootOperatorImpl) createOpsMethod.invoke(opGraph, input1, mockConfig, mockContext);
    RootOperatorImpl chain2 = (RootOperatorImpl) createOpsMethod.invoke(opGraph, input2, mockConfig, mockContext);
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
