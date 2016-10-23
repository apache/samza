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

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MockMessage;
import org.apache.samza.operators.MockOutputMessage;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.internal.Operators.PartialJoinOperator;
import org.apache.samza.operators.internal.Operators.SinkOperator;
import org.apache.samza.operators.internal.Operators.StreamOperator;
import org.apache.samza.operators.internal.Operators.WindowOperator;
import org.apache.samza.operators.impl.join.PartialJoinOpImpl;
import org.apache.samza.operators.impl.window.SessionWindowImpl;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestOperatorFactory {

  @Test public void testGetOperator() throws NoSuchFieldException, IllegalAccessException {
    // get window operator
    WindowOperator mockWnd = mock(WindowOperator.class);
    Map.Entry<OperatorImpl<MockMessage, ? extends Message>, Boolean>
        factoryEntry = OperatorFactory.<MockMessage, MockOutputMessage>getOperator(mockWnd);
    assertFalse(factoryEntry.getValue());
    OperatorImpl<MockMessage, MockOutputMessage> opImpl = (OperatorImpl<MockMessage, MockOutputMessage>) factoryEntry.getKey();
    assertTrue(opImpl instanceof SessionWindowImpl);
    Field sessWndField = SessionWindowImpl.class.getDeclaredField("sessWnd");
    sessWndField.setAccessible(true);
    WindowOperator sessWnd = (WindowOperator) sessWndField.get(opImpl);
    assertEquals(sessWnd, mockWnd);

    // get simple operator
    StreamOperator<MockMessage, MockOutputMessage> mockSimpleOp = mock(StreamOperator.class);
    Function<MockMessage, Collection<MockOutputMessage>>  mockTxfmFn = mock(Function.class);
    when(mockSimpleOp.getFunction()).thenReturn(mockTxfmFn);
    factoryEntry = OperatorFactory.<MockMessage, MockOutputMessage>getOperator(mockSimpleOp);
    opImpl = (OperatorImpl<MockMessage, MockOutputMessage>) factoryEntry.getKey();
    assertTrue(opImpl instanceof SimpleOperatorImpl);
    Field txfmFnField = SimpleOperatorImpl.class.getDeclaredField("transformFn");
    txfmFnField.setAccessible(true);
    assertEquals(mockTxfmFn, txfmFnField.get(opImpl));

    // get sink operator
    MessageStream.VoidFunction3<MockMessage, MessageCollector, TaskCoordinator> sinkFn = (m, mc, tc) -> {};
    SinkOperator<MockMessage> sinkOp = mock(SinkOperator.class);
    when(sinkOp.getFunction()).thenReturn(sinkFn);
    factoryEntry = OperatorFactory.<MockMessage, MockOutputMessage>getOperator(sinkOp);
    opImpl = (OperatorImpl<MockMessage, MockOutputMessage>) factoryEntry.getKey();
    assertTrue(opImpl instanceof SinkOperatorImpl);
    Field sinkFnField = SinkOperatorImpl.class.getDeclaredField("sinkFunc");
    sinkFnField.setAccessible(true);
    assertEquals(sinkFn, sinkFnField.get(opImpl));

    // get join operator
    PartialJoinOperator<MockMessage, String, MockMessage, MockOutputMessage> joinOp = mock(PartialJoinOperator.class);
    MockOutputMessage mockOutput = mock(MockOutputMessage.class);
    BiFunction<MockMessage, MockMessage, MockOutputMessage> joinFn = (m1, m2) -> mockOutput;
    when(joinOp.getFunction()).thenReturn(joinFn);
    factoryEntry = OperatorFactory.<MockMessage, MockOutputMessage>getOperator(joinOp);
    opImpl = (OperatorImpl<MockMessage, MockOutputMessage>) factoryEntry.getKey();
    assertTrue(opImpl instanceof PartialJoinOpImpl);
  }

}
