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
import org.apache.samza.operators.TestMessage;
import org.apache.samza.operators.TestOutputMessage;
import org.apache.samza.operators.Windows;
import org.apache.samza.task.TaskContext;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;


public class TestChainedOperators {
  Field subsField = null;
  Field opSubsField = null;

  @Before public void prep() throws NoSuchFieldException {
    subsField = ChainedOperators.class.getDeclaredField("subscribers");
    subsField.setAccessible(true);
    opSubsField = OperatorImpl.class.getDeclaredField("subscribers");
    opSubsField.setAccessible(true);
  }

  @Test public void testCreate() {
    // test creation of empty chain
    MessageStream<TestMessage> testStream = new MessageStream<>();
    TaskContext mockContext = mock(TaskContext.class);
    ChainedOperators<TestMessage> operatorChain = (ChainedOperators<TestMessage>) ChainedOperators.create(testStream, mockContext);
    assertTrue(operatorChain != null);
  }

  @Test public void testLinearChain() throws IllegalAccessException {
    // test creation of linear chain
    MessageStream<TestMessage> testInput = new MessageStream<>();
    TaskContext mockContext = mock(TaskContext.class);
    testInput.map(m -> m).window(Windows.intoSessionCounter(TestMessage::getKey));
    ChainedOperators<TestMessage> operatorChain = (ChainedOperators<TestMessage>) ChainedOperators.create(testInput, mockContext);
    Set<OperatorImpl> subsSet = (Set<OperatorImpl>) subsField.get(operatorChain);
    assertEquals(subsSet.size(), 1);
    OperatorImpl<TestMessage, TestMessage> firstOpImpl = subsSet.iterator().next();
    Set<Subscriber<? super ProcessorContext<TestMessage>>> subsOps = (Set<Subscriber<? super ProcessorContext<TestMessage>>>) opSubsField.get(firstOpImpl);
    assertEquals(subsOps.size(), 1);
    Subscriber<? super ProcessorContext<TestMessage>> wndOpImpl = subsOps.iterator().next();
    subsOps = (Set<Subscriber<? super ProcessorContext<TestMessage>>>) opSubsField.get(wndOpImpl);
    assertEquals(subsOps.size(), 0);
  }

  @Test public void testBroadcastChain() throws IllegalAccessException {
    // test creation of broadcast chain
    MessageStream<TestMessage> testInput = new MessageStream<>();
    TaskContext mockContext = mock(TaskContext.class);
    testInput.filter(m -> m.getTimestamp() > 123456L).flatMap(m -> new ArrayList() {{ this.add(m); this.add(m); }});
    testInput.filter(m -> m.getTimestamp() < 123456L).map(m -> m);
    ChainedOperators<TestMessage> operatorChain = (ChainedOperators<TestMessage>) ChainedOperators.create(testInput, mockContext);
    Set<OperatorImpl> subsSet = (Set<OperatorImpl>) subsField.get(operatorChain);
    assertEquals(subsSet.size(), 2);
    Iterator<OperatorImpl> iter = subsSet.iterator();
    // check the first branch w/ flatMap
    OperatorImpl<TestMessage, TestMessage> opImpl = iter.next();
    Set<Subscriber<? super ProcessorContext<TestMessage>>> subsOps = (Set<Subscriber<? super ProcessorContext<TestMessage>>>) opSubsField.get(opImpl);
    assertEquals(subsOps.size(), 1);
    Subscriber<? super ProcessorContext<TestMessage>> flatMapImpl = subsOps.iterator().next();
    subsOps = (Set<Subscriber<? super ProcessorContext<TestMessage>>>) opSubsField.get(flatMapImpl);
    assertEquals(subsOps.size(), 0);
    // check the second branch w/ map
    opImpl = iter.next();
    subsOps = (Set<Subscriber<? super ProcessorContext<TestMessage>>>) opSubsField.get(opImpl);
    assertEquals(subsOps.size(), 1);
    Subscriber<? super ProcessorContext<TestMessage>> mapImpl = subsOps.iterator().next();
    subsOps = (Set<Subscriber<? super ProcessorContext<TestMessage>>>) opSubsField.get(mapImpl);
    assertEquals(subsOps.size(), 0);
  }

  @Test public void testJoinChain() throws IllegalAccessException {
    // test creation of join chain
    MessageStream<TestMessage> input1 = new MessageStream<>();
    MessageStream<TestMessage> input2 = new MessageStream<>();
    TaskContext mockContext = mock(TaskContext.class);
    input1.join(input2, (m1, m2) -> new TestOutputMessage(m1.getKey(), m1.getMessage().length() + m2.getMessage().length(), m1.getTimestamp())).map(m -> m);
    // now, we create chained operators from each input sources
    ChainedOperators<TestMessage> chain1 = (ChainedOperators<TestMessage>) ChainedOperators.create(input1, mockContext);
    ChainedOperators<TestMessage> chain2 = (ChainedOperators<TestMessage>) ChainedOperators.create(input2, mockContext);
    // check that those two chains will merge at map operator
    // first branch of the join
    Set<OperatorImpl> subsSet = (Set<OperatorImpl>) subsField.get(chain1);
    assertEquals(subsSet.size(), 1);
    OperatorImpl<TestMessage, TestOutputMessage> joinOp1 = subsSet.iterator().next();
    Set<Subscriber<? super ProcessorContext<TestOutputMessage>>> subsOps = (Set<Subscriber<? super ProcessorContext<TestOutputMessage>>>) opSubsField.get(joinOp1);
    assertEquals(subsOps.size(), 1);
    // the map operator consumes the common join output, where two branches merge
    Subscriber<? super ProcessorContext<TestOutputMessage>> mapImpl = subsOps.iterator().next();
    // second branch of the join
    subsSet = (Set<OperatorImpl>) subsField.get(chain2);
    assertEquals(subsSet.size(), 1);
    OperatorImpl<TestMessage, TestOutputMessage> joinOp2 = subsSet.iterator().next();
    assertNotSame(joinOp1, joinOp2);
    subsOps = (Set<Subscriber<? super ProcessorContext<TestOutputMessage>>>) opSubsField.get(joinOp2);
    assertEquals(subsOps.size(), 1);
    // make sure that the map operator is the same
    assertEquals(mapImpl, subsOps.iterator().next());
  }

  public void test() throws Exception{
    //Object hey = Class.forName("hey").newInstance();
  }
}
