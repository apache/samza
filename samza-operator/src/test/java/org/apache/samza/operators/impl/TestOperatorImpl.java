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

import org.apache.samza.operators.MockMessage;
import org.apache.samza.operators.MockOutputMessage;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.reactivestreams.Subscriber;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;


public class TestOperatorImpl {

  MockMessage curInputMsg;
  MessageCollector curCollector;
  TaskCoordinator curCoordinator;

  @Test public void testSubscribers() {
    this.curInputMsg = null;
    this.curCollector = null;
    this.curCoordinator = null;
    OperatorImpl<MockMessage, MockOutputMessage> opImpl = new OperatorImpl<MockMessage, MockOutputMessage>() {
      @Override protected void onNext(MockMessage message, MessageCollector collector, TaskCoordinator coordinator) {
        TestOperatorImpl.this.curInputMsg = message;
        TestOperatorImpl.this.curCollector = collector;
        TestOperatorImpl.this.curCoordinator = coordinator;
      }
    };
    // verify subscribe() added the mockSub and nextProcessors() invoked the mockSub.onNext()
    Subscriber<ProcessorContext<MockOutputMessage>> mockSub = mock(Subscriber.class);
    opImpl.subscribe(mockSub);
    MockOutputMessage xOutput = mock(MockOutputMessage.class);
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    opImpl.nextProcessors(xOutput, mockCollector, mockCoordinator);
    verify(mockSub, times(1)).onNext(argThat(new ArgumentMatcher<ProcessorContext<MockOutputMessage>>() {
      @Override public boolean matches(Object argument) {
        ProcessorContext<MockOutputMessage> pCntx = (ProcessorContext<MockOutputMessage>) argument;
        return pCntx.getMessage().equals(xOutput) && pCntx.getCoordinator().equals(mockCoordinator) && pCntx.getCollector().equals(mockCollector);
      }
    }));
    // verify onNext() is invoked correctly
    MockMessage mockInput = mock(MockMessage.class);
    ProcessorContext<MockMessage> inCntx = new ProcessorContext<>(mockInput, mockCollector, mockCoordinator);
    opImpl.onNext(inCntx);
    assertEquals(mockInput, this.curInputMsg);
    assertEquals(mockCollector, this.curCollector);
    assertEquals(mockCoordinator, this.curCoordinator);
  }
}
