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

import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.data.TestOutputMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestOperatorImpl {

  TestMessageEnvelope curInputMsg;
  MessageCollector curCollector;
  TaskCoordinator curCoordinator;

  @Test
  public void testSubscribers() {
    this.curInputMsg = null;
    this.curCollector = null;
    this.curCoordinator = null;
    OperatorImpl<TestMessageEnvelope, TestOutputMessageEnvelope> opImpl = new OperatorImpl<TestMessageEnvelope, TestOutputMessageEnvelope>() {
      @Override
      public void onNext(TestMessageEnvelope message, MessageCollector collector, TaskCoordinator coordinator) {
        TestOperatorImpl.this.curInputMsg = message;
        TestOperatorImpl.this.curCollector = collector;
        TestOperatorImpl.this.curCoordinator = coordinator;
      }
      @Override
      public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {

      }

      };
    // verify registerNextOperator() added the mockSub and propagateResult() invoked the mockSub.onNext()
    OperatorImpl mockSub = mock(OperatorImpl.class);
    opImpl.registerNextOperator(mockSub);
    TestOutputMessageEnvelope xOutput = mock(TestOutputMessageEnvelope.class);
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    opImpl.propagateResult(xOutput, mockCollector, mockCoordinator);
    verify(mockSub, times(1)).onNext(
        argThat(new IsEqual<>(xOutput)),
        argThat(new IsEqual<>(mockCollector)),
        argThat(new IsEqual<>(mockCoordinator))
    );
    // verify onNext() is invoked correctly
    TestMessageEnvelope mockInput = mock(TestMessageEnvelope.class);
    opImpl.onNext(mockInput, mockCollector, mockCoordinator);
    assertEquals(mockInput, this.curInputMsg);
    assertEquals(mockCollector, this.curCollector);
    assertEquals(mockCoordinator, this.curCoordinator);
  }
}
