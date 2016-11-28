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

import org.apache.samza.operators.TestMessage;
import org.apache.samza.operators.TestOutputMessage;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestSimpleOperatorImpl {

  @Test
  public void testSimpleOperator() {
    StreamOperatorSpec<TestMessage, TestOutputMessage> mockOp = mock(StreamOperatorSpec.class);
    FlatMapFunction<TestMessage, TestOutputMessage> txfmFn = mock(FlatMapFunction.class);
    when(mockOp.getTransformFn()).thenReturn(txfmFn);

    StreamOperatorImpl<TestMessage, TestOutputMessage> opImpl = spy(new StreamOperatorImpl<>(mockOp));
    TestMessage inMsg = mock(TestMessage.class);
    TestOutputMessage outMsg = mock(TestOutputMessage.class);
    Collection<TestOutputMessage> mockOutputs = new ArrayList() { {
        this.add(outMsg);
      } };
    when(txfmFn.apply(inMsg)).thenReturn(mockOutputs);
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    opImpl.onNext(inMsg, mockCollector, mockCoordinator);
    verify(txfmFn, times(1)).apply(inMsg);
    verify(opImpl, times(1)).propagateResult(outMsg, mockCollector, mockCoordinator);
  }
}
