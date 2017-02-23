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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.TestMessageEnvelope;
import org.apache.samza.operators.TestOutputMessageEnvelope;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class TestStreamOperatorImpl {

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleOperator() {
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> mockOp = mock(StreamOperatorSpec.class);
    FlatMapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> txfmFn = mock(FlatMapFunction.class);
    when(mockOp.getTransformFn()).thenReturn(txfmFn);
    MessageStreamImpl<TestMessageEnvelope> mockInput = mock(MessageStreamImpl.class);
    Config mockConfig = mock(Config.class);
    TaskContext mockContext = mock(TaskContext.class);
    StreamOperatorImpl<TestMessageEnvelope, TestOutputMessageEnvelope> opImpl = spy(new StreamOperatorImpl<>(mockOp, mockInput, mockConfig, mockContext));
    TestMessageEnvelope inMsg = mock(TestMessageEnvelope.class);
    TestOutputMessageEnvelope outMsg = mock(TestOutputMessageEnvelope.class);
    Collection<TestOutputMessageEnvelope> mockOutputs = new ArrayList() { {
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
