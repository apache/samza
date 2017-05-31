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

import org.apache.samza.config.Config;
import org.apache.samza.operators.data.TestOutputMessageEnvelope;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestSinkOperatorImpl {

  @Test
  public void testSinkOperatorSinkFunction() {
    SinkFunction<TestOutputMessageEnvelope> sinkFn = mock(SinkFunction.class);
    SinkOperatorImpl<TestOutputMessageEnvelope> sinkImpl = createSinkOperator(sinkFn);
    TestOutputMessageEnvelope mockMsg = mock(TestOutputMessageEnvelope.class);
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);

    sinkImpl.handleMessage(mockMsg, mockCollector, mockCoordinator);
    verify(sinkFn, times(1)).apply(mockMsg, mockCollector, mockCoordinator);
  }

  @Test
  public void testSinkOperatorClose() {
    TestOutputMessageEnvelope mockMsg = mock(TestOutputMessageEnvelope.class);
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    SinkFunction<TestOutputMessageEnvelope> sinkFn = mock(SinkFunction.class);

    SinkOperatorImpl<TestOutputMessageEnvelope> sinkImpl = createSinkOperator(sinkFn);
    sinkImpl.handleMessage(mockMsg, mockCollector, mockCoordinator);
    verify(sinkFn, times(1)).apply(mockMsg, mockCollector, mockCoordinator);

    // ensure that close is not called yet
    verify(sinkFn, times(0)).close();

    sinkImpl.handleClose();
    // ensure that close is called once from handleClose()
    verify(sinkFn, times(1)).close();
  }

  private SinkOperatorImpl createSinkOperator(SinkFunction<TestOutputMessageEnvelope> sinkFn) {
    SinkOperatorSpec<TestOutputMessageEnvelope> sinkOp = mock(SinkOperatorSpec.class);
    when(sinkOp.getSinkFn()).thenReturn(sinkFn);

    Config mockConfig = mock(Config.class);
    TaskContext mockContext = mock(TaskContext.class);
    return new SinkOperatorImpl<>(sinkOp, mockConfig, mockContext);
  }
}
