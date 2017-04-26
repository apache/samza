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
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestOperatorImpl {

  @Test(expected = IllegalStateException.class)
  public void testMultipleInitShouldThrow() {
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mock(Object.class));
    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    opImpl.init(mock(Config.class), mockTaskContext);
    opImpl.init(mock(Config.class), mockTaskContext);
  }

  @Test(expected = IllegalStateException.class)
  public void testRegisterNextOperatorBeforeInitShouldThrow() {
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mock(Object.class));
    opImpl.registerNextOperator(mock(OperatorImpl.class));
  }

  @Test
  public void testOnMessagePropagatesResults() {
    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());

    Object mockTestOpImplOutput = mock(Object.class);
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mockTestOpImplOutput);
    opImpl.init(mock(Config.class), mockTaskContext);

    // register a couple of operators
    OperatorImpl mockNextOpImpl1 = mock(OperatorImpl.class);
    when(mockNextOpImpl1.getOpSpec()).thenReturn(new TestOpSpec());
    when(mockNextOpImpl1.handleMessage(anyObject(), anyObject(), anyObject())).thenReturn(Collections.emptyList());
    mockNextOpImpl1.init(mock(Config.class), mockTaskContext);
    opImpl.registerNextOperator(mockNextOpImpl1);

    OperatorImpl mockNextOpImpl2 = mock(OperatorImpl.class);
    when(mockNextOpImpl2.getOpSpec()).thenReturn(new TestOpSpec());
    when(mockNextOpImpl2.handleMessage(anyObject(), anyObject(), anyObject())).thenReturn(Collections.emptyList());
    mockNextOpImpl2.init(mock(Config.class), mockTaskContext);
    opImpl.registerNextOperator(mockNextOpImpl2);

    // send a message to this operator
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    opImpl.onMessage(mock(Object.class), mockCollector, mockCoordinator);

    // verify that it propagates its handleMessage results to next operators
    verify(mockNextOpImpl1, times(1)).handleMessage(mockTestOpImplOutput, mockCollector, mockCoordinator);
    verify(mockNextOpImpl2, times(1)).handleMessage(mockTestOpImplOutput, mockCollector, mockCoordinator);
  }

  @Test
  public void testOnMessageUpdatesMetrics() {
    TaskContext mockTaskContext = mock(TaskContext.class);
    MetricsRegistry mockMetricsRegistry = mock(MetricsRegistry.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(mockMetricsRegistry);
    Counter mockCounter = mock(Counter.class);
    Timer mockTimer = mock(Timer.class);
    when(mockMetricsRegistry.newCounter(anyString(), anyString())).thenReturn(mockCounter);
    when(mockMetricsRegistry.newTimer(anyString(), anyString())).thenReturn(mockTimer);

    Object mockTestOpImplOutput = mock(Object.class);
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mockTestOpImplOutput);
    opImpl.init(mock(Config.class), mockTaskContext);

    // send a message to this operator
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    opImpl.onMessage(mock(Object.class), mockCollector, mockCoordinator);

    // verify that it updates message count and timer metrics
    verify(mockCounter, times(1)).inc();
    verify(mockTimer, times(1)).update(anyLong());
  }

  @Test
  public void testOnTimerPropagatesResultsAndTimer() {
    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());

    Object mockTestOpImplOutput = mock(Object.class);
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mockTestOpImplOutput);
    opImpl.init(mock(Config.class), mockTaskContext);

    // register a couple of operators
    OperatorImpl mockNextOpImpl1 = mock(OperatorImpl.class);
    when(mockNextOpImpl1.getOpSpec()).thenReturn(new TestOpSpec());
    when(mockNextOpImpl1.handleMessage(anyObject(), anyObject(), anyObject())).thenReturn(Collections.emptyList());
    mockNextOpImpl1.init(mock(Config.class), mockTaskContext);
    opImpl.registerNextOperator(mockNextOpImpl1);

    OperatorImpl mockNextOpImpl2 = mock(OperatorImpl.class);
    when(mockNextOpImpl2.getOpSpec()).thenReturn(new TestOpSpec());
    when(mockNextOpImpl2.handleMessage(anyObject(), anyObject(), anyObject())).thenReturn(Collections.emptyList());
    mockNextOpImpl2.init(mock(Config.class), mockTaskContext);
    opImpl.registerNextOperator(mockNextOpImpl2);

    // send a timer tick to this operator
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    opImpl.onTimer(mockCollector, mockCoordinator);

    // verify that it propagates its handleTimer results to next operators
    verify(mockNextOpImpl1, times(1)).handleMessage(mockTestOpImplOutput, mockCollector, mockCoordinator);
    verify(mockNextOpImpl2, times(1)).handleMessage(mockTestOpImplOutput, mockCollector, mockCoordinator);

    // verify that it propagates the timer tick to next operators
    verify(mockNextOpImpl1, times(1)).handleTimer(mockCollector, mockCoordinator);
    verify(mockNextOpImpl2, times(1)).handleTimer(mockCollector, mockCoordinator);
  }

  @Test
  public void testOnTimerUpdatesMetrics() {
    TaskContext mockTaskContext = mock(TaskContext.class);
    MetricsRegistry mockMetricsRegistry = mock(MetricsRegistry.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(mockMetricsRegistry);
    Counter mockMessageCounter = mock(Counter.class);
    Timer mockTimer = mock(Timer.class);
    when(mockMetricsRegistry.newCounter(anyString(), anyString())).thenReturn(mockMessageCounter);
    when(mockMetricsRegistry.newTimer(anyString(), anyString())).thenReturn(mockTimer);

    Object mockTestOpImplOutput = mock(Object.class);
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mockTestOpImplOutput);
    opImpl.init(mock(Config.class), mockTaskContext);

    // send a message to this operator
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    opImpl.onTimer(mockCollector, mockCoordinator);

    // verify that it updates metrics
    verify(mockMessageCounter, times(0)).inc();
    verify(mockTimer, times(1)).update(anyLong());
  }

  private static class TestOpImpl extends OperatorImpl<Object, Object> {
    private final Object mockOutput;

    TestOpImpl(Object mockOutput) {
      this.mockOutput = mockOutput;
    }

    @Override
    protected void doInit(Config config, TaskContext context) {}

    @Override
    public Collection<Object> handleMessage(Object message,
        MessageCollector collector, TaskCoordinator coordinator) {
      return Collections.singletonList(mockOutput);
    }

    @Override
    public Collection<Object> handleTimer(MessageCollector collector, TaskCoordinator coordinator) {
      return Collections.singletonList(mockOutput);
    }

    @Override
    protected OperatorSpec<Object> getOpSpec() {
      return new TestOpSpec();
    }
  }

  private static class TestOpSpec implements OperatorSpec<Object> {
    @Override
    public MessageStreamImpl<Object> getNextStream() {
      return null;
    }

    @Override
    public OpCode getOpCode() {
      return OpCode.INPUT;
    }

    @Override
    public int getOpId() {
      return -1;
    }
  }
}

