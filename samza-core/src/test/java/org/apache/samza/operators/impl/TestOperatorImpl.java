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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.samza.config.Config;
import org.apache.samza.container.TaskContextImpl;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.TimerRegistry;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

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
    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
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
    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());

    Object mockTestOpImplOutput = mock(Object.class);
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mockTestOpImplOutput);
    opImpl.init(mock(Config.class), mockTaskContext);

    // register a couple of operators
    OperatorImpl mockNextOpImpl1 = mock(OperatorImpl.class);
    when(mockNextOpImpl1.getOperatorSpec()).thenReturn(new TestOpSpec());
    when(mockNextOpImpl1.handleMessage(anyObject(), anyObject(), anyObject())).thenReturn(Collections.emptyList());
    mockNextOpImpl1.init(mock(Config.class), mockTaskContext);
    opImpl.registerNextOperator(mockNextOpImpl1);

    OperatorImpl mockNextOpImpl2 = mock(OperatorImpl.class);
    when(mockNextOpImpl2.getOperatorSpec()).thenReturn(new TestOpSpec());
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
    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    ReadableMetricsRegistry mockMetricsRegistry = mock(ReadableMetricsRegistry.class);
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
    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());

    Object mockTestOpImplOutput = mock(Object.class);
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mockTestOpImplOutput);
    opImpl.init(mock(Config.class), mockTaskContext);

    // register a couple of operators
    OperatorImpl mockNextOpImpl1 = mock(OperatorImpl.class);
    when(mockNextOpImpl1.getOperatorSpec()).thenReturn(new TestOpSpec());
    when(mockNextOpImpl1.handleMessage(anyObject(), anyObject(), anyObject())).thenReturn(Collections.emptyList());
    mockNextOpImpl1.init(mock(Config.class), mockTaskContext);
    opImpl.registerNextOperator(mockNextOpImpl1);

    OperatorImpl mockNextOpImpl2 = mock(OperatorImpl.class);
    when(mockNextOpImpl2.getOperatorSpec()).thenReturn(new TestOpSpec());
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
    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    ReadableMetricsRegistry mockMetricsRegistry = mock(ReadableMetricsRegistry.class);
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
    private final TestOpSpec testOpSpec;

    TestOpImpl(Object mockOutput) {
      this.mockOutput = mockOutput;
      this.testOpSpec = new TestOpSpec();
    }

    @Override
    protected void handleInit(Config config, TaskContext context, TimerRegistry timerRegistry) {}

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
    protected void handleClose() {}

    protected OperatorSpec<Object, Object> getOperatorSpec() {
      return testOpSpec;
    }
  }

  private static class TestOpSpec extends OperatorSpec<Object, Object> {
    TestOpSpec() {
     super(OpCode.INPUT, "1");
    }

    @Override
    public WatermarkFunction getWatermarkFn() {
      return null;
    }

    @Override
    public TimerFunction getTimerFn() {
      return null;
    }
  }

  public static Set<OperatorImpl> getNextOperators(OperatorImpl op) {
    return op.registeredOperators;
  }

  public static OperatorSpec.OpCode getOpCode(OperatorImpl op) {
    return op.getOperatorSpec().getOpCode();
  }

  public static long getInputWatermark(OperatorImpl op) {
    return op.getInputWatermark();
  }

  public static long getOutputWatermark(OperatorImpl op) {
    return op.getOutputWatermark();
  }

}

