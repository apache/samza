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

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.context.InternalTaskContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.context.TaskContext;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.functions.ScheduledFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;


public class TestOperatorImpl {
  private Context context;
  private InternalTaskContext internalTaskContext;

  private JobContext jobContext;

  private TaskContext taskContext;

  private ContainerContext containerContext;

  @Before
  public void setup() {
    this.context = mock(Context.class);
    this.internalTaskContext = mock(InternalTaskContext.class);
    this.jobContext = mock(JobContext.class);
    this.taskContext = mock(TaskContext.class);
    this.containerContext = mock(ContainerContext.class);
    when(this.internalTaskContext.getContext()).thenReturn(this.context);
    // might be necessary in the future
    when(this.internalTaskContext.fetchObject(EndOfStreamStates.class.getName())).thenReturn(mock(EndOfStreamStates.class));
    when(this.internalTaskContext.fetchObject(WatermarkStates.class.getName())).thenReturn(mock(WatermarkStates.class));
    when(this.context.getJobContext()).thenReturn(jobContext);
    when(this.context.getTaskContext()).thenReturn(taskContext);
    when(this.taskContext.getTaskMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    when(this.taskContext.getTaskModel()).thenReturn(mock(TaskModel.class));
    when(this.taskContext.getOperatorExecutor()).thenReturn(Executors.newSingleThreadExecutor());
    when(this.context.getContainerContext()).thenReturn(containerContext);
    when(containerContext.getContainerMetricsRegistry()).thenReturn(new MetricsRegistryMap());
  }

  @Test
  public void testComposeFutureWithExecutorWithFrameworkExecutorEnabled() {
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mock(Object.class));
    ExecutorService mockExecutor = mock(ExecutorService.class);
    CompletionStage<Object> mockFuture = mock(CompletionStage.class);
    Function<Object, CompletionStage<Object>> mockFunction = mock(Function.class);

    Config config = new MapConfig(ImmutableMap.of("job.operator.framework.executor.enabled", "true"));

    when(this.taskContext.getOperatorExecutor()).thenReturn(mockExecutor);
    when(this.jobContext.getConfig()).thenReturn(config);

    opImpl.init(this.internalTaskContext);
    opImpl.composeFutureWithExecutor(mockFuture, mockFunction);

    verify(mockFuture).thenComposeAsync(eq(mockFunction), eq(mockExecutor));
  }

  @Test
  public void testComposeFutureWithExecutorWithFrameworkExecutorDisabled() {
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mock(Object.class));
    ExecutorService mockExecutor = mock(ExecutorService.class);
    CompletionStage<Object> mockFuture = mock(CompletionStage.class);
    Function<Object, CompletionStage<Object>> mockFunction = mock(Function.class);

    Config config = new MapConfig(ImmutableMap.of("job.operator.framework.executor.enabled", "false"));

    when(this.taskContext.getOperatorExecutor()).thenReturn(mockExecutor);
    when(this.jobContext.getConfig()).thenReturn(config);

    opImpl.init(this.internalTaskContext);
    opImpl.composeFutureWithExecutor(mockFuture, mockFunction);

    verify(mockFuture).thenCompose(eq(mockFunction));
  }

  @Test
  public void testAcceptFutureWithExecutorWithFrameworkExecutorDisabled() {
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mock(Object.class));
    ExecutorService mockExecutor = mock(ExecutorService.class);
    CompletionStage<Object> mockFuture = mock(CompletionStage.class);
    Consumer<Object> mockConsumer = mock(Consumer.class);

    Config config = new MapConfig(ImmutableMap.of("job.operator.framework.executor.enabled", "false"));

    when(this.taskContext.getOperatorExecutor()).thenReturn(mockExecutor);
    when(this.jobContext.getConfig()).thenReturn(config);

    opImpl.init(this.internalTaskContext);
    opImpl.acceptFutureWithExecutor(mockFuture, mockConsumer);

    verify(mockFuture).thenAccept(eq(mockConsumer));
  }

  @Test
  public void testAcceptFutureWithExecutorWithFrameworkExecutorEnabled() {
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mock(Object.class));
    ExecutorService mockExecutor = mock(ExecutorService.class);
    CompletionStage<Object> mockFuture = mock(CompletionStage.class);
    Consumer<Object> mockConsumer = mock(Consumer.class);

    Config config = new MapConfig(ImmutableMap.of("job.operator.framework.executor.enabled", "true"));

    when(this.taskContext.getOperatorExecutor()).thenReturn(mockExecutor);
    when(this.jobContext.getConfig()).thenReturn(config);

    opImpl.init(this.internalTaskContext);
    opImpl.acceptFutureWithExecutor(mockFuture, mockConsumer);

    verify(mockFuture).thenAcceptAsync(eq(mockConsumer), eq(mockExecutor));
  }
  @Test(expected = IllegalStateException.class)
  public void testMultipleInitShouldThrow() {
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mock(Object.class));
    opImpl.init(this.internalTaskContext);
    opImpl.init(this.internalTaskContext);
  }

  @Test(expected = IllegalStateException.class)
  public void testRegisterNextOperatorBeforeInitShouldThrow() {
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mock(Object.class));
    opImpl.registerNextOperator(mock(OperatorImpl.class));
  }

  @Test
  public void testRegisterOperatorMaintainsInsertionOrder() {
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mock(Object.class));
    opImpl.init(this.internalTaskContext);

    OperatorImpl<Object, Object> firstOp = mock(OperatorImpl.class);
    OperatorImpl<Object, Object> secondOp = mock(OperatorImpl.class);
    OperatorImpl<Object, Object> thirdOp = mock(OperatorImpl.class);

    opImpl.registerNextOperator(firstOp);
    opImpl.registerNextOperator(secondOp);
    opImpl.registerNextOperator(thirdOp);

    Iterator<OperatorImpl<Object, ?>> iterator = opImpl.registeredOperators.iterator();
    assertTrue("Expecting non-empty iterator", iterator.hasNext());
    assertEquals("Expecting first operator to be returned", firstOp, iterator.next());

    assertTrue("Expecting non-empty iterator", iterator.hasNext());
    assertEquals("Expecting second operator to be returned", secondOp, iterator.next());

    assertTrue("Expecting non-empty iterator", iterator.hasNext());
    assertEquals("Expecting third operator to be returned", thirdOp, iterator.next());

    assertFalse("Expecting no more registered operators", iterator.hasNext());
  }
  @Test
  public void testOnMessagePropagatesResults() {
    Object mockTestOpImplOutput = mock(Object.class);
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mockTestOpImplOutput);
    opImpl.init(this.internalTaskContext);

    // register a couple of operators
    OperatorImpl mockNextOpImpl1 = mock(OperatorImpl.class);
    when(mockNextOpImpl1.getOperatorSpec()).thenReturn(new TestOpSpec());
    when(mockNextOpImpl1.handleMessageAsync(anyObject(), anyObject(), anyObject()))
        .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    mockNextOpImpl1.init(this.internalTaskContext);
    opImpl.registerNextOperator(mockNextOpImpl1);

    OperatorImpl mockNextOpImpl2 = mock(OperatorImpl.class);
    when(mockNextOpImpl2.getOperatorSpec()).thenReturn(new TestOpSpec());
    when(mockNextOpImpl2.handleMessageAsync(anyObject(), anyObject(), anyObject()))
        .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    mockNextOpImpl2.init(this.internalTaskContext);
    opImpl.registerNextOperator(mockNextOpImpl2);

    // send a message to this operator
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    opImpl.onMessage(mock(Object.class), mockCollector, mockCoordinator);

    // verify that it propagates its handleMessage results to next operators
    verify(mockNextOpImpl1, times(1)).handleMessageAsync(mockTestOpImplOutput, mockCollector, mockCoordinator);
    verify(mockNextOpImpl2, times(1)).handleMessageAsync(mockTestOpImplOutput, mockCollector, mockCoordinator);
  }

  @Test
  public void testOnMessageUpdatesMetrics() {
    ReadableMetricsRegistry mockMetricsRegistry = mock(ReadableMetricsRegistry.class);
    when(this.context.getContainerContext().getContainerMetricsRegistry()).thenReturn(mockMetricsRegistry);
    Counter mockCounter = mock(Counter.class);
    Timer mockTimer = mock(Timer.class);
    when(mockMetricsRegistry.newCounter(anyString(), anyString())).thenReturn(mockCounter);
    when(mockMetricsRegistry.newTimer(anyString(), anyString())).thenReturn(mockTimer);

    Object mockTestOpImplOutput = mock(Object.class);
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mockTestOpImplOutput);
    opImpl.init(this.internalTaskContext);

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
    Object mockTestOpImplOutput = mock(Object.class);
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mockTestOpImplOutput);
    opImpl.init(this.internalTaskContext);

    // register a couple of operators
    OperatorImpl mockNextOpImpl1 = mock(OperatorImpl.class);
    when(mockNextOpImpl1.getOperatorSpec()).thenReturn(new TestOpSpec());
    when(mockNextOpImpl1.handleMessageAsync(anyObject(), anyObject(), anyObject()))
        .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    mockNextOpImpl1.init(this.internalTaskContext);
    opImpl.registerNextOperator(mockNextOpImpl1);

    OperatorImpl mockNextOpImpl2 = mock(OperatorImpl.class);
    when(mockNextOpImpl2.getOperatorSpec()).thenReturn(new TestOpSpec());
    when(mockNextOpImpl2.handleMessageAsync(anyObject(), anyObject(), anyObject()))
        .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    mockNextOpImpl2.init(this.internalTaskContext);
    opImpl.registerNextOperator(mockNextOpImpl2);

    // send a timer tick to this operator
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    CompletionStage<?> future = opImpl.onTimer(mockCollector, mockCoordinator);
    future.toCompletableFuture().join();

    // verify that it propagates its handleTimer results to next operators
    verify(mockNextOpImpl1, times(1)).handleMessageAsync(mockTestOpImplOutput, mockCollector, mockCoordinator);
    verify(mockNextOpImpl2, times(1)).handleMessageAsync(mockTestOpImplOutput, mockCollector, mockCoordinator);

    // verify that it propagates the timer tick to next operators
    verify(mockNextOpImpl1, times(1)).handleTimer(mockCollector, mockCoordinator);
    verify(mockNextOpImpl2, times(1)).handleTimer(mockCollector, mockCoordinator);
  }

  @Test
  public void testOnTimerUpdatesMetrics() {
    ReadableMetricsRegistry mockMetricsRegistry = mock(ReadableMetricsRegistry.class);
    when(this.context.getContainerContext().getContainerMetricsRegistry()).thenReturn(mockMetricsRegistry);
    Counter mockMessageCounter = mock(Counter.class);
    Timer mockTimer = mock(Timer.class);
    when(mockMetricsRegistry.newCounter(anyString(), anyString())).thenReturn(mockMessageCounter);
    when(mockMetricsRegistry.newTimer(anyString(), anyString())).thenReturn(mockTimer);

    Object mockTestOpImplOutput = mock(Object.class);
    OperatorImpl<Object, Object> opImpl = new TestOpImpl(mockTestOpImplOutput);
    opImpl.init(this.internalTaskContext);

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
    protected void handleInit(Context context) {}

    @Override
    public CompletionStage<Collection<Object>> handleMessageAsync(Object message, MessageCollector collector,
        TaskCoordinator coordinator) {
      return CompletableFuture.completedFuture(Collections.singletonList(mockOutput));
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
    public ScheduledFunction getScheduledFn() {
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

