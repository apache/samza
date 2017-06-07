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
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.HighResolutionClock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * Abstract base class for all stream operator implementations.
 */
public abstract class OperatorImpl<M, RM> {
  private static final String METRICS_GROUP = OperatorImpl.class.getName();

  private boolean initialized;
  private boolean closed;
  private HighResolutionClock highResClock;
  private Counter numMessage;
  private Timer handleMessageNs;
  private Timer handleTimerNs;

  Set<OperatorImpl<RM, ?>> registeredOperators;

  /**
   * Initialize this {@link OperatorImpl} and its user-defined functions.
   *
   * @param config  the {@link Config} for the task
   * @param context  the {@link TaskContext} for the task
   */
  public final void init(Config config, TaskContext context) {
    String opName = getOperatorName();

    if (initialized) {
      throw new IllegalStateException(String.format("Attempted to initialize Operator %s more than once.", opName));
    }

    if (closed) {
      throw new IllegalStateException(String.format("Attempted to initialize Operator %s after it was closed.", opName));
    }

    this.highResClock = createHighResClock(config);
    registeredOperators = new HashSet<>();
    MetricsRegistry metricsRegistry = context.getMetricsRegistry();
    this.numMessage = metricsRegistry.newCounter(METRICS_GROUP, opName + "-messages");
    this.handleMessageNs = metricsRegistry.newTimer(METRICS_GROUP, opName + "-handle-message-ns");
    this.handleTimerNs = metricsRegistry.newTimer(METRICS_GROUP, opName + "-handle-timer-ns");

    handleInit(config, context);

    initialized = true;
  }

  /**
   * Initialize this {@link OperatorImpl} and its user-defined functions.
   *
   * @param config  the {@link Config} for the task
   * @param context  the {@link TaskContext} for the task
   */
  protected abstract void handleInit(Config config, TaskContext context);

  /**
   * Register an operator that this operator should propagate its results to.
   *
   * @param nextOperator  the next operator to propagate results to
   */
  void registerNextOperator(OperatorImpl<RM, ?> nextOperator) {
    if (!initialized) {
      throw new IllegalStateException(
          String.format("Attempted to register next operator before initializing operator %s.",
              getOperatorName()));
    }
    this.registeredOperators.add(nextOperator);
  }

  /**
   * Handle the incoming {@code message} for this {@link OperatorImpl} and propagate results to registered operators.
   * <p>
   * Delegates to {@link #handleMessage(Object, MessageCollector, TaskCoordinator)} for handling the message.
   *
   * @param message  the input message
   * @param collector  the {@link MessageCollector} for this message
   * @param coordinator  the {@link TaskCoordinator} for this message
   */
  public final void onMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    this.numMessage.inc();
    long startNs = this.highResClock.nanoTime();
    Collection<RM> results = handleMessage(message, collector, coordinator);
    long endNs = this.highResClock.nanoTime();
    this.handleMessageNs.update(endNs - startNs);

    results.forEach(rm ->
        this.registeredOperators.forEach(op ->
            op.onMessage(rm, collector, coordinator)));
  }

  /**
   * Handle the incoming {@code message} and return the results to be propagated to registered operators.
   *
   * @param message  the input message
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   * @return  results of the transformation
   */
  protected abstract Collection<RM> handleMessage(M message, MessageCollector collector,
      TaskCoordinator coordinator);

  /**
   * Handle timer ticks for this {@link OperatorImpl} and propagate the results and timer tick to registered operators.
   * <p>
   * Delegates to {@link #handleTimer(MessageCollector, TaskCoordinator)} for handling the timer tick.
   *
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   */
  public final void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    long startNs = this.highResClock.nanoTime();
    Collection<RM> results = handleTimer(collector, coordinator);
    long endNs = this.highResClock.nanoTime();
    this.handleTimerNs.update(endNs - startNs);

    results.forEach(rm ->
        this.registeredOperators.forEach(op ->
            op.onMessage(rm, collector, coordinator)));
    this.registeredOperators.forEach(op ->
        op.onTimer(collector, coordinator));
  }

  /**
   * Handle the the timer tick for this operator and return the results to be propagated to registered operators.
   * <p>
   * Defaults to a no-op implementation.
   *
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   * @return  results of the timed operation
   */
  protected Collection<RM> handleTimer(MessageCollector collector, TaskCoordinator coordinator) {
    return Collections.emptyList();
  }

  public void close() {
    if (closed) {
      throw new IllegalStateException(
          String.format("Attempted to close Operator %s more than once.", getOperatorSpec().getOpName()));
    }
    handleClose();
    closed = true;
  }

  protected abstract void handleClose();

  /**
   * Get the {@link OperatorSpec} for this {@link OperatorImpl}.
   *
   * @return the {@link OperatorSpec} for this {@link OperatorImpl}
   */
  protected abstract OperatorSpec<M, RM> getOperatorSpec();

  /**
   * Get the name for this {@link OperatorImpl}.
   *
   * Some {@link OperatorImpl}s don't have a 1:1 mapping with their {@link OperatorSpec}. E.g., there are
   * 2 PartialJoinOperatorImpls for a JoinOperatorSpec. Overriding this method allows them to provide an
   * implementation specific name, e.g., for use in metrics.
   *
   * @return the operator name
   */
  protected String getOperatorName() {
    return getOperatorSpec().getOpName();
  }

  private HighResolutionClock createHighResClock(Config config) {
    if (new MetricsConfig(config).getMetricsTimerEnabled()) {
      return System::nanoTime;
    } else {
      return () -> 0;
    }
  }
}
