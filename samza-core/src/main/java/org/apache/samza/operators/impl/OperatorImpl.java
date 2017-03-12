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

import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.HashSet;
import java.util.Set;


/**
 * Abstract base class for all stream operator implementations.
 */
public abstract class OperatorImpl<M, RM> {

  private final Set<OperatorImpl<RM, ?>> nextOperators = new HashSet<>();

  /**
   * Register the next operator in the chain that this operator should propagate its output to.
   * @param nextOperator  the next operator in the chain.
   */
  void registerNextOperator(OperatorImpl<RM, ?> nextOperator) {
    nextOperators.add(nextOperator);
  }

  /**
   * Perform the transformation required for this operator and call the downstream operators.
   *
   * Must call {@link #propagateResult} to propage the output to registered downstream operators correctly.
   *
   * @param message  the input message
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   */
  public abstract void onNext(M message, MessageCollector collector, TaskCoordinator coordinator);

  /**
   * Invoked at every tick. This method delegates to {@link #onTimer(MessageCollector, TaskCoordinator)}
   *
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   */
  public final void onTick(MessageCollector collector, TaskCoordinator coordinator) {
    onTimer(collector, coordinator);
    nextOperators.forEach(sub -> sub.onTick(collector, coordinator));
  }

  /**
   * Invoked at every tick. Implementations must call {@link #propagateResult} to propagate any generated output
   * to registered downstream operators.
   *
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   */
  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {};

  /**
   * Helper method to propagate the output of this operator to all registered downstream operators.
   *
   * This method <b>must</b> be called from {@link #onNext} and {@link #onTimer}
   * to propagate the operator output correctly.
   *
   * @param outputMessage  output message
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   */
  void propagateResult(RM outputMessage, MessageCollector collector, TaskCoordinator coordinator) {
    nextOperators.forEach(sub -> sub.onNext(outputMessage, collector, coordinator));
  }
}
