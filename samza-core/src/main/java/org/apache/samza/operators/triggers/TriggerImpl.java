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

package org.apache.samza.operators.triggers;


import org.apache.samza.operators.impl.TriggerScheduler;

/**
 * Implementation class for a {@link Trigger}. A {@link TriggerImpl} is used with a
 * which is invoked when the trigger fires.
 *
 * <p> When messages arrive in the {@code WindowOperatorImpl}, they are assigned to one or more windows. An
 * instance of a {@link TriggerImpl} is created corresponding to each {@link Trigger} configured for a window. For every
 * message added to the window, the {@code WindowOperatorImpl} invokes the {@link #onMessage} on its corresponding
 * {@link TriggerImpl}s. A {@link TriggerImpl} instance is scoped to a window and its firing determines when results for
 * its window are emitted.
 *
 * {@link TriggerImpl}s can use the {@link TriggerScheduler} to schedule and cancel callbacks (for example, implementations
 * of time-based triggers).
 *
 * <p> State management: The state maintained by {@link TriggerImpl}s is not durable across re-starts and is transient.
 * New instances of {@link TriggerImpl} are created on a re-start.
 *
 */
public interface TriggerImpl<M, WK> {

  /**
   * Invoked when a message is added to the window corresponding to this {@link TriggerImpl}.
   * @param message the incoming message
   * @param context the {@link TriggerScheduler} to schedule and cancel callbacks
   */
  public void onMessage(M message, TriggerScheduler<WK> context);

  /**
   * Returns {@code true} if the current state of the trigger indicates that its condition
   * is satisfied and it is ready to fire.
   * @return if this trigger should fire.
   */
  public boolean shouldFire();

  /**
   * Invoked when the execution of this {@link TriggerImpl} is canceled by an up-stream {@link TriggerImpl}.
   *
   * No calls to {@link #onMessage(Object, TriggerScheduler)} or {@link #shouldFire()} will be invoked
   * after this invocation.
   */
  public void cancel();

}
