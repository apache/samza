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


/**
 * Implementation class for a {@link Trigger}. A {@link TriggerImpl} is used with a {@link TriggerCallbackHandler}
 * which is invoked when the trigger fires.
 *
 * <p> When MessageEnvelopes arrive in the {@code WindowOperatorImpl}, they are assigned to one or more windows. An
 * instance of a {@link TriggerImpl} is created corresponding to each {@link Trigger} configured for a window. For every
 * MessageEnvelope added to the window, the {@code WindowOperatorImpl} invokes the {@link #onMessage} on its corresponding
 * {@link TriggerImpl}s. A {@link TriggerImpl} instance is scoped to a window and its firing determines when results for
 * its window are emitted.
 *
 * {@link TriggerImpl}s can use the {@link TriggerContext} to schedule and cancel callbacks (for example, implementations
 * of time-based triggers).
 *
 * <p> State management: The state maintained by {@link TriggerImpl}s is not durable across re-starts and is transient.
 * New instances of {@link TriggerImpl} are created on a re-start.
 *
 */
public interface TriggerImpl<M> {

  /**
   * Invoked when a MessageEnvelope added to the window corresponding to this {@link TriggerImpl}.
   * @param message the incoming MessageEnvelope
   */
  public void onMessage(M message, TriggerContext context, TriggerCallbackHandler handler);

  /**
   * Invoked when the execution of this {@link TriggerImpl} is canceled by an up-stream {@link TriggerImpl}.
   * No call to #onMessage() is delivered after cancellation.
   */
  public void onCancel();

  public interface TriggerCallbackHandler {
    public void onTrigger();
  }
}
