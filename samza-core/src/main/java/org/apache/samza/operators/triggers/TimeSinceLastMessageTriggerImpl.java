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

import org.apache.samza.operators.data.MessageEnvelope;

/**
 * Implementation class for a {@link TimeSinceLastMessageTrigger}
 * @param <M>
 */
public class TimeSinceLastMessageTriggerImpl<M extends MessageEnvelope> implements TriggerImpl<M> {

  private final TimeSinceLastMessageTrigger trigger;
  private final long durationMs;
  private long callbackTime = Integer.MIN_VALUE;
  private Cancellable latestFuture = null;

  public TimeSinceLastMessageTriggerImpl(TimeSinceLastMessageTrigger<M> trigger) {
    this.trigger = trigger;
    this.durationMs = trigger.getDuration().toMillis();
  }


  @Override
  public void onMessage(M message, TriggerContext context, TriggerCallbackHandler handler) {

    long currTime = System.currentTimeMillis();

    if (currTime < callbackTime && latestFuture != null) {
      latestFuture.cancel();
    }

    callbackTime = currTime + durationMs;
    Runnable runnable = () -> {
      handler.onTrigger();
    };

    latestFuture = context.scheduleCallback(runnable, callbackTime);
  }

  @Override
  public void onCancel() {
    latestFuture.cancel();
  }
}
