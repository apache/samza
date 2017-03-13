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

import org.apache.samza.util.Clock;

/**
 * Implementation class for a {@link TimeSinceLastMessageTrigger}
 * @param <M> the type of the incoming message
 */
public class TimeSinceLastMessageTriggerImpl<M> implements TriggerImpl<M> {

  private final TimeSinceLastMessageTrigger<M> trigger;
  private final long durationMs;
  private final Clock clock;
  private long callbackTime = Integer.MIN_VALUE;
  private Cancellable cancellable = null;
  private boolean shouldFire = false;

  public TimeSinceLastMessageTriggerImpl(TimeSinceLastMessageTrigger<M> trigger, Clock clock) {
    this.trigger = trigger;
    this.durationMs = trigger.getDuration().toMillis();
    this.clock = clock;
  }

  @Override
  public void onMessage(M message, TriggerContext context) {
    if (!shouldFire) {
      long currTime = clock.currentTimeMillis();

      if (currTime < callbackTime && cancellable != null) {
        cancellable.cancel();
      }

      callbackTime = currTime + durationMs;
      Runnable runnable = () -> {
        shouldFire = true;
      };

      cancellable = context.scheduleCallback(runnable, callbackTime);
    }
  }

  @Override
  public void cancel() {
    if (cancellable != null) {
      cancellable.cancel();
    }
  }

  @Override
  public boolean shouldFire() {
    return shouldFire;
  }
}
