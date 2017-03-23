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

import org.apache.samza.operators.impl.TriggerKey;
import org.apache.samza.operators.impl.TriggerScheduler;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation class for a {@link TimeSinceLastMessageTrigger}
 * @param <M> the type of the incoming message
 */
public class TimeSinceLastMessageTriggerImpl<M, WK> implements TriggerImpl<M, WK> {

  private static final Logger LOG = LoggerFactory.getLogger(TimeSinceLastMessageTriggerImpl.class);
  private final TimeSinceLastMessageTrigger<M> trigger;
  private final long durationMs;
  private final Clock clock;
  private final TriggerKey<WK> triggerKey;
  private long callbackTime = Integer.MIN_VALUE;
  private Cancellable cancellable = null;
  private boolean shouldFire = false;

  public TimeSinceLastMessageTriggerImpl(TimeSinceLastMessageTrigger<M> trigger, Clock clock, TriggerKey<WK> key) {
    this.trigger = trigger;
    this.durationMs = trigger.getDuration().toMillis();
    this.clock = clock;
    this.triggerKey = key;
  }

  @Override
  public void onMessage(M message, TriggerScheduler<WK> context) {
    if (!shouldFire) {
      long currTime = clock.currentTimeMillis();

      if (currTime < callbackTime && cancellable != null) {
        cancellable.cancel();
      }

      callbackTime = currTime + durationMs;
      Runnable runnable = () -> {
        LOG.trace("Time since last message trigger fired");
        shouldFire = true;
      };

      cancellable = context.scheduleCallback(runnable, callbackTime, triggerKey);
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
