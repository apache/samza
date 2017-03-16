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

import org.apache.samza.operators.impl.TriggerContext;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation class for a {@link TimeSinceFirstMessageTrigger}
 * @param <M> the type of the incoming message
 */
public class TimeSinceFirstMessageTriggerImpl<M, WK> implements TriggerImpl<M, WK> {

  private static final Logger LOG = LoggerFactory.getLogger(TimeSinceFirstMessageTriggerImpl.class);

  private final TimeSinceFirstMessageTrigger<M> trigger;
  private final Clock clock;
  private Cancellable cancellable;
  private boolean shouldFire = false;

  public TimeSinceFirstMessageTriggerImpl(TimeSinceFirstMessageTrigger<M> trigger, Clock clock) {
    this.trigger = trigger;
    this.clock = clock;
  }

  public void onMessage(M message, TriggerContext<WK> context) {
    if (cancellable == null && !shouldFire) {
      final long now = clock.currentTimeMillis();
      long triggerDurationMs = trigger.getDuration().toMillis();
      Long callbackTime = now + triggerDurationMs;
      cancellable =  context.scheduleCallback(() -> {
          LOG.trace("Time since first message trigger fired");
          shouldFire = true;
        }, callbackTime);
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
