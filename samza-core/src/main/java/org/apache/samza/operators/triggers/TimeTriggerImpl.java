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
 * Implementation class for a {@link TimeTrigger}
 */
public class TimeTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {

  private final TimeTrigger<M> trigger;
  private Cancellable latestFuture;

  public TimeTriggerImpl(TimeTrigger<M> trigger, TriggerContext context, TriggerCallbackHandler handler) {
    super(context, handler);
    this.trigger = trigger;
  }

  public void onMessage(M message) {

    final long now = System.currentTimeMillis();
    long triggerDurationMs = trigger.getDuration().toMillis();
    Long callbackTime = (now - now % triggerDurationMs) + triggerDurationMs;

    if (latestFuture == null) {
      latestFuture =  context.scheduleCallback(() -> {
          handler.onTrigger(TimeTriggerImpl.this, context.getWindowKey());
        }, callbackTime);
    }
  }

  @Override
  public void onCancel() {
    latestFuture.cancel();
  }
}
