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

import org.apache.samza.operators.triggers.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;

/**
 * Allows to schedule and cancel callbacks.
 */
public class TriggerScheduler<WK> {

  private static final Logger LOG = LoggerFactory.getLogger(TriggerScheduler.class);

  private final PriorityQueue<TriggerCallbackState<WK>> pendingCallbacks;

  public TriggerScheduler(PriorityQueue<TriggerCallbackState<WK>> pendingCallbacks) {
    this.pendingCallbacks = pendingCallbacks;
  }

  public Cancellable scheduleCallback(Runnable runnable, long callbackTimeMs, TriggerKey<WK> triggerKey) {
    TriggerCallbackState<WK> timerState = new TriggerCallbackState(triggerKey, runnable, callbackTimeMs);
    pendingCallbacks.add(timerState);
    LOG.trace("Scheduled a new callback: {} at {} for triggerKey {}", new Object[] {runnable, callbackTimeMs, triggerKey});
    return timerState;
  }

  /**
   * State corresponding to pending timer callbacks scheduled by various triggers.
   */
  class TriggerCallbackState<WK> implements Comparable<TriggerCallbackState<WK>>, Cancellable {

    private final TriggerKey<WK> triggerKey;
    private final Runnable callback;

    // the time at which the callback should trigger
    private final long scheduledTimeMs;

    public TriggerCallbackState(TriggerKey<WK> triggerKey, Runnable callback, long scheduledTimeMs) {
      this.triggerKey = triggerKey;
      this.callback = callback;
      this.scheduledTimeMs = scheduledTimeMs;
    }

    public Runnable getCallback() {
      return callback;
    }

    public long getScheduledTimeMs() {
      return scheduledTimeMs;
    }

    public TriggerKey<WK> getTriggerKey() {
      return triggerKey;
    }

    @Override
    public int compareTo(TriggerCallbackState<WK> other) {
      return Long.compare(this.scheduledTimeMs, other.scheduledTimeMs);
    }

    @Override
    public boolean cancel() {
      LOG.trace("Cancelled a callback: {} at {} for triggerKey {}", new Object[] {callback, scheduledTimeMs, triggerKey});
      return pendingCallbacks.remove(this);
    }
  }
}
