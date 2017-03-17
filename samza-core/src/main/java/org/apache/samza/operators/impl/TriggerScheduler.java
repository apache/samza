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
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Allows to schedule and cancel callbacks for triggers.
 */
public class TriggerScheduler<WK> {

  private static final Logger LOG = LoggerFactory.getLogger(TriggerScheduler.class);

  private final PriorityQueue<TriggerCallbackState<WK>> pendingCallbacks;
  private final Clock clock;

  public TriggerScheduler(Clock clock) {
    this.pendingCallbacks = new PriorityQueue<>();
    this.clock = clock;
  }

  /**
   * Schedule the provided runnable for execution at the specified duration.
   * @param runnable the provided runnable to schedule.
   * @param scheduledTimeMs time at which the runnable must be scheduled for execution
   * @param triggerKey a key that uniquely identifies the corresponding trigger firing.
   * @return a {@link Cancellable} that can be used to cancel the execution of this runnable.
   */
  public Cancellable scheduleCallback(Runnable runnable, long scheduledTimeMs, TriggerKey<WK> triggerKey) {
    TriggerCallbackState<WK> timerState = new TriggerCallbackState(triggerKey, runnable, scheduledTimeMs);
    pendingCallbacks.add(timerState);
    LOG.trace("Scheduled a new callback: {} at {} for triggerKey {}", new Object[] {runnable, scheduledTimeMs, triggerKey});
    return timerState;
  }

  /**
   * Run all pending callbacks that are ready to be scheduled. A callback is defined as "ready" if it's scheduledTime
   * is less than or equal to {@link Clock#currentTimeMillis()}
   *
   * @return the list of {@link TriggerKey}s corresponding to the callbacks that were run.
   */
  public List<TriggerKey<WK>> runPendingCallbacks() {
    TriggerCallbackState<WK> state;
    List<TriggerKey<WK>> keys = new ArrayList<>();
    long now = clock.currentTimeMillis();

    while ((state = pendingCallbacks.peek()) != null && state.getScheduledTimeMs() <= now) {
      pendingCallbacks.remove();
      state.getCallback().run();
      TriggerKey<WK> key = state.getTriggerKey();
      keys.add(key);
    }
    return keys;
  }

  /**
   * State corresponding to pending timer callbacks scheduled by various triggers.
   */
  private class TriggerCallbackState<WK> implements Comparable<TriggerCallbackState<WK>>, Cancellable {

    private final TriggerKey<WK> triggerKey;
    private final Runnable callback;

    // the time at which the callback should trigger
    private final long scheduledTimeMs;

    private TriggerCallbackState(TriggerKey<WK> triggerKey, Runnable callback, long scheduledTimeMs) {
      this.triggerKey = triggerKey;
      this.callback = callback;
      this.scheduledTimeMs = scheduledTimeMs;
    }

    private Runnable getCallback() {
      return callback;
    }

    private long getScheduledTimeMs() {
      return scheduledTimeMs;
    }

    private TriggerKey<WK> getTriggerKey() {
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
