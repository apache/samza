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

package org.apache.samza.scheduler;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-task scheduler for keyed timers.
 * It does the following things:
 * 1) schedules the timer on the {@link ScheduledExecutorService}.
 * 2) keeps track of the timers created and timers that are ready.
 * 3) triggers listener whenever a timer fires.
 */
public class EpochTimeScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(EpochTimeScheduler.class);
  /**
   * For run loop to listen to timer firing so it can schedule the callbacks.
   */
  public interface TimerListener {
    void onTimer();
  }

  private final ScheduledExecutorService executor;
  private final Map<Object, ScheduledFuture> scheduledFutures = new ConcurrentHashMap<>();
  private final Map<TimerKey<?>, ScheduledCallback> readyTimers = new ConcurrentHashMap<>();
  private volatile TimerListener timerListener;

  public static EpochTimeScheduler create(ScheduledExecutorService executor) {
    return new EpochTimeScheduler(executor);
  }

  private EpochTimeScheduler(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  @VisibleForTesting
  Map<Object, ScheduledFuture> getScheduledFutures() {
    return scheduledFutures;
  }

  public <K> void setTimer(K key, long timestamp, ScheduledCallback<K> callback) {
    if (scheduledFutures.containsKey(key)) {
      LOG.warn("Registering duplicate callback for key: {}. Attempting to cancel the previous callback", key);
      ScheduledFuture<?> scheduledFuture = scheduledFutures.get(key);

      /*
       * We can have a race between the time we check for the presence of the key and the time we attempt to cancel;
       * Hence we check for non-null criteria to ensure the executor hasn't kicked off the callback for the key which
       * removes the future from the map before invoking onTimer.
       *  1. In the event that callback is running then we will not attempt to interrupt the action and
       *     cancel will return as unsuccessful.
       *  2. In case of the callback successfully executed, we want to allow duplicate registration to keep the
       *     behavior consistent with the scenario where the callback is already executed or in progress even before
       *     we entered this condition.
       */
      if (scheduledFuture != null
          && !scheduledFuture.cancel(false)
          && !scheduledFuture.isDone()) {
        LOG.debug("Failed to cancel the previous callback successfully. Ignoring the current request to register new callback");
        return;
      }
    }

    final long delay = timestamp - System.currentTimeMillis();
    final ScheduledFuture<?> scheduledFuture = executor.schedule(() -> {
        scheduledFutures.remove(key);
        readyTimers.put(TimerKey.of(key, timestamp), callback);

        if (timerListener != null) {
          timerListener.onTimer();
        }
      }, delay > 0 ? delay : 0, TimeUnit.MILLISECONDS);
    scheduledFutures.put(key, scheduledFuture);
  }

  public <K> void deleteTimer(K key) {
    final ScheduledFuture<?> scheduledFuture = scheduledFutures.remove(key);
    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
    }
  }

  public void registerListener(TimerListener listener) {
    timerListener = listener;

    if (!readyTimers.isEmpty()) {
      timerListener.onTimer();
    }
  }

  public Map<TimerKey<?>, ScheduledCallback> removeReadyTimers() {
    final Map<TimerKey<?>, ScheduledCallback> timers = new TreeMap<>(readyTimers);
    // Remove keys on the map directly instead of using key set iterator and remove all
    // on the key set as it results in duplicate firings due to weakly consistent SetView
    for (TimerKey<?> key : timers.keySet()) {
      readyTimers.remove(key);
    }
    return timers;
  }

  public static class TimerKey<K> implements Comparable<TimerKey<K>> {
    private final K key;
    private final long time;

    static <K> TimerKey<K> of(K key, long time) {
      return new TimerKey<>(key, time);
    }

    private TimerKey(K key, long time) {
      this.key = key;
      this.time = time;
    }

    public K getKey() {
      return key;
    }

    public long getTime() {
      return time;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TimerKey<?> timerKey = (TimerKey<?>) o;
      if (time != ((TimerKey<?>) o).time) {
        return false;
      }
      return key.equals(timerKey.key);
    }

    @Override
    public int hashCode() {
      int result = key.hashCode();
      result = 31 * result + Long.valueOf(time).hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "TimerKey{"
          + "key=" + key
          + ", time='" + time + '\''
          + '}';
    }

    @Override
    public int compareTo(TimerKey<K> o) {
      final int timeCompare = Long.compare(time, o.time);
      if (timeCompare != 0) {
        return timeCompare;
      }

      return key.hashCode() - o.key.hashCode();
    }
  }
}
