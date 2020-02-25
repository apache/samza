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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/**
 * Per-task scheduler for keyed timers.
 * It does the following things:
 * 1) schedules the timer on the {@link ScheduledExecutorService}.
 * 2) keeps track of the timers created and timers that are ready.
 * 3) triggers listener whenever a timer fires.
 */
public class EpochTimeScheduler {

  /**
   * For run loop to listen to timer firing so it can schedule the callbacks.
   */
  public interface TimerListener {
    void onTimer();
  }

  private final ScheduledExecutorService executor;
  private final Map<Object, List<ScheduledFuture>> scheduledFutures = new ConcurrentHashMap<>();
  private final Map<TimerKey<?>, List<ScheduledCallback>> readyTimers = new ConcurrentHashMap<>();
  private volatile TimerListener timerListener;

  public static EpochTimeScheduler create(ScheduledExecutorService executor) {
    return new EpochTimeScheduler(executor);
  }

  private EpochTimeScheduler(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  public <K> void setTimer(K key, long timestamp, ScheduledCallback<K> callback) {

    final long delay = timestamp - System.currentTimeMillis();
    final ScheduledFuture<?> scheduledFuture = executor.schedule(() -> {
        scheduledFutures.remove(key);
        TimerKey timer = TimerKey.of(key, timestamp);
        if (readyTimers.containsKey(timer)) {
          List<ScheduledCallback> scheduledCallbackList = readyTimers.get(timer);
          scheduledCallbackList.add(callback);
          readyTimers.put(timer, scheduledCallbackList);
        } else {
          List<ScheduledCallback> scheduledCallbackList = Collections.singletonList(callback);
          readyTimers.put(timer, scheduledCallbackList);
        }

        if (timerListener != null) {
          timerListener.onTimer();
        }
      }, delay > 0 ? delay : 0, TimeUnit.MILLISECONDS);

    if (scheduledFutures.containsKey(key)) {
      List<ScheduledFuture> scheduledFutureList = scheduledFutures.get(key);
      scheduledFutureList.add(scheduledFuture);
      scheduledFutures.put(key, scheduledFutureList);
    } else {
      List<ScheduledFuture> scheduledFutureList = Collections.singletonList(scheduledFuture);
      scheduledFutures.put(key, scheduledFutureList);
    }
  }

  public <K> void deleteTimer(K key) {
    final List<ScheduledFuture> scheduledFuture = scheduledFutures.remove(key);
    for (ScheduledFuture s: scheduledFuture) {
      if (s != null) {
        s.cancel(false);
      }
    }
  }

  public void registerListener(TimerListener listener) {
    timerListener = listener;

    if (!readyTimers.isEmpty()) {
      timerListener.onTimer();
    }
  }

  public Map<TimerKey<?>, List<ScheduledCallback>> removeReadyTimers() {
    final Map<TimerKey<?>, List<ScheduledCallback>> timers = new TreeMap<>(readyTimers);
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
