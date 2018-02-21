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

package org.apache.samza.task;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Per-task factory for keyed timer schedulers.
 * It does the following things:
 * 1) creates timer scheduler based on a given key.
 * 2) keeps track of the schedulers created and timers that are ready.
 * 3) triggers listener whenever a timer fires.
 */
public class SystemTimerSchedulerFactory {

  /**
   * For run loop to listen to timer firing so it can schedule the callbacks.
   */
  public interface TimerListener {
    void onTimer();
  }

  private final ScheduledExecutorService executor;
  private final Map<Object, SystemTimerScheduler> schedulers = new HashMap<>();
  private final Map<TimerKey<?>, TimerCallback> readyTimers = new ConcurrentHashMap<>();
  private TimerListener timerListener;

  public static SystemTimerSchedulerFactory create(ScheduledExecutorService executor) {
    return new SystemTimerSchedulerFactory(executor);
  }

  private SystemTimerSchedulerFactory(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  public <K> SystemTimerScheduler<K> getScheduler(K key) {
    SystemTimerScheduler<K> scheduler = schedulers.get(key);
    if (scheduler == null) {
      scheduler = new SystemTimerScheduler<>(key);
      schedulers.put(key, scheduler);
    }
    return scheduler;
  }

  public <K> void removeScheduler(K key) {
    final SystemTimerScheduler<K> scheduler = schedulers.get(key);
    if (scheduler != null) {
      scheduler.cancel();
      schedulers.remove(key);
    }
  }

  void registerListener(TimerListener listener) {
    timerListener = listener;
  }

  public Map<TimerKey<?>, TimerCallback> removeReadyTimers() {
    final Map<TimerKey<?>, TimerCallback> timers = new TreeMap<>(readyTimers);
    readyTimers.keySet().removeAll(timers.keySet());
    return timers;
  }

  public class SystemTimerScheduler<K> {
    private final K key;
    private ScheduledFuture<?> scheduledFuture;

    private SystemTimerScheduler(K key) {
      this.key = key;
    }

    public void schedule(long time, TimerCallback<K> callback) {
      final long delay = time - System.currentTimeMillis();
      scheduledFuture = executor.schedule(() -> {
          readyTimers.put(TimerKey.of(key, time), callback);

          if (timerListener != null) {
            timerListener.onTimer();
          }
        }, delay > 0 ? delay : 0, TimeUnit.MILLISECONDS);
    }

    public void cancel() {
      if (scheduledFuture != null) {
        scheduledFuture.cancel(false);
      }
    }
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
      return key != null ? key.equals(timerKey.key) : timerKey.key == null;
    }

    @Override
    public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
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

      if (key == null) {
        return o.key == null ? 0 : -1;
      }

      if (o.key == null) {
        return 1;
      }

      return key.hashCode() - o.key.hashCode();
    }

  }
}
