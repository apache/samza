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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Per task scheduler factory
 */
public class SystemTimerSchedulerFactory {

  /**
   * interface for run loop to listen to timer firing
   */
  public interface TimerListener {
    void onTimer();
  }

  private final ScheduledExecutorService executor;
  private final Map<Object, SystemTimerScheduler> schedulers = new HashMap<>();
  private final Map<Object, TimerCallback> readyTimers = new ConcurrentHashMap<>();
  private TimerListener timerListener;

  public static SystemTimerSchedulerFactory create(ScheduledExecutorService executor) {
    return new SystemTimerSchedulerFactory(executor);
  }

  private SystemTimerSchedulerFactory(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  public<K> SystemTimerScheduler<K> getScheduler(K key) {
    SystemTimerScheduler<K> scheduler = schedulers.get(key);
    if (scheduler == null) {
      scheduler = new SystemTimerScheduler<>(key);
      schedulers.put(key, scheduler);
    }
    return scheduler;
  }

  public<K> void removeScheduler(K key) {
    final SystemTimerScheduler<K> scheduler = schedulers.get(key);
    if (scheduler != null) {
      scheduler.cancel();
      schedulers.remove(key);
    }
  }

  void registerListener(TimerListener listener) {
    timerListener = listener;
  }

  public Map<Object, TimerCallback> removeReadyTimers() {
    final Map<Object, TimerCallback> timers = new HashMap<>(readyTimers);
    readyTimers.keySet().removeAll(timers.keySet());
    return timers;
  }

  public class SystemTimerScheduler<K> {
    private final K key;
    private ScheduledFuture<?> scheduledFuture;

    private SystemTimerScheduler(K key) {
      this.key = key;
    }

    public void schedule(long delay, TimerCallback<K> callback) {
      scheduledFuture = executor.schedule(() -> {
        readyTimers.put(key, callback);

        if (timerListener != null) {
          timerListener.onTimer();
        }
      }, delay, TimeUnit.MILLISECONDS);
    }

    public void cancel() {
      if (scheduledFuture != null) {
        scheduledFuture.cancel(false);
      }
    }
  }
}
