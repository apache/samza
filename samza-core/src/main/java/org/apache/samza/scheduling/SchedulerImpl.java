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
package org.apache.samza.scheduling;

import org.apache.samza.task.SystemTimerScheduler;
import org.apache.samza.task.TimerCallback;


/**
 * Delegates to {@link SystemTimerScheduler}. This is useful because it provides a write-only interface for user-facing
 * purposes.
 */
public class SchedulerImpl implements Scheduler {
  private final SystemTimerScheduler systemTimerScheduler;

  public SchedulerImpl(SystemTimerScheduler systemTimerScheduler) {
    this.systemTimerScheduler = systemTimerScheduler;
  }

  @Override
  public <K> void scheduleCallback(K key, long timestamp, TimerCallback<K> callback) {
    this.systemTimerScheduler.setTimer(key, timestamp, callback);
  }

  @Override
  public <K> void deleteCallback(K key) {
    this.systemTimerScheduler.deleteTimer(key);
  }
}
