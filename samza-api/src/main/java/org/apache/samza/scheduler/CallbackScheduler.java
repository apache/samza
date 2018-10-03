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

/**
 * Provides a way for applications to register some logic to be executed at a future time.
 */
public interface CallbackScheduler {
  /**
   * Schedule the {@code callback} for the provided {@code key} to be invoked at epoch-time {@code timestamp}.
   * The callback will be invoked exclusively with any other operations for this task, e.g. processing, windowing, and
   * commit.
   * @param key callback key
   * @param timestamp epoch time when the callback will be fired, in milliseconds
   * @param callback callback to run
   * @param <K> type of the key
   */
  <K> void scheduleCallback(K key, long timestamp, ScheduledCallback<K> callback);

  /**
   * Delete the scheduled {@code callback} for the {@code key}.
   * Deletion only happens if the callback hasn't been fired. Otherwise it will not interrupt.
   * @param key callback key
   * @param <K> type of the key
   */
  <K> void deleteCallback(K key);
}
