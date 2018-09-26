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

package org.apache.samza.operators;

/**
 * Allows scheduling {@link org.apache.samza.operators.functions.ScheduledFunction} callbacks to be invoked later.
 * @param <K> type of the key to schedule
 */
public interface Scheduler<K> {
  /**
   * Schedule a callback for the {@code key} to be invoked at {@code timestamp}.
   * @param key unique key associated with the callback to schedule
   * @param timestamp epoch time when the callback for the key will be invoked, in milliseconds
   */
  void schedule(K key, long timestamp);

  /**
   * Delete the scheduled callback for the provided {@code key}.
   * @param key key to delete
   */
  void delete(K key);
}
