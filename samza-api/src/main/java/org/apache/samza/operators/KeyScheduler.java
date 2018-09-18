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
 * Manages scheduling keys to be triggered later. See {@link org.apache.samza.operators.functions.SchedulingFunction}
 * for details.
 * @param <K> type of the key
 */
public interface KeyScheduler<K> {

  /**
   * Schedule a key to be triggered at {@code timestamp}.
   * @param key unique key
   * @param timestamp epoch time when the key will be triggered, in milliseconds
   */
  void schedule(K key, long timestamp);

  /**
   * Delete the scheduler entry for the provided key.
   * @param key key to delete
   */
  void delete(K key);
}
