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
 * Allows registering epoch-time timer callbacks from the operators.
 * See {@link org.apache.samza.operators.functions.TimerFunction} for details.
 * @param <K> type of the timer key
 */
public interface TimerRegistry<K> {

  /**
   * Register a epoch-time timer with key.
   * @param key unique timer key
   * @param timestamp epoch time when the timer will be fired, in milliseconds
   */
  void register(K key, long timestamp);

  /**
   * Delete the timer for the provided key.
   * @param key key for the timer to delete
   */
  void delete(K key);
}
