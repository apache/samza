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
 * A registry that allows user to register arbitrary system-clock timers in the operators. It's accessible
 * from the {@link OpContext} in {@link org.apache.samza.operators.functions.InitableFunction#init(org.apache.samza.config.Config, OpContext)}.
 * User needs to implement {@link org.apache.samza.operators.functions.TimerFunction} to receive timer firings.
 * See {@link org.apache.samza.operators.functions.TimerFunction} for details.
 * @param <K> type of the timer key
 */
public interface TimerRegistry<K> {

  /**
   * Register a processing-time timer with key.
   * @param key key of the timer
   * @param time time when the timer will fire, in milliseconds
   */
  void register(K key, long time);

  /**
   * Delete the timer of key from the registry.
   * @param key key of the timer.
   */
  void delete(K key);
}
