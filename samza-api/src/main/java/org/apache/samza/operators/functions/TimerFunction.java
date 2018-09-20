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

package org.apache.samza.operators.functions;

import org.apache.samza.operators.TimerRegistry;

import java.util.Collection;

/**
 * Allows timer registration with a key and is invoked when the timer is fired.
 * Key must be a unique identifier for this timer, and is provided in the callback when the timer fires.
 *
 * <p>
 * Example of a {@link FlatMapFunction} with timer:
 * <pre>{@code
 *    public class ExampleTimerFn implements FlatMapFunction<String, String>, TimerFunction<String, String> {
 *      public void registerTimer(TimerRegistry timerRegistry) {
 *        long time = System.currentTimeMillis() + 5000; // fire after 5 sec
 *        timerRegistry.register("example-timer", time);
 *      }
 *      public Collection<String> apply(String s) {
 *        ...
 *      }
 *      public Collection<String> onTimer(String key, long timestamp) {
 *        // example-timer fired
 *        ...
 *      }
 *    }
 * }</pre>
 * @param <K> type of the key
 * @param <OM> type of the output
 */
public interface TimerFunction<K, OM> {

  /**
   * Registers any epoch-time timers using the registry
   * @param timerRegistry a keyed {@link TimerRegistry}
   */
  void registerTimer(TimerRegistry<K> timerRegistry);

  /**
   * Returns the output after the timer with key fires.
   * @param key timer key
   * @param timestamp time of the epoch-time timer fired, in milliseconds
   * @return {@link Collection} of output elements
   */
  Collection<OM> onTimer(K key, long timestamp);
}
