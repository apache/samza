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

import org.apache.samza.operators.Scheduler;

import java.util.Collection;


/**
 * Allows scheduling with key(s) and is invoked when the specified time(s) occurs.
 * Key must be a unique identifier for its corresponding logic to execute, and is provided in the callback when the
 * corresponding schedule time occurs.
 *
 * <p>
 * Example of a {@link FlatMapFunction} with {@link ScheduledFunction}:
 * <pre>{@code
 *    public class ExampleScheduledFn implements FlatMapFunction<String, String>, ScheduledFunction<String, String> {
 *      public void schedule(Scheduler scheduler) {
 *        long time = System.currentTimeMillis() + 5000; // fire after 5 sec
 *        scheduler.schedule("example-scheduler-logic", time);
 *      }
 *      public Collection<String> apply(String s) {
 *        ...
 *      }
 *      public Collection<String> onCallback(String key, long timestamp) {
 *        // fired with key as "example-scheduler-logic"
 *        ...
 *      }
 *    }
 * }</pre>
 * @param <K> type of the key
 * @param <OM> type of the output
 */
public interface ScheduledFunction<K, OM> {

  /**
   * Initialize the function for scheduling, such as setting some initial scheduling logic or saving the
   * {@code scheduler} for later use.
   * @param scheduler used to specify the schedule time(s) and key(s)
   */
  void schedule(Scheduler<K> scheduler);

  /**
   * Returns the output from the scheduling logic corresponding to the key that was triggered.
   * @param key key corresponding to the scheduling logic that got triggered
   * @param timestamp schedule time that was set for the key, in milliseconds since epoch
   * @return {@link Collection} of output elements
   */
  Collection<OM> onCallback(K key, long timestamp);
}
