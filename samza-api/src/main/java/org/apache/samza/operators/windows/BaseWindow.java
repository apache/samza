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
package org.apache.samza.operators.windows;

import org.apache.samza.operators.data.MessageEnvelope;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *  The base class of all types of {@link Window}s. Sub-classes can specify the default triggering semantics
 *  for the {@link Window}, semantics for emission of early or late results and whether to accumulate or discard
 *  previous results.
 */

public class BaseWindow<M extends MessageEnvelope, K, WV> implements Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> {

  /**
   * Defines the default triggering semantics for the {@link Window}.
   */
  private final List<TriggersBuilder.Trigger> defaultTriggers;


  /**
   * Defines the triggering semantics for emission of early or late results.
   */
  private List<TriggersBuilder.Trigger> earlyTriggers;
  private List<TriggersBuilder.Trigger> lateTriggers;

  /**
   * Defines the fold function that is applied each time a {@link MessageEnvelope} is added to this window.
   */
  private final BiFunction<M, WV, WV> aggregator;

  /*
   * Defines the function that extracts the event time from a {@link MessageEnvelope}
   */
  private final Function<M, Long> eventTimeExtractor;

  /*
   * Defines the function that extracts the key from a {@link MessageEnvelope}
   */
  private final Function<M, K> keyExtractor;


  public BaseWindow(Function<M, K> keyExtractor, BiFunction<M, WV, WV> aggregator, Function<M, Long> eventTimeExtractor, List<TriggersBuilder.Trigger> defaultTriggers) {
    this.aggregator = aggregator;
    this.eventTimeExtractor = eventTimeExtractor;
    this.keyExtractor = keyExtractor;
    this.defaultTriggers = defaultTriggers;
  }

  @Override
  public Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> setTriggers(TriggersBuilder.Triggers wndTrigger) {
    this.earlyTriggers = wndTrigger.getEarlyTriggers();
    this.lateTriggers = wndTrigger.getLateTriggers();
    return this;
  }

  public List<TriggersBuilder.Trigger> getDefaultTriggers() {
    return defaultTriggers;
  }

  public List<TriggersBuilder.Trigger> getEarlyTriggers() {
    return earlyTriggers;
  }

  public List<TriggersBuilder.Trigger> getLateTriggers() {
    return lateTriggers;
  }

  public BiFunction<M, WV, WV> getAggregator() {
    return aggregator;
  }

  public Function<M, Long> getEventTimeExtractor() {
    return eventTimeExtractor;
  }

  public Function<M, K> getKeyExtractor() {
    return keyExtractor;
  }
}
