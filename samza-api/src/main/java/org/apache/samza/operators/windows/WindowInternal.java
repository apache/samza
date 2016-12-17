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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.triggers.Trigger;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *  Internal representation of a {@link Window}. This specifies default triggers for the {@link Window}, emission
 *  of early or late results and whether to accumulate or discard previous results.
 */

@InterfaceStability.Unstable
public class WindowInternal<M extends MessageEnvelope, K, WV> implements Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> {

  public enum AccumulationMode { ACCUMULATING, DISCARDING }

  private final Trigger defaultTrigger;

  private Trigger earlyTrigger;

  private Trigger lateTrigger;

  /*
   * The function that is applied each time a {@link MessageEnvelope} is added to this window.
   */
  private final BiFunction<M, WV, WV> foldFunction;

  /*
   * The function that extracts the key from a {@link MessageEnvelope}
   */
  private final Function<M, K> keyExtractor;

  /*
   * The function that extracts the event time from a {@link MessageEnvelope}
   */
  private final Function<M, Long> eventTimeExtractor;

  private AccumulationMode mode;

  public WindowInternal(Trigger defaultTrigger, BiFunction<M, WV, WV> foldFunction, Function<M, K> keyExtractor, Function<M, Long> eventTimeExtractor) {
    this.foldFunction = foldFunction;
    this.eventTimeExtractor = eventTimeExtractor;
    this.keyExtractor = keyExtractor;
    this.defaultTrigger = defaultTrigger;
  }

  @Override
  public Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> setEarlyTrigger(Trigger trigger) {
    this.earlyTrigger = trigger;
    return this;
  }

  @Override
  public Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> setLateTrigger(Trigger trigger) {
    this.lateTrigger = trigger;
    return this;
  }

  @Override
  public Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> discardFiredPanes() {
    this.mode = AccumulationMode.DISCARDING;
    return this;
  }

  @Override
  public Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> accumulateFiredPanes() {
    this.mode = AccumulationMode.ACCUMULATING;
    return this;
  }

  public Trigger getDefaultTrigger() {
    return defaultTrigger;
  }

  public Trigger getEarlyTrigger() {
    return earlyTrigger;
  }

  public Trigger getLateTrigger() {
    return lateTrigger;
  }

  public BiFunction<M, WV, WV> getFoldFunction() {
    return foldFunction;
  }

  public Function<M, K> getKeyExtractor() {
    return keyExtractor;
  }

  public Function<M, Long> getEventTimeExtractor() {
    return eventTimeExtractor;
  }

}
