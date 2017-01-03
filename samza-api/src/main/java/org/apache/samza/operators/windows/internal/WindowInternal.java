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
package org.apache.samza.operators.windows.internal;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *  Internal representation of a {@link Window}. This specifies default, early and late triggers for the {@link Window}
 *  and whether to accumulate or discard previously emitted panes.
 *
 *  Note: This class is meant to be used internally by Samza, and is not to be instantiated by programmers.
 */
@InterfaceStability.Unstable
public final class WindowInternal<M extends MessageEnvelope, K, WV> implements Window<M, K, WV, WindowPane<K, WV>> {

  private final Trigger defaultTrigger;

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

  private Trigger earlyTrigger;

  private Trigger lateTrigger;

  private AccumulationMode mode;

  public WindowInternal(Trigger defaultTrigger, BiFunction<M, WV, WV> foldFunction, Function<M, K> keyExtractor, Function<M, Long> eventTimeExtractor) {
    this.foldFunction = foldFunction;
    this.eventTimeExtractor = eventTimeExtractor;
    this.keyExtractor = keyExtractor;
    this.defaultTrigger = defaultTrigger;
  }

  @Override
  public Window<M, K, WV, WindowPane<K, WV>> setEarlyTrigger(Trigger trigger) {
    this.earlyTrigger = trigger;
    return this;
  }

  @Override
  public Window<M, K, WV, WindowPane<K, WV>> setLateTrigger(Trigger trigger) {
    this.lateTrigger = trigger;
    return this;
  }

  @Override
  public Window<M, K, WV, WindowPane<K, WV>> setAccumulationMode(AccumulationMode mode) {
    this.mode = mode;
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
