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
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SupplierFunction;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.Window;


/**
 *  Internal representation of a {@link Window}. This specifies default, early and late triggers for the {@link Window}
 *  and whether to accumulate or discard previously emitted panes.
 *
 *  Note: This class is meant to be used internally by Samza, and is not to be instantiated by programmers.
 *
 * @param <M>  the type of input message
 * @param <WK>  the type of key for the window
 * @param <WV>  the type of aggregated value in the window output
 */
@InterfaceStability.Unstable
public final class WindowInternal<M, WK, WV> implements Window<M, WK, WV> {

  private final Trigger<M> defaultTrigger;

  /**
   * The supplier of initial value to be used for windowed aggregations
   */
  private final SupplierFunction<WV> initializer;

  /*
   * The function that is applied each time a {@link MessageEnvelope} is added to this window.
   */
  private final FoldLeftFunction<M, WV> foldLeftFunction;

  /*
   * The function that extracts the key from a {@link MessageEnvelope}
   */
  private final MapFunction<M, WK> keyExtractor;

  /*
   * The function that extracts the event time from a {@link MessageEnvelope}
   */
  private final MapFunction<M, Long> eventTimeExtractor;

  /**
   * The type of this window. Tumbling and Session windows are supported for now.
   */
  private final WindowType windowType;

  private Trigger<M> earlyTrigger;

  private Trigger<M> lateTrigger;

  private AccumulationMode mode;

  public WindowInternal(Trigger<M> defaultTrigger, SupplierFunction<WV> initialValue, FoldLeftFunction<M, WV> foldLeftFunction, MapFunction<M, WK> keyExtractor, MapFunction<M, Long> eventTimeExtractor, WindowType windowType) {
    this.defaultTrigger = defaultTrigger;
    this.initializer = initialValue;
    this.foldLeftFunction = foldLeftFunction;
    this.eventTimeExtractor = eventTimeExtractor;
    this.keyExtractor = keyExtractor;
    this.windowType = windowType;
  }

  @Override
  public Window<M, WK, WV> setEarlyTrigger(Trigger<M> trigger) {
    this.earlyTrigger = trigger;
    return this;
  }

  @Override
  public Window<M, WK, WV> setLateTrigger(Trigger<M> trigger) {
    this.lateTrigger = trigger;
    return this;
  }

  @Override
  public Window<M, WK, WV> setAccumulationMode(AccumulationMode mode) {
    this.mode = mode;
    return this;
  }

  public Trigger<M> getDefaultTrigger() {
    return defaultTrigger;
  }

  public Trigger<M> getEarlyTrigger() {
    return earlyTrigger;
  }

  public Trigger<M> getLateTrigger() {
    return lateTrigger;
  }

  public SupplierFunction<WV> getInitializer() {
    return initializer;
  }

  public FoldLeftFunction<M, WV> getFoldLeftFunction() {
    return foldLeftFunction;
  }

  public MapFunction<M, WK> getKeyExtractor() {
    return keyExtractor;
  }

  public MapFunction<M, Long> getEventTimeExtractor() {
    return eventTimeExtractor;
  }

  public WindowType getWindowType() {
    return windowType;
  }

  public AccumulationMode getAccumulationMode() {
    return mode;
  }
}
