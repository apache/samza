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
import org.apache.samza.storage.kv.Entry;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This class defines a session window function class
 *
 * @param <M>  the type of input {@link MessageEnvelope}
 * @param <WK>  the type of session key in the session window
 * @param <WV>  the type of output value in each session window
 */
public class SessionWindow<M extends MessageEnvelope, WK, WV> implements Window<M, WK, WV, WindowOutput<WK, WV>> {

  /**
   * Constructor. Made private s.t. it can only be instantiated via the static API methods in {@link Windows}
   *
   * @param sessionKeyFunction  function to get the session key from the input {@link MessageEnvelope}
   * @param aggregator  function to calculate the output value based on the input {@link MessageEnvelope} and current output value
   */
  SessionWindow(Function<M, WK> sessionKeyFunction, BiFunction<M, WV, WV> aggregator) {
    this.wndKeyFunction = sessionKeyFunction;
    this.aggregator = aggregator;
  }

  /**
   * function to calculate the window key from input {@link MessageEnvelope}
   */
  private final Function<M, WK> wndKeyFunction;

  /**
   * function to calculate the output value from the input {@link MessageEnvelope} and the current output value
   */
  private final BiFunction<M, WV, WV> aggregator;

  /**
   * trigger condition that determines when to send the {@link WindowOutput}
   */
  private Trigger<M, WindowState<WV>> trigger = null;

  //TODO: need to create a set of {@link StoreFunctions} that is default to input {@link MessageEnvelope} type for {@link Window}
  private StoreFunctions<M, WK, WindowState<WV>> storeFunctions = null;

  /**
   * Public API methods start here
   */

  /**
   * Public API method to define the watermark trigger for the window operator
   *
   * @param wndTrigger {@link Trigger} function defines the watermark trigger for this {@link SessionWindow}
   * @return The window operator w/ the defined watermark trigger
   */
  @Override
  public Window<M, WK, WV, WindowOutput<WK, WV>> setTriggers(TriggerBuilder<M, WV> wndTrigger) {
    this.trigger = wndTrigger.build();
    return this;
  }

  private BiFunction<M, Entry<WK, WindowState<WV>>, WindowOutput<WK, WV>> getTransformFunc() {
    // TODO: actual implementation of the main session window logic, based on the wndKeyFunction, aggregator, and triggers;
    return null;
  }

  public WindowFn<M, WK, WindowState<WV>, WindowOutput<WK, WV>> getInternalWindowFn() {
    return new WindowFn<M, WK, WindowState<WV>, WindowOutput<WK, WV>>() {

      @Override public BiFunction<M, Entry<WK, WindowState<WV>>, WindowOutput<WK, WV>> getTransformFn() {
        return SessionWindow.this.getTransformFunc();
      }

      @Override public StoreFunctions<M, WK, WindowState<WV>> getStoreFns() {
        return SessionWindow.this.storeFunctions;
      }

      @Override public Trigger<M, WindowState<WV>> getTrigger() {
        return SessionWindow.this.trigger;
      }
    };
  }
}
