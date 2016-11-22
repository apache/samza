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

import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.internal.Operators;
import org.apache.samza.operators.internal.Trigger;
import org.apache.samza.operators.internal.WindowFn;
import org.apache.samza.operators.internal.WindowOutput;
import org.apache.samza.storage.kv.Entry;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * This class defines a collection of {@link Window} functions. The public classes and methods here are intended to be
 * used by the user (i.e. programmers) to create {@link Window} function directly.
 *
 */
public final class Windows {

  /**
   * private constructor to prevent instantiation
   */
  private Windows() {}

  /**
   * This class defines a session window function class
   *
   * @param <M>  the type of input {@link Message}
   * @param <WK>  the type of session key in the session window
   * @param <WV>  the type of output value in each session window
   */
  static class SessionWindow<M extends Message, WK, WV> implements Window<M, WK, WV, WindowOutput<WK, WV>> {

    /**
     * Constructor. Made private s.t. it can only be instantiated via the static API methods in {@link Windows}
     *
     * @param sessionKeyFunction  function to get the session key from the input {@link Message}
     * @param aggregator  function to calculate the output value based on the input {@link Message} and current output value
     */
    private SessionWindow(Function<M, WK> sessionKeyFunction, BiFunction<M, WV, WV> aggregator) {
      this.wndKeyFunction = sessionKeyFunction;
      this.aggregator = aggregator;
    }

    /**
     * function to calculate the window key from input message
     */
    private final Function<M, WK> wndKeyFunction;

    /**
     * function to calculate the output value from the input message and the current output value
     */
    private final BiFunction<M, WV, WV> aggregator;

    /**
     * trigger condition that determines when to send out the output value in a {@link WindowOutput} message
     */
    private Trigger<M, WindowState<WV>> trigger = null;

    //TODO: need to create a set of {@link StoreFunctions} that is default to input {@link Message} type for {@link Window}
    private Operators.StoreFunctions<M, WK, WindowState<WV>> storeFunctions = null;

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

    private WindowFn<M, WK, WindowState<WV>, WindowOutput<WK, WV>> getInternalWindowFn() {
      return new WindowFn<M, WK, WindowState<WV>, WindowOutput<WK, WV>>() {

        @Override public BiFunction<M, Entry<WK, WindowState<WV>>, WindowOutput<WK, WV>> getTransformFunc() {
          return SessionWindow.this.getTransformFunc();
        }

        @Override public Operators.StoreFunctions<M, WK, WindowState<WV>> getStoreFuncs() {
          return SessionWindow.this.storeFunctions;
        }

        @Override public Trigger<M, WindowState<WV>> getTrigger() {
          return SessionWindow.this.trigger;
        }
      };
    }
  }

  static <M extends Message, WK, WV, WS extends WindowState<WV>, WM extends WindowOutput<WK, WV>> WindowFn<M, WK, WS, WM> getInternalWindowFn(
      Window<M, WK, WV, WM> window) {
    if (window instanceof SessionWindow) {
      SessionWindow<M, WK, WV> sessionWindow = (SessionWindow<M, WK, WV>) window;
      return (WindowFn<M, WK, WS, WM>) sessionWindow.getInternalWindowFn();
    }
    throw new IllegalArgumentException("Input window type not supported.");
  }

  /**
   * Public static API methods start here
   *
   */

  /**
   * The public programming interface class for window function
   *
   * @param <M>  the type of input {@link Message}
   * @param <WK>  the type of key to the {@link Window}
   * @param <WV>  the type of output value in the {@link WindowOutput}
   * @param <WM>  the type of message in the window output stream
   */
  public interface Window<M extends Message, WK, WV, WM extends WindowOutput<WK, WV>> {

    /**
     * Set the triggers for this {@link Window}
     *
     * @param wndTrigger  trigger conditions set by the programmers
     * @return  the {@link Window} function w/ the trigger {@code wndTrigger}
     */
    Window<M, WK, WV, WM> setTriggers(TriggerBuilder<M, WV> wndTrigger);
  }

  /**
   * Static API method to create a {@link SessionWindow} in which the output value is simply the collection of input messages
   *
   * @param sessionKeyFunction  function to calculate session window key
   * @param <M>  type of input {@link Message}
   * @param <WK>  type of the session window key
   * @return  the {@link Window} function for the session
   */
  public static <M extends Message, WK> Window<M, WK, Collection<M>, WindowOutput<WK, Collection<M>>> intoSessions(Function<M, WK> sessionKeyFunction) {
    return new SessionWindow<>(sessionKeyFunction, (m, c) -> {
        c.add(m);
        return c;
      });
  }

  /**
   * Static API method to create a {@link SessionWindow} in which the output value is a collection of {@code SI} from the input messages
   *
   * @param sessionKeyFunction  function to calculate session window key
   * @param sessionInfoExtractor  function to retrieve session info of type {@code SI} from the input message of type {@code M}
   * @param <M>  type of the input {@link Message}
   * @param <WK>  type of the session window key
   * @param <SI>  type of the session information retrieved from each input message of type {@code M}
   * @return  the {@link Window} function for the session
   */
  public static <M extends Message, WK, SI> Window<M, WK, Collection<SI>, WindowOutput<WK, Collection<SI>>> intoSessions(Function<M, WK> sessionKeyFunction,
      Function<M, SI> sessionInfoExtractor) {
    return new SessionWindow<>(sessionKeyFunction, (m, c) -> {
        c.add(sessionInfoExtractor.apply(m));
        return c;
      });
  }

  /**
   * Static API method to create a {@link SessionWindow} as a counter of input messages
   *
   * @param sessionKeyFunction  function to calculate session window key
   * @param <M>  type of the input {@link Message}
   * @param <WK>  type of the session window key
   * @return  the {@link Window} function for the session
   */
  public static <M extends Message, WK> Window<M, WK, Integer, WindowOutput<WK, Integer>> intoSessionCounter(Function<M, WK> sessionKeyFunction) {
    return new SessionWindow<>(sessionKeyFunction, (m, c) -> c + 1);
  }

}
