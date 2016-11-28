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

import org.apache.samza.operators.data.Message;

import java.util.Collection;
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
      }
    );
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
      }
    );
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
