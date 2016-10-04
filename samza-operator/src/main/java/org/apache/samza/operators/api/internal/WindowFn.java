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
package org.apache.samza.operators.api.internal;

import org.apache.samza.operators.api.WindowState;
import org.apache.samza.operators.api.data.Message;
import org.apache.samza.storage.kv.Entry;

import java.util.function.BiFunction;


/**
 * Defines an internal representation of a window function. This class SHOULD NOT be used by the programmer directly. It is used
 * by the internal representation and implementation classes in operators.
 *
 * @param <M> type of input stream {@link Message} for window
 * @param <WK>  type of window key in the output {@link Message}
 * @param <WS>  type of {@link WindowState} variable in the state store
 * @param <WM>  type of the message in the output stream
 */
public interface WindowFn<M extends Message, WK, WS extends WindowState, WM extends WindowOutput<WK, ?>> {

  /**
   * get the transformation function of the {@link WindowFn}
   *
   * @return  the transformation function takes type {@code M} message and the window state entry, then transform to an {@link WindowOutput}
   */
  BiFunction<M, Entry<WK, WS>, WM> getTransformFunc();

  /**
   * get the state store functions for this {@link WindowFn}
   *
   * @return  the collection of state store methods
   */
  Operators.StoreFunctions<M, WK, WS> getStoreFuncs();

  /**
   * get the trigger conditions for this {@link WindowFn}
   *
   * @return  the trigger condition for the {@link WindowFn} function
   */
  Trigger<M, WS> getTrigger();

}
