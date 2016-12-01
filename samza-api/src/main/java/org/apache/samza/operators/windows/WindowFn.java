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


/**
 * Defines an internal representation of a window function.
 *
 * @param <M>  type of the input {@link MessageEnvelope} for the window
 * @param <WK>  type of the window key in the output {@link MessageEnvelope}
 * @param <WS>  type of the {@link WindowState} in the state store
 * @param <WM>  type of the {@link MessageEnvelope} in the output stream
 */
public interface WindowFn<M extends MessageEnvelope, WK, WS extends WindowState, WM extends WindowOutput<WK, ?>> {

  /**
   * Get the transformation function of the {@link WindowFn}.
   *
   * @return  the transformation function which takes a {@link MessageEnvelope} of type {@code M} and its window state entry,
   *          and transforms it to an {@link WindowOutput}
   */
  BiFunction<M, Entry<WK, WS>, WM> getTransformFn();

  /**
   * Get the state store functions for this {@link WindowFn}.
   *
   * @return  the state store functions
   */
  StoreFunctions<M, WK, WS> getStoreFns();

  /**
   * Get the trigger conditions for this {@link WindowFn}.
   *
   * @return  the trigger condition for this {@link WindowFn}
   */
  Trigger<M, WS> getTrigger();

}
