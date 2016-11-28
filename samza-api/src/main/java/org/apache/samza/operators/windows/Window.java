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

  /**
   * Internal implementation helper to get the functions associated with this Window.
   *
   * <b>NOTE:</b> This is purely an internal API and should not be used directly by users.
   *
   * @return the functions associated with this Window.
   */
  WindowFn<M, WK, WindowState<WV>, WindowOutput<WK, WV>> getInternalWindowFn();
}
