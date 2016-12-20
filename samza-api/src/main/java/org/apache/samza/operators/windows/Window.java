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

/**
 * A {@link Window} slices a {@link org.apache.samza.operators.MessageStream} into smaller finite chunks for
 * further processing.
 *
 * <p> A window has the following aspects:
 * <ul>
 *   <li> Pane: Every result emitted from a window contains one or more {@link MessageEnvelope}s and is referred
 *   to as a window pane.
 *   <li> Key: A {@link Window} can group its {@link MessageEnvelope}s by a key. If a key is provided, the triggers are fired and
 *   panes are emitted per-key.
 *   <li> Early and Late Triggers: An early trigger allows emitting early, partial window results speculatively. A late trigger
 *   allows handling of late data arrivals.
 * </ul>
 *
 * <p> Use the {@link Windows} APIs to specify various windowing functions and the {@link org.apache.samza.operators.triggers.Triggers}
 * APIs to create triggers.
 *
 * @param <M> the type of the input {@link MessageEnvelope}
 * @param <K> the type of the key in the {@link MessageEnvelope} in this {@link org.apache.samza.operators.MessageStream}. If a key is specified,
 *            panes are emitted per-key
 * @param <WK> the type of the key in the {@link Window} output
 * @param <WV> the type of the value in the {@link Window}
 * @param <WM> the type of the {@link Window} result
 */
@InterfaceStability.Unstable
public interface Window<M extends MessageEnvelope, K, WK, WV, WM extends WindowPane<WK, WV>> {

  /**
   * Set the early triggers for this {@link Window}.
   * <p>Use the {@link org.apache.samza.operators.triggers.Triggers} APIs to create instances of {@link Trigger}
   *
   * @param trigger the early trigger
   * @return the {@link Window} function with the early trigger
   */
  Window<M, K, WK, WV, WM> setEarlyTrigger(Trigger<M, K, WV> trigger);

  /**
   * Set the late triggers for this {@link Window}.
   * <p>Use the {@link org.apache.samza.operators.triggers.Triggers} APIs to create instances of {@link Trigger}
   *
   * @param trigger the late trigger
   * @return the {@link Window} function with the late trigger
   */
  Window<M, K, WK, WV, WM> setLateTrigger(Trigger<M, K, WV> trigger);

  /**
   * Specifies that window panes should include all messages collected for the window (key) so far, even if they were
   * included in previously emitted window panes.
   *
   * @return the {@link Window} function that accumulates previously emitted panes.
   */
  Window<M, K, WK, WV, WM> accumulateFiredPanes();

  /**
   * Specifies that window panes should only include messages collected for this window (key) since the last emitted
   * window pane.
   *
   * @return the {@link Window} function that discards previously emitted panes.
   */
  Window<M, K, WK, WV, WM> discardFiredPanes();
}
