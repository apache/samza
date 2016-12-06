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
 * A {@link WindowFunction} slices a {@link org.apache.samza.operators.MessageStream} into smaller finite chunks for
 * further processing. Every result fired from a window is referred to as a pane. A pane has one or more {@link MessageEnvelope}s.
 * Use the {@link Windows} APIs to specify various windowing functions.
 *
 * <p> A window has the following aspects:
 * <ul>
 *   <li> Key: A {@link WindowFunction} transform can be evaluated on a "per-key" basis. For instance, A common use-case
 *   is to group a stream based on a specified key over a tumbling time window. In this case, the triggering behavior is
 *   per-key and per-window.
 *   <li> Default Trigger: Every {@link WindowFunction} has a default trigger that specifies when to emit
 *   results for the window.
 *   <li> Early and Late Triggers: An early trigger allows to emit early, partial window results speculatively. A late trigger
 *   allows to handle arrival of late data. Refer to the {@link org.apache.samza.operators.triggers.Triggers} APIs for
 *   configuring early and late triggers.
 * </ul>
 *
 * @param <M> type of input {@link MessageEnvelope}.
 * @param <K> type of key in the {@link MessageEnvelope} in this {@link org.apache.samza.operators.MessageStream}. If a key is specified,
 *           results are emitted per-key.
 * @param <WK> type of key in the {@link WindowFunction} output.
 * @param <WV> type of value stored in the {@link WindowFunction}.
 * @param <WM> type of the {@link WindowFunction} result.
 */

@InterfaceStability.Unstable
public interface WindowFunction<M extends MessageEnvelope, K, WK, WV, WM extends WindowOutput<WK, WV>> {

  /**
   * Set the early triggers for this {@link WindowFunction}. Use the {@link org.apache.samza.operators.triggers.Triggers}
   * APIs to create instances of {@link Trigger}
   *
   * @param trigger the early trigger
   * @return the {@link WindowFunction} function w/ the trigger
   */
  WindowFunction<M, K, WK, WV, WM> setEarlyTrigger(Trigger<M, K, WV> trigger);

  /**
   * Set the late triggers for this {@link WindowFunction}. Use the {@link org.apache.samza.operators.triggers.Triggers}
   * APIs to create instances of {@link Trigger}
   *
   * @param trigger the late trigger
   * @return the {@link WindowFunction} function w/ the trigger
   */
  WindowFunction<M, K, WK, WV, WM> setLateTrigger(Trigger<M, K, WV> trigger);

  /**
   * Specifies that previously fired panes should be discarded. This is applicable when each window pane is
   * independent.
   *
   * @return the {@link WindowFunction} function that discards previously fired results.
   */
  WindowFunction<M, K, WK, WV, WM> discardFiredPanes();

  /**
   * Specifies that results of previously fired panes should be accumulated.
   *
   * @return the {@link WindowFunction} function that accumulates previously fired results.
   */
  WindowFunction<M, K, WK, WV, WM> accumulateFiredPanes();

}
