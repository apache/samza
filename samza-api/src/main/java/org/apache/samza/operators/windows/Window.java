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
 * further processing.  Use the {@link Windows} APIs to specify various windowing functions.
 *
 * <p> A window has the following aspects:
 * <ul>
 *   <li> Pane: Every result emitted from a window contains one or more {@link MessageEnvelope}s and is referred
 *   to as a window pane.
 *   <li> Key: A {@link Window} can group its results by a key. For instance, A common use-case is to group a
 *   stream by a specified key over a tumbling time window. In this case, the triggers are evaluated per-key.
 *   <li> Early and Late Triggers: An early trigger allows to emit early, partial window results speculatively. A late trigger
 *   allows handling of late data arrivals. Refer to the {@link org.apache.samza.operators.triggers.Triggers} APIs for
 *   creating early and late triggers.
 * </ul>
 *
 * @param <M> the type of the input {@link MessageEnvelope}.
 * @param <K> the type of the key in the {@link MessageEnvelope} in this {@link org.apache.samza.operators.MessageStream}. If a key is specified,
 *           results are emitted per-key.
 * @param <WK> the type of the key in the {@link Window} output.
 * @param <WV> the type of the value stored in the {@link Window}.
 * @param <WM> the type of the {@link Window} result.
 */
@InterfaceStability.Unstable
public interface Window<M extends MessageEnvelope, K, WK, WV, WM extends WindowOutput<WK, WV>> {

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
   * Specifies that results from a pane should be retained after they are emitted. This is applicable when each
   * window pane accumulates results from previously fired panes.
   *
   * @return the {@link Window} function that accumulates previously emitted results.
   */
  Window<M, K, WK, WV, WM> accumulateFiredPanes();

  /**
   * Specifies that results from a pane should be discarded once they are emitted. This is applicable when each window pane is
   * independent.
   *
   * @return the {@link Window} function that discards previously emitted results.
   */
  Window<M, K, WK, WV, WM> discardFiredPanes();

}