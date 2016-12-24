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
 * Groups incoming {@link MessageEnvelope}s in the {@link org.apache.samza.operators.MessageStream} into finite
 * windows for processing.
 *
 * <p> A window is uniquely identified by its {@link WindowKey}. A window can have one or more associated {@link Trigger}s
 * that determine when results from the {@link Window} are emitted.
 *
 * <p> Each emitted result contains one or more {@link MessageEnvelope}s in the window and is called a {@link WindowPane}.
 * A pane can include all {@link MessageEnvelope}s collected for the window so far or only the new {@link MessageEnvelope}s
 * since the last emitted pane. (as determined by the {@link AccumulationMode})
 *
 * <p> A window can have early triggers that allow emitting {@link WindowPane}s speculatively before all data for the window
 * has arrived or late triggers that allow handling of late data arrivals.
 *
 * <p> A {@link Window} is defined as "keyed" when the incoming {@link org.apache.samza.operators.MessageStream} is first
 * partitioned based on the provided key, and windowing is applied on the partitioned stream.
 *
 *                                     window wk1 (with its triggers)
 *                                      +--------------------------------+
 *                                      ------------+--------+-----------+
 *                                      |           |        |           |
 *                                      | pane 1    |pane2   |   pane3   |
 *                                      +-----------+--------+-----------+
 *
 -----------------------------------
 *incoming message stream ------+
 -----------------------------------
 *                                      window wk2
 *                                      +---------------------+---------+
 *                                      |   pane 1|   pane 2  |  pane 3 |
 *                                      |         |           |         |
 *                                      +---------+-----------+---------+
 *
 *                                      window wk3
 *                                      +----------+-----------+---------+
 *                                      |          |           |         |
 *                                      | pane 1   |  pane 2   |   pane 3|
 *                                      |          |           |         |
 *                                      +----------+-----------+---------+
 *
 *
 * <p> Use the {@link Windows} APIs to create various windows and the {@link org.apache.samza.operators.triggers.Triggers}
 * APIs to create triggers.
 *
 * @param <M> the type of the input {@link MessageEnvelope}
 * @param <K> the type of the key in the {@link MessageEnvelope} in this {@link org.apache.samza.operators.MessageStream}. If a key is specified,
 *            panes are emitted per-key
 * @param <WV> the type of the value in the {@link WindowPane}.
 * @param <WM> the type of the output.
 */
@InterfaceStability.Unstable
public interface Window<M extends MessageEnvelope, K, WV, WM extends WindowPane<K, WV>> {

  /**
   * Set the early triggers for this {@link Window}.
   * <p>Use the {@link org.apache.samza.operators.triggers.Triggers} APIs to create instances of {@link Trigger}
   *
   * @param trigger the early trigger
   * @return the {@link Window} function with the early trigger
   */
  Window<M, K, WV, WM> setEarlyTrigger(Trigger<M> trigger);

  /**
   * Set the late triggers for this {@link Window}.
   * <p>Use the {@link org.apache.samza.operators.triggers.Triggers} APIs to create instances of {@link Trigger}
   *
   * @param trigger the late trigger
   * @return the {@link Window} function with the late trigger
   */
  Window<M, K, WV, WM> setLateTrigger(Trigger<M> trigger);

  /**
   * Specify how a {@link Window} should process its previously emitted {@link WindowPane}s.
   *
   * <p> There are two types of {@link AccumulationMode}s:
   * <ul>
   *  <li> ACCUMULATING: Specifies that window panes should include all messages collected for the window (key) so far, even if they were
   * included in previously emitted window panes.
   *  <li> DISCARDING: Specifies that window panes should only include messages collected for this window (key) since the last emitted
   * window pane.
   * </ul>
   *
   * @param mode the accumulation mode
   * @return the {@link Window} function with the specified {@link AccumulationMode}.
   */
  Window<M, K, WV, WM> setAccumulationMode(AccumulationMode mode);

}
