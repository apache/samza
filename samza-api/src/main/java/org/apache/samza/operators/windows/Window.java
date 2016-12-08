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

/**
 * A {@link Window} transform slices a {@link org.apache.samza.operators.MessageStream} into smaller finite chunks for
 * further processing. Programmers should use the API methods of {@link Windows} to specify their windowing functions.
 *
 * <p>There are the following aspects to windowing in Samza:
 *
 * <ul>
 * <li> Default Trigger: Every {@link Window} has a default trigger that specifies when to emit
 * results for the window.
 *
 * <li>Early and Late Triggers: Users can choose to emit early, partial results speculatively by configuring an early trigger.
 * Users can choose to handle arrival of late data by configuring a late trigger. Refer to the {@link TriggersBuilder} APIs for
 * configuring early and late triggers.
 *
 * <li>Key: A {@link Window} transform can be evaluated on a "per-key" basis. For instance, A common use-case is to group a
 * stream based on a specified key over a tumbling time window. In this case, the triggering behavior is per-key and per-window.
 *
 * </ul>
 *
 * @param <M> type of input {@link MessageEnvelope}.
 * @param <K> type of key to group by when evaluating a {@link Window}.
 * @param <WK> type of key in the {@link Window} output.
 * @param <WV> type of value stored in the {@link Window}.
 * @param <WM> type of the {@link Window} result.
 */

@InterfaceStability.Unstable
public interface Window<M extends MessageEnvelope, K, WK, WV, WM extends WindowOutput<WK, WV>> {

  /**
   * Set the triggers for this {@link Window}
   *
   * @param wndTrigger trigger conditions set by the programmers
   * @return the {@link Window} function w/ the trigger {@code wndTrigger}
   */
  Window<M, K, WK, WV, WM> setTriggers(TriggersBuilder.Triggers wndTrigger);
}
