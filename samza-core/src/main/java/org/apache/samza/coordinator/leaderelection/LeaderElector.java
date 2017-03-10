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

package org.apache.samza.coordinator.leaderelection;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.coordinator.LeaderElectorListener;


@InterfaceStability.Evolving
public interface LeaderElector {
  /**
   * Method that helps the caller participate in leader election and returns when the participation is complete
   *
   * @return True, if caller is chosen as a leader through the leader election process. False, otherwise.
   */
  boolean tryBecomeLeader(LeaderElectorListener leaderElectorListener);

  /**
   * Method that allows a caller to resign from leadership role. Caller can resign from leadership due to various
   * reasons such as shutdown, connection failures etc.
   * This method should clear any state created by the leader and clean-up the resources used by the leader.
   */
  void resignLeadership();

  /**
   * Method that can be used to know if the caller is the current leader or not
   *
   * @return True, if the caller is the current leader. False, otherwise
   */
  boolean amILeader();
}
