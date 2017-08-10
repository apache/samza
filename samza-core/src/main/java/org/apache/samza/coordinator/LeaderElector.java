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

package org.apache.samza.coordinator;

import org.apache.samza.annotation.InterfaceStability;


/**
 * Leader elector async primitives, implemented based on ZK.
 * The callback is a async, and run in a separate (common) thread.
 * So the caller should never block in the callback.
 * Callbacks will be delivered on callback at a time. Others will wait.
 *
 */
@InterfaceStability.Evolving
public interface LeaderElector {
  /**
   * Register a LeaderElectorListener
   *
   * @param listener {@link LeaderElectorListener} interfaces to be invoked upon completion of leader election participation
   */
  void setLeaderElectorListener(LeaderElectorListener listener);

  /**
   * Async method that helps the caller participate in leader election.
   **/
  void tryBecomeLeader();

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

  void close();
}
