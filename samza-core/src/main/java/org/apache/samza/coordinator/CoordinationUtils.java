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

/**  THIS API WILL CHANGE
 *
 * Coordination service provides synchronization primitives.
 * The actual implementation (for example ZK based) is left to each implementation class.
 * This service provide three primitives:
 *   - LeaderElection
 *   - Latch
 *   - barrier for version upgrades
 */
public interface CoordinationUtils {

  /**
   * reset the internal structure. Does not happen automatically with stop()
   */
  void reset();


  // facilities for group coordination
  LeaderElector getLeaderElector(); // leaderElector is unique based on the groupId

  Latch getLatch(int size, String latchId);

  BarrierForVersionUpgrade getBarrier(String barrierId);
}
