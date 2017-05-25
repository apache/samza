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
package org.apache.samza.zk;

/**
 * An interface for listening to {@link ZkBarrierForVersionUpgrade} related events
 */
public interface ZkBarrierListener {
  /**
   * Invoked when the root of barrier for a given version is created in Zk
   *
   * @param version Version associated with the Barrier
   */
  void onBarrierCreated(String version);

  /**
   * Invoked when the data written to the Barrier state changes
   *
   * @param version Version associated with the Barrier
   * @param state {@link org.apache.samza.zk.ZkBarrierForVersionUpgrade.State} value
   */
  void onBarrierStateChanged(String version, ZkBarrierForVersionUpgrade.State state);

  /**
   * Invoked when Barrier encounters error
   *
   * @param version Version associated with the Barrier
   * @param t Throwable describing the cause of the barrier error
   */
  void onBarrierError(String version, Throwable t);
}
