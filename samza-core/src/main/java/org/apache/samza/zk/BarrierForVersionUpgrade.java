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

import java.util.List;


/**
 * Interface for a barrier - to allow synchronization between different processors to switch to a newly published
 * JobModel.
 */
public interface BarrierForVersionUpgrade {
  /**
   * Barrier is usually started by the leader.
   * @param version - for which the barrier is started.
   * @param processorsNames - list of processors available at the time of the JobModel generation.
   */
  void start(String version, List<String> processorsNames);

  /**
   * Called by the processor.
   * Updates the processor readiness to use the new version and wait on the barrier, until all other processors
   * joined.
   * The call is async. The callback will be invoked when the barrier is reached.
   * @param version of the jobModel this barrier is protecting.
   * @param processorsName as it appears in the list of processors.
   * @param callback  will be invoked, when barrier is reached.
   */
  void waitForBarrier(String version, String processorsName, Runnable callback);
}
