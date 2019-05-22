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
 * Coordination Primitive to maintain the list of processors in the quorum
 *
 * Guarantees:
 * 1. operations are linearizable
 * 2. registration persistence in the absence of connection errors
 *
 * Non-guarantees:
 * 1. thread safe
 * 2. concurrent access of the list of processors in the quorum
 * 3. persistence of registration across connection errors
 * 4. processorId as indicator of registration order
 *
 *  Implementor responsibilities:
 * 1. registerProcessor returns a unique processorId
 * 2. getNumberOfProcessors by a processor should reflect at least its own registration status
 * 3. unregisterProcessor for a null or unregistered processorId is a no-op
 */
@InterfaceStability.Evolving
public interface ClusterMembership {
  /**
   * add processor to the list of processors in the quorum
   * @return unique id of the processor registration
   */
  String registerProcessor();

  /**
   * @return number of processors in the list
   */
  int getNumberOfProcessors();

  /**
   * remove processor from the list of processors in the quorum
   * @param processorId to be removed from the list
   */
  void unregisterProcessor(String processorId);
}