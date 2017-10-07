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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public interface DistributedLockWithState {

  /**
   * Try to acquire the lock, but first check if the state flag is set. If it is set, return false.
   * If the flag is not set, and lock is acquired - return true.
   * @param timeout Duration of lock acquiring timeout.
   * @param unit Time Unit of the timeout defined above.
   * @return true if lock is acquired successfully, false if state is already set.
   * @throws TimeoutException if could not acquire the lock.
   */
  boolean lockIfNotSet(long timeout, TimeUnit unit) throws TimeoutException;

  /**
   * Release the lock and set the state
   */
  void unlockAndSet();
}