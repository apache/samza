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


/**
 * Lock to acquire read access or write access
 * At any point in time, only one processor can hold the lock
 * The processor that has acquired the lock, holds it until it does `unlock` explicitly
 * The first processor to acquire the lock should be given a WRITE access
 * All other subsequent requests should acquire only READ access
 *
 * `State` is information pertaining to the requesting processor maintained by the lock
 * to determine how to grant read or write access
 *
 *
 *                                       lock()                     unlock()                   cleanState()
 * LOCK STATUS:  ──────────▶   NEW ─────────────────▶ LOCKED ──────────────────▶ UNLOCKED ─────────────────────▶ CLEANED
 * `State`                [No state]            [State created]            [State preserved]               [State Cleaned]
 *
 *
 */
public interface DistributedReadWriteLock {

  /**
   * Try to acquire lock: returns the type of access lock was acquired for
   * @param timeout Duration of lock acquiring timeout.
   * @param unit Time Unit of the timeout defined above.
   * @return AccessType.READ if lock is acquired for read, AccessType.WRITE if acquired for write
   * @throws TimeoutException if could not acquire the lock.
   */
  AccessType lock(long timeout, TimeUnit unit) throws TimeoutException;

  /**
   * Release the lock
   */
  void unlock();

  /**
   * Clean state of the lock
   */
  void cleanState();

  public enum AccessType {
    /**
     * indicates that lock acquired  was for read access
     */
    READ,

    /**
     * indicates that lock acquired  was for write access
     */
    WRITE,

    /**
     * indicates that lock was not acquired
     */
    NONE;
  }
}