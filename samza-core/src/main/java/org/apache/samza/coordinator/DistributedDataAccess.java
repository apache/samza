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

/**
 * DistributedDataAccess is an interface for accessing data
 * shared between several processors in the quorum.
 * 1. Not thread safe
 * 2. No internal synchronization
 * 3. A watcher is registered to listen to data changes at the key.
 */
public interface DistributedDataAccess {
  /**
   * Read data associated with key.
   * Does not modify the value at key.
   * @param key
   * @param watcher through which notifications of data changes at key are sent
   * @return data present at key
   */
  Object readData(String key, DistributedDataWatcher watcher);

  /**
   * Write data for the given key.
   * Data written is persisted until next write.
   * @param key
   * @param data
   * @param watcher through which notifications of data changes at key are sent
   */
  void writeData(String key, Object data, DistributedDataWatcher watcher);
}
