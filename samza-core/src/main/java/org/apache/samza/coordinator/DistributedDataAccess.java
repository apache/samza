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
 * Concurrent writes are not allowed but concurrent reads are.
 * Data written for a key persists until overwritten
 * and is immediately available for all subsequent reads of that key.
 */
public interface DistributedDataAccess {
  /**
   * read data associated with key
   * @param key
   * @return data present at key
   */
  Object readData(String key);

  /**
   * write data for the given key
   * @param key
   * @param data
   */
  void writeData(String key, Object data);
}
