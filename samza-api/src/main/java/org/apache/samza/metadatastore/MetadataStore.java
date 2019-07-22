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
package org.apache.samza.metadatastore;

import java.util.Map;
import org.apache.samza.annotation.InterfaceStability;

/**
 * Store abstraction responsible for managing the metadata of a Samza job.
 */
@InterfaceStability.Evolving
public interface MetadataStore {

  /**
   * Initializes the metadata store, if applicable, setting up the underlying resources
   * and connections to the store endpoints.
   */
  void init();

  /**
   * Gets the value associated with the specified {@code key}.
   *
   * @param key the key with which the associated value is to be fetched.
   * @return if found, the value associated with the specified {@code key}; otherwise, {@code null}.
   */
  byte[] get(String key);

  /**
   * Updates the mapping of the specified key-value pair.
   *
   * @param key the key with which the specified {@code value} is to be associated.
   * @param value the value with which the specified {@code key} is to be associated.
   */
  void put(String key, byte[] value);

  /**
   * Updates the mapping with the specified map.
   *
   * @param entries mapping of key to values to write to the metadata store
   */
  default void putAll(Map<String, byte[]> entries) {
    for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Deletes the mapping for the specified {@code key} from this metadata store (if such mapping exists).
   *
   * @param key the key for which the mapping is to be deleted.
   */
  void delete(String key);

  /**
   * Returns all the entries in this metadata store.
   *
   * @return all entries in this metadata store.
   */
  Map<String, byte[]> all();

  /**
   * Flushes the metadata store, if applicable.
   */
  void flush();

  /**
   * Closes the metadata store, if applicable, relinquishing all the underlying resources
   * and connections.
   */
  void close();
}
