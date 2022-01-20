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
package org.apache.samza.table;

import java.util.List;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.storage.kv.Entry;

/**
 * A table that supports synchronous and asynchronous get, put, update and delete by one or more keys
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <U> the type of the update applied to records in this table
 */
@InterfaceStability.Unstable
public interface ReadWriteUpdateTable<K, V, U> extends AsyncReadWriteUpdateTable<K, V, U> {

  /**
   * Gets the value associated with the specified {@code key}.
   *
   * @param key the key with which the associated value is to be fetched.
   * @param args additional arguments
   * @return if found, the value associated with the specified {@code key}; otherwise, {@code null}.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  V get(K key, Object ... args);

  /**
   * Gets the values with which the specified {@code keys} are associated.
   *
   * @param keys the keys with which the associated values are to be fetched.
   * @param args additional arguments
   * @return a map of the keys that were found and their respective values.
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
   */
  Map<K, V> getAll(List<K> keys, Object ... args);

  /**
   * Executes a read operation. opId is used to allow tracking of different
   * types of operation.
   * @param opId operation identifier
   * @param args additional arguments
   * @param <T> return type
   * @return read result
   */

  default <T> T read(int opId, Object ... args) {
    throw new SamzaException("Not supported");
  }

  /**
   * Updates the record associated with a given {@code key} with the specified {@code update}.
   *
   * @param key the key with which the specified {@code value} is to be associated.
   * @param update the update to be applied to the record specified by {@code key}.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  void update(K key, U update);

  /**
   * Updates the mappings of the given keys with the corresponding updates.
   *
   * @param updates the updates for the given keys
   * @throws NullPointerException if any of the specified {@code entries} has {@code null} as key.
   */
  void updateAll(List<Entry<K, U>> updates);


  /**
   * Updates the mapping of the specified key-value pair;
   * Associates the specified {@code key} with the specified {@code value}.
   *
   * The key is deleted from the table if value is {@code null}.
   *
   * @param key the key with which the specified {@code value} is to be associated.
   * @param value the value with which the specified {@code key} is to be associated.
   * @param args additional arguments
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  void put(K key, V value, Object ... args);

  /**
   * Updates the mappings of the specified key-value {@code entries}.
   *
   * A key is deleted from the table if its corresponding value is {@code null}.
   *
   * @param entries the updated mappings to put into this table.
   * @param args additional arguments
   * @throws NullPointerException if any of the specified {@code entries} has {@code null} as key.
   */
  void putAll(List<Entry<K, V>> entries, Object ... args);

  /**
   * Deletes the mapping for the specified {@code key} from this table (if such mapping exists).
   *
   * @param key the key for which the mapping is to be deleted.
   * @param args additional arguments
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  void delete(K key, Object ... args);

  /**
   * Deletes the mappings for the specified {@code keys} from this table.
   *
   * @param keys the keys for which the mappings are to be deleted.
   * @param args additional arguments
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
   */
  void deleteAll(List<K> keys, Object ... args);

  /**
   * Executes a write operation. opId is used to allow tracking of different
   * types of operation.
   * @param opId operation identifier
   * @param args additional arguments
   * @param <T> return type
   * @return write result
   */
  default <T> T write(int opId, Object ... args) {
    throw new SamzaException("Not supported");
  }
}
