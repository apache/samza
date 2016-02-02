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

package org.apache.samza.storage.kv;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A key-value store that supports put, get, delete, and range queries.
 *
 * @param <K> the type of keys maintained by this key-value store.
 * @param <V> the type of values maintained by this key-value store.
 */
public interface KeyValueStore<K, V> {
  /**
   * Gets the value associated with the specified {@code key}.
   *
   * @param key the key with which the associated value is to be fetched.
   * @return if found, the value associated with the specified {@code key}; otherwise, {@code null}.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  V get(K key);

  /**
   * Gets the values with which the specified {@code keys} are associated.
   *
   * @param keys the keys with which the associated values are to be fetched.
   * @return a map of the keys that were found and their respective values.
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
   */
  Map<K, V> getAll(List<K> keys);

  /**
   * Updates the mapping of the specified key-value pair; Associates the specified {@code key} with the specified {@code value}.
   *
   * @param key the key with which the specified {@code value} is to be associated.
   * @param value the value with which the specified {@code key} is to be associated.
   * @throws NullPointerException if the specified {@code key} or {@code value} is {@code null}.
   */
  void put(K key, V value);

  /**
   * Updates the mappings of the specified key-value {@code entries}.
   *
   * @param entries the updated mappings to put into this key-value store.
   * @throws NullPointerException if any of the specified {@code entries} has {@code null} as key or value.
   */
  void putAll(List<Entry<K, V>> entries);

  /**
   * Deletes the mapping for the specified {@code key} from this key-value store (if such mapping exists).
   *
   * @param key the key for which the mapping is to be deleted.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  void delete(K key);

  /**
   * Deletes the mappings for the specified {@code keys} from this key-value store (if such mappings exist).
   *
   * @param keys the keys for which the mappings are to be deleted.
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
   */
  void deleteAll(List<K> keys);

  /**
   * Returns an iterator for a sorted range of entries specified by [{@code from}, {@code to}).
   *
   * <p><b>API Note:</b> The returned iterator MUST be closed after use. The comparator used for finding entries that belong to the specified
   * range compares the underlying serialized big-endian byte array representation of keys, lexicographically.
   * @see <a href="http://en.wikipedia.org/wiki/Lexicographical_order">Lexicographical order article at Wikipedia</a></p>
   * @param from the key specifying the low endpoint (inclusive) of the keys in the returned range.
   * @param to the key specifying the high endpoint (exclusive) of the keys in the returned range.
   * @return an iterator for the specified key range.
   * @throws NullPointerException if null is used for {@code from} or {@code to}.
   */
  KeyValueIterator<K, V> range(K from, K to);

  /**
   * Returns an iterator for all entries in this key-value store.
   *
   * <p><b>API Note:</b> The returned iterator MUST be closed after use.</p>
   * @return an iterator for all entries in this key-value store.
   */
  KeyValueIterator<K, V> all();

  /**
   * Closes this key-value store, if applicable, relinquishing any underlying resources.
   */
  void close();

  /**
   * Flushes this key-value store, if applicable.
   */
  void flush();

  /**
   * Represents an extension for classes that implement {@link KeyValueStore}.
   */
  // TODO replace with default interface methods when we can use Java 8 features.
  class Extension {
    private Extension() {
      // This class cannot be instantiated
    }

    /**
     * Gets the values with which the specified {@code keys} are associated.
     *
     * @param store the key-value store for which this operation is to be performed.
     * @param keys the keys with which the associated values are to be fetched.
     * @param <K> the type of keys maintained by the specified {@code store}.
     * @param <V> the type of values maintained by the specified {@code store}.
     * @return a map of the keys that were found and their respective values.
     * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
     */
    public static <K, V> Map<K, V> getAll(final KeyValueStore<K, V> store, final List<K> keys) {
      final Map<K, V> map = new HashMap<>(keys.size());

      for (final K key : keys) {
        final V value = store.get(key);

        if (value != null) {
          map.put(key, value);
        }
      }

      return map;
    }

    /**
     * Deletes the mappings for the specified {@code keys} from this key-value store (if such mappings exist).
     *
     * @param store the key-value store for which this operation is to be performed.
     * @param keys the keys for which the mappings are to be deleted.
     * @param <K> the type of keys maintained by the specified {@code store}.
     * @param <V> the type of values maintained by the specified {@code store}.
     * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
     */
    public static <K, V> void deleteAll(final KeyValueStore<K, V> store, final List<K> keys) {
      for (final K key : keys) {
        store.delete(key);
      }
    }
  }
}
