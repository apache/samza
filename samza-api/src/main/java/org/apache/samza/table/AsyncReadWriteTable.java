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
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;
import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;


/**
 * A table that supports asynchronous get, put, update and delete by one or more keys
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <U> the type of the update applied to this table
 */
public interface AsyncReadWriteTable<K, V, U> extends Table {
  /**
   * Asynchronously gets the value associated with the specified {@code key}.
   *
   * @param key the key with which the associated value is to be fetched.
   * @param args additional arguments
   * @return completableFuture for the requested value
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  CompletableFuture<V> getAsync(K key, Object ... args);

  /**
   * Asynchronously gets the values with which the specified {@code keys} are associated.
   *
   * @param keys the keys with which the associated values are to be fetched.
   * @param args additional arguments
   * @return completableFuture for the requested entries
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
   */
  CompletableFuture<Map<K, V>> getAllAsync(List<K> keys, Object ... args);

  /**
   * Asynchronously executes a read operation. opId is used to allow tracking of different
   * types of operation.
   * @param opId operation identifier
   * @param args additional arguments
   * @param <T> return type
   * @return completableFuture for read result
   */
  default <T> CompletableFuture<T> readAsync(int opId, Object ... args) {
    throw new SamzaException("Not supported");
  }

  /**
   * Asynchronously updates the mapping of the specified key-value pair;
   * Associates the specified {@code key} with the specified {@code value}.
   * The key is deleted from the table if value is {@code null}.
   *
   * @param key the key with which the specified {@code value} is to be associated.
   * @param value the value with which the specified {@code key} is to be associated.
   * @param args additional arguments
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   * @return CompletableFuture for the operation
   */
  CompletableFuture<Void> putAsync(K key, V value, Object ... args);

  /**
   * Asynchronously updates the mappings of the specified key-value {@code entries}.
   * A key is deleted from the table if its corresponding value is {@code null}.
   *
   * @param entries the updated mappings to put into this table.
   * @param args additional arguments
   * @throws NullPointerException if any of the specified {@code entries} has {@code null} as key.
   * @return CompletableFuture for the operation
   */
  CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries, Object ... args);

  /**
   * Asynchronously updates the record associated with the specified update for a given key.
   * If the update is attempted on a non-existent record, a default value can be inserted in the table depending
   * on the table's implementation.
   *
   * @param key the key with which the specified {@code value} is to be associated.
   * @param update the update applied to the record associated with a given {@code key}.
   * @param defaultValue the default value that is inserted in the table if the update is applied to a non-existent record.
   * @param args additional arguments
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   * @return CompletableFuture for the operation
   */
  CompletableFuture<Void> updateAsync(K key, U update, @Nullable V defaultValue, Object ... args);

  /**
   * Asynchronously updates the records associated with the specified keys with the specified updates.
   * If the update is applied to a non-existent record, a default value can be inserted in the table depending
   * on the table's implementation.
   *
   * @param updates the key and update mappings.
   * @param defaults the key and default value mappings.
   * @param args additional arguments
   * @throws NullPointerException if any of the specified {@code entries} has {@code null} as key.
   * @return CompletableFuture for the operation
   */
  CompletableFuture<Void> updateAllAsync(List<Entry<K, U>> updates, @Nullable List<Entry<K, V>> defaults, Object ... args);

  /**
   * Asynchronously deletes the mapping for the specified {@code key} from this table (if such mapping exists).
   * @param key the key for which the mapping is to be deleted.
   * @param args additional arguments
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   * @return CompletableFuture for the operation
   */
  CompletableFuture<Void> deleteAsync(K key, Object ... args);

  /**
   * Asynchronously deletes the mappings for the specified {@code keys} from this table.
   * @param keys the keys for which the mappings are to be deleted.
   * @param args additional arguments
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
   * @return CompletableFuture for the operation
   */
  CompletableFuture<Void> deleteAllAsync(List<K> keys, Object ... args);

  /**
   * Asynchronously executes a write operation. opId is used to allow tracking of different
   * types of operation.
   * @param opId operation identifier
   * @param args additional arguments
   * @param <T> return type
   * @return completableFuture for write result
   */
  default <T> CompletableFuture<T> writeAsync(int opId, Object ... args) {
    throw new SamzaException("Not supported");
  }

  /**
   * Initializes the table during container initialization.
   * Guaranteed to be invoked as the first operation on the table.
   * @param context {@link Context} corresponding to this table
   */
  default void init(Context context) {
  }

  /**
   * Flushes the underlying store of this table, if applicable.
   */
  void flush();

  /**
   * Close the table and release any resources acquired
   */
  void close();
}
