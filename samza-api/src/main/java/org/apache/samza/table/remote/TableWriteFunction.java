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

package org.apache.samza.table.remote;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.samza.SamzaException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.storage.kv.Entry;

import com.google.common.collect.Iterables;
import org.apache.samza.table.RecordNotFoundException;


/**
 * A function object to be used with a remote read/write table implementation. It encapsulates the functionality
 * of writing table record(s) for a provided set of key(s) to the store.
 *
 * <p> Instances of {@link TableWriteFunction} are meant to be serializable. ie. any non-serializable state
 * (eg: network sockets) should be marked as transient and recreated inside readObject().
 *
 * <p> Implementations are expected to be thread-safe.
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <U> the type of the update
 */
@InterfaceStability.Unstable
public interface TableWriteFunction<K, V, U> extends TableFunction {
  /**
   * Store single table {@code record} with specified {@code key}. This method must be thread-safe.
   * The default implementation calls putAsync and blocks on the completion afterwards.
   *
   * @param key key for the table record
   * @param record table record to be written
   */
  default void put(K key, V record) {
    try {
      putAsync(key, record).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SamzaException("PUT failed for " + key, e);
    }
  }

  /**
   * Asynchronously store single table {@code record} with specified {@code key}. This method must be thread-safe.
   * @param key key for the table record
   * @param record table record to be written
   * @return CompletableFuture for the put request
   */
  CompletableFuture<Void> putAsync(K key, V record);

  /**
   * Asynchronously store single table {@code record} with specified {@code key} and additional arguments.
   * This method must be thread-safe.
   *
   * @param key key for the table record
   * @param record table record to be written
   * @param args additional arguments
   * @return CompletableFuture for the put request
   */
  default CompletableFuture<Void> putAsync(K key, V record, Object ... args) {
    throw new SamzaException("Not supported");
  }

  /**
   * Store the table {@code records} with specified {@code keys}. This method must be thread-safe.
   * The default implementation calls putAllAsync and blocks on the completion afterwards.
   * @param records table records to be written
   */
  default void putAll(List<Entry<K, V>> records) {
    try {
      putAllAsync(records).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SamzaException("PUT_ALL failed for " + records, e);
    }
  }

  /**
   * Asynchronously store the table {@code records} with specified {@code keys}. This method must be thread-safe.
   * The default implementation calls putAsync for each entry and return a combined future.
   * @param records table records to be written
   * @return CompletableFuture for the put request
   */
  default CompletableFuture<Void> putAllAsync(Collection<Entry<K, V>> records) {
    List<CompletableFuture<Void>> putFutures =
        records.stream().map(e -> putAsync(e.getKey(), e.getValue())).collect(Collectors.toList());
    return CompletableFuture.allOf(Iterables.toArray(putFutures, CompletableFuture.class));
  }

  /**
   * Asynchronously store the table {@code records} with specified {@code keys} and additional arguments.
   * This method must be thread-safe.
   * @param records table records to be written
   * @param args additional arguments
   * @return CompletableFuture for the put request
   */
  default CompletableFuture<Void> putAllAsync(Collection<Entry<K, V>> records, Object ... args) {
    throw new SamzaException("Not supported");
  }

  /**
   * Asynchronously update the record with specified {@code key} and additional arguments.
   * This method must be thread-safe.
   *
   * If the update operation failed due to the an existing record missing for the key, the implementation can return
   * a future completed exceptionally with a {@link RecordNotFoundException} which will
   * allow to Put a default value if one is provided.
   *
   * @param key key for the table record
   * @param update update record for the given key
   * @return CompletableFuture for the update request
   */
  CompletableFuture<Void> updateAsync(K key, U update);

  /**
   * Asynchronously updates the table with {@code records} with specified {@code keys}. This method must be thread-safe.
   * The default implementation calls updateAsync for each entry and return a combined future.
   * @param records updates for the table
   * @return CompletableFuture for the update request
   */
  default CompletableFuture<Void> updateAllAsync(Collection<Entry<K, U>> records) {
    final List<CompletableFuture<Void>> updateFutures = records.stream()
        .map(entry -> updateAsync(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
    return CompletableFuture.allOf(Iterables.toArray(updateFutures, CompletableFuture.class));
  }

  /**
   * Delete the {@code record} with specified {@code key} from the remote store.
   * The default implementation calls deleteAsync and blocks on the completion afterwards.
   * @param key key to the table record to be deleted
   */
  default void delete(K key) {
    try {
      deleteAsync(key).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SamzaException("DELETE failed for " + key, e);
    }
  }

  /**
   * Asynchronously delete the {@code record} with specified {@code key} from the remote store
   * @param key key to the table record to be deleted
   * @return CompletableFuture for the delete request
   */
  CompletableFuture<Void> deleteAsync(K key);

  /**
   * Asynchronously delete the {@code record} with specified {@code key} and additional arguments from the remote store
   * @param key key to the table record to be deleted
   * @param args additional arguments
   * @return CompletableFuture for the delete request
   */
  default CompletableFuture<Void> deleteAsync(K key, Object ... args) {
    throw new SamzaException("Not supported");
  }

  /**
   * Delete all {@code records} with the specified {@code keys} from the remote store
   * The default implementation calls deleteAllAsync and blocks on the completion afterwards.
   * @param keys keys for the table records to be written
   */
  default void deleteAll(Collection<K> keys) {
    try {
      deleteAllAsync(keys).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SamzaException("DELETE failed for " + keys, e);
    }
  }

  /**
   * Asynchronously delete all {@code records} with the specified {@code keys} from the remote store.
   * The default implementation calls deleteAsync for each key and return a combined future.
   *
   * @param keys keys for the table records to be written
   * @return CompletableFuture for the deleteAll request
   */
  default CompletableFuture<Void> deleteAllAsync(Collection<K> keys) {
    List<CompletableFuture<Void>> deleteFutures =
        keys.stream().map(this::deleteAsync).collect(Collectors.toList());
    return CompletableFuture.allOf(Iterables.toArray(deleteFutures, CompletableFuture.class));
  }

  /**
   * Asynchronously delete all {@code records} with the specified {@code keys} and additional arguments from
   * the remote store.
   *
   * @param keys keys for the table records to be written
   * @param args additional arguments
   * @return CompletableFuture for the deleteAll request
   */
  default CompletableFuture<Void> deleteAllAsync(Collection<K> keys, Object ... args) {
    throw new SamzaException("Not supported");
  }

  /**
   * Asynchronously write data to table for specified {@code opId} and additional arguments.
   * This method must be thread-safe.
   * @param opId operation identifier
   * @param args additional arguments
   * @param <T> return type
   * @return CompletableFuture for the write request
   */
  default <T> CompletableFuture<T> writeAsync(int opId, Object ... args) {
    throw new SamzaException("Not supported");
  }

  /**
   * Flush the remote store (optional)
   */
  default void flush() {
  }

  // optionally implement readObject() to initialize transient states
}
