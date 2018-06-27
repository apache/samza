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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.samza.SamzaException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.operators.functions.InitableFunction;

import com.google.common.collect.Iterables;


/**
 * A function object to be used with a {@link RemoteReadableTable} implementation. It encapsulates the functionality
 * of reading table record(s) for a provided set of key(s).
 *
 * <p> Instances of {@link TableReadFunction} are meant to be serializable. ie. any non-serializable state
 * (eg: network sockets) should be marked as transient and recreated inside readObject().
 *
 * <p> Implementations are expected to be thread-safe.
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
@InterfaceStability.Unstable
public interface TableReadFunction<K, V> extends Serializable, InitableFunction, ClosableFunction {
  /**
   * Fetch single table record for a specified {@code key}. This method must be thread-safe.
   * The default implementation calls getAsync and blocks on the completion afterwards.
   * @param key key for the table record
   * @return table record for the specified {@code key}
   */
  default V get(K key) {
    try {
      return getAsync(key).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SamzaException("GET failed for " + key, e);
    }
  }

  /**
   * Asynchronously fetch single table record for a specified {@code key}. This method must be thread-safe.
   * @param key key for the table record
   * @return CompletableFuture for the get request
   */
  CompletableFuture<V> getAsync(K key);

  /**
   * Fetch the table {@code records} for specified {@code keys}. This method must be thread-safe.
   * The default implementation calls getAllAsync and blocks on the completion afterwards.
   * @param keys keys for the table records
   * @return all records for the specified keys.
   */
  default Map<K, V> getAll(Collection<K> keys) {
    try {
      return getAllAsync(keys).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SamzaException("GET_ALL failed for " + keys, e);
    }
  }

  /**
   * Asynchronously fetch the table {@code records} for specified {@code keys}. This method must be thread-safe.
   * The default implementation calls getAsync for each key and return a combined future.
   * @param keys keys for the table records
   * @return CompletableFuture for the get request
   */
  default CompletableFuture<Map<K, V>> getAllAsync(Collection<K> keys) {
    Map<K, CompletableFuture<V>> getFutures =  keys.stream().collect(
        Collectors.toMap(k -> k, k -> getAsync(k)));

    return CompletableFuture.allOf(
        Iterables.toArray(getFutures.values(), CompletableFuture.class))
        .thenApply(future ->
          getFutures.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().join())));
  }

  // optionally implement readObject() to initialize transient states
}
