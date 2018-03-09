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
import java.util.List;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.storage.kv.Entry;


/**
 * A function object to be used with a {@link RemoteReadWriteTable} implementation. It encapsulates the functionality
 * of writing table record(s) for a provided set of key(s) to the store.
 *
 * <p> Instances of {@link TableWriteFunction} are meant to be serializable. ie. any non-serializable state
 * (eg: network sockets) should be marked as transient and recreated inside readObject().
 *
 * <p> Implementations are expected to be thread-safe.
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
@InterfaceStability.Unstable
public interface TableWriteFunction<K, V> extends Serializable, InitableFunction, ClosableFunction {
  /**
   * Store single table {@code record} with specified {@code key}. This method must be thread-safe.
   * @param key key for the table record
   * @param record table record to be written
   */
  void put(K key, V record);

  /**
   * Store the table {@code records} with specified {@code keys}. This method must be thread-safe.
   * @param records table records to be written
   */
  default void putAll(List<Entry<K, V>> records) {
    records.forEach(e -> put(e.getKey(), e.getValue()));
  }

  /**
   * Delete the {@code record} with specified {@code key} from the remote store
   * @param key key to the table record to be deleted
   */
  void delete(K key);

  /**
   * Delete all {@code records} with the specified {@code keys} from the remote store
   * @param keys keys for the table records to be written
   */
  default void deleteAll(Collection<K> keys) {
    keys.stream().forEach(k -> delete(k));
  }

  /**
   * Flush the remote store (optional)
   */
  default void flush() {
  }

  // optionally implement readObject() to initialize transient states
}
