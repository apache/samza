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
import org.apache.samza.SamzaException;
import org.apache.samza.storage.kv.Entry;

/**
 * This class delegates all it's method calls to the underlying ReadWriteUpdateTable
 * except for update methods.
 * */
public class ReadWriteTableDelegate<K, V> implements ReadWriteTable<K, V> {
  private final ReadWriteUpdateTable<K, V, Void> updateTable;

  /**
   * @param updateTable input table.
   * */
  public ReadWriteTableDelegate(ReadWriteUpdateTable<K, V, Void> updateTable) {
    this.updateTable = updateTable;
  }

  @Override
  public CompletableFuture<V> getAsync(K key, Object... args) {
    return updateTable.getAsync(key, args);
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys, Object... args) {
    return updateTable.getAllAsync(keys, args);
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value, Object... args) {
    return updateTable.putAsync(key, value, args);
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries, Object... args) {
    return updateTable.putAllAsync(entries, args);
  }

  @Override
  public CompletableFuture<Void> updateAsync(K key, Void update) {
    throw new SamzaException("Not supported");
  }

  @Override
  public CompletableFuture<Void> updateAllAsync(List<Entry<K, Void>> updates) {
    throw new SamzaException("Not supported");
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key, Object... args) {
    return updateTable.deleteAsync(key, args);
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys, Object... args) {
    return updateTable.deleteAllAsync(keys, args);
  }

  @Override
  public void flush() {
    updateTable.flush();
  }

  @Override
  public void close() {
    updateTable.close();
  }

  @Override
  public V get(K key, Object... args) {
    return updateTable.get(key, args);
  }

  @Override
  public Map<K, V> getAll(List<K> keys, Object... args) {
    return updateTable.getAll(keys, args);
  }

  @Override
  public void put(K key, V value, Object... args) {
    updateTable.put(key, value, args);
  }

  @Override
  public void putAll(List<Entry<K, V>> entries, Object... args) {
    updateTable.putAll(entries, args);
  }

  @Override
  public void delete(K key, Object... args) {
    updateTable.delete(key, args);
  }

  @Override
  public void deleteAll(List<K> keys, Object... args) {
    updateTable.deleteAll(keys, args);
  }
}
