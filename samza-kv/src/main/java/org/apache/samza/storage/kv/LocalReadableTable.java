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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.context.Context;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.utils.DefaultTableReadMetrics;


/**
 * A store backed readable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class LocalReadableTable<K, V> implements ReadableTable<K, V> {

  protected final KeyValueStore<K, V> kvStore;
  protected final String tableId;

  protected DefaultTableReadMetrics readMetrics;

  /**
   * Constructs an instance of {@link LocalReadableTable}
   * @param tableId the table Id
   * @param kvStore the backing store
   */
  public LocalReadableTable(String tableId, KeyValueStore<K, V> kvStore) {
    Preconditions.checkArgument(tableId != null & !tableId.isEmpty() , "invalid tableId");
    Preconditions.checkNotNull(kvStore, "null KeyValueStore");
    this.tableId = tableId;
    this.kvStore = kvStore;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(Context context) {
    readMetrics = new DefaultTableReadMetrics(context, this, tableId);
  }

  @Override
  public V get(K key) {
    readMetrics.numGets.inc();
    long startNs = System.nanoTime();
    V result = kvStore.get(key);
    readMetrics.getNs.update(System.nanoTime() - startNs);
    if (result == null) {
      readMetrics.numMissedLookups.inc();
    }
    return result;
  }

  @Override
  public CompletableFuture<V> getAsync(K key) {
    CompletableFuture<V> future = new CompletableFuture();
    try {
      future.complete(get(key));
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    readMetrics.numGetAlls.inc();
    long startNs = System.nanoTime();
    Map<K, V> result = kvStore.getAll(keys);
    readMetrics.getAllNs.update(System.nanoTime() - startNs);
    result.values().stream().filter(Objects::isNull).map(v -> readMetrics.numMissedLookups.inc());
    return result;
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys) {
    CompletableFuture<Map<K, V>> future = new CompletableFuture();
    try {
      future.complete(getAll(keys));
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void close() {
    // The KV store is not closed here as it may still be needed by downstream operators,
    // it will be closed by the SamzaContainer
  }
}
