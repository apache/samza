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

import com.google.common.base.Supplier;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.BaseReadableTable;

import static org.apache.samza.table.utils.TableMetricsUtil.incCounter;
import static org.apache.samza.table.utils.TableMetricsUtil.updateTimer;

/**
 * A store backed readable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class LocalReadableTable<K, V> extends BaseReadableTable<K, V> {

  protected final KeyValueStore<K, V> kvStore;

  /**
   * Constructs an instance of {@link LocalReadableTable}
   * @param tableId the table Id
   * @param kvStore the backing store
   */
  public LocalReadableTable(String tableId, KeyValueStore<K, V> kvStore) {
    super(tableId);
    Preconditions.checkNotNull(kvStore, "null KeyValueStore");
    this.kvStore = kvStore;
  }

  @Override
  public V get(K key) {
    V result = instrument(readMetrics.numGets, readMetrics.getNs, () -> kvStore.get(key));
    if (result == null) {
      incCounter(readMetrics.numMissedLookups);
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
    Map<K, V> result = instrument(readMetrics.numGetAlls, readMetrics.getAllNs, () -> kvStore.getAll(keys));
    result.values().stream().filter(Objects::isNull).forEach(v -> incCounter(readMetrics.numMissedLookups));
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

  private <T> T instrument(Counter counter, Timer timer, Supplier<T> func) {
    incCounter(counter);
    long startNs = System.nanoTime();
    T result = func.get();
    updateTimer(timer, System.nanoTime() - startNs);
    return result;
  }
}
