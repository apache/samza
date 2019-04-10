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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.BaseReadWriteTable;

import static org.apache.samza.table.utils.TableMetricsUtil.incCounter;
import static org.apache.samza.table.utils.TableMetricsUtil.updateTimer;


/**
 * A store backed readable and writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class LocalTable<K, V> extends BaseReadWriteTable<K, V> {

  protected final KeyValueStore<K, V> kvStore;

  /**
   * Constructs an instance of {@link LocalTable}
   * @param tableId the table Id
   * @param kvStore the backing store
   */
  public LocalTable(String tableId, KeyValueStore kvStore) {
    super(tableId);
    Preconditions.checkNotNull(kvStore, "null KeyValueStore");
    this.kvStore = kvStore;
  }

  @Override
  public V get(K key) {
    V result = instrument(metrics.numGets, metrics.getNs, () -> kvStore.get(key));
    if (result == null) {
      incCounter(metrics.numMissedLookups);
    }
    return result;
  }

  @Override
  public CompletionStage<V> getAsync(K key) {
    CompletableFuture<V> future = new CompletableFuture<>();
    try {
      future.complete(get(key));
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    Map<K, V> result = instrument(metrics.numGetAlls, metrics.getAllNs, () -> kvStore.getAll(keys));
    result.values().stream().filter(Objects::isNull).forEach(v -> incCounter(metrics.numMissedLookups));
    return result;
  }

  @Override
  public CompletionStage<Map<K, V>> getAllAsync(List<K> keys) {
    CompletableFuture<Map<K, V>> future = new CompletableFuture<>();
    try {
      future.complete(getAll(keys));
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void put(K key, V value) {
    if (value != null) {
      instrument(metrics.numPuts, metrics.putNs, () -> kvStore.put(key, value));
    } else {
      delete(key);
    }
  }

  @Override
  public CompletionStage<Void> putAsync(K key, V value) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      put(key, value);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    List<Entry<K, V>> toPut = new LinkedList<>();
    List<K> toDelete = new LinkedList<>();
    entries.forEach(e -> {
        if (e.getValue() != null) {
          toPut.add(e);
        } else {
          toDelete.add(e.getKey());
        }
      });

    if (!toPut.isEmpty()) {
      instrument(metrics.numPutAlls, metrics.putAllNs, () -> kvStore.putAll(toPut));
    }

    if (!toDelete.isEmpty()) {
      deleteAll(toDelete);
    }
  }

  @Override
  public CompletionStage<Void> putAllAsync(List<Entry<K, V>> entries) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      putAll(entries);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void delete(K key) {
    instrument(metrics.numDeletes, metrics.deleteNs, () -> kvStore.delete(key));
  }

  @Override
  public CompletionStage<Void> deleteAsync(K key) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      delete(key);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void deleteAll(List<K> keys) {
    instrument(metrics.numDeleteAlls, metrics.deleteAllNs, () -> kvStore.deleteAll(keys));
  }

  @Override
  public CompletionStage<Void> deleteAllAsync(List<K> keys) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      deleteAll(keys);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void flush() {
    // Since the KV store will be flushed by task storage manager, only update metrics here
    instrument(metrics.numFlushes, metrics.flushNs, () -> { });
  }

  @Override
  public void close() {
    // The KV store is not closed here as it may still be needed by downstream operators,
    // it will be closed by the SamzaContainer
  }

  private <T> T instrument(Counter counter, Timer timer, Supplier<T> func) {
    incCounter(counter);
    long startNs = clock.nanoTime();
    T result = func.get();
    updateTimer(timer, clock.nanoTime() - startNs);
    return result;
  }

  private void instrument(Counter counter, Timer timer, Func0 func) {
    incCounter(counter);
    long startNs = clock.nanoTime();
    func.apply();
    updateTimer(timer, clock.nanoTime() - startNs);
  }

}
