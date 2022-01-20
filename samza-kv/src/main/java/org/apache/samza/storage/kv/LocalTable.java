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
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.BaseReadWriteUpdateTable;

import static org.apache.samza.table.utils.TableMetricsUtil.incCounter;
import static org.apache.samza.table.utils.TableMetricsUtil.updateTimer;


/**
 * A store backed readable and writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public final class LocalTable<K, V, U> extends BaseReadWriteUpdateTable<K, V, U> {

  protected final KeyValueStore<K, V> kvStore;

  /**
   * Constructs an instance of {@link LocalTable}
   * @param tableId the table Id
   * @param kvStore the backing store
   */
  public LocalTable(String tableId, KeyValueStore<K, V> kvStore) {
    super(tableId);
    Preconditions.checkNotNull(kvStore, "null KeyValueStore");
    this.kvStore = kvStore;
  }

  @Override
  public V get(K key, Object ... args) {
    V result = instrument(metrics.numGets, metrics.getNs, () -> kvStore.get(key));
    if (result == null) {
      incCounter(metrics.numMissedLookups);
    }
    return result;
  }

  @Override
  public CompletableFuture<V> getAsync(K key, Object ... args) {
    CompletableFuture<V> future = new CompletableFuture<>();
    try {
      future.complete(get(key, args));
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public Map<K, V> getAll(List<K> keys, Object ... args) {
    Map<K, V> result = instrument(metrics.numGetAlls, metrics.getAllNs, () -> kvStore.getAll(keys));
    result.values().stream().filter(Objects::isNull).forEach(v -> incCounter(metrics.numMissedLookups));
    return result;
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys, Object ... args) {
    CompletableFuture<Map<K, V>> future = new CompletableFuture<>();
    try {
      future.complete(getAll(keys, args));
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void put(K key, V value, Object ... args) {
    if (value != null) {
      instrument(metrics.numPuts, metrics.putNs, () -> kvStore.put(key, value));
    } else {
      delete(key);
    }
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value, Object ... args) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      put(key, value, args);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void putAll(List<Entry<K, V>> entries, Object ... args) {
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
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries, Object ... args) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      putAll(entries, args);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void update(K key, U update) {
    throw new SamzaException("Local tables do not support update operations");
  }

  @Override
  public CompletableFuture<Void> updateAsync(K key, U update) {
    throw new SamzaException("Local tables do not support update operations");
  }

  @Override
  public void updateAll(List<Entry<K, U>> updates) {
    throw new SamzaException("Local tables do not support update operations");
  }

  @Override
  public CompletableFuture<Void> updateAllAsync(List<Entry<K, U>> updates) {
    throw new SamzaException("Local tables do not support update operations");
  }

  @Override
  public void delete(K key, Object ... args) {
    instrument(metrics.numDeletes, metrics.deleteNs, () -> kvStore.delete(key));
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key, Object ... args) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      delete(key, args);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void deleteAll(List<K> keys, Object ... args) {
    instrument(metrics.numDeleteAlls, metrics.deleteAllNs, () -> kvStore.deleteAll(keys));
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys, Object ... args) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      deleteAll(keys, args);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Refer to {@link KeyValueStore#range(Object, Object)}
   *
   * @param from the key specifying the low endpoint (inclusive) of the keys in the returned range.
   * @param to the key specifying the high endpoint (exclusive) of the keys in the returned range.
   * @return an iterator for the specified key range.
   * @throws NullPointerException if null is used for {@code from} or {@code to}.
   */
  public KeyValueIterator<K, V> range(K from, K to) {
    return kvStore.range(from, to);
  }

  /**
   * Refer to {@link KeyValueStore#snapshot(Object, Object)}
   *
   * @param from the key specifying the low endpoint (inclusive) of the keys in the returned range.
   * @param to the key specifying the high endpoint (exclusive) of the keys in the returned range.
   * @return a snapshot for the specified key range.
   * @throws NullPointerException if null is used for {@code from} or {@code to}.
   */
  public KeyValueSnapshot<K, V> snapshot(K from, K to) {
    return kvStore.snapshot(from, to);
  }

  /**
   * Refer to {@link KeyValueStore#all()}
   *
   * @return an iterator for all entries in this key-value store.
   */
  public KeyValueIterator<K, V> all() {
    return kvStore.all();
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
