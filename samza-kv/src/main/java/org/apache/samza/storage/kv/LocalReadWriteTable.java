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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.ReadWriteTable;

import static org.apache.samza.table.utils.TableMetricsUtil.incCounter;
import static org.apache.samza.table.utils.TableMetricsUtil.updateTimer;


/**
 * A store backed readable and writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class LocalReadWriteTable<K, V> extends LocalReadableTable<K, V>
    implements ReadWriteTable<K, V> {

  /**
   * Constructs an instance of {@link LocalReadWriteTable}
   * @param tableId the table Id
   * @param kvStore the backing store
   */
  public LocalReadWriteTable(String tableId, KeyValueStore kvStore) {
    super(tableId, kvStore);
  }

  @Override
  public void put(K key, V value) {
    if (value != null) {
      instrument(writeMetrics.numPuts, writeMetrics.putNs, () -> kvStore.put(key, value));
    } else {
      delete(key);
    }
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value) {
    CompletableFuture<Void> future = new CompletableFuture();
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
      instrument(writeMetrics.numPutAlls, writeMetrics.putAllNs, () -> kvStore.putAll(toPut));
    }

    if (!toDelete.isEmpty()) {
      deleteAll(toDelete);
    }
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries) {
    CompletableFuture<Void> future = new CompletableFuture();
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
    instrument(writeMetrics.numDeletes, writeMetrics.deleteNs, () -> kvStore.delete(key));
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key) {
    CompletableFuture<Void> future = new CompletableFuture();
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
    instrument(writeMetrics.numDeleteAlls, writeMetrics.deleteAllNs, () -> kvStore.deleteAll(keys));
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys) {
    CompletableFuture<Void> future = new CompletableFuture();
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
    instrument(writeMetrics.numFlushes, writeMetrics.flushNs, () -> kvStore.flush());
  }

  private interface Func0 {
    void apply();
  }

  private void instrument(Counter counter, Timer timer, Func0 func) {
    incCounter(counter);
    long startNs = System.nanoTime();
    func.apply();
    updateTimer(timer, System.nanoTime() - startNs);
  }

}
