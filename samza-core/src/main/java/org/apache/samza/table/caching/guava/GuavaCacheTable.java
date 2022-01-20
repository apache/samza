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

package org.apache.samza.table.caching.guava;

import com.google.common.cache.Cache;
import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.BaseReadWriteUpdateTable;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.table.utils.TableMetricsUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * Simple cache table backed by a Guava cache instance. Application is expect to build
 * a cache instance with desired parameters and specify it to the table descriptor.
 *
 * @param <K> type of the key in the cache
 * @param <V> type of the value in the cache
 */
public class GuavaCacheTable<K, V, U> extends BaseReadWriteUpdateTable<K, V, U>
    implements ReadWriteUpdateTable<K, V, U> {

  private final Cache<K, V> cache;

  public GuavaCacheTable(String tableId, Cache<K, V> cache) {
    super(tableId);
    this.cache = cache;
  }

  @Override
  public void init(Context context) {
    super.init(context);
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(context, this, tableId);
    // hit- and miss-rate are provided by CachingTable.
    tableMetricsUtil.newGauge("evict-count", () -> cache.stats().evictionCount());
  }

  @Override
  public V get(K key, Object ... args) {
    try {
      return getAsync(key).get();
    } catch (Exception e) {
      throw new SamzaException("GET failed for " + key, e);
    }
  }

  @Override
  public CompletableFuture<V> getAsync(K key, Object ... args) {
    CompletableFuture<V> future = new CompletableFuture<>();
    try {
      future.complete(cache.getIfPresent(key));
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public Map<K, V> getAll(List<K> keys, Object ... args) {
    try {
      return getAllAsync(keys).get();
    } catch (Exception e) {
      throw new SamzaException("GET_ALL failed for " + keys, e);
    }
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys, Object ... args) {
    CompletableFuture<Map<K, V>> future = new CompletableFuture<>();
    try {
      future.complete(cache.getAllPresent(keys));
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void put(K key, V value, Object ... args) {
    try {
      putAsync(key, value).get();
    } catch (Exception e) {
      throw new SamzaException("PUT failed for " + key, e);
    }
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value, Object ... args) {
    if (key == null) {
      return deleteAsync(key);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      cache.put(key, value);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void putAll(List<Entry<K, V>> entries, Object ... args) {
    try {
      putAllAsync(entries).get();
    } catch (Exception e) {
      throw new SamzaException("PUT_ALL failed", e);
    }
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries, Object ... args) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      // Separate out put vs delete records
      List<K> delKeys = new ArrayList<>();
      List<Entry<K, V>> putRecords = new ArrayList<>();
      entries.forEach(r -> {
        if (r.getValue() != null) {
          putRecords.add(r);
        } else {
          delKeys.add(r.getKey());
        }
      });

      cache.invalidateAll(delKeys);
      putRecords.forEach(e -> put(e.getKey(), e.getValue()));
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void update(K key, U update) {
    throw new SamzaException("Cache tables do not support update operations");
  }

  @Override
  public CompletableFuture<Void> updateAsync(K key, U update) {
    throw new SamzaException("Cache tables do not support update operations");
  }

  @Override
  public void updateAll(List<Entry<K, U>> updates) {
    throw new SamzaException("Cache tables do not support update operations");
  }

  @Override
  public CompletableFuture<Void> updateAllAsync(List<Entry<K, U>> updates) {
    throw new SamzaException("Cache tables do not support update operations");
  }

  @Override
  public void delete(K key, Object ... args) {
    try {
      deleteAsync(key).get();
    } catch (Exception e) {
      throw new SamzaException("DELETE failed", e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key, Object ... args) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      cache.invalidate(key);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void deleteAll(List<K> keys, Object ... args) {
    try {
      deleteAllAsync(keys).get();
    } catch (Exception e) {
      throw new SamzaException("DELETE_ALL failed", e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys, Object ... args) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      cache.invalidateAll(keys);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public synchronized void flush() {
    cache.cleanUp();
  }

  @Override
  public synchronized void close() {
    cache.invalidateAll();
  }
}
