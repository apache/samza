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

package org.apache.samza.table.caching;

import com.google.common.base.Preconditions;
import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.BaseReadWriteUpdateTable;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.table.utils.TableMetricsUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.samza.table.utils.TableMetricsUtil.incCounter;
import static org.apache.samza.table.utils.TableMetricsUtil.updateTimer;


/**
 * A hybrid table incorporating a cache with a Samza table. The cache is
 * represented as a {@link ReadWriteUpdateTable}.
 *
 * The intented use case is to optimize the latency of accessing the actual table, e.g.
 * remote tables, when eventual consistency between cache and table is acceptable.
 * The cache is expected to support TTL such that the values can be refreshed at some
 * point.
 *
 * {@link CachingTable} supports write-through and write-around (writes bypassing cache) policies.
 * For write-through policy, it supports read-after-write semantics because the value is
 * cached after written to the table.
 *
 * Note that there is no synchronization in {@link CachingTable} because it is impossible to
 * implement a critical section between table read/write and cache update in the async
 * code paths without serializing all async operations for the same keys. Given stale
 * data is a presumed trade-off for using a cache with table, it should be acceptable
 * for the data in table and cache to be temporarily out-of-sync. Moreover, unsynchronized
 * operations in {@link CachingTable} also deliver higher performance when there is contention.
 *
 * @param <K> type of the table key
 * @param <V> type of the table value
 */
public class CachingTable<K, V, U> extends BaseReadWriteUpdateTable<K, V, U>
    implements ReadWriteUpdateTable<K, V, U> {

  private final ReadWriteUpdateTable<K, V, U> table;
  private final ReadWriteUpdateTable<K, V, U> cache;
  private final boolean isWriteAround;

  // Common caching stats
  private AtomicLong hitCount = new AtomicLong();
  private AtomicLong missCount = new AtomicLong();

  public CachingTable(String tableId, ReadWriteUpdateTable<K, V, U> table, ReadWriteUpdateTable<K, V, U> cache, boolean isWriteAround) {
    super(tableId);
    this.table = table;
    this.cache = cache;
    this.isWriteAround = isWriteAround;
  }

  @Override
  public void init(Context context) {
    super.init(context);
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(context, this, tableId);
    tableMetricsUtil.newGauge("hit-rate", () -> hitRate());
    tableMetricsUtil.newGauge("miss-rate", () -> missRate());
    tableMetricsUtil.newGauge("req-count", () -> requestCount());
  }

  /**
   * Lookup the cache and return the keys that are missed in cache
   * @param keys keys to be looked up
   * @param records result map
   * @return list of keys missed in the cache
   */
  private List<K> lookupCache(List<K> keys, Map<K, V> records, Object ... args) {
    List<K> missKeys = new ArrayList<>();
    records.putAll(cache.getAll(keys, args));
    keys.forEach(k -> {
      if (!records.containsKey(k)) {
        missKeys.add(k);
      }
    });
    return missKeys;
  }

  @Override
  public V get(K key, Object ... args) {
    try {
      return getAsync(key, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<V> getAsync(K key, Object ... args) {
    incCounter(metrics.numGets);
    V value = cache.get(key, args);
    if (value != null) {
      hitCount.incrementAndGet();
      return CompletableFuture.completedFuture(value);
    }

    long startNs = clock.nanoTime();
    missCount.incrementAndGet();

    return table.getAsync(key, args).handle((result, e) -> {
      if (e != null) {
        throw new SamzaException("Failed to get the record for " + key, e);
      } else {
        if (result != null) {
          cache.put(key, result, args);
        }
        updateTimer(metrics.getNs, clock.nanoTime() - startNs);
        return result;
      }
    });
  }

  @Override
  public Map<K, V> getAll(List<K> keys, Object ... args) {
    try {
      return getAllAsync(keys, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys, Object ... args) {
    incCounter(metrics.numGetAlls);
    // Make a copy of entries which might be immutable
    Map<K, V> getAllResult = new HashMap<>();
    List<K> missingKeys = lookupCache(keys, getAllResult);

    if (missingKeys.isEmpty()) {
      return CompletableFuture.completedFuture(getAllResult);
    }

    long startNs = clock.nanoTime();
    return table.getAllAsync(missingKeys, args).handle((records, e) -> {
      if (e != null) {
        throw new SamzaException("Failed to get records for " + keys, e);
      } else {
        if (records != null) {
          cache.putAll(records.entrySet().stream()
              .map(r -> new Entry<>(r.getKey(), r.getValue()))
              .collect(Collectors.toList()), args);
          getAllResult.putAll(records);
        }
        updateTimer(metrics.getAllNs, clock.nanoTime() - startNs);
        return getAllResult;
      }
    });
  }

  @Override
  public void put(K key, V value, Object ... args) {
    try {
      putAsync(key, value, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value, Object ... args) {
    incCounter(metrics.numPuts);
    Preconditions.checkNotNull(table, "Cannot write to a read-only table: " + table);

    long startNs = clock.nanoTime();
    return table.putAsync(key, value, args).handle((result, e) -> {
      if (e != null) {
        throw new SamzaException("Failed to put a record, key=" + key + ", value=" + value, e);
      } else if (!isWriteAround) {
        if (value == null) {
          cache.delete(key, args);
        } else {
          cache.put(key, value, args);
        }
      }
      updateTimer(metrics.putNs, clock.nanoTime() - startNs);
      return result;
    });
  }

  @Override
  public void putAll(List<Entry<K, V>> records, Object ... args) {
    try {
      putAllAsync(records, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> records, Object ... args) {
    incCounter(metrics.numPutAlls);
    long startNs = clock.nanoTime();
    Preconditions.checkNotNull(table, "Cannot write to a read-only table: " + table);
    return table.putAllAsync(records, args).handle((result, e) -> {
      if (e != null) {
        throw new SamzaException("Failed to put records " + records, e);
      } else if (!isWriteAround) {
        cache.putAll(records, args);
      }

      updateTimer(metrics.putAllNs, clock.nanoTime() - startNs);
      return result;
    });
  }

  @Override
  public void update(K key, U update) {
    throw new SamzaException("Caching not supported with updates");
  }

  @Override
  public void updateAll(List<Entry<K, U>> updates) {
    throw new SamzaException("Caching not supported with updates");
  }

  @Override
  public CompletableFuture<Void> updateAsync(K key, U update) {
    throw new SamzaException("Caching not supported with updates");
  }

  @Override
  public CompletableFuture<Void> updateAllAsync(List<Entry<K, U>> updates) {
    throw new SamzaException("Caching not supported with updates");
  }

  @Override
  public void delete(K key, Object ... args) {
    try {
      deleteAsync(key, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key, Object ... args) {
    incCounter(metrics.numDeletes);
    long startNs = clock.nanoTime();
    Preconditions.checkNotNull(table, "Cannot delete from a read-only table: " + table);
    return table.deleteAsync(key, args).handle((result, e) -> {
      if (e != null) {
        throw new SamzaException("Failed to delete the record for " + key, e);
      } else if (!isWriteAround) {
        cache.delete(key, args);
      }
      updateTimer(metrics.deleteNs, clock.nanoTime() - startNs);
      return result;
    });
  }

  @Override
  public void deleteAll(List<K> keys, Object ... args) {
    try {
      deleteAllAsync(keys, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys, Object ... args) {
    incCounter(metrics.numDeleteAlls);
    long startNs = clock.nanoTime();
    Preconditions.checkNotNull(table, "Cannot delete from a read-only table: " + table);
    return table.deleteAllAsync(keys, args).handle((result, e) -> {
      if (e != null) {
        throw new SamzaException("Failed to delete the record for " + keys, e);
      } else if (!isWriteAround) {
        cache.deleteAll(keys, args);
      }
      updateTimer(metrics.deleteAllNs, clock.nanoTime() - startNs);
      return result;
    });
  }

  @Override
  public <T> CompletableFuture<T> readAsync(int opId, Object... args) {
    incCounter(metrics.numReads);
    long startNs = clock.nanoTime();
    return table.readAsync(opId, args).handle((result, e) -> {
      if (e != null) {
        throw new SamzaException("Failed to read, opId=" + opId, e);
      }
      updateTimer(metrics.readNs, clock.nanoTime() - startNs);
      return (T) result;
    });
  }

  @Override
  public <T> CompletableFuture<T> writeAsync(int opId, Object... args) {
    incCounter(metrics.numWrites);
    long startNs = clock.nanoTime();
    return table.writeAsync(opId, args).handle((result, e) -> {
      if (e != null) {
        throw new SamzaException("Failed to write, opId=" + opId, e);
      }
      updateTimer(metrics.writeNs, clock.nanoTime() - startNs);
      return (T) result;
    });
  }

  @Override
  public synchronized void flush() {
    incCounter(metrics.numFlushes);
    long startNs = clock.nanoTime();
    Preconditions.checkNotNull(table, "Cannot flush a read-only table: " + table);
    table.flush();
    updateTimer(metrics.flushNs, clock.nanoTime() - startNs);
  }

  @Override
  public void close() {
    cache.close();
    table.close();
  }

  double hitRate() {
    long reqs = requestCount();
    return reqs == 0 ? 1.0 : (double) hitCount.get() / reqs;
  }

  double missRate() {
    long reqs = requestCount();
    return reqs == 0 ? 1.0 : (double) missCount.get() / reqs;
  }

  long requestCount() {
    return hitCount.get() + missCount.get();
  }
}
