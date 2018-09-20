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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.utils.DefaultTableReadMetrics;
import org.apache.samza.table.utils.DefaultTableWriteMetrics;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;

import com.google.common.base.Preconditions;


/**
 * A composite table incorporating a cache with a Samza table. The cache is
 * represented as a {@link ReadWriteTable}.
 *
 * The intented use case is to optimize the latency of accessing the actual table, eg.
 * remote tables, when eventual consistency between cache and table is acceptable.
 * The cache is expected to support TTL such that the values can be refreshed at some
 * point.
 *
 * If the actual table is read-write table, CachingTable supports both write-through
 * and write-around (writes bypassing cache) policies. For write-through policy, it
 * supports read-after-write semantics because the value is cached after written to
 * the table.
 *
 * Note that there is no synchronization in CachingTable because it is impossible to
 * implement a critical section between table read/write and cache update in the async
 * code paths without serializing all async operations for the same keys. Given stale
 * data is a presumed trade off for using a cache for table, it should be acceptable
 * for the data in table and cache are out-of-sync. Moreover, unsynchronized operations
 * in CachingTable also deliver higher performance when there is contention.
 *
 * @param <K> type of the table key
 * @param <V> type of the table value
 */
public class CachingTable<K, V> implements ReadWriteTable<K, V> {
  private final String tableId;
  private final ReadableTable<K, V> rdTable;
  private final ReadWriteTable<K, V> rwTable;
  private final ReadWriteTable<K, V> cache;
  private final boolean isWriteAround;

  // Metrics
  private DefaultTableReadMetrics readMetrics;
  private DefaultTableWriteMetrics writeMetrics;

  // Common caching stats
  private AtomicLong hitCount = new AtomicLong();
  private AtomicLong missCount = new AtomicLong();

  public CachingTable(String tableId, ReadableTable<K, V> table, ReadWriteTable<K, V> cache, boolean isWriteAround) {
    this.tableId = tableId;
    this.rdTable = table;
    this.rwTable = table instanceof ReadWriteTable ? (ReadWriteTable) table : null;
    this.cache = cache;
    this.isWriteAround = isWriteAround;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    readMetrics = new DefaultTableReadMetrics(containerContext, taskContext, this, tableId);
    writeMetrics = new DefaultTableWriteMetrics(containerContext, taskContext, this, tableId);
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
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
  private List<K> lookupCache(List<K> keys, Map<K, V> records) {
    List<K> missKeys = new ArrayList<>();
    records.putAll(cache.getAll(keys));
    keys.forEach(k -> {
        if (!records.containsKey(k)) {
          missKeys.add(k);
        }
      });
    return missKeys;
  }

  @Override
  public V get(K key) {
    try {
      return getAsync(key).get();
    } catch (InterruptedException e) {
      throw new SamzaException(e);
    } catch (Exception e) {
      throw (SamzaException) e.getCause();
    }
  }

  @Override
  public CompletableFuture<V> getAsync(K key) {
    readMetrics.numGets.inc();
    V value = cache.get(key);
    if (value != null) {
      hitCount.incrementAndGet();
      return CompletableFuture.completedFuture(value);
    }

    long startNs = System.nanoTime();
    missCount.incrementAndGet();

    return rdTable.getAsync(key).handle((result, e) -> {
        if (e != null) {
          throw new SamzaException("Failed to get the record for " + key, e);
        } else {
          if (result != null) {
            cache.put(key, result);
          }
          readMetrics.getNs.update(System.nanoTime() - startNs);
          return result;
        }
      });
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    try {
      return getAllAsync(keys).get();
    } catch (InterruptedException e) {
      throw new SamzaException(e);
    } catch (Exception e) {
      throw (SamzaException) e.getCause();
    }
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys) {
    readMetrics.numGetAlls.inc();
    // Make a copy of entries which might be immutable
    Map<K, V> getAllResult = new HashMap<>();
    List<K> missingKeys = lookupCache(keys, getAllResult);

    if (missingKeys.isEmpty()) {
      return CompletableFuture.completedFuture(getAllResult);
    }

    long startNs = System.nanoTime();
    return rdTable.getAllAsync(missingKeys).handle((records, e) -> {
        if (e != null) {
          throw new SamzaException("Failed to get records for " + keys, e);
        } else {
          if (records != null) {
            cache.putAll(records.entrySet().stream()
                .map(r -> new Entry<>(r.getKey(), r.getValue()))
                .collect(Collectors.toList()));
            getAllResult.putAll(records);
          }
          readMetrics.getAllNs.update(System.nanoTime() - startNs);
          return getAllResult;
        }
      });
  }

  @Override
  public void put(K key, V value) {
    try {
      putAsync(key, value).get();
    } catch (InterruptedException e) {
      throw new SamzaException(e);
    } catch (Exception e) {
      throw (SamzaException) e.getCause();
    }
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value) {
    writeMetrics.numPuts.inc();
    Preconditions.checkNotNull(rwTable, "Cannot write to a read-only table: " + rdTable);

    long startNs = System.nanoTime();
    return rwTable.putAsync(key, value).handle((result, e) -> {
        if (e != null) {
          throw new SamzaException(String.format("Failed to put a record, key=%s, value=%s", key, value), e);
        } else if (!isWriteAround) {
          if (value == null) {
            cache.delete(key);
          } else {
            cache.put(key, value);
          }
        }
        writeMetrics.putNs.update(System.nanoTime() - startNs);
        return result;
      });
  }

  @Override
  public void putAll(List<Entry<K, V>> records) {
    try {
      putAllAsync(records).get();
    } catch (InterruptedException e) {
      throw new SamzaException(e);
    } catch (Exception e) {
      throw (SamzaException) e.getCause();
    }
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> records) {
    writeMetrics.numPutAlls.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot write to a read-only table: " + rdTable);
    return rwTable.putAllAsync(records).handle((result, e) -> {
        if (e != null) {
          throw new SamzaException("Failed to put records " + records, e);
        } else if (!isWriteAround) {
          cache.putAll(records);
        }

        writeMetrics.putAllNs.update(System.nanoTime() - startNs);
        return result;
      });
  }

  @Override
  public void delete(K key) {
    try {
      deleteAsync(key).get();
    } catch (InterruptedException e) {
      throw new SamzaException(e);
    } catch (Exception e) {
      throw (SamzaException) e.getCause();
    }
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key) {
    writeMetrics.numDeletes.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot delete from a read-only table: " + rdTable);
    return rwTable.deleteAsync(key).handle((result, e) -> {
        if (e != null) {
          throw new SamzaException("Failed to delete the record for " + key, e);
        } else if (!isWriteAround) {
          cache.delete(key);
        }
        writeMetrics.deleteNs.update(System.nanoTime() - startNs);
        return result;
      });
  }

  @Override
  public void deleteAll(List<K> keys) {
    try {
      deleteAllAsync(keys).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys) {
    writeMetrics.numDeleteAlls.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot delete from a read-only table: " + rdTable);
    return rwTable.deleteAllAsync(keys).handle((result, e) -> {
        if (e != null) {
          throw new SamzaException("Failed to delete the record for " + keys, e);
        } else if (!isWriteAround) {
          cache.deleteAll(keys);
        }
        writeMetrics.deleteAllNs.update(System.nanoTime() - startNs);
        return result;
      });
  }

  @Override
  public synchronized void flush() {
    writeMetrics.numFlushes.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot flush a read-only table: " + rdTable);
    rwTable.flush();
    writeMetrics.flushNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void close() {
    this.cache.close();
    this.rdTable.close();
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
