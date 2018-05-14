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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.task.TaskContext;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Striped;


/**
 * A composite table incorporating a cache with a Samza table. The cache is
 * represented as a {@link ReadWriteTable}. Optionally if the table is writable,
 * CachingTable supports both write-through and write-around policy.
 *
 * @param <K> type of the table key
 * @param <V> type of the table value
 */
public class CachingTable<K, V> implements ReadWriteTable<K, V> {
  private static final String GROUP_NAME = CachingTable.class.getSimpleName();

  private final String tableId;
  private final ReadableTable<K, V> rdTable;
  private final ReadWriteTable<K, V> rwTable;
  private final ReadWriteTable<K, V> cache;
  private final boolean isWriteAround;

  // Use stripe based locking to allow parallelism of disjoint keys.
  private final Striped<Lock> stripedLocks;

  // Common caching stats
  private volatile long hitCount;
  private volatile long missCount;

  public CachingTable(String tableId, ReadableTable<K, V> table, ReadWriteTable<K, V> cache, int stripes, boolean isWriteAround) {
    this.tableId = tableId;
    this.rdTable = table;
    this.rwTable = table instanceof ReadWriteTable ? (ReadWriteTable) table : null;
    this.cache = cache;
    this.isWriteAround = isWriteAround;
    this.stripedLocks = Striped.lazyWeakLock(stripes);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    MetricsRegistry metricsReg = taskContext.getMetricsRegistry();
    metricsReg.newGauge(GROUP_NAME, new SupplierGauge(tableId + "-hit-rate", () -> getRate(hitCount)));
    metricsReg.newGauge(GROUP_NAME, new SupplierGauge(tableId + "-miss-rate", () -> getRate(missCount)));
    metricsReg.newGauge(GROUP_NAME, new SupplierGauge(tableId + "-req-count", () -> requestCount()));
  }

  @Override
  public V get(K key) {
    V value = cache.get(key);
    if (value == null) {
      ++missCount;
      value = atomicGet(key);
    } else {
      ++hitCount;
    }
    return value;
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    Map<K, V> retMap = new HashMap<>();
    keys.stream().forEach(k -> retMap.put(k, get(k)));
    return retMap;
  }

  @Override
  public void close() {
    this.cache.close();
    this.rdTable.close();
  }

  @Override
  public void put(K key, V value) {
    Preconditions.checkNotNull(rwTable, "Cannot write to a read-only table: " + rdTable);
    atomicPut(key, value);
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    Preconditions.checkNotNull(rwTable, "Cannot write to a read-only table: " + rdTable);
    entries.forEach(e -> put(e.getKey(), e.getValue()));
  }

  @Override
  public void delete(K key) {
    Preconditions.checkNotNull(rwTable, "Cannot delete from a read-only table: " + rdTable);
    atomicDelete(key);
  }

  @Override
  public void deleteAll(List<K> keys) {
    Preconditions.checkNotNull(rwTable, "Cannot delete from a read-only table: " + rdTable);
    keys.stream().forEach(k -> delete(k));
  }

  @Override
  public synchronized void flush() {
    Preconditions.checkNotNull(rwTable, "Cannot flush a read-only table: " + rdTable);
    rwTable.flush();
  }

  private V atomicGet(K key) {
    Lock lock = stripedLocks.get(key);
    V value = null;
    try {
      lock.lock();
      if (cache.get(key) == null) {
        // Due to the lack of contains() API in ReadableTable, there is
        // no way to tell whether a null return by cache.get(key) means
        // cache miss or the value is actually null. As such, we cannot
        // support negative cache semantics.
        value = rdTable.get(key);
        if (value != null) {
          cache.put(key, value);
        }
      }
    } finally {
      lock.unlock();
    }

    return value;
  }

  private void atomicPut(K key, V value) {
    Lock lock = stripedLocks.get(key);
    try {
      lock.lock();
      rwTable.put(key, value);
      if (!isWriteAround) {
        cache.put(key, value);
      }
    } finally {
      lock.unlock();
    }
  }

  private void atomicDelete(K key) {
    Lock lock = stripedLocks.get(key);
    try {
      lock.lock();
      rwTable.delete(key);
      cache.delete(key);
    } finally {
      lock.unlock();
    }
  }

  private double getRate(long count) {
    long reqs = requestCount();
    return reqs == 0 ? 1.0 : (double) count / reqs;
  }

  double hitRate() {
    return getRate(hitCount);
  }

  double missRate() {
    return getRate(missCount);
  }

  long requestCount() {
    return hitCount + missCount;
  }
}
