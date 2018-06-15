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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Striped;


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
 * Table and cache are updated (put/delete) in an atomic manner as such it is thread
 * safe for concurrent accesses. Strip locks are used for fine-grained synchronization
 * and the number of stripes is configurable.
 *
 * NOTE: Cache get is not synchronized with put for better parallelism in the read path.
 * As such, cache table implementation is expected to be thread-safe for concurrent
 * accesses.
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

  // Metrics
  private Timer getNs;
  private Timer getAllNs;
  private Timer putNs;
  private Timer putAllNs;
  private Timer deleteNs;
  private Timer deleteAllNs;
  private Timer flushNs;
  private Counter numGets;
  private Counter numGetAlls;
  private Counter numPuts;
  private Counter numPutAlls;
  private Counter numDeletes;
  private Counter numDeleteAlls;
  private Counter numFlushes;

  // Common caching stats
  private AtomicLong hitCount = new AtomicLong();
  private AtomicLong missCount = new AtomicLong();

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
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
    getNs = tableMetricsUtil.newTimer("get-ns");
    getAllNs = tableMetricsUtil.newTimer("getAll-ns");
    putNs = tableMetricsUtil.newTimer("put-ns");
    putAllNs = tableMetricsUtil.newTimer("putAll-ns");
    deleteNs = tableMetricsUtil.newTimer("delete-ns");
    deleteAllNs = tableMetricsUtil.newTimer("deleteAll-ns");
    flushNs = tableMetricsUtil.newTimer("flush-ns");
    numGets = tableMetricsUtil.newCounter("num-gets");
    numGetAlls = tableMetricsUtil.newCounter("num-getAlls");
    numPuts = tableMetricsUtil.newCounter("num-puts");
    numPutAlls = tableMetricsUtil.newCounter("num-putAlls");
    numDeletes = tableMetricsUtil.newCounter("num-deletes");
    numDeleteAlls = tableMetricsUtil.newCounter("num-deleteAlls");
    numFlushes = tableMetricsUtil.newCounter("num-flushes");
    tableMetricsUtil.newGauge("hit-rate", () -> hitRate());
    tableMetricsUtil.newGauge("miss-rate", () -> missRate());
    tableMetricsUtil.newGauge("req-count", () -> requestCount());
  }

  @Override
  public V get(K key) {
    numGets.inc();
    long startNs = System.nanoTime();
    V value = cache.get(key);
    if (value == null) {
      missCount.incrementAndGet();
      Lock lock = stripedLocks.get(key);
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
    } else {
      hitCount.incrementAndGet();
    }
    getNs.update(System.nanoTime() - startNs);
    return value;
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    numGetAlls.inc();
    long startNs = System.nanoTime();
    Map<K, V> getAllResult = new HashMap<>();
    keys.stream().forEach(k -> getAllResult.put(k, get(k)));
    getAllNs.update(System.nanoTime() - startNs);
    return getAllResult;
  }

  @Override
  public void put(K key, V value) {
    numPuts.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot write to a read-only table: " + rdTable);
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
    putNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    numPutAlls.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot write to a read-only table: " + rdTable);
    entries.forEach(e -> put(e.getKey(), e.getValue()));
    putAllNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void delete(K key) {
    numDeletes.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot delete from a read-only table: " + rdTable);
    Lock lock = stripedLocks.get(key);
    try {
      lock.lock();
      rwTable.delete(key);
      cache.delete(key);
    } finally {
      lock.unlock();
    }
    deleteNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void deleteAll(List<K> keys) {
    numDeleteAlls.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot delete from a read-only table: " + rdTable);
    keys.stream().forEach(k -> delete(k));
    deleteAllNs.update(System.nanoTime() - startNs);
  }

  @Override
  public synchronized void flush() {
    numFlushes.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot flush a read-only table: " + rdTable);
    rwTable.flush();
    flushNs.update(System.nanoTime() - startNs);
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
