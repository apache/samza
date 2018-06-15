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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;

import com.google.common.cache.Cache;


/**
 * Simple cache table backed by a Guava cache instance. Application is expect to build
 * a cache instance with desired parameters and specify it to the table descriptor.
 *
 * @param <K> type of the key in the cache
 * @param <V> type of the value in the cache
 */
public class GuavaCacheTable<K, V> implements ReadWriteTable<K, V> {
  private static final String GROUP_NAME = GuavaCacheTableProvider.class.getSimpleName();

  private final String tableId;
  private final Cache<K, V> cache;

  public GuavaCacheTable(String tableId, Cache<K, V> cache) {
    this.tableId = tableId;
    this.cache = cache;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
    // hit- and miss-rate are provided by CachingTable.
    tableMetricsUtil.newGauge("evict-count", () -> cache.stats().evictionCount());
  }

  @Override
  public void put(K key, V value) {
    if (value != null) {
      cache.put(key, value);
    } else {
      delete(key);
    }
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    entries.forEach(e -> put(e.getKey(), e.getValue()));
  }

  @Override
  public void delete(K key) {
    cache.invalidate(key);
  }

  @Override
  public void deleteAll(List<K> keys) {
    keys.forEach(k -> delete(k));
  }

  @Override
  public synchronized void flush() {
    cache.cleanUp();
  }

  @Override
  public V get(K key) {
    return cache.getIfPresent(key);
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    Map<K, V> getAllResult = new HashMap<>();
    keys.stream().forEach(k -> getAllResult.put(k, get(k)));
    return getAllResult;
  }

  @Override
  public synchronized void close() {
    cache.invalidateAll();
  }
}
