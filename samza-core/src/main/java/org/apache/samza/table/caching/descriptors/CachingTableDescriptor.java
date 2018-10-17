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

package org.apache.samza.table.caching.descriptors;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.table.descriptors.BaseTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.descriptors.BaseHybridTableDescriptor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * Table descriptor for {@link org.apache.samza.table.caching.CachingTable}.
 * @param <K> type of the key in the cache
 * @param <V> type of the value in the cache
 */
public class CachingTableDescriptor<K, V> extends BaseHybridTableDescriptor<K, V, CachingTableDescriptor<K, V>> {
  private Duration readTtl;
  private Duration writeTtl;
  private long cacheSize;
  private TableDescriptor<K, V, ?> cache;
  private TableDescriptor<K, V, ?> table;
  private boolean isWriteAround;

  /**
   * Constructs a table descriptor instance with internal cache
   *
   * @param tableId Id of the table, it must conform to pattern { @literal [\\d\\w-_]+ }
   * @param table target table descriptor
   */
  public CachingTableDescriptor(String tableId, TableDescriptor<K, V, ?> table) {
    super(tableId);
    this.table = table;
  }

  /**
   * Constructs a table descriptor instance and specify a cache (as Table descriptor)
   * to be used for caching. Cache get is not synchronized with put for better parallelism
   * in the read path of {@link org.apache.samza.table.caching.CachingTable}. As such, cache table implementation is
   * expected to be thread-safe for concurrent accesses.
   *
   * @param tableId Id of the table, it must conform to pattern { @literal [\\d\\w-_]+ }
   * @param table target table descriptor
   * @param cache cache table descriptor
   */
  public CachingTableDescriptor(String tableId, TableDescriptor<K, V, ?> table,
      TableDescriptor<K, V, ?> cache) {
    this(tableId, table);
    this.cache = cache;
  }

  @Override
  public List<? extends TableDescriptor<K, V, ?>> getTableDescriptors() {
    return cache != null
        ? Arrays.asList(cache, table)
        : Arrays.asList(table);
  }

  @Override
  public TableSpec getTableSpec() {
    validate();

    Map<String, String> tableSpecConfig = new HashMap<>();
    generateTableSpecConfig(tableSpecConfig);

    if (cache != null) {
      tableSpecConfig.put(CachingTableProvider.CACHE_TABLE_ID, ((BaseTableDescriptor) cache).getTableSpec().getId());
    } else {
      if (readTtl != null) {
        tableSpecConfig.put(CachingTableProvider.READ_TTL_MS, String.valueOf(readTtl.toMillis()));
      }
      if (writeTtl != null) {
        tableSpecConfig.put(CachingTableProvider.WRITE_TTL_MS, String.valueOf(writeTtl.toMillis()));
      }
      if (cacheSize > 0) {
        tableSpecConfig.put(CachingTableProvider.CACHE_SIZE, String.valueOf(cacheSize));
      }
    }

    tableSpecConfig.put(CachingTableProvider.REAL_TABLE_ID, ((BaseTableDescriptor) table).getTableSpec().getId());
    tableSpecConfig.put(CachingTableProvider.WRITE_AROUND, String.valueOf(isWriteAround));

    return new TableSpec(tableId, serde, CachingTableProviderFactory.class.getName(), tableSpecConfig);
  }

  /**
   * Specify the TTL for each read access, ie. record is expired after
   * the TTL duration since last read access of each key.
   * @param readTtl read TTL
   * @return this descriptor
   */
  public CachingTableDescriptor<K, V> withReadTtl(Duration readTtl) {
    this.readTtl = readTtl;
    return this;
  }

  /**
   * Specify the TTL for each write access, ie. record is expired after
   * the TTL duration since last write access of each key.
   * @param writeTtl write TTL
   * @return this descriptor
   */
  public CachingTableDescriptor<K, V> withWriteTtl(Duration writeTtl) {
    this.writeTtl = writeTtl;
    return this;
  }

  /**
   * Specify the max cache size for size-based eviction.
   * @param cacheSize max size of the cache
   * @return this descriptor
   */
  public CachingTableDescriptor<K, V> withCacheSize(long cacheSize) {
    this.cacheSize = cacheSize;
    return this;
  }

  /**
   * Specify if write-around policy should be used to bypass writing
   * to cache for put operations. This is useful when put() is the
   * dominant operation and get() has no locality with recent puts.
   * @return this descriptor
   */
  public CachingTableDescriptor<K, V> withWriteAround() {
    this.isWriteAround = true;
    return this;
  }

  @Override
  @VisibleForTesting
  public void validate() {
    super.validate();
    Preconditions.checkNotNull(table, "Actual table is required.");
    if (cache == null) {
      Preconditions.checkNotNull(readTtl, "readTtl must be specified.");
    } else {
      Preconditions.checkArgument(readTtl == null && writeTtl == null && cacheSize == 0,
          "Invalid to specify both {cache} and {readTtl|writeTtl|cacheSize} at the same time.");
    }
  }
}
