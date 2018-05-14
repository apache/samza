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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;

import com.google.common.base.Preconditions;

/**
 * Table descriptor for {@link CachingTable}.
 * @param <K> type of the key in the cache
 * @param <V> type of the value in the cache
 */
public class CachingTableDescriptor<K, V> extends BaseTableDescriptor<K, V, CachingTableDescriptor<K, V>> {
  private Duration readTtl;
  private Duration writeTtl;
  private long cacheSize;
  private Table<KV<K, V>> cache;
  private Table<KV<K, V>> table;
  private int stripes = 16;
  private boolean isWriteAround;

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table
   */
  public CachingTableDescriptor(String tableId) {
    super(tableId);
  }

  @Override
  public TableSpec getTableSpec() {
    validate();

    Map<String, String> tableSpecConfig = new HashMap<>();
    generateTableSpecConfig(tableSpecConfig);

    if (cache != null) {
      tableSpecConfig.put(CachingTableProvider.CACHE_TABLE_ID, ((TableImpl) cache).getTableSpec().getId());
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

    tableSpecConfig.put(CachingTableProvider.REAL_TABLE_ID, ((TableImpl) table).getTableSpec().getId());
    tableSpecConfig.put(CachingTableProvider.LOCK_STRIPES, String.valueOf(stripes));
    tableSpecConfig.put(CachingTableProvider.WRITE_AROUND, String.valueOf(isWriteAround));

    return new TableSpec(tableId, serde, CachingTableProviderFactory.class.getName(), tableSpecConfig);
  }

  /**
   * Specify a cache instance (as Table abstraction) to be used for caching.
   * @param cache cache instance
   * @return this descriptor
   */
  public CachingTableDescriptor withCache(Table<KV<K, V>> cache) {
    this.cache = cache;
    return this;
  }

  /**
   * Specify the table instance for the actual table input/output.
   * @param table table instance
   * @return this descriptor
   */
  public CachingTableDescriptor withTable(Table<KV<K, V>> table) {
    this.table = table;
    return this;
  }

  /**
   * Specify the TTL for each read access, ie. record is expired after
   * the TTL duration since last read access of each key.
   * @param readTtl read TTL
   * @return this descriptor
   */
  public CachingTableDescriptor withReadTtl(Duration readTtl) {
    this.readTtl = readTtl;
    return this;
  }

  /**
   * Specify the TTL for each write access, ie. record is expired after
   * the TTL duration since last write access of each key.
   * @param writeTtl write TTL
   * @return this descriptor
   */
  public CachingTableDescriptor withWriteTtl(Duration writeTtl) {
    this.writeTtl = writeTtl;
    return this;
  }

  /**
   * Specify the max cache size for size-based eviction.
   * @param cacheSize max size of the cache
   * @return this descriptor
   */
  public CachingTableDescriptor withCacheSize(long cacheSize) {
    this.cacheSize = cacheSize;
    return this;
  }

  /**
   * Specify the number of stripes for striped locking for atomically updating
   * cache and the actual table. Default number of stripes is 16.
   * @param stripes number of stripes for locking
   * @return this descriptor
   */
  public CachingTableDescriptor withStripes(int stripes) {
    this.stripes = stripes;
    return this;
  }

  /**
   * Specify if write-around policy should be used to bypass writing
   * to cache for put operations. This is useful when put() is the
   * dominant operation and get() has no locality with recent puts.
   * @return this descriptor
   */
  public CachingTableDescriptor withWriteAround() {
    this.isWriteAround = true;
    return this;
  }

  @Override
  protected void validate() {
    super.validate();
    Preconditions.checkNotNull(table, "Actual table is required.");
    if (cache == null) {
      Preconditions.checkNotNull(readTtl, "readTtl must be specified.");
    } else {
      Preconditions.checkArgument(readTtl == null && writeTtl == null && cacheSize == 0,
          "Invalid to specify both {cache} and {readTtl|writeTtl|cacheSize} at the same time.");
    }
    Preconditions.checkArgument(stripes > 0, "Number of cache stripes must be positive.");
  }
}
