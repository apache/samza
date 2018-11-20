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

package org.apache.samza.table.descriptors;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.config.Config;

import com.google.common.base.Preconditions;

/**
 * Table descriptor for a caching table.
 * @param <K> type of the key in the cache
 * @param <V> type of the value in the cache
 */
public class CachingTableDescriptor<K, V> extends HybridTableDescriptor<K, V, CachingTableDescriptor<K, V>> {

  public static final String PROVIDER_FACTORY_CLASS_NAME = "org.apache.samza.table.caching.CachingTableProviderFactory";

  public static final String REAL_TABLE_ID = "realTableId";
  public static final String CACHE_TABLE_ID = "cacheTableId";
  public static final String READ_TTL_MS = "readTtl";
  public static final String WRITE_TTL_MS = "writeTtl";
  public static final String CACHE_SIZE = "cacheSize";
  public static final String WRITE_AROUND = "writeAround";

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
   * in the read path of caching table. As such, cache table implementation is
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

  /**
   * Retrieve user-defined table descriptors contained in this table
   * @return table descriptors
   */
  @Override
  public List<? extends TableDescriptor<K, V, ?>> getTableDescriptors() {
    return cache != null
        ? Arrays.asList(cache, table)
        : Arrays.asList(table);
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

  /**
   * {@inheritDoc}
   */
  @Override
  public String getProviderFactoryClassName() {
    return PROVIDER_FACTORY_CLASS_NAME;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, String> toConfig(Config jobConfig) {

    Map<String, String> tableConfig = new HashMap<>(super.toConfig(jobConfig));

    if (cache != null) {
      addTableConfig(CACHE_TABLE_ID, cache.getTableId(), tableConfig);
    } else {
      if (readTtl != null) {
        addTableConfig(READ_TTL_MS, String.valueOf(readTtl.toMillis()), tableConfig);
      }
      if (writeTtl != null) {
        addTableConfig(WRITE_TTL_MS, String.valueOf(writeTtl.toMillis()), tableConfig);
      }
      if (cacheSize > 0) {
        addTableConfig(CACHE_SIZE, String.valueOf(cacheSize), tableConfig);
      }
    }

    addTableConfig(REAL_TABLE_ID, table.getTableId(), tableConfig);
    addTableConfig(WRITE_AROUND, String.valueOf(isWriteAround), tableConfig);

    return Collections.unmodifiableMap(tableConfig);
  }

  /**
   * {@inheritDoc}
   */
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
  }
}
