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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.caching.guava.GuavaCacheTable;
import org.apache.samza.table.BaseTableProvider;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;

/**
 * Table provider for {@link CachingTable}.
 */
public class CachingTableProvider extends BaseTableProvider {

  // Store the cache instances created by default
  private final List<ReadWriteUpdateTable> defaultCaches = new ArrayList<>();

  public CachingTableProvider(String tableId) {
    super(tableId);
  }

  @Override
  public ReadWriteUpdateTable getTable() {
    Preconditions.checkNotNull(context, String.format("Table %s not initialized", tableId));

    JavaTableConfig tableConfig = new JavaTableConfig(context.getJobContext().getConfig());
    String realTableId = tableConfig.getForTable(tableId, CachingTableDescriptor.REAL_TABLE_ID);
    ReadWriteUpdateTable table = this.context.getTaskContext().getUpdatableTable(realTableId);

    String cacheTableId = tableConfig.getForTable(tableId, CachingTableDescriptor.CACHE_TABLE_ID);
    ReadWriteUpdateTable cache;

    if (cacheTableId != null) {
      cache = this.context.getTaskContext().getUpdatableTable(cacheTableId);
    } else {
      cache = createDefaultCacheTable(realTableId, tableConfig);
      defaultCaches.add(cache);
    }

    boolean isWriteAround = Boolean.parseBoolean(tableConfig.getForTable(tableId, CachingTableDescriptor.WRITE_AROUND));
    CachingTable cachingTable = new CachingTable(tableId, table, cache, isWriteAround);
    cachingTable.init(this.context);
    return cachingTable;
  }

  @Override
  public void close() {
    super.close();
    defaultCaches.forEach(c -> c.close());
  }

  private ReadWriteUpdateTable createDefaultCacheTable(String tableId, JavaTableConfig tableConfig) {
    long readTtlMs = Long.parseLong(tableConfig.getForTable(tableId, CachingTableDescriptor.READ_TTL_MS, "-1"));
    long writeTtlMs = Long.parseLong(tableConfig.getForTable(tableId, CachingTableDescriptor.WRITE_TTL_MS, "-1"));
    long cacheSize = Long.parseLong(tableConfig.getForTable(tableId, CachingTableDescriptor.CACHE_SIZE, "-1"));

    CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
    if (readTtlMs != -1) {
      cacheBuilder.expireAfterAccess(readTtlMs, TimeUnit.MILLISECONDS);
    }
    if (writeTtlMs != -1) {
      cacheBuilder.expireAfterWrite(writeTtlMs, TimeUnit.MILLISECONDS);
    }
    if (cacheSize != -1) {
      cacheBuilder.maximumSize(cacheSize);
    }

    logger.info(String.format("Creating default cache with: readTtl=%d, writeTtl=%d, maxSize=%d",
        readTtlMs, writeTtlMs, cacheSize));

    GuavaCacheTable cacheTable = new GuavaCacheTable(tableId + "-def-cache", cacheBuilder.build());
    cacheTable.init(this.context);

    return cacheTable;
  }
}
