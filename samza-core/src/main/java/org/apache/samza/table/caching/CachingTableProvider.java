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

import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.caching.guava.GuavaCacheTable;
import org.apache.samza.table.BaseTableProvider;

import com.google.common.cache.CacheBuilder;

/**
 * Table provider for {@link CachingTable}.
 */
public class CachingTableProvider extends BaseTableProvider {

  // Store the cache instances created by default
  private final List<ReadWriteTable> defaultCaches = new ArrayList<>();

  public CachingTableProvider(TableSpec tableSpec) {
    super(tableSpec);
  }

  @Override
  public Table getTable() {
    String realTableId = tableSpec.getConfig().get(CachingTableDescriptor.REAL_TABLE_ID);
    ReadableTable table = (ReadableTable) this.context.getTaskContext().getTable(realTableId);

    String cacheTableId = tableSpec.getConfig().get(CachingTableDescriptor.CACHE_TABLE_ID);
    ReadWriteTable cache;

    if (cacheTableId != null) {
      cache = (ReadWriteTable) this.context.getTaskContext().getTable(cacheTableId);
    } else {
      cache = createDefaultCacheTable(realTableId);
      defaultCaches.add(cache);
    }

    boolean isWriteAround = Boolean.parseBoolean(tableSpec.getConfig().get(CachingTableDescriptor.WRITE_AROUND));
    CachingTable cachingTable = new CachingTable(tableSpec.getId(), table, cache, isWriteAround);
    cachingTable.init(this.context);
    return cachingTable;
  }

  @Override
  public void close() {
    defaultCaches.forEach(c -> c.close());
  }

  private ReadWriteTable createDefaultCacheTable(String tableId) {
    long readTtlMs = Long.parseLong(tableSpec.getConfig().getOrDefault(CachingTableDescriptor.READ_TTL_MS, "-1"));
    long writeTtlMs = Long.parseLong(tableSpec.getConfig().getOrDefault(CachingTableDescriptor.WRITE_TTL_MS, "-1"));
    long cacheSize = Long.parseLong(tableSpec.getConfig().getOrDefault(CachingTableDescriptor.CACHE_SIZE, "-1"));

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
