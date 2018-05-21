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
import java.util.concurrent.TimeUnit;

import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.caching.guava.GuavaCacheTable;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;

/**
 * Table provider for {@link CachingTable}.
 */
public class CachingTableProvider implements TableProvider {
  private static final Logger LOG = LoggerFactory.getLogger(CachingTableProvider.class);

  public static final String REAL_TABLE_ID = "realTableId";
  public static final String CACHE_TABLE_ID = "cacheTableId";
  public static final String READ_TTL_MS = "readTtl";
  public static final String WRITE_TTL_MS = "writeTtl";
  public static final String CACHE_SIZE = "cacheSize";
  public static final String LOCK_STRIPES = "lockStripes";
  public static final String WRITE_AROUND = "writeAround";

  private final TableSpec cachingTableSpec;

  // Store the cache instances created by default
  private final List<ReadWriteTable> defaultCaches = new ArrayList<>();

  private SamzaContainerContext containerContext;
  private TaskContext taskContext;

  public CachingTableProvider(TableSpec tableSpec) {
    this.cachingTableSpec = tableSpec;
  }

  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    this.taskContext = taskContext;
    this.containerContext = containerContext;
  }

  @Override
  public Table getTable() {
    String realTableId = cachingTableSpec.getConfig().get(REAL_TABLE_ID);
    ReadableTable table = (ReadableTable) taskContext.getTable(realTableId);

    String cacheTableId = cachingTableSpec.getConfig().get(CACHE_TABLE_ID);
    ReadWriteTable cache;

    if (cacheTableId != null) {
      cache = (ReadWriteTable) taskContext.getTable(cacheTableId);
    } else {
      cache = createDefaultCacheTable(realTableId);
      defaultCaches.add(cache);
    }

    int stripes = Integer.parseInt(cachingTableSpec.getConfig().get(LOCK_STRIPES));
    boolean isWriteAround = Boolean.parseBoolean(cachingTableSpec.getConfig().get(WRITE_AROUND));
    return new CachingTable(cachingTableSpec.getId(), table, cache, stripes, isWriteAround);
  }

  @Override
  public Map<String, String> generateConfig(Map<String, String> config) {
    Map<String, String> tableConfig = new HashMap<>();

    // Insert table_id prefix to config entries
    cachingTableSpec.getConfig().forEach((k, v) -> {
        String realKey = String.format(JavaTableConfig.TABLE_ID_PREFIX, cachingTableSpec.getId()) + "." + k;
        tableConfig.put(realKey, v);
      });

    LOG.info("Generated configuration for table " + cachingTableSpec.getId());

    return tableConfig;
  }

  @Override
  public void close() {
    defaultCaches.forEach(c -> c.close());
  }

  private ReadWriteTable createDefaultCacheTable(String tableId) {
    long readTtlMs = Long.parseLong(cachingTableSpec.getConfig().getOrDefault(READ_TTL_MS, "-1"));
    long writeTtlMs = Long.parseLong(cachingTableSpec.getConfig().getOrDefault(WRITE_TTL_MS, "-1"));
    long cacheSize = Long.parseLong(cachingTableSpec.getConfig().getOrDefault(CACHE_SIZE, "-1"));

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

    LOG.info(String.format("Creating default cache with: readTtl=%d, writeTtl=%d, maxSize=%d",
        readTtlMs, writeTtlMs, cacheSize));

    GuavaCacheTable cacheTable = new GuavaCacheTable(tableId + "-def-cache", cacheBuilder.build());
    cacheTable.init(containerContext, taskContext);

    return cacheTable;
  }
}
