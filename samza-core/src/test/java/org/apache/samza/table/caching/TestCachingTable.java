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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.context.Context;
import org.apache.samza.context.MockContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.descriptors.BaseTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.table.caching.guava.GuavaCacheTable;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.descriptors.GuavaCacheTableDescriptor;
import org.apache.samza.table.remote.RemoteTable;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


public class TestCachingTable {

  private MetricsRegistry metricsRegistry;

  @Test
  public void testSerializeSimple() {
    doTestSerialize(null);
  }

  @Test
  public void testSerializeWithCacheInstance() {
    String tableId = "guavaCacheId";
    GuavaCacheTableDescriptor guavaTableDesc = new GuavaCacheTableDescriptor(tableId)
        .withCache(CacheBuilder.newBuilder().build());
    Map<String, String> tableConfig = guavaTableDesc.toConfig(new MapConfig());
    assertExists(GuavaCacheTableDescriptor.GUAVA_CACHE, tableId, tableConfig);
    doTestSerialize(guavaTableDesc);
  }

  private void doTestSerialize(TableDescriptor cache) {
    CachingTableDescriptor desc;
    TableDescriptor table = createDummyTableDescriptor("2");
    if (cache == null) {
      desc = new CachingTableDescriptor("1", table)
          .withReadTtl(Duration.ofMinutes(3))
          .withWriteTtl(Duration.ofMinutes(4))
          .withCacheSize(1000);
    } else {
      desc = new CachingTableDescriptor("1", table, cache);
    }

    desc.withWriteAround();

    Map<String, String> tableConfig = desc.toConfig(new MapConfig());

    assertEquals("2", CachingTableDescriptor.REAL_TABLE_ID, "1", tableConfig);

    if (cache == null) {
      assertEquals("180000", CachingTableDescriptor.READ_TTL_MS, "1", tableConfig);
      assertEquals("240000", CachingTableDescriptor.WRITE_TTL_MS, "1", tableConfig);
    } else {
      assertEquals(cache.getTableId(), CachingTableDescriptor.CACHE_TABLE_ID, "1", tableConfig);
    }

    assertEquals("true", CachingTableDescriptor.WRITE_AROUND, "1", tableConfig);
  }

  private static Pair<ReadWriteUpdateTable<String, String, String>, Map<String, String>> getMockCache() {
    // To allow concurrent writes for disjoint keys by testConcurrentAccess, we must use CHM here.
    // This is okay because the atomic section in CachingTable covers both cache and table so using
    // CHM for each does not serialize such two-step operation so the atomicity is still tested.
    // Regular HashMap is not thread-safe even for disjoint keys.
    final Map<String, String> cacheStore = new ConcurrentHashMap<>();
    final ReadWriteUpdateTable cacheTable = mock(ReadWriteUpdateTable.class);

    doAnswer(invocation -> {
      String key = invocation.getArgumentAt(0, String.class);
      String value = invocation.getArgumentAt(1, String.class);
      cacheStore.put(key, value);
      return null;
    }).when(cacheTable).put(any(), any());

    doAnswer(invocation -> {
      String key = invocation.getArgumentAt(0, String.class);
      return cacheStore.get(key);
    }).when(cacheTable).get(any());

    doAnswer(invocation -> {
      String key = invocation.getArgumentAt(0, String.class);
      return cacheStore.remove(key);
    }).when(cacheTable).delete(any());

    return Pair.of(cacheTable, cacheStore);
  }

  private void initTables(ReadWriteUpdateTable... tables) {
    initTables(false, tables);
  }

  private void initTables(boolean isTimerMetricsDisabled, ReadWriteUpdateTable... tables) {
    Map<String, String> config = new HashMap<>();
    if (isTimerMetricsDisabled) {
      config.put(MetricsConfig.METRICS_TIMER_ENABLED, "false");
    }
    Context context = new MockContext();
    doReturn(new MapConfig(config)).when(context.getJobContext()).getConfig();
    metricsRegistry = mock(MetricsRegistry.class);
    doReturn(mock(Timer.class)).when(metricsRegistry).newTimer(anyString(), anyString());
    doReturn(mock(Counter.class)).when(metricsRegistry).newCounter(anyString(), anyString());
    doReturn(mock(Gauge.class)).when(metricsRegistry).newGauge(anyString(), any());
    doReturn(metricsRegistry).when(context.getContainerContext()).getContainerMetricsRegistry();

    Arrays.asList(tables).forEach(t -> t.init(context));
  }

  private void doTestCacheOps(boolean isWriteAround) {
    CachingTableDescriptor desc = new CachingTableDescriptor("1",
        createDummyTableDescriptor("realTable"),
        createDummyTableDescriptor("cacheTable"));
    if (isWriteAround) {
      desc.withWriteAround();
    }

    Context context = new MockContext();
    final ReadWriteUpdateTable cacheTable = getMockCache().getLeft();

    final ReadWriteUpdateTable realTable = mock(ReadWriteUpdateTable.class);

    doAnswer(invocation -> {
      String key = invocation.getArgumentAt(0, String.class);
      return CompletableFuture.completedFuture("test-data-" + key);
    }).when(realTable).getAsync(any());

    doReturn(CompletableFuture.completedFuture(null)).when(realTable).putAsync(any(), any());

    doAnswer(invocation -> {
      String tableId = invocation.getArgumentAt(0, String.class);
      if (tableId.equals("realTable")) {
        // cache
        return realTable;
      } else if (tableId.equals("cacheTable")) {
        return cacheTable;
      }

      Assert.fail();
      return null;
    }).when(context.getTaskContext()).getUpdatableTable(anyString());

    when(context.getContainerContext().getContainerMetricsRegistry()).thenReturn(new NoOpMetricsRegistry());

    Map<String, String> tableConfig = desc.toConfig(new MapConfig());
    when(context.getJobContext().getConfig()).thenReturn(new MapConfig(tableConfig));

    CachingTableProvider tableProvider = new CachingTableProvider(desc.getTableId());
    tableProvider.init(context);

    CachingTable cachingTable = (CachingTable) tableProvider.getTable();

    Assert.assertEquals("test-data-1", cachingTable.get("1"));
    verify(realTable, times(1)).getAsync(any());
    verify(cacheTable, times(1)).get(any()); // cache miss
    verify(cacheTable, times(1)).put(any(), any());
    Assert.assertEquals(cachingTable.hitRate(), 0.0, 0.0); // 0 hit, 1 request
    Assert.assertEquals(cachingTable.missRate(), 1.0, 0.0);

    Assert.assertEquals("test-data-1", cachingTable.get("1"));
    verify(realTable, times(1)).getAsync(any()); // no change
    verify(cacheTable, times(2)).get(any());
    verify(cacheTable, times(1)).put(any(), any()); // no change
    Assert.assertEquals(0.5, cachingTable.hitRate(), 0.0); // 1 hit, 2 requests
    Assert.assertEquals(0.5, cachingTable.missRate(), 0.0);

    cachingTable.put("2", "test-data-XXXX");
    verify(cacheTable, times(isWriteAround ? 1 : 2)).put(any(), any());
    verify(realTable, times(1)).putAsync(any(), any());

    if (isWriteAround) {
      Assert.assertEquals("test-data-2", cachingTable.get("2")); // expects value from table
      verify(realTable, times(2)).getAsync(any()); // should have one more fetch
      Assert.assertEquals(cachingTable.hitRate(), 0.33, 0.1); // 1 hit, 3 requests
    } else {
      Assert.assertEquals("test-data-XXXX", cachingTable.get("2")); // expect value from cache
      verify(realTable, times(1)).getAsync(any()); // no change
      Assert.assertEquals(cachingTable.hitRate(), 0.66, 0.1); // 2 hits, 3 requests
    }
  }

  @Test
  public void testCacheOpsWriteThrough() {
    doTestCacheOps(false);
  }

  @Test
  public void testCacheOpsWriteAround() {
    doTestCacheOps(true);
  }

  @Test
  public void testNonexistentKeyInTable() {
    ReadWriteUpdateTable<String, String, String> table = mock(ReadWriteUpdateTable.class);
    doReturn(CompletableFuture.completedFuture(null)).when(table).getAsync(any());
    ReadWriteUpdateTable<String, String, String> cache = getMockCache().getLeft();
    CachingTable<String, String, String> cachingTable = new CachingTable<>("myTable", table, cache, false);
    initTables(cachingTable);
    Assert.assertNull(cachingTable.get("abc"));
    verify(cache, times(1)).get(any());
    Assert.assertNull(cache.get("abc"));
    verify(cache, times(0)).put(any(), any());
  }

  @Test
  public void testKeyEviction() {
    ReadWriteUpdateTable<String, String, Void> table = mock(ReadWriteUpdateTable.class);
    doReturn(CompletableFuture.completedFuture("3")).when(table).getAsync(any());
    ReadWriteUpdateTable<String, String, Void> cache = mock(ReadWriteUpdateTable.class);

    // no handler added to mock cache so get/put are noop, this can simulate eviction
    CachingTable<String, String, Void> cachingTable = new CachingTable<>("myTable", table, cache, false);
    initTables(cachingTable);
    cachingTable.get("abc");
    verify(table, times(1)).getAsync(any());

    // get() should go to table again
    cachingTable.get("abc");
    verify(table, times(2)).getAsync(any());
  }

  /**
   * Testing caching in a more realistic scenario with Guava cache + remote table
   */
  @Test
  public void testGuavaCacheAndRemoteTable() throws Exception {
    String tableId = "testGuavaCacheAndRemoteTable";
    Cache<String, String> guavaCache = CacheBuilder.newBuilder().initialCapacity(100).build();
    final ReadWriteUpdateTable<String, String, String> guavaTable = new GuavaCacheTable<>(tableId + "-cache", guavaCache);

    // It is okay to share rateLimitHelper and async helper for read/write in test
    TableRateLimiter<String, String> readRateLimitHelper = mock(TableRateLimiter.class);
    TableRateLimiter<String, String> writeRateLimitHelper = mock(TableRateLimiter.class);
    TableRateLimiter<String, String> updateRateLimitHelper = mock(TableRateLimiter.class);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);

    final RemoteTable<String, String, String> remoteTable = new RemoteTable<>(tableId + "-remote", readFn,
        writeFn, readRateLimitHelper, writeRateLimitHelper, updateRateLimitHelper,
        Executors.newSingleThreadExecutor(), null, null, null, null,
        null,  Executors.newSingleThreadExecutor());

    final CachingTable<String, String, String> cachingTable = new CachingTable<>(
        tableId, remoteTable, guavaTable, false);

    initTables(cachingTable, guavaTable, remoteTable);

    // 4 per readable table (12)
    // 8 per read/write table (24)
    verify(metricsRegistry, times(36)).newCounter(any(), anyString());

    // 3 per readable table (9)
    // 8 per read/write table (24)
    // 1 per remote readable table (1)
    // 2 per remote read/write table (2)
    verify(metricsRegistry, times(36)).newTimer(any(), anyString());

    // 1 per guava table (1)
    // 3 per caching table (2)
    verify(metricsRegistry, times(4)).newGauge(anyString(), any());

    // GET
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(any());
    Assert.assertEquals(cachingTable.getAsync("foo").get(), "bar");
    // Ensure cache is updated
    Assert.assertEquals(guavaCache.getIfPresent("foo"), "bar");

    // PUT
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any());
    cachingTable.putAsync("foo", "baz").get();
    // Ensure cache is updated
    Assert.assertEquals(guavaCache.getIfPresent("foo"), "baz");

    // DELETE
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any());
    cachingTable.deleteAsync("foo").get();
    // Ensure cache is updated
    Assert.assertNull(guavaCache.getIfPresent("foo"));

    // GET-ALL
    Map<String, String> records = new HashMap<>();
    records.put("foo1", "bar1");
    records.put("foo2", "bar2");
    doReturn(CompletableFuture.completedFuture(records)).when(readFn).getAllAsync(any());
    Assert.assertEquals(cachingTable.getAllAsync(Arrays.asList("foo1", "foo2")).get(), records);
    // Ensure cache is updated
    Assert.assertEquals(guavaCache.getIfPresent("foo1"), "bar1");
    Assert.assertEquals(guavaCache.getIfPresent("foo2"), "bar2");

    // GET-ALL with partial miss
    doReturn(CompletableFuture.completedFuture(Collections.singletonMap("foo3", "bar3"))).when(readFn).getAllAsync(any());
    records = cachingTable.getAllAsync(Arrays.asList("foo1", "foo2", "foo3")).get();
    Assert.assertEquals(records.get("foo3"), "bar3");
    // Ensure cache is updated
    Assert.assertEquals(guavaCache.getIfPresent("foo3"), "bar3");

    // Calling again for the same keys should not trigger IO, ie. no exception is thrown
    CompletableFuture<String> exFuture = new CompletableFuture<>();
    exFuture.completeExceptionally(new RuntimeException("Test exception"));
    doReturn(exFuture).when(readFn).getAllAsync(any());
    cachingTable.getAllAsync(Arrays.asList("foo1", "foo2", "foo3")).get();

    // Partial results should throw
    try {
      cachingTable.getAllAsync(Arrays.asList("foo1", "foo2", "foo5")).get();
      Assert.fail();
    } catch (Exception e) {
    }

    // PUT-ALL
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(any());
    List<Entry<String, String>> entries = new ArrayList<>();
    entries.add(new Entry<>("foo1", "bar111"));
    entries.add(new Entry<>("foo2", "bar222"));
    cachingTable.putAllAsync(entries).get();
    // Ensure cache is updated
    Assert.assertEquals(guavaCache.getIfPresent("foo1"), "bar111");
    Assert.assertEquals(guavaCache.getIfPresent("foo2"), "bar222");

    // PUT-ALL with delete
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(any());
    entries = new ArrayList<>();
    entries.add(new Entry<>("foo1", "bar111"));
    entries.add(new Entry<>("foo2", null));
    cachingTable.putAllAsync(entries).get();
    // Ensure cache is updated
    Assert.assertNull(guavaCache.getIfPresent("foo2"));

    // At this point, foo1 and foo3 should still exist
    Assert.assertNotNull(guavaCache.getIfPresent("foo1"));
    Assert.assertNotNull(guavaCache.getIfPresent("foo3"));

    // DELETE-ALL
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(any());
    cachingTable.deleteAllAsync(Arrays.asList("foo1", "foo3")).get();
    // Ensure foo1 and foo3 are gone
    Assert.assertNull(guavaCache.getIfPresent("foo1"));
    Assert.assertNull(guavaCache.getIfPresent("foo3"));
  }

  @Test
  public void testTimerDisabled() throws Exception {
    String tableId = "testTimerDisabled";

    Cache<String, String> guavaCache = CacheBuilder.newBuilder().initialCapacity(100).build();
    final ReadWriteUpdateTable<String, String, String> guavaTable = new GuavaCacheTable<>(tableId, guavaCache);

    TableRateLimiter<String, String> readRateLimitHelper = mock(TableRateLimiter.class);
    TableRateLimiter<String, String> writeRateLimitHelper = mock(TableRateLimiter.class);
    TableRateLimiter<String, String> updateRateLimitHelper = mock(TableRateLimiter.class);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(CompletableFuture.completedFuture("")).when(readFn).getAsync(any());

    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any());

    final RemoteTable<String, String, String> remoteTable =
        new RemoteTable<>(tableId, readFn, writeFn, readRateLimitHelper, writeRateLimitHelper, updateRateLimitHelper,
            Executors.newSingleThreadExecutor(), null, null, null,
            null, null, Executors.newSingleThreadExecutor());

    final CachingTable<String, String, String> cachingTable = new CachingTable<>(
        tableId, remoteTable, guavaTable, false);

    initTables(true, cachingTable, guavaTable, remoteTable);

    cachingTable.get("");
    cachingTable.getAsync("").get();
    cachingTable.getAll(Collections.emptyList());
    cachingTable.getAllAsync(Collections.emptyList());
    cachingTable.flush();
    cachingTable.put("", "");
    cachingTable.putAsync("", "");
    cachingTable.putAll(Collections.emptyList());
    cachingTable.putAllAsync(Collections.emptyList());
    cachingTable.delete("");
    cachingTable.deleteAsync("");
    cachingTable.deleteAll(Collections.emptyList());
    cachingTable.deleteAllAsync(Collections.emptyList());
  }

  private TableDescriptor createDummyTableDescriptor(String tableId) {
    BaseTableDescriptor tableDescriptor = mock(BaseTableDescriptor.class);
    when(tableDescriptor.getTableId()).thenReturn(tableId);
    return tableDescriptor;
  }

  private void assertExists(String key, String tableId, Map<String, String> config) {
    String realKey = JavaTableConfig.buildKey(tableId, key);
    Assert.assertTrue(config.containsKey(realKey));
  }

  private void assertEquals(String expectedValue, String key, String tableId, Map<String, String> config) {
    String realKey = JavaTableConfig.buildKey(tableId, key);
    Assert.assertEquals(expectedValue, config.get(realKey));
  }
}