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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.caching.guava.GuavaCacheTable;
import org.apache.samza.table.caching.guava.GuavaCacheTableDescriptor;
import org.apache.samza.table.caching.guava.GuavaCacheTableProvider;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.remote.RemoteReadWriteTable;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestCachingTable {
  @Test
  public void testSerializeSimple() {
    doTestSerialize(null);
  }

  @Test
  public void testSerializeWithCacheInstance() {
    GuavaCacheTableDescriptor guavaTableDesc = new GuavaCacheTableDescriptor("guavaCacheId");
    guavaTableDesc.withCache(CacheBuilder.newBuilder().build());
    TableSpec spec = guavaTableDesc.getTableSpec();
    Assert.assertTrue(spec.getConfig().containsKey(GuavaCacheTableProvider.GUAVA_CACHE));
    doTestSerialize(new TableImpl(guavaTableDesc.getTableSpec()));
  }

  private void doTestSerialize(Table cache) {
    CachingTableDescriptor desc = new CachingTableDescriptor("1");
    desc.withTable(new TableImpl(new TableSpec("2", null, null, new HashMap<>())));
    if (cache == null) {
      desc.withReadTtl(Duration.ofMinutes(3));
      desc.withWriteTtl(Duration.ofMinutes(3));
      desc.withCacheSize(1000);
    } else {
      desc.withCache(cache);
    }

    desc.withWriteAround();

    TableSpec spec = desc.getTableSpec();
    Assert.assertTrue(spec.getConfig().containsKey(CachingTableProvider.REAL_TABLE_ID));

    if (cache == null) {
      Assert.assertTrue(spec.getConfig().containsKey(CachingTableProvider.READ_TTL_MS));
      Assert.assertTrue(spec.getConfig().containsKey(CachingTableProvider.WRITE_TTL_MS));
    } else {
      Assert.assertTrue(spec.getConfig().containsKey(CachingTableProvider.CACHE_TABLE_ID));
    }

    Assert.assertEquals("true", spec.getConfig().get(CachingTableProvider.WRITE_AROUND));

    desc.validate();
  }

  private static Pair<ReadWriteTable<String, String>, Map<String, String>> getMockCache() {
    // To allow concurrent writes for disjoint keys by testConcurrentAccess, we must use CHM here.
    // This is okay because the atomic section in CachingTable covers both cache and table so using
    // CHM for each does not serialize such two-step operation so the atomicity is still tested.
    // Regular HashMap is not thread-safe even for disjoint keys.
    final Map<String, String> cacheStore = new ConcurrentHashMap<>();
    final ReadWriteTable cacheTable = mock(ReadWriteTable.class);

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

  private void initTables(ReadableTable ... tables) {
    SamzaContainerContext containerContext = mock(SamzaContainerContext.class);
    TaskContext taskContext = mock(TaskContext.class);
    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    doReturn(mock(Timer.class)).when(metricsRegistry).newTimer(anyString(), anyString());
    doReturn(mock(Counter.class)).when(metricsRegistry).newCounter(anyString(), anyString());
    doReturn(mock(Gauge.class)).when(metricsRegistry).newGauge(anyString(), any());
    when(taskContext.getMetricsRegistry()).thenReturn(metricsRegistry);
    for (ReadableTable table : tables) {
      table.init(containerContext, taskContext);
    }
  }

  private void doTestCacheOps(boolean isWriteAround) {
    CachingTableDescriptor desc = new CachingTableDescriptor("1");
    desc.withTable(new TableImpl(new TableSpec("realTable", null, null, new HashMap<>())));
    desc.withCache(new TableImpl(new TableSpec("cacheTable", null, null, new HashMap<>())));
    if (isWriteAround) {
      desc.withWriteAround();
    }
    CachingTableProvider tableProvider = new CachingTableProvider(desc.getTableSpec());

    SamzaContainerContext containerContext = mock(SamzaContainerContext.class);

    TaskContext taskContext = mock(TaskContext.class);
    final ReadWriteTable cacheTable = getMockCache().getLeft();

    final ReadWriteTable realTable = mock(ReadWriteTable.class);

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
      }).when(taskContext).getTable(anyString());

    when(taskContext.getMetricsRegistry()).thenReturn(new NoOpMetricsRegistry());

    tableProvider.init(containerContext, taskContext);

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
    ReadableTable<String, String> table = mock(ReadableTable.class);
    doReturn(CompletableFuture.completedFuture(null)).when(table).getAsync(any());
    ReadWriteTable<String, String> cache = getMockCache().getLeft();
    CachingTable<String, String> cachingTable = new CachingTable<>("myTable", table, cache, false);
    initTables(cachingTable);
    Assert.assertNull(cachingTable.get("abc"));
    verify(cache, times(1)).get(any());
    Assert.assertNull(cache.get("abc"));
    verify(cache, times(0)).put(any(), any());
  }

  @Test
  public void testKeyEviction() {
    ReadableTable<String, String> table = mock(ReadableTable.class);
    doReturn(CompletableFuture.completedFuture("3")).when(table).getAsync(any());
    ReadWriteTable<String, String> cache = mock(ReadWriteTable.class);

    // no handler added to mock cache so get/put are noop, this can simulate eviction
    CachingTable<String, String> cachingTable = new CachingTable<>("myTable", table, cache, false);
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
    final ReadWriteTable<String, String> guavaTable = new GuavaCacheTable<>(tableId, guavaCache);

    // It is okay to share rateLimitHelper and async helper for read/write in test
    TableRateLimiter<String, String> rateLimitHelper = mock(TableRateLimiter.class);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    final RemoteReadWriteTable<String, String> remoteTable = new RemoteReadWriteTable<>(
        tableId, readFn, writeFn, rateLimitHelper, rateLimitHelper,
        Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());

    final CachingTable<String, String> cachingTable = new CachingTable<>(
        tableId, remoteTable, guavaTable, false);

    initTables(cachingTable, guavaTable, remoteTable);

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
}