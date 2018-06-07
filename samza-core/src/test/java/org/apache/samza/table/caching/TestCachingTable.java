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
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.caching.guava.GuavaCacheTableDescriptor;
import org.apache.samza.table.caching.guava.GuavaCacheTableProvider;
import org.apache.samza.task.TaskContext;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.cache.CacheBuilder;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


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

    desc.withStripes(32);
    desc.withWriteAround();

    TableSpec spec = desc.getTableSpec();
    Assert.assertTrue(spec.getConfig().containsKey(CachingTableProvider.REAL_TABLE_ID));

    if (cache == null) {
      Assert.assertTrue(spec.getConfig().containsKey(CachingTableProvider.READ_TTL_MS));
      Assert.assertTrue(spec.getConfig().containsKey(CachingTableProvider.WRITE_TTL_MS));
    } else {
      Assert.assertTrue(spec.getConfig().containsKey(CachingTableProvider.CACHE_TABLE_ID));
    }

    Assert.assertEquals("32", spec.getConfig().get(CachingTableProvider.LOCK_STRIPES));
    Assert.assertEquals("true", spec.getConfig().get(CachingTableProvider.WRITE_AROUND));

    desc.validate();
  }

  private static Pair<ReadWriteTable<String, String>, Map<String, String>> getMockCache() {
    // To allow concurrent writes for disjoint keys by testConcurrentAccess, we must use CHM here.
    // This is okay because the atomic section in CachingTable covers both cache and table so using
    // CHM for each does not serialize such two-step operation so the atomicity is still tested.
    // Regular HashMap is not thread-safe even for disjoint keys.
    final Map<String, String> cacheStore = new ConcurrentHashMap<>();
    final ReadWriteTable tableCache = mock(ReadWriteTable.class);

    doAnswer(invocation -> {
        String key = invocation.getArgumentAt(0, String.class);
        String value = invocation.getArgumentAt(1, String.class);
        cacheStore.put(key, value);
        return null;
      }).when(tableCache).put(any(), any());

    doAnswer(invocation -> {
        String key = invocation.getArgumentAt(0, String.class);
        return cacheStore.get(key);
      }).when(tableCache).get(any());

    doAnswer(invocation -> {
        String key = invocation.getArgumentAt(0, String.class);
        return cacheStore.remove(key);
      }).when(tableCache).delete(any());

    return Pair.of(tableCache, cacheStore);
  }

  private void doTestCacheOps(boolean isWriteAround) {
    CachingTableDescriptor desc = new CachingTableDescriptor("1");
    desc.withTable(new TableImpl(new TableSpec("realTable", null, null, new HashMap<>())));
    desc.withCache(new TableImpl(new TableSpec("cacheTable", null, null, new HashMap<>())));
    if (isWriteAround) {
      desc.withWriteAround();
    }
    CachingTableProvider tableProvider = new CachingTableProvider(desc.getTableSpec());

    TaskContext taskContext = mock(TaskContext.class);
    final ReadWriteTable tableCache = getMockCache().getLeft();

    final ReadWriteTable realTable = mock(ReadWriteTable.class);

    doAnswer(invocation -> {
        String key = invocation.getArgumentAt(0, String.class);
        return "test-data-" + key;
      }).when(realTable).get(any());

    doAnswer(invocation -> {
        String tableId = invocation.getArgumentAt(0, String.class);
        if (tableId.equals("realTable")) {
          // cache
          return realTable;
        } else if (tableId.equals("cacheTable")) {
          return tableCache;
        }

        Assert.fail();
        return null;
      }).when(taskContext).getTable(anyString());

    tableProvider.init(null, taskContext);

    CachingTable cacheTable = (CachingTable) tableProvider.getTable();

    Assert.assertEquals("test-data-1", cacheTable.get("1"));
    verify(realTable, times(1)).get(any());
    verify(tableCache, times(2)).get(any()); // cache miss leads to 2 more get() calls
    verify(tableCache, times(1)).put(any(), any());
    Assert.assertEquals(cacheTable.hitRate(), 0.0, 0.0); // 0 hit, 1 request
    Assert.assertEquals(cacheTable.missRate(), 1.0, 0.0);

    Assert.assertEquals("test-data-1", cacheTable.get("1"));
    verify(realTable, times(1)).get(any()); // no change
    verify(tableCache, times(3)).get(any());
    verify(tableCache, times(1)).put(any(), any()); // no change
    Assert.assertEquals(cacheTable.hitRate(), 0.5, 0.0); // 1 hit, 2 requests
    Assert.assertEquals(cacheTable.missRate(), 0.5, 0.0);

    cacheTable.put("2", "test-data-XXXX");
    verify(tableCache, times(isWriteAround ? 1 : 2)).put(any(), any());
    verify(realTable, times(1)).put(any(), any());

    if (isWriteAround) {
      Assert.assertEquals("test-data-2", cacheTable.get("2")); // expects value from table
      verify(tableCache, times(5)).get(any()); // cache miss leads to 2 more get() calls
      Assert.assertEquals(cacheTable.hitRate(), 0.33, 0.1); // 1 hit, 3 requests
    } else {
      Assert.assertEquals("test-data-XXXX", cacheTable.get("2")); // expect value from cache
      verify(tableCache, times(4)).get(any()); // cache hit
      Assert.assertEquals(cacheTable.hitRate(), 0.66, 0.1); // 2 hits, 3 requests
    }
  }

  @Test
  public void testCacheOps() {
    doTestCacheOps(false);
  }

  @Test
  public void testCacheOpsWriteAround() {
    doTestCacheOps(true);
  }

  @Test
  public void testNonexistentKeyInTable() {
    ReadableTable<String, String> table = mock(ReadableTable.class);
    doReturn(null).when(table).get(any());
    ReadWriteTable<String, String> cache = getMockCache().getLeft();
    CachingTable<String, String> cachingTable = new CachingTable<>("myTable", table, cache, 16, false);
    Assert.assertNull(cachingTable.get("abc"));
    verify(cache, times(2)).get(any());
    Assert.assertNull(cache.get("abc"));
    verify(cache, times(0)).put(any(), any());
  }

  @Test
  public void testKeyEviction() {
    ReadableTable<String, String> table = mock(ReadableTable.class);
    doReturn("3").when(table).get(any());
    ReadWriteTable<String, String> cache = mock(ReadWriteTable.class);

    // no handler added to mock cache so get/put are noop, this can simulate eviction
    CachingTable<String, String> cachingTable = new CachingTable<>("myTable", table, cache, 16, false);
    cachingTable.get("abc");
    verify(table, times(1)).get(any());

    // get() should go to table again
    cachingTable.get("abc");
    verify(table, times(2)).get(any());
  }

  /**
   * Test the atomic operations in CachingTable by simulating 10 threads each executing
   * 5000 random operations (GET/PUT/DELETE) with random keys, which are picked from a
   * narrow range (0-9) for higher concurrency. Consistency is verified by comparing
   * the cache content and table content both of which should match exactly. Eviction
   * is not simulated because it would be impossible to compare the cache/table.
   * @throws InterruptedException
   */
  @Test
  public void testConcurrentAccess() throws InterruptedException {
    final int numThreads = 10;
    final int iterations = 5000;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    // Ensure all threads to reach rendezvous before starting the simulation
    final CountDownLatch startLatch = new CountDownLatch(numThreads);

    Pair<ReadWriteTable<String, String>, Map<String, String>> tableMapPair = getMockCache();
    final ReadableTable<String, String> table = tableMapPair.getLeft();

    Pair<ReadWriteTable<String, String>, Map<String, String>> cacheMapPair = getMockCache();
    final CachingTable<String, String> cachingTable = new CachingTable<>("myTable", table, cacheMapPair.getLeft(), 16, false);

    Map<String, String> cacheMap = cacheMapPair.getRight();
    Map<String, String> tableMap = tableMapPair.getRight();

    final Random rand = new Random(System.currentTimeMillis());

    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
          try {
            startLatch.countDown();
            startLatch.await();
          } catch (InterruptedException e) {
            Assert.fail();
          }

          String lastPutKey = null;
          for (int j = 0; j < iterations; j++) {
            int cmd = rand.nextInt(3);
            String key = String.valueOf(rand.nextInt(10));
            switch (cmd) {
              case 0:
                cachingTable.get(key);
                break;
              case 1:
                cachingTable.put(key, "test-data-" + rand.nextInt());
                lastPutKey = key;
                break;
              case 2:
                if (lastPutKey != null) {
                  cachingTable.delete(lastPutKey);
                }
                break;
            }
          }
        });
    }

    executor.shutdown();

    // Wait up to 1 minute for all threads to finish
    Assert.assertTrue(executor.awaitTermination(60, TimeUnit.MINUTES));

    // Verify cache and table contents fully match
    Assert.assertEquals(cacheMap.size(), tableMap.size());
    cacheMap.keySet().forEach(k -> Assert.assertEquals(cacheMap.get(k), tableMap.get(k)));
  }
}