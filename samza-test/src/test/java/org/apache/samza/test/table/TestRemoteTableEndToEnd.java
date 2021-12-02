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

package org.apache.samza.test.table;

import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.Context;
import org.apache.samza.context.MockContext;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.operators.UpdateMessage;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.KV;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.RecordDoesNotExistException;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.descriptors.GuavaCacheTableDescriptor;
import org.apache.samza.table.remote.BaseTableFunction;
import org.apache.samza.table.remote.NoOpTableReadFunction;
import org.apache.samza.table.remote.RemoteTable;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.util.Base64Serializer;
import org.apache.samza.util.RateLimiter;

import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.test.table.TestTableData.EnrichedPageView;
import static org.apache.samza.test.table.TestTableData.PageView;
import static org.apache.samza.test.table.TestTableData.Profile;
import static org.apache.samza.test.table.TestTableData.generateProfiles;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


public class TestRemoteTableEndToEnd {
  private static final Map<String, AtomicInteger> COUNTERS = new HashMap<>();
  private static final Map<String, List<EnrichedPageView>> WRITTEN_RECORDS = new HashMap<>();
  private static final Map<Integer, EnrichedPageView> WRITTEN_RECORD_MAP = new HashMap<>();

  static class InMemoryProfileReadFunction extends BaseTableFunction
      implements TableReadFunction<Integer, Profile> {

    private final String serializedProfiles;
    private transient Map<Integer, Profile> profileMap;

    private InMemoryProfileReadFunction(String profiles) {
      this.serializedProfiles = profiles;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      Profile[] profiles = Base64Serializer.deserialize(this.serializedProfiles, Profile[].class);
      this.profileMap = Arrays.stream(profiles).collect(Collectors.toMap(p -> p.getMemberId(), Function.identity()));
    }

    @Override
    public CompletableFuture<Profile> getAsync(Integer key) {
      return CompletableFuture.completedFuture(profileMap.get(key));
    }

    @Override
    public CompletableFuture<Profile> getAsync(Integer key, Object ... args) {
      Profile profile = profileMap.get(key);
      boolean append = (boolean) args[0];
      if (append) {
        profile = new Profile(profile.memberId, profile.company + "-r");
      }
      return CompletableFuture.completedFuture(profile);
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }

    static InMemoryProfileReadFunction getInMemoryReadFunction(String serializedProfiles) {
      return new InMemoryProfileReadFunction(serializedProfiles);
    }
  }

  static class InMemoryEnrichedPageViewWriteFunction extends BaseTableFunction
      implements TableWriteFunction<Integer, EnrichedPageView, EnrichedPageView> {

    private String testName;
    private transient List<EnrichedPageView> records;

    public InMemoryEnrichedPageViewWriteFunction(String testName) {
      this.testName = testName;
    }

    // Verify serializable functionality
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();

      // Write to the global list for verification
      records = WRITTEN_RECORDS.get(testName);
    }

    @Override
    public CompletableFuture<Void> putAsync(Integer key, EnrichedPageView record) {
      System.out.println("==> " + testName + " writing " + record.getPageKey());
      records.add(record);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> putAsync(Integer key, EnrichedPageView record, Object ... args) {
      boolean append = (boolean) args[0];
      if (append) {
        record = new EnrichedPageView(record.pageKey, record.memberId, record.company + "-w");
      }
      records.add(record);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateAsync(Integer key, EnrichedPageView update) {
      records.add(update);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateAsync(Integer key, EnrichedPageView update, Object ... args) {
      boolean append = (boolean) args[0];
      System.out.println(append);
      if (append) {
        update = new EnrichedPageView(update.pageKey, update.memberId, update.company + "-w");
      }
      records.add(update);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteAsync(Integer key) {
      records.remove(key);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }
  }

  static class InMemoryEnrichedPageViewWriteFunction2 extends BaseTableFunction
      implements TableWriteFunction<Integer, EnrichedPageView, EnrichedPageView> {
    private transient Map<Integer, EnrichedPageView> recordsMap;
    private String testName;


    public InMemoryEnrichedPageViewWriteFunction2(String testName) {
      this.testName = testName;
    }

    // Verify serializable functionality
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();

      // Write to the global list for verification
      recordsMap = WRITTEN_RECORD_MAP;
    }

    @Override
    public CompletableFuture<Void> putAsync(Integer key, EnrichedPageView record) {
      recordsMap.put(key, record);
      COUNTERS.get(testName + "-put").incrementAndGet();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> putAsync(Integer key, EnrichedPageView record, Object ... args) {
      boolean append = (boolean) args[0];
      if (append) {
        record = new EnrichedPageView(record.pageKey, record.memberId, record.company + "-w");
      }
      COUNTERS.get(testName + "-put").incrementAndGet();
      recordsMap.put(key, record);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateAsync(Integer key, EnrichedPageView update) {
      if (!recordsMap.containsKey(key)) {
        return CompletableFuture.supplyAsync(() -> {
          throw new RecordDoesNotExistException("Record with key : " + key + " does not exist");
        });
      } else {
        COUNTERS.get(testName + "-update").incrementAndGet();
        recordsMap.put(key, update);
        return CompletableFuture.completedFuture(null);
      }
    }

    @Override
    public CompletableFuture<Void> updateAsync(Integer key, EnrichedPageView update, Object ... args) {
      if (!recordsMap.containsKey(key)) {
        return CompletableFuture.supplyAsync(() -> {
          throw new RecordDoesNotExistException("Record with key : " + key + " does not exist");
        });
      } else {
        COUNTERS.get(testName + "-update").incrementAndGet();
        recordsMap.put(key, update);
        return CompletableFuture.completedFuture(null);
      }
    }

    @Override
    public CompletableFuture<Void> deleteAsync(Integer key) {
      recordsMap.remove(key);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }
  }

  static class InMemoryCounterReadFunction extends BaseTableFunction
      implements TableReadFunction {

    private final String testName;

    private InMemoryCounterReadFunction(String testName) {
      this.testName = testName;
    }

    @Override
    public CompletableFuture readAsync(int opId, Object... args) {
      if (1 == opId) {
        boolean shouldReturnValue = (boolean) args[0];
        AtomicInteger counter = COUNTERS.get(testName);
        Integer result = shouldReturnValue ? counter.get() : null;
        return CompletableFuture.completedFuture(result);
      } else {
        throw new SamzaException("Invalid opId: " + opId);
      }
    }

    @Override
    public CompletableFuture getAsync(Object key) {
      throw new SamzaException("Not supported");
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }
  }

  static class InMemoryCounterWriteFunction extends BaseTableFunction
      implements TableWriteFunction {

    private final String testName;

    private InMemoryCounterWriteFunction(String testName) {
      this.testName = testName;
    }

    @Override
    public CompletableFuture<Void> putAsync(Object key, Object record) {
      throw new SamzaException("Not supported");
    }

    @Override
    public CompletableFuture<Void> updateAsync(Object key, @Nullable Object record) {
      throw new SamzaException("Not supported");
    }

    @Override
    public CompletableFuture<Void> deleteAsync(Object key) {
      throw new SamzaException("Not supported");
    }

    @Override
    public CompletableFuture writeAsync(int opId, Object... args) {
      Integer result;
      AtomicInteger counter = COUNTERS.get(testName);
      boolean shouldModify = (boolean) args[0];
      switch (opId) {
        case 1:
          result = shouldModify ? counter.incrementAndGet() : counter.get();
          break;
        case 2:
          result = shouldModify ? counter.decrementAndGet() : counter.get();
          break;
        default:
          throw new SamzaException("Invalid opId: " + opId);
      }
      return CompletableFuture.completedFuture(result);
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }
  }

  private static class TestReadWriteMapFunction implements MapFunction<PageView, PageView> {

    private final String counterTableName;
    private ReadWriteTable counterTable;

    private TestReadWriteMapFunction(String counterTableName) {
      this.counterTableName = counterTableName;
    }

    @Override
    public void init(Context context) {
      counterTable = context.getTaskContext().getTable(counterTableName);
    }

    @Override
    public PageView apply(PageView pageView) {
      try {
        // Counter manipulation
        badOpId();
        Assert.assertNull(getCounterValue(false));
        Integer beforeValue = getCounterValue(true);
        Assert.assertEquals(beforeValue, incCounterValue(false));
        Assert.assertEquals(beforeValue, decCounterValue(false));
        Assert.assertEquals(Integer.valueOf(beforeValue + 1), incCounterValue(true));
        Assert.assertEquals(beforeValue, decCounterValue(true));
        Assert.assertEquals(beforeValue, getCounterValue(true));
        incCounterValue(true);
        return pageView;
      } catch (Exception ex) {
        throw new SamzaException(ex);
      }
    }

    private Integer getCounterValue(boolean shouldReturn) {
      return (Integer) counterTable.readAsync(1, shouldReturn).join();
    }

    private Integer incCounterValue(boolean shouldModifdy) {
      return (Integer) counterTable.writeAsync(1, shouldModifdy).join();
    }

    private Integer decCounterValue(boolean shouldModifdy) {
      return (Integer) counterTable.writeAsync(2, shouldModifdy).join();
    }

    private void badOpId() {
      try {
        counterTable.readAsync(0).join();
        Assert.fail("Shouldn't reach here");
      } catch (SamzaException ex) {
        // Expected exception
      }
    }
  }

  private <K, V> Table<KV<K, V>> getCachingTable(TableDescriptor<K, V, ?> actualTableDesc, boolean defaultCache,
      StreamApplicationDescriptor appDesc) {
    String id = actualTableDesc.getTableId();
    CachingTableDescriptor<K, V> cachingDesc;
    if (defaultCache) {
      cachingDesc = new CachingTableDescriptor<>("caching-table-" + id, actualTableDesc);
      cachingDesc.withReadTtl(Duration.ofMinutes(5));
      cachingDesc.withWriteTtl(Duration.ofMinutes(5));
    } else {
      GuavaCacheTableDescriptor<K, V> guavaTableDesc = new GuavaCacheTableDescriptor<>("guava-table-" + id);
      guavaTableDesc.withCache(CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).build());
      cachingDesc = new CachingTableDescriptor<>("caching-table-" + id, actualTableDesc, guavaTableDesc);
    }

    return appDesc.getTable(cachingDesc);
  }

  private void doTestStreamTableJoinRemoteTable(boolean withCache, boolean defaultCache, boolean withArgs,
      boolean withUpdate, String testName) throws Exception {
    WRITTEN_RECORDS.put(testName, new ArrayList<>());

    // max member id for page views is 10
    final String profiles = Base64Serializer.serialize(generateProfiles(10));

    final RateLimiter readRateLimiter = mock(RateLimiter.class, withSettings().serializable());
    final TableRateLimiter.CreditFunction creditFunction = (k, v, args) -> 1;
    final StreamApplication app = appDesc -> {

      final RemoteTableDescriptor joinTableDesc =
              new RemoteTableDescriptor<Integer, TestTableData.Profile, Void>("profile-table-1")
          .withReadFunction(InMemoryProfileReadFunction.getInMemoryReadFunction(profiles))
          .withRateLimiter(readRateLimiter, creditFunction, null);

      final RemoteTableDescriptor outputTableDesc =
              new RemoteTableDescriptor<Integer, EnrichedPageView, EnrichedPageView>("enriched-page-view-table-1")
          .withReadFunction(new NoOpTableReadFunction<>())
          .withReadRateLimiterDisabled()
          .withWriteFunction(new InMemoryEnrichedPageViewWriteFunction(testName))
          .withWriteRateLimit(1000);

      final Table<KV<Integer, Profile>> outputTable = withCache
          ? getCachingTable(outputTableDesc, defaultCache, appDesc)
          : appDesc.getTable(outputTableDesc);

      final Table<KV<Integer, Profile>> joinTable = withCache
          ? getCachingTable(joinTableDesc, defaultCache, appDesc)
          : appDesc.getTable(joinTableDesc);

      final DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
      final GenericInputDescriptor<PageView> isd = ksd.getInputDescriptor("PageView", new NoOpSerde<>());

      if (!withArgs) {
        if (withUpdate) {
          appDesc.getInputStream(isd)
              .map(pv -> new KV<>(pv.getMemberId(), pv))
              .join(joinTable, new PageViewToProfileJoinFunction())
              .map(m -> new KV(m.getMemberId(), UpdateMessage.of(m, m)))
              .sendUpdateTo(outputTable);
        } else {
          appDesc.getInputStream(isd)
              .map(pv -> new KV<>(pv.getMemberId(), pv))
              .join(joinTable, new PageViewToProfileJoinFunction())
              .map(m -> KV.of(m.getMemberId(), m))
              .sendTo(outputTable);
        }
      } else {
        COUNTERS.put(testName, new AtomicInteger());

        final RemoteTableDescriptor counterTableDesc =
            new RemoteTableDescriptor("counter-table-1")
                .withReadFunction(new InMemoryCounterReadFunction(testName))
                .withWriteFunction(new InMemoryCounterWriteFunction(testName))
                .withRateLimiterDisabled();

        final Table counterTable = withCache
            ? getCachingTable(counterTableDesc, defaultCache, appDesc)
            : appDesc.getTable(counterTableDesc);

        final String counterTableName = ((TableImpl) counterTable).getTableId();

        if (withUpdate) {
          appDesc.getInputStream(isd)
              .map(new TestReadWriteMapFunction(counterTableName))
              .map(pv -> new KV<>(pv.getMemberId(), pv))
              .join(joinTable, new PageViewToProfileJoinFunction(), true)
              .map(m -> KV.of(m.getMemberId(), UpdateMessage.of(m, m)))
              .sendUpdateTo(outputTable, true);
        } else {
          appDesc.getInputStream(isd)
              .map(new TestReadWriteMapFunction(counterTableName))
              .map(pv -> new KV<>(pv.getMemberId(), pv))
              .join(joinTable, new PageViewToProfileJoinFunction(), true)
              .map(m -> new KV(m.getMemberId(), m))
              .sendTo(outputTable, true);
        }
      }
    };

    int numPageViews = 40;
    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<PageView> inputDescriptor = isd
        .getInputDescriptor("PageView", new NoOpSerde<>());
    TestRunner.of(app)
        .addInputStream(inputDescriptor, TestTableData.generatePartitionedPageViews(numPageViews, 4))
        .run(Duration.ofSeconds(10));

    Assert.assertEquals(numPageViews, WRITTEN_RECORDS.get(testName).size());
    Assert.assertNotNull(WRITTEN_RECORDS.get(testName).get(0));
    if (!withArgs) {
      WRITTEN_RECORDS.get(testName).forEach(epv -> Assert.assertFalse(epv.company.contains("-")));
    } else {
      WRITTEN_RECORDS.get(testName).forEach(epv -> {
        Assert.assertTrue(epv.company.endsWith("-r-w"));
      });
      Assert.assertEquals(numPageViews, COUNTERS.get(testName).get());
    }
  }

  private void doTestStreamTableJoinRemoteTableWithFirstTimeUpdates(String testName) throws IOException {
    final String profiles = Base64Serializer.serialize(generateProfiles(30));

    final RateLimiter readRateLimiter = mock(RateLimiter.class, withSettings().serializable());
    final TableRateLimiter.CreditFunction creditFunction = (k, v, args) -> 1;
    final StreamApplication app = appDesc -> {

      final RemoteTableDescriptor joinTableDesc =
          new RemoteTableDescriptor<Integer, TestTableData.Profile, Void>("profile-table-1").withReadFunction(
              InMemoryProfileReadFunction.getInMemoryReadFunction(profiles))
              .withRateLimiter(readRateLimiter, creditFunction, null);

      final RemoteTableDescriptor outputTableDesc =
          new RemoteTableDescriptor<Integer, EnrichedPageView, EnrichedPageView>("enriched-page-view-table-1").withReadFunction(new NoOpTableReadFunction<>())
              .withReadRateLimiterDisabled()
              .withWriteFunction(new InMemoryEnrichedPageViewWriteFunction2(testName))
              .withWriteRateLimit(1000);

      // counters to count puts and updates
      COUNTERS.put(testName + "-put", new AtomicInteger());
      COUNTERS.put(testName + "-update", new AtomicInteger());

      final Table<KV<Integer, Profile>> outputTable = appDesc.getTable(outputTableDesc);

      final Table<KV<Integer, Profile>> joinTable = appDesc.getTable(joinTableDesc);

      final DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
      final GenericInputDescriptor<PageView> isd = ksd.getInputDescriptor("PageView", new NoOpSerde<>());

      appDesc.getInputStream(isd)
          .map(pv -> new KV<>(pv.getMemberId(), pv))
          .join(joinTable, new PageViewToProfileJoinFunction())
          .map(m -> new KV(m.getMemberId(), UpdateMessage.of(m, m)))
          .sendUpdateTo(outputTable);
    };

    int numPageViews = 15;
    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<PageView> inputDescriptor = isd.getInputDescriptor("PageView", new NoOpSerde<>());
    Map<Integer, List<PageView>> integerListMap = TestTableData.generatePartitionedPageViews(numPageViews, 1);
    TestRunner.of(app)
        .addInputStream(inputDescriptor, integerListMap)
        .run(Duration.ofSeconds(10));

    Assert.assertEquals(10, COUNTERS.get(testName + "-put").intValue());
    Assert.assertEquals(15, COUNTERS.get(testName + "-update").intValue());
  }

  @Test
  public void testStreamTableJoinRemoteTableWithFirstTimeUpdates() throws Exception {
    doTestStreamTableJoinRemoteTableWithFirstTimeUpdates("testStreamTableJoinRemoteTableWithFirstTimeUpdates");
  }

  @Test
  public void testStreamTableJoinRemoteTable() throws Exception {
    doTestStreamTableJoinRemoteTable(false, false, false, false, "testStreamTableJoinRemoteTable");
  }

  @Test
  public void testStreamTableJoinRemoteTableWithCache() throws Exception {
    doTestStreamTableJoinRemoteTable(true, false, false, false, "testStreamTableJoinRemoteTableWithCache");
  }

  @Test
  public void testStreamTableJoinRemoteTableWithDefaultCache() throws Exception {
    doTestStreamTableJoinRemoteTable(true, true, false, false, "testStreamTableJoinRemoteTableWithDefaultCache");
  }

  @Test
  public void testStreamTableJoinRemoteTableWithArgs() throws Exception {
    doTestStreamTableJoinRemoteTable(false, false, true, false, "testStreamTableJoinRemoteTableWithArgs");
  }

  @Test
  public void testStreamTableJoinRemoteTableWithUpdates() throws Exception {
    doTestStreamTableJoinRemoteTable(false, false, false, true, "testStreamTableJoinRemoteTableWithUpdates");
  }

  @Test
  public void testStreamTableJoinRemoteTableWithArgsAndUpdates() throws Exception {
    doTestStreamTableJoinRemoteTable(false, false, true, true, "testStreamTableJoinRemoteTableWithArgsAndUpdates");
  }

  @Test(expected = SamzaException.class)
  public void testStreamTableJoinRemoteTableWithUpdatesWithCache() throws Exception {
    doTestStreamTableJoinRemoteTable(true, false, false, true, "testStreamTableJoinRemoteTableWithUpdatesWithCache");
  }

  @Test(expected = SamzaException.class)
  public void testStreamTableJoinRemoteTableWithUpdatesWithDefaultCache() throws Exception {
    doTestStreamTableJoinRemoteTable(true, true, false, true, "testStreamTableJoinRemoteTableWithUpdatesWithDefaultCache");
  }

  @Test
  public void testStreamTableJoinRemoteTableWithCacheWithArgs() throws Exception {
    doTestStreamTableJoinRemoteTable(true, false, true, false, "testStreamTableJoinRemoteTableWithCacheWithArgs");
  }

  @Test
  public void testStreamTableJoinRemoteTableWithDefaultCacheWithArgs() throws Exception {
    doTestStreamTableJoinRemoteTable(true, true, true, false, "testStreamTableJoinRemoteTableWithDefaultCacheWithArgs");
  }

  private Context createMockContext() {
    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    doReturn(new Counter("")).when(metricsRegistry).newCounter(anyString(), anyString());
    doReturn(new Timer("")).when(metricsRegistry).newTimer(anyString(), anyString());
    Context context = new MockContext();
    doReturn(new MapConfig()).when(context.getJobContext()).getConfig();
    doReturn(metricsRegistry).when(context.getContainerContext()).getContainerMetricsRegistry();
    return context;
  }

  @Test(expected = SamzaException.class)
  public void testCatchReaderException() {
    TableReadFunction<String, ?> reader = mock(TableReadFunction.class);
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Expected test exception"));
    doReturn(future).when(reader).getAsync(anyString());
    TableRateLimiter rateLimitHelper = mock(TableRateLimiter.class);
    RemoteTable<String, String, Void> table = new RemoteTable<>("table1", reader, null,
        rateLimitHelper, null, null, Executors.newSingleThreadExecutor(),
        null, null, null,
        null, null, null);
    table.init(createMockContext());
    table.get("abc");
  }

  @Test(expected = SamzaException.class)
  public void testCatchWriterException() {
    TableReadFunction<String, String> reader = mock(TableReadFunction.class);
    TableWriteFunction<String, String, Void> writer = mock(TableWriteFunction.class);
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Expected test exception"));
    doReturn(future).when(writer).putAsync(anyString(), any());
    TableRateLimiter rateLimitHelper = mock(TableRateLimiter.class);
    RemoteTable<String, String, Void> table = new RemoteTable<>("table1", reader, writer,
        rateLimitHelper, rateLimitHelper, rateLimitHelper, Executors.newSingleThreadExecutor(),
        null, null, null,
        null, null, null);
    table.init(createMockContext());
    table.put("abc", "efg");
  }

  @Test(expected = SamzaException.class)
  public void testCatchWriterExceptionWithUpdates() {
    TableReadFunction<String, String> reader = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writer = mock(TableWriteFunction.class);
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Expected test exception"));
    doReturn(future).when(writer).updateAsync(anyString(), anyString(), anyString());
    TableRateLimiter rateLimitHelper = mock(TableRateLimiter.class);
    RemoteTable<String, String, String> table = new RemoteTable<>("table1", reader, writer,
        rateLimitHelper, rateLimitHelper, rateLimitHelper, Executors.newSingleThreadExecutor(),
        null, null, null,
        null, null, null);
    table.init(createMockContext());
    table.update("abc", "xyz", "xyz");
  }

  @Test
  public void testUninitializedWriter() {
    TableReadFunction<String, String> reader = mock(TableReadFunction.class);
    TableRateLimiter rateLimitHelper = mock(TableRateLimiter.class);
    RemoteTable<String, String, Void> table = new RemoteTable<>("table1", reader, null,
        rateLimitHelper, null, null, Executors.newSingleThreadExecutor(),
        null, null, null,
        null, null, null);
    table.init(createMockContext());
    try {
      table.put("abc", "efg");
      Assert.fail();
    } catch (SamzaException ex) {
      // Ignore
    }
    try {
      table.delete("abc");
      Assert.fail();
    } catch (SamzaException ex) {
      // Ignore
    }
    table.flush();
    table.close();
  }
}
