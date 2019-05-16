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
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.Context;
import org.apache.samza.context.MockContext;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.KV;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.descriptors.GuavaCacheTableDescriptor;
import org.apache.samza.table.remote.BaseTableFunction;
import org.apache.samza.table.remote.RemoteTable;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.test.harness.IntegrationTestHarness;
import org.apache.samza.test.util.Base64Serializer;
import org.apache.samza.util.RateLimiter;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.test.table.TestTableData.EnrichedPageView;
import static org.apache.samza.test.table.TestTableData.PageView;
import static org.apache.samza.test.table.TestTableData.Profile;
import static org.apache.samza.test.table.TestTableData.generatePageViews;
import static org.apache.samza.test.table.TestTableData.generateProfiles;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


public class TestRemoteTableEndToEnd extends IntegrationTestHarness {

  static Map<String, AtomicInteger> counters = new HashMap<>();
  static Map<String, List<EnrichedPageView>> writtenRecords = new HashMap<>();

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

    static InMemoryProfileReadFunction getInMemoryReadFunction(String testName, String serializedProfiles) {
      return new InMemoryProfileReadFunction(serializedProfiles);
    }
  }

  static class InMemoryEnrichedPageViewWriteFunction extends BaseTableFunction
      implements TableWriteFunction<Integer, EnrichedPageView> {

    private String testName;
    private transient List<EnrichedPageView> records;

    public InMemoryEnrichedPageViewWriteFunction(String testName) {
      this.testName = testName;
    }

    // Verify serializable functionality
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();

      // Write to the global list for verification
      records = writtenRecords.get(testName);
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
    public CompletableFuture<Void> deleteAsync(Integer key) {
      records.remove(key);
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
        AtomicInteger counter = counters.get(testName);
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
    public CompletableFuture<Void> deleteAsync(Object key) {
      throw new SamzaException("Not supported");
    }

    @Override
    public CompletableFuture writeAsync(int opId, Object... args) {
      Integer result;
      AtomicInteger counter = counters.get(testName);
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

  static class DummyReadFunction extends BaseTableFunction
      implements TableReadFunction {

    @Override
    public CompletableFuture getAsync(Object key) {
      throw new SamzaException("Not supported");
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
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

  private void doTestStreamTableJoinRemoteTable(boolean withCache, boolean defaultCache, String testName) throws Exception {

    writtenRecords.put(testName, new ArrayList<>());

    int count = 10;
    final PageView[] pageViews = generatePageViews(count);
    final String profiles = Base64Serializer.serialize(generateProfiles(count));

    final int partitionCount = 4;
    final Map<String, String> configs = TestLocalTableEndToEnd.getBaseJobConfig(bootstrapUrl(), zkConnect());
    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));

    final RateLimiter readRateLimiter = mock(RateLimiter.class, withSettings().serializable());
    final TableRateLimiter.CreditFunction creditFunction = (k, v, args) -> 1;
    final StreamApplication app = appDesc -> {

      final RemoteTableDescriptor joinTableDesc =
              new RemoteTableDescriptor<Integer, TestTableData.Profile>("profile-table-1")
          .withReadFunction(InMemoryProfileReadFunction.getInMemoryReadFunction(testName, profiles))
          .withRateLimiter(readRateLimiter, creditFunction, null);

      final RemoteTableDescriptor outputTableDesc =
              new RemoteTableDescriptor<Integer, EnrichedPageView>("enriched-page-view-table-1")
          .withReadFunction(new DummyReadFunction())
          .withReadRateLimiterDisabled()
          .withWriteFunction(new InMemoryEnrichedPageViewWriteFunction(testName))
          .withWriteRateLimit(1000);

      final Table<KV<Integer, EnrichedPageView>> outputTable = withCache
          ? getCachingTable(outputTableDesc, defaultCache, appDesc)
          : appDesc.getTable(outputTableDesc);

      final Table<KV<Integer, Profile>> joinTable = withCache
          ? getCachingTable(joinTableDesc, defaultCache, appDesc)
          : appDesc.getTable(joinTableDesc);

      final DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
      final GenericInputDescriptor<PageView> isd = ksd.getInputDescriptor("PageView", new NoOpSerde<>());

      appDesc.getInputStream(isd)
          .map(pv -> new KV<>(pv.getMemberId(), pv))
          .join(joinTable, new PageViewToProfileJoinFunction())
          .map(m -> new KV(m.getMemberId(), m))
          .sendTo(outputTable);
    };

    final Config config = new MapConfig(configs);
    final LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
    executeRun(runner, config);
    runner.waitForFinish();

    final int numExpected = count * partitionCount;
    Assert.assertEquals(numExpected, writtenRecords.get(testName).size());
    Assert.assertTrue(writtenRecords.get(testName).get(0) instanceof EnrichedPageView);
  }

  @Test
  public void testStreamTableJoinRemoteTable() throws Exception {
    doTestStreamTableJoinRemoteTable(false, false, "testStreamTableJoinRemoteTable");
  }

  @Test
  public void testStreamTableJoinRemoteTableWithCache() throws Exception {
    doTestStreamTableJoinRemoteTable(true, false, "testStreamTableJoinRemoteTableWithCache");
  }

  @Test
  public void testStreamTableJoinRemoteTableWithDefaultCache() throws Exception {
    doTestStreamTableJoinRemoteTable(true, true, "testStreamTableJoinRemoteTableWithDefaultCache");
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
    RemoteTable<String, String> table = new RemoteTable<>("table1", reader, null,
        rateLimitHelper, null, Executors.newSingleThreadExecutor(),
        null, null, null,
        null);
    table.init(createMockContext());
    table.get("abc");
  }

  @Test(expected = SamzaException.class)
  public void testCatchWriterException() {
    TableReadFunction<String, String> reader = mock(TableReadFunction.class);
    TableWriteFunction<String, String> writer = mock(TableWriteFunction.class);
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Expected test exception"));
    doReturn(future).when(writer).putAsync(anyString(), any());
    TableRateLimiter rateLimitHelper = mock(TableRateLimiter.class);
    RemoteTable<String, String> table = new RemoteTable<String, String>("table1", reader, writer,
        rateLimitHelper, rateLimitHelper, Executors.newSingleThreadExecutor(),
        null, null, null,
        null);
    table.init(createMockContext());
    table.put("abc", "efg");
  }

  @Test
  public void testUninitializedWriter() {
    TableReadFunction<String, String> reader = mock(TableReadFunction.class);
    TableRateLimiter rateLimitHelper = mock(TableRateLimiter.class);
    RemoteTable<String, String> table = new RemoteTable<String, String>("table1", reader, null,
        rateLimitHelper, null, Executors.newSingleThreadExecutor(),
        null, null, null,
        null);
    table.init(createMockContext());
    int failureCount = 0;
    try {
      table.put("abc", "efg");
    } catch (SamzaException ex) {
      ++failureCount;
    }
    try {
      table.delete("abc");
    } catch (SamzaException ex) {
      ++failureCount;
    }
    table.flush();
    table.close();
  }

  private void doTestReadWriteWithArgs(boolean withCache, boolean defaultCache, String testName) throws Exception {

    writtenRecords.put(testName, new ArrayList<>());
    counters.put(testName, new AtomicInteger());

    final int count = 10;
    final PageView[] pageViews = generatePageViews(count);
    final String profiles = Base64Serializer.serialize(generateProfiles(count));

    final int partitionCount = 4;
    final Map<String, String> configs = TestLocalTableEndToEnd.getBaseJobConfig(bootstrapUrl(), zkConnect());
    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));

    final RateLimiter readRateLimiter = mock(RateLimiter.class, withSettings().serializable());
    final TableRateLimiter.CreditFunction creditFunction = (k, v, args) -> 1;
    final StreamApplication app = appDesc -> {

      final RemoteTableDescriptor joinTableDesc =
          new RemoteTableDescriptor<Integer, TestTableData.Profile>("profile-table-1")
              .withReadFunction(InMemoryProfileReadFunction.getInMemoryReadFunction(testName, profiles))
              .withRateLimiter(readRateLimiter, creditFunction, null);

      final RemoteTableDescriptor outputTableDesc =
          new RemoteTableDescriptor<Integer, EnrichedPageView>("enriched-page-view-table-1")
              .withReadFunction(new DummyReadFunction())
              .withReadRateLimiterDisabled()
              .withWriteFunction(new InMemoryEnrichedPageViewWriteFunction(testName))
              .withWriteRateLimit(1000);

      final RemoteTableDescriptor counterTableDesc =
          new RemoteTableDescriptor("counter-table-1")
              .withReadFunction(new InMemoryCounterReadFunction(testName))
              .withWriteFunction(new InMemoryCounterWriteFunction(testName))
              .withRateLimiterDisabled();

      final Table joinTable = withCache
          ? getCachingTable(joinTableDesc, defaultCache, appDesc)
          : appDesc.getTable(joinTableDesc);

      final Table outputTable = withCache
          ? getCachingTable(outputTableDesc, defaultCache, appDesc)
          : appDesc.getTable(outputTableDesc);

      final Table counterTable = withCache
          ? getCachingTable(counterTableDesc, defaultCache, appDesc)
          : appDesc.getTable(counterTableDesc);

      final String joinTableName = ((TableImpl) joinTable).getTableId();
      final String outputTableName = ((TableImpl) outputTable).getTableId();
      final String counterTableName = ((TableImpl) counterTable).getTableId();

      final DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
      final GenericInputDescriptor<PageView> isd = ksd.getInputDescriptor("PageView", new NoOpSerde<>());

      appDesc.getInputStream(isd)
          .map(new JoinMapFunction(joinTableName, outputTableName, counterTableName));
    };

    final Config config = new MapConfig(configs);
    final LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
    executeRun(runner, config);
    runner.waitForFinish();

    final int numExpected = count * partitionCount;
    Assert.assertEquals(numExpected, writtenRecords.get(testName).size());
    Assert.assertTrue(writtenRecords.get(testName).get(0) instanceof EnrichedPageView);
    writtenRecords.get(testName).forEach(epv -> {
        Assert.assertTrue(epv.company.endsWith("-r-w"));
      });
    Assert.assertEquals(numExpected, counters.get(testName).get());
  }

  static private class JoinMapFunction implements MapFunction<PageView, EnrichedPageView> {

    private final String joinTableName;
    private final String outputTableName;
    private final String counterTableName;
    private ReadWriteTable<Integer, Profile> inputTable;
    private ReadWriteTable<Integer, EnrichedPageView> outputTable;
    private ReadWriteTable counterTable;

    private JoinMapFunction(String joinTableName, String outputTableName, String counterTableName) {
      this.joinTableName = joinTableName;
      this.outputTableName = outputTableName;
      this.counterTableName = counterTableName;
    }

    @Override
    public void init(Context context) {
      inputTable = context.getTaskContext().getTable(joinTableName);
      outputTable = context.getTaskContext().getTable(outputTableName);
      counterTable = context.getTaskContext().getTable(counterTableName);
    }

    @Override
    public EnrichedPageView apply(PageView pageView) {
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

        // Generate EnrichedPageView
        Profile profile = inputTable.getAsync(pageView.memberId, true).join();
        EnrichedPageView epv = new EnrichedPageView(pageView.getPageKey(), profile.memberId, profile.company);
        outputTable.putAsync(epv.memberId, epv, true).join();
        return epv;
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
        outputTable.readAsync(0).join();
        Assert.fail("Shouldn't reach here");
      } catch (SamzaException ex) {
        // Expected exception
      }
    }
  }

  @Test
  public void testReadWriteWithArgs() throws Exception {
    doTestReadWriteWithArgs(false, false, "testReadWriteWithArgs");
  }

  @Test
  public void testReadWriteWithArgsWithCache() throws Exception {
    doTestReadWriteWithArgs(true, false, "testReadWriteWithArgsWithCache");
  }

  @Test
  public void testReadWriteWithArgsWithDefaultCache() throws Exception {
    doTestReadWriteWithArgs(true, true, "testReadWriteWithArgsWithDefaultCache");
  }
}
