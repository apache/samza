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
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.Context;
import org.apache.samza.context.MockContext;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.KV;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.descriptors.GuavaCacheTableDescriptor;
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;


public class TestRemoteTableEndToEnd extends IntegrationTestHarness {

  static Map<String, List<EnrichedPageView>> writtenRecords = new HashMap<>();

  static class InMemoryReadFunction implements TableReadFunction<Integer, Profile> {
    private final String serializedProfiles;
    private transient Map<Integer, Profile> profileMap;

    private InMemoryReadFunction(String profiles) {
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
    public boolean isRetriable(Throwable exception) {
      return false;
    }

    static InMemoryReadFunction getInMemoryReadFunction(String serializedProfiles) {
      return new InMemoryReadFunction(serializedProfiles);
    }
  }

  static class InMemoryWriteFunction implements TableWriteFunction<Integer, EnrichedPageView> {
    private transient List<EnrichedPageView> records;
    private String testName;

    public InMemoryWriteFunction(String testName) {
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

  private <K, V> Table<KV<K, V>> getCachingTable(TableDescriptor<K, V, ?> actualTableDesc, boolean defaultCache, String id, StreamApplicationDescriptor appDesc) {
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

  static class MyReadFunction implements TableReadFunction {
    @Override
    public CompletableFuture getAsync(Object key) {
      return null;
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }
  }

  private void doTestStreamTableJoinRemoteTable(boolean withCache, boolean defaultCache, String testName) throws Exception {
    final InMemoryWriteFunction writer = new InMemoryWriteFunction(testName);

    writtenRecords.put(testName, new ArrayList<>());

    int count = 10;
    PageView[] pageViews = generatePageViews(count);
    String profiles = Base64Serializer.serialize(generateProfiles(count));

    int partitionCount = 4;
    Map<String, String> configs = TestLocalTableEndToEnd.getBaseJobConfig(bootstrapUrl(), zkConnect());

    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));

    final RateLimiter readRateLimiter = mock(RateLimiter.class, withSettings().serializable());
    final RateLimiter writeRateLimiter = mock(RateLimiter.class, withSettings().serializable());
    final StreamApplication app = appDesc -> {
      RemoteTableDescriptor<Integer, TestTableData.Profile> inputTableDesc = new RemoteTableDescriptor<>("profile-table-1");
      inputTableDesc
          .withReadFunction(InMemoryReadFunction.getInMemoryReadFunction(profiles))
          .withRateLimiter(readRateLimiter, null, null);

      // dummy reader
      TableReadFunction readFn = new MyReadFunction();

      RemoteTableDescriptor<Integer, EnrichedPageView> outputTableDesc = new RemoteTableDescriptor<>("enriched-page-view-table-1");
      outputTableDesc
          .withReadFunction(readFn)
          .withWriteFunction(writer)
          .withRateLimiter(writeRateLimiter, null, null);

      Table<KV<Integer, EnrichedPageView>> outputTable = withCache
          ? getCachingTable(outputTableDesc, defaultCache, "output", appDesc)
          : appDesc.getTable(outputTableDesc);

      Table<KV<Integer, Profile>> inputTable = withCache
          ? getCachingTable(inputTableDesc, defaultCache, "input", appDesc)
          : appDesc.getTable(inputTableDesc);

      DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
      GenericInputDescriptor<PageView> isd = ksd.getInputDescriptor("PageView", new NoOpSerde<>());
      appDesc.getInputStream(isd)
          .map(pv -> new KV<>(pv.getMemberId(), pv))
          .join(inputTable, new PageViewToProfileJoinFunction())
          .map(m -> new KV(m.getMemberId(), m))
          .sendTo(outputTable);
    };

    Config config = new MapConfig(configs);
    final LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
    executeRun(runner, config);
    runner.waitForFinish();

    int numExpected = count * partitionCount;
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
    Assert.assertEquals(2, failureCount);
  }
}
