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
import org.apache.samza.application.StreamAppDescriptor;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.internal.StreamAppDescriptorImpl;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.KV;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.table.Table;
import org.apache.samza.table.caching.CachingTableDescriptor;
import org.apache.samza.table.caching.guava.GuavaCacheTableDescriptor;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.remote.RemoteReadableTable;
import org.apache.samza.table.remote.RemoteTableDescriptor;
import org.apache.samza.table.remote.RemoteReadWriteTable;
import org.apache.samza.task.TaskContext;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.util.Base64Serializer;
import org.apache.samza.util.RateLimiter;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.cache.CacheBuilder;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;


public class TestRemoteTable extends AbstractIntegrationTestHarness {

  static Map<String, List<TestTableData.EnrichedPageView>> writtenRecords = new HashMap<>();

  static class InMemoryReadFunction implements TableReadFunction<Integer, TestTableData.Profile> {
    private final String serializedProfiles;
    private transient Map<Integer, TestTableData.Profile> profileMap;

    private InMemoryReadFunction(String profiles) {
      this.serializedProfiles = profiles;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      TestTableData.Profile[] profiles = Base64Serializer.deserialize(this.serializedProfiles, TestTableData.Profile[].class);
      this.profileMap = Arrays.stream(profiles).collect(Collectors.toMap(p -> p.getMemberId(), Function.identity()));
    }

    @Override
    public CompletableFuture<TestTableData.Profile> getAsync(Integer key) {
      return CompletableFuture.completedFuture(profileMap.get(key));
    }

    static InMemoryReadFunction getInMemoryReadFunction(String serializedProfiles) {
      return new InMemoryReadFunction(serializedProfiles);
    }
  }

  static class InMemoryWriteFunction implements TableWriteFunction<Integer, TestTableData.EnrichedPageView> {
    private transient List<TestTableData.EnrichedPageView> records;
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
    public CompletableFuture<Void> putAsync(Integer key, TestTableData.EnrichedPageView record) {
      records.add(record);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteAsync(Integer key) {
      records.remove(key);
      return CompletableFuture.completedFuture(null);
    }
  }

  private <K, V> Table<KV<K, V>> getCachingTable(Table<KV<K, V>> actualTable, boolean defaultCache, String id, StreamAppDescriptor appDesc) {
    CachingTableDescriptor<K, V> cachingDesc = new CachingTableDescriptor<>("caching-table-" + id);
    if (defaultCache) {
      cachingDesc.withReadTtl(Duration.ofMinutes(5));
      cachingDesc.withWriteTtl(Duration.ofMinutes(5));
    } else {
      GuavaCacheTableDescriptor<K, V> guavaDesc = new GuavaCacheTableDescriptor<>("guava-table-" + id);
      guavaDesc.withCache(CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).build());
      Table<KV<K, V>> guavaTable = appDesc.getTable(guavaDesc);
      cachingDesc.withCache(guavaTable);
    }

    cachingDesc.withTable(actualTable);
    return appDesc.getTable(cachingDesc);
  }

  private void doTestStreamTableJoinRemoteTable(boolean withCache, boolean defaultCache, String testName) throws Exception {
    final InMemoryWriteFunction writer = new InMemoryWriteFunction(testName);

    writtenRecords.put(testName, new ArrayList<>());

    int count = 10;
    TestTableData.PageView[] pageViews = TestTableData.generatePageViews(count);
    String profiles = Base64Serializer.serialize(TestTableData.generateProfiles(count));

    int partitionCount = 4;
    Map<String, String> configs = TestLocalTable.getBaseJobConfig(bootstrapUrl(), zkConnect());

    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));

    final RateLimiter readRateLimiter = mock(RateLimiter.class);
    final RateLimiter writeRateLimiter = mock(RateLimiter.class);
    final StreamApplication app = appDesc -> {
      RemoteTableDescriptor<Integer, TestTableData.Profile> inputTableDesc = new RemoteTableDescriptor<>("profile-table-1");
      inputTableDesc
          .withReadFunction(InMemoryReadFunction.getInMemoryReadFunction(profiles))
          .withRateLimiter(readRateLimiter, null, null);

      RemoteTableDescriptor<Integer, TestTableData.EnrichedPageView> outputTableDesc = new RemoteTableDescriptor<>("enriched-page-view-table-1");
      outputTableDesc
          .withReadFunction(key -> null) // dummy reader
          .withWriteFunction(writer)
          .withRateLimiter(writeRateLimiter, null, null);

      Table<KV<Integer, TestTableData.EnrichedPageView>> outputTable = appDesc.getTable(outputTableDesc);

      if (withCache) {
        outputTable = getCachingTable(outputTable, defaultCache, "output", appDesc);
      }

      Table<KV<Integer, TestTableData.Profile>> inputTable = appDesc.getTable(inputTableDesc);

      if (withCache) {
        inputTable = getCachingTable(inputTable, defaultCache, "input", appDesc);
      }

      appDesc.getInputStream("PageView", new NoOpSerde<TestTableData.PageView>())
          .map(pv -> new KV<>(pv.getMemberId(), pv))
          .join(inputTable, new TestLocalTable.PageViewToProfileJoinFunction())
          .map(m -> new KV(m.getMemberId(), m))
          .sendTo(outputTable);
    };

    final LocalApplicationRunner runner = new LocalApplicationRunner(new StreamAppDescriptorImpl(app, new MapConfig(configs)));
    runner.run();
    runner.waitForFinish();

    int numExpected = count * partitionCount;
    Assert.assertEquals(numExpected, writtenRecords.get(testName).size());
    Assert.assertTrue(writtenRecords.get(testName).get(0) instanceof TestTableData.EnrichedPageView);
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

  private TaskContext createMockTaskContext() {
    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    doReturn(new Counter("")).when(metricsRegistry).newCounter(anyString(), anyString());
    doReturn(new Timer("")).when(metricsRegistry).newTimer(anyString(), anyString());
    TaskContext context = mock(TaskContext.class);
    doReturn(metricsRegistry).when(context).getMetricsRegistry();
    return context;
  }

  @Test(expected = SamzaException.class)
  public void testCatchReaderException() {
    TableReadFunction<String, ?> reader = mock(TableReadFunction.class);
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Expected test exception"));
    doReturn(future).when(reader).getAsync(anyString());
    TableRateLimiter rateLimitHelper = mock(TableRateLimiter.class);
    RemoteReadableTable<String, ?> table = new RemoteReadableTable<>(
        "table1", reader, rateLimitHelper, Executors.newSingleThreadExecutor(), null);
    table.init(mock(SamzaContainerContext.class), createMockTaskContext());
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
    RemoteReadWriteTable<String, String> table = new RemoteReadWriteTable<String, String>(
        "table1", reader, writer, rateLimitHelper, rateLimitHelper, Executors.newSingleThreadExecutor(), null);
    table.init(mock(SamzaContainerContext.class), createMockTaskContext());
    table.put("abc", "efg");
  }
}
