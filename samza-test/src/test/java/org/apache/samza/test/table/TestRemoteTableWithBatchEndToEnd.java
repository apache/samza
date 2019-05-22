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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.batching.CompactBatchProvider;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.remote.BaseTableFunction;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.test.harness.IntegrationTestHarness;
import org.apache.samza.test.util.Base64Serializer;
import org.apache.samza.util.RateLimiter;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.test.table.TestTableData.*;
import static org.mockito.Mockito.*;


public class TestRemoteTableWithBatchEndToEnd extends IntegrationTestHarness {

  static Map<String, List<EnrichedPageView>> writtenRecords = new HashMap<>();
  static Map<String, AtomicInteger> batchWrites = new HashMap<>();
  static Map<String, AtomicInteger> batchReads = new HashMap<>();

  static class InMemoryReadFunction extends BaseTableFunction
      implements TableReadFunction<Integer, Profile> {
    private final String serializedProfiles;
    private final String testName;
    private transient Map<Integer, Profile> profileMap;
    private transient AtomicInteger batchReadCounter;

    private InMemoryReadFunction(String testName, String profiles) {
      this.testName = testName;
      this.serializedProfiles = profiles;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      Profile[] profiles = Base64Serializer.deserialize(this.serializedProfiles, Profile[].class);
      this.profileMap = Arrays.stream(profiles).collect(Collectors.toMap(p -> p.getMemberId(), Function.identity()));
      batchReadCounter = batchReads.get(testName);
    }

    @Override
    public CompletableFuture<Profile> getAsync(Integer key) {
      return CompletableFuture.completedFuture(profileMap.get(key));
    }

    @Override
    public CompletableFuture<Map<Integer, Profile>> getAllAsync(Collection<Integer> keys) {
      batchReadCounter.incrementAndGet();
      return TableReadFunction.super.getAllAsync(keys);
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }

    static InMemoryReadFunction getInMemoryReadFunction(String testName, String serializedProfiles) {
      return new InMemoryReadFunction(testName, serializedProfiles);
    }
  }

  static class InMemoryWriteFunction extends BaseTableFunction
      implements TableWriteFunction<Integer, EnrichedPageView> {
    private transient List<EnrichedPageView> records;
    private transient AtomicInteger batchWritesCounter;
    private final String testName;

    public InMemoryWriteFunction(String testName) {
      this.testName = testName;
    }

    // Verify serializable functionality
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();

      // Write to the global list for verification
      records = writtenRecords.get(testName);
      batchWritesCounter = batchWrites.get(testName);
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
    public CompletableFuture<Void> putAllAsync(Collection<Entry<Integer, EnrichedPageView>> records) {
      batchWritesCounter.incrementAndGet();
      return TableWriteFunction.super.putAllAsync(records);
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }
  }


  static class MyReadFunction extends BaseTableFunction
      implements TableReadFunction {
    @Override
    public CompletableFuture getAsync(Object key) {
      return null;
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }
  }

  private void doTestStreamTableJoinRemoteTable(String testName, boolean batchRead, boolean batchWrite) throws Exception {
    final InMemoryWriteFunction writer = new InMemoryWriteFunction(testName);

    batchReads.put(testName, new AtomicInteger());
    batchWrites.put(testName, new AtomicInteger());
    writtenRecords.put(testName, new CopyOnWriteArrayList<>());

    final int count = 16;
    final int batchSize = 4;
    PageView[] pageViews = generatePageViewsWithDistinctKeys(count);
    String profiles = Base64Serializer.serialize(generateProfiles(count));

    int partitionCount = 1;
    Map<String, String> configs = TestLocalTableEndToEnd.getBaseJobConfig(bootstrapUrl(), zkConnect());

    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));
    configs.put("task.max.concurrency", String.valueOf(count));
    configs.put("task.async.commit", String.valueOf(true));

    final RateLimiter readRateLimiter = mock(RateLimiter.class, withSettings().serializable());
    final RateLimiter writeRateLimiter = mock(RateLimiter.class, withSettings().serializable());
    final TableRateLimiter.CreditFunction creditFunction = (k, v, args)->1;
    final StreamApplication app = appDesc -> {
      RemoteTableDescriptor<Integer, Profile> inputTableDesc = new RemoteTableDescriptor<>("profile-table-1");
      inputTableDesc
          .withReadFunction(InMemoryReadFunction.getInMemoryReadFunction(testName, profiles))
          .withRateLimiter(readRateLimiter, creditFunction, null);
      if (batchRead) {
        inputTableDesc.withBatchProvider(new CompactBatchProvider().withMaxBatchSize(batchSize).withMaxBatchDelay(Duration.ofHours(1)));
      }

      // dummy reader
      TableReadFunction readFn = new MyReadFunction();

      RemoteTableDescriptor<Integer, EnrichedPageView> outputTableDesc = new RemoteTableDescriptor<>("enriched-page-view-table-1");
      outputTableDesc
          .withReadFunction(readFn)
          .withWriteFunction(writer)
          .withRateLimiter(writeRateLimiter, creditFunction, creditFunction);
      if (batchWrite) {
        outputTableDesc.withBatchProvider(new CompactBatchProvider().withMaxBatchSize(batchSize).withMaxBatchDelay(Duration.ofHours(1)));
      }

      Table<KV<Integer, EnrichedPageView>> outputTable = appDesc.getTable(outputTableDesc);

      Table<KV<Integer, Profile>> inputTable = appDesc.getTable(inputTableDesc);

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

    if (batchRead) {
      Assert.assertEquals(numExpected / batchSize, batchReads.get(testName).get());
    }
    if (batchWrite) {
      Assert.assertEquals(numExpected / batchSize, batchWrites.get(testName).get());
    }
  }

  @Test
  public void testStreamTableJoinRemoteTableBatchingReadWrite() throws Exception {
    doTestStreamTableJoinRemoteTable("testStreamTableJoinRemoteTableBatchingReadWrite", true, true);
  }

  @Test
  public void testStreamTableJoinRemoteTableBatchingRead() throws Exception {
    doTestStreamTableJoinRemoteTable("testStreamTableJoinRemoteTableBatchingRead", true, false);
  }

  @Test
  public void testStreamTableJoinRemoteTableBatchingWrite() throws Exception {
    doTestStreamTableJoinRemoteTable("testStreamTableJoinRemoteTableBatchingWrite", false, true);
  }
}
