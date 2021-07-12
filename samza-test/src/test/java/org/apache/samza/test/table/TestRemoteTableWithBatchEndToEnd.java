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
import org.apache.samza.operators.KV;
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
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.util.Base64Serializer;
import org.apache.samza.util.RateLimiter;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.test.table.TestTableData.*;
import static org.mockito.Mockito.*;


public class TestRemoteTableWithBatchEndToEnd {
  private static final Map<String, List<EnrichedPageView>> WRITTEN_RECORDS = new HashMap<>();
  private static final Map<String, AtomicInteger> BATCH_WRITES = new HashMap<>();
  private static final Map<String, AtomicInteger> BATCH_READS = new HashMap<>();

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
      batchReadCounter = BATCH_READS.get(testName);
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
      records = WRITTEN_RECORDS.get(testName);
      batchWritesCounter = BATCH_WRITES.get(testName);
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

    BATCH_READS.put(testName, new AtomicInteger());
    BATCH_WRITES.put(testName, new AtomicInteger());
    WRITTEN_RECORDS.put(testName, new CopyOnWriteArrayList<>());

    int count = 16;
    int batchSize = 4;
    String profiles = Base64Serializer.serialize(generateProfiles(count));

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

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<PageView> inputDescriptor = isd
        .getInputDescriptor("PageView", new NoOpSerde<>());
    TestRunner.of(app)
        .addInputStream(inputDescriptor, Arrays.asList(generatePageViewsWithDistinctKeys(count)))
        .addConfig("task.max.concurrency", String.valueOf(count))
        .addConfig("task.async.commit", String.valueOf(true))
        .run(Duration.ofSeconds(10));

    Assert.assertEquals(count, WRITTEN_RECORDS.get(testName).size());
    Assert.assertNotNull(WRITTEN_RECORDS.get(testName).get(0));

    if (batchRead) {
      Assert.assertEquals(count / batchSize, BATCH_READS.get(testName).get());
    }
    if (batchWrite) {
      Assert.assertEquals(count / batchSize, BATCH_WRITES.get(testName).get());
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
