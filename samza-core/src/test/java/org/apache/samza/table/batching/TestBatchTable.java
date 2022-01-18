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

package org.apache.samza.table.batching;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

public class TestBatchTable {
  private static final int BATCH_SIZE = 5;
  private static final Duration BATCH_DELAY = Duration.ofMillis(Integer.MAX_VALUE);

  private AsyncBatchingTable<Integer, Integer, Integer> asyncBatchingTable;
  private ReadWriteUpdateTable<Integer, Integer, Integer> table;
  private Map<Integer, Integer> tableDb;

  @Before
  public void setup() {
    final Answer getAnswer = invocation -> {
      Integer key = invocation.getArgumentAt(0, Integer.class);
      return tableDb.get(key);
    };

    final Answer getAsyncAnswer = invocation -> {
      Integer key = invocation.getArgumentAt(0, Integer.class);
      return CompletableFuture.completedFuture(tableDb.get(key));
    };

    final Answer getAllAsyncAnswer = invocation -> {
      final List<Integer> list = invocation.getArgumentAt(0, List.class);
      final Map<Integer, Integer> map = new HashMap<>();
      list.forEach(k -> map.put(k, tableDb.get(k)));
      return CompletableFuture.completedFuture(map);
    };

    final Answer putAnswer = invocation -> {
      Integer key = invocation.getArgumentAt(0, Integer.class);
      Integer value = invocation.getArgumentAt(1, Integer.class);
      tableDb.put(key, value);
      return null;
    };

    final Answer putAsyncAnswer = invocation -> {
      final Integer key = invocation.getArgumentAt(0, Integer.class);
      final Integer value = invocation.getArgumentAt(1, Integer.class);
      tableDb.put(key, value);
      return CompletableFuture.completedFuture(null);
    };

    final Answer putAllAsyncAnswer = invocation -> {
      final List<Entry<Integer, Integer>> list = invocation.getArgumentAt(0, List.class);
      list.forEach(entry -> tableDb.put(entry.getKey(), entry.getValue()));
      return CompletableFuture.completedFuture(null);
    };

    final Answer deleteAnswer = invocation -> {
      final Integer key = invocation.getArgumentAt(0, Integer.class);
      tableDb.remove(key);
      return null;
    };

    final Answer deleteAsyncAnswer = invocation -> {
      final Integer key = invocation.getArgumentAt(0, Integer.class);
      tableDb.remove(key);
      return CompletableFuture.completedFuture(null);
    };

    final Answer deleteAllAsyncAnswer = invocation -> {
      final List<Integer> list = invocation.getArgumentAt(0, List.class);
      list.forEach(k -> tableDb.remove(k));
      return CompletableFuture.completedFuture(null);
    };

    table = mock(ReadWriteUpdateTable.class);
    final BatchMetrics batchMetrics = mock(BatchMetrics.class);
    tableDb = new HashMap<>();
    asyncBatchingTable = new AsyncBatchingTable("id", table, new CompactBatchProvider()
        .withMaxBatchSize(BATCH_SIZE)
        .withMaxBatchDelay(BATCH_DELAY), Executors.newSingleThreadScheduledExecutor());
    asyncBatchingTable.createBatchProcessor(() -> 0, mock(BatchMetrics.class));

    doAnswer(putAnswer).when(table).put(anyInt(), anyInt());
    doAnswer(putAsyncAnswer).when(table).putAsync(anyInt(), anyInt());
    doAnswer(putAllAsyncAnswer).when(table).putAllAsync(anyList());

    doAnswer(i -> null).when(table).update(anyInt(), anyInt());
    doAnswer(i -> CompletableFuture.completedFuture(null)).when(table).updateAsync(anyInt(), anyInt());
    doAnswer(i -> CompletableFuture.completedFuture(null)).when(table).updateAllAsync(anyList());

    doAnswer(deleteAnswer).when(table).delete(anyInt());
    doAnswer(deleteAsyncAnswer).when(table).deleteAsync(anyInt());
    doAnswer(deleteAllAsyncAnswer).when(table).deleteAllAsync(anyList());

    doAnswer(getAnswer).when(table).get(anyInt());
    doAnswer(getAsyncAnswer).when(table).getAsync(anyInt());
    doAnswer(getAllAsyncAnswer).when(table).getAllAsync(anyList());
  }

  @After
  public void tearDown() {
    asyncBatchingTable.close();
  }

  @Test
  public void testPutAsync() {
    final List<CompletableFuture<Void>> futures = new LinkedList<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      futures.add(asyncBatchingTable.putAsync(i, i));
    }
    sleep();

    final BatchProcessor<Integer, Integer, Integer> batchProcessor = asyncBatchingTable.getBatchProcessor();

    // Verify that all async puts are finished.
    futures.forEach(future -> Assert.assertTrue(future.isDone()));
    verify(table, times(1)).putAllAsync(any());

    // There should be no operations in the batch processor.
    Assert.assertEquals(0, batchProcessor.size());

    asyncBatchingTable.putAsync(BATCH_SIZE, BATCH_SIZE);

    // Now batch size should be 1.
    Assert.assertEquals(1, batchProcessor.size());
  }

  @Test
  public void testPutAllAsync() {
    final List<Entry<Integer, Integer>> entries = new LinkedList<>();

    for (int i = 0; i < BATCH_SIZE; i++) {
      entries.add(new Entry<>(i, i));
    }

    CompletableFuture<Void> future = asyncBatchingTable.putAllAsync(entries);
    final BatchProcessor<Integer, Integer, Integer> batchProcessor = asyncBatchingTable.getBatchProcessor();

    sleep();

    // Verify that putAllAsync is finished.
    Assert.assertTrue(future.isDone());

    // There should be no pending operations.
    Assert.assertEquals(0, batchProcessor.size());

    // The addBatchUpdates batch operations propagates to the table.
    verify(table, times(1)).putAllAsync(anyList());

    // This new addBatchUpdates will make the batch size to be 1.
    asyncBatchingTable.putAsync(BATCH_SIZE, BATCH_SIZE);

    Assert.assertEquals(1, batchProcessor.size());
  }

  @Test
  public void testUpdateAsync() {
    // Use CompleteBatch instead of CompactBatch as CompactBatch doesn't support updates
    asyncBatchingTable = new AsyncBatchingTable("id", table, new CompleteBatchProvider()
        .withMaxBatchSize(BATCH_SIZE)
        .withMaxBatchDelay(BATCH_DELAY), Executors.newSingleThreadScheduledExecutor());
    asyncBatchingTable.createBatchProcessor(() -> 0, mock(BatchMetrics.class));

    // mocking of updateAsync and updateAllAsync of AsyncReadWriteTable table done in setup method
    final List<CompletableFuture<Void>> futures = new LinkedList<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      futures.add(asyncBatchingTable.updateAsync(i, i));
    }
    sleep();

    final BatchProcessor<Integer, Integer, Integer> batchProcessor =
        asyncBatchingTable.getBatchProcessor();

    // Verify that all async updates are finished.
    futures.forEach(future -> Assert.assertTrue(future.isDone()));
    verify(table, times(1)).updateAllAsync(anyList());

    // There should be no operations in the batch processor.
    Assert.assertEquals(0, batchProcessor.size());

    asyncBatchingTable.updateAsync(1, 1);
    asyncBatchingTable.updateAsync(2, 2);

    // Now batch size should be 2.
    Assert.assertEquals(2, batchProcessor.size());
  }

  @Test
  public void testUpdateAllAsync() {
    // Use CompleteBatch instead of CompactBatch as CompactBatch doesn't support updates
    asyncBatchingTable = new AsyncBatchingTable("id", table, new CompleteBatchProvider()
        .withMaxBatchSize(BATCH_SIZE)
        .withMaxBatchDelay(BATCH_DELAY), Executors.newSingleThreadScheduledExecutor());
    asyncBatchingTable.createBatchProcessor(() -> 0, mock(BatchMetrics.class));

    // mocking of updateAsync and updateAllAsync of AsyncReadWriteTable table done in setup method
    final List<Entry<Integer, Integer>> updates = new LinkedList<>();

    for (int i = 0; i < BATCH_SIZE; i++) {
      updates.add(new Entry<>(i, i));
    }

    CompletableFuture<Void> future = asyncBatchingTable.updateAllAsync(updates);
    final BatchProcessor<Integer, Integer, Integer> batchProcessor = asyncBatchingTable.getBatchProcessor();

    sleep();

    // Verify that updateAllAsync is finished.
    Assert.assertTrue(future.isDone());

    Assert.assertEquals(0, batchProcessor.size());

    // The addBatchUpdates batch operations propagates to the table.
    verify(table, times(1)).updateAllAsync(anyList());

    // This new addBatchUpdates will make the batch size to be 1.
    asyncBatchingTable.updateAsync(BATCH_SIZE, BATCH_SIZE);

    Assert.assertEquals(1, batchProcessor.size());
  }

  @Test
  public void testGetAsync() throws ExecutionException, InterruptedException {
    for (int i = 0; i < BATCH_SIZE; i++) {
      asyncBatchingTable.putAsync(i, i);
    }
    sleep();

    final List<CompletableFuture<Integer>> futures = new ArrayList<>(BATCH_SIZE);
    for (int i = 0; i < BATCH_SIZE; i++) {
      futures.add(asyncBatchingTable.getAsync(i));
    }
    sleep();

    for (Integer i = 0; i < BATCH_SIZE; i++) {
      Assert.assertTrue(futures.get(i).isDone());
      Assert.assertEquals(i, futures.get(i).get());
    }
    verify(table, times(1)).getAllAsync(anyList());
  }

  @Test
  public void testGetAllAsync() throws ExecutionException, InterruptedException {
    for (int i = 0; i < BATCH_SIZE; i++) {
      asyncBatchingTable.putAsync(i, i);
    }
    sleep();

    final List<Integer> keys = new LinkedList<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      keys.add(new Integer(i));
    }

    CompletableFuture<Map<Integer, Integer>> future = asyncBatchingTable.getAllAsync(keys);
    sleep();

    Assert.assertTrue(future.isDone());
    Assert.assertEquals(BATCH_SIZE, future.get().size());

    verify(table, times(1)).getAllAsync(anyList());
  }

  @Test
  public void testDeleteAsync() throws Exception {
    for (int i = 0; i < BATCH_SIZE; i++) {
      asyncBatchingTable.putAsync(i, i);
    }
    sleep();

    // The 1st batch is done.
    verify(table, times(1)).putAllAsync(anyList());

    final List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      completableFutures.add(asyncBatchingTable.deleteAsync(i));
    }
    sleep();

    for (int i = 0; i < BATCH_SIZE; i++) {
      Assert.assertEquals(null, completableFutures.get(i).get());
    }
  }

  @Test
  public void testDeleteAllAsync() throws Exception {
    for (int i = 0; i < BATCH_SIZE; i++) {
      asyncBatchingTable.putAsync(i, i);
    }
    sleep();

    final List<Integer> keys = new LinkedList<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      keys.add(new Integer(i));
    }

    final CompletableFuture<Void> future = asyncBatchingTable.deleteAllAsync(keys);
    sleep();
    Assert.assertTrue(future.isDone());

    final CompletableFuture<Map<Integer, Integer>> getAllFuture = asyncBatchingTable.getAllAsync(keys);
    sleep();

    Assert.assertTrue(getAllFuture.isDone());
    getAllFuture.get().forEach((k, v) -> Assert.assertEquals(null, v));
  }

  private void sleep() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      // ignore
    }
  }
}
