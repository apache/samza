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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.junit.Assert;
import org.junit.Test;

import static java.lang.Thread.sleep;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestBatchProcessor {

  public static class TestCreate {
    @Test
    public void testCreate() {
      final ReadWriteUpdateTable<Integer, Integer, Integer> table = mock(ReadWriteUpdateTable.class);
      final BatchProcessor<Integer, Integer, Integer> batchProcessor = createBatchProcessor(table,
          3, Integer.MAX_VALUE);

      // The batch processor initially has no operation.
      Assert.assertEquals(0, batchProcessor.size());
      batchProcessor.processPutDeleteOrUpdateOperations(new PutOperation<>(1, 1));
      // The batch processor now has one operation.
      Assert.assertEquals(1, batchProcessor.size());
    }
  }

  public static class TestUpdatesAndLookup {
    @Test
    public void testUpdateAndLookup() {
      final ReadWriteUpdateTable<Integer, Integer, Integer> table = mock(ReadWriteUpdateTable.class);
      final BatchProcessor<Integer, Integer, Integer> batchProcessor =
          createBatchProcessor(table, Integer.MAX_VALUE, Integer.MAX_VALUE);

      int numberOfPuts = 10;
      for (int i = 0; i < numberOfPuts; i++) {
        batchProcessor.processPutDeleteOrUpdateOperations(new PutOperation<>(i, i));
      }

      // verify that the number of addBatch operations is correct.
      Assert.assertEquals(numberOfPuts, batchProcessor.size());

      // Verify that the value is correct for each key.
      for (int i = 0; i < numberOfPuts; i++) {
        final Operation<Integer, Integer, Integer> operation = batchProcessor.getLatestPutUpdateOrDelete(i);
        Assert.assertEquals(i, operation.getKey().intValue());
        Assert.assertEquals(i, operation.getValue().intValue());
      }
    }
  }

  public static class TestBatchTriggered {
    @Test
    public void testBatchOperationTriggeredByBatchSize() {
      final int maxBatchSize = 3;
      final CountDownLatch batchCompletionTriggerLatch = new CountDownLatch(1);
      final Supplier<Void> tableUpdateSupplier = () -> {
        try {
          batchCompletionTriggerLatch.await();
        } catch (InterruptedException e) {
          // ignore
        }
        return null;
      };

      final ReadWriteUpdateTable<Integer, Integer, Integer> table = mock(ReadWriteUpdateTable.class);
      when(table.putAllAsync(anyList())).thenReturn(CompletableFuture.supplyAsync(tableUpdateSupplier));

      final BatchProcessor<Integer, Integer, Integer> batchProcessor =
          createBatchProcessor(table, maxBatchSize, Integer.MAX_VALUE);

      List<CompletableFuture<Void>> futureList = new ArrayList<>();
      // One batch will be created and sent to the remote table after the for-loop.
      for (int i = 0; i < maxBatchSize; i++) {
        futureList.add(batchProcessor.processPutDeleteOrUpdateOperations(new PutOperation<>(i, i)));
      }

      for (int i = 0; i < maxBatchSize; i++) {
        Assert.assertFalse(futureList.get(i).isDone());
      }
      Assert.assertEquals(0, batchProcessor.size());

      // Complete the async call to the underlying table
      batchCompletionTriggerLatch.countDown();
      // The latch should eventually trigger completion to the future returned by the batch processor
      CompletableFuture
          .allOf(futureList.toArray(new CompletableFuture[0]))
          .join();
    }

    @Test
    public void testBatchOperationTriggeredByTimer() {
      final int maxBatchDelayMs = 100;
      final int putOperationCount = 100;

      final ReadWriteUpdateTable<Integer, Integer, Integer> table = mock(ReadWriteUpdateTable.class);
      when(table.putAllAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
      when(table.deleteAllAsync(anyList())).thenReturn(CompletableFuture.completedFuture(null));

      final BatchProcessor<Integer, Integer, Integer> batchProcessor =
          createBatchProcessor(table, Integer.MAX_VALUE, maxBatchDelayMs);

      for (int i = 0; i < putOperationCount; i++) {
        batchProcessor.processPutDeleteOrUpdateOperations(new PutOperation<>(i, i));
      }

      // There's one batch with infinite maximum size, it has 100ms maximum delay.
      Assert.assertEquals(putOperationCount, batchProcessor.size());

      try {
        sleep(maxBatchDelayMs * 2);
      } catch (InterruptedException e) {
        // ignore
      }

      // After the timer fired, a new batch will be created.
      Assert.assertEquals(0, batchProcessor.size());
    }
  }

  private static BatchProcessor<Integer, Integer, Integer> createBatchProcessor(
      ReadWriteUpdateTable<Integer, Integer, Integer> table,
      int maxSize, int maxDelay) {
    final BatchProvider<Integer, Integer, Integer> batchProvider = new CompactBatchProvider<Integer, Integer, Integer>()
        .withMaxBatchDelay(Duration.ofMillis(maxDelay)).withMaxBatchSize(maxSize);
    final BatchHandler<Integer, Integer, Integer> batchHandler = new TableBatchHandler<>(table);
    final BatchMetrics batchMetrics = mock(BatchMetrics.class);
    return new BatchProcessor<>(batchMetrics, batchHandler, batchProvider, () -> 0, Executors.newSingleThreadScheduledExecutor());
  }
}
