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
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.samza.table.ReadWriteTable;
import org.junit.Assert;
import org.junit.Test;

import static java.lang.Thread.*;
import static org.mockito.Mockito.*;

public class TestBatchProcessor {
  private static final int SLOW_OPERATION_TIME_MS = 500;
  private static final Supplier<Void> SLOW_UPDATE_SUPPLIER = () -> {
    try {
      sleep(SLOW_OPERATION_TIME_MS);
    } catch (InterruptedException e) {
      // ignore
    }
    return null;
  };

  public static class TestCreate {
    @Test
    public void testCreate() {
      final ReadWriteTable<Integer, Integer> table = mock(ReadWriteTable.class);
      final BatchProcessor<Integer, Integer> batchProcessor = createBatchProcessor(table,
          3, Integer.MAX_VALUE);

      // The batch processor initially has no operation.
      Assert.assertEquals(0, batchProcessor.size());
      batchProcessor.processUpdateOperation(new PutOperation<>(1, 1));
      // The batch processor now has one operation.
      Assert.assertEquals(1, batchProcessor.size());
    }
  }

  public static class TestUpdatesAndLookup {
    @Test
    public void testUpdateAndLookup() {
      final ReadWriteTable<Integer, Integer> table = mock(ReadWriteTable.class);
      final BatchProcessor<Integer, Integer> batchProcessor =
          createBatchProcessor(table, Integer.MAX_VALUE, Integer.MAX_VALUE);

      int numberOfPuts = 10;
      for (int i = 0; i < numberOfPuts; i++) {
        batchProcessor.processUpdateOperation(new PutOperation<>(i, i));
      }

      // verify that the number of addBatch operations is correct.
      Assert.assertEquals(numberOfPuts, batchProcessor.size());

      // Verify that the value is correct for each key.
      for (int i = 0; i < numberOfPuts; i++) {
        final Operation<Integer, Integer> operation = batchProcessor.getLastUpdate(i);
        Assert.assertEquals(i, operation.getKey().intValue());
        Assert.assertEquals(i, operation.getValue().intValue());
      }
    }
  }

  public static class TestBatchTriggered {
    @Test
    public void testBatchOperationTriggeredByBatchSize() {
      final int maxBatchSize = 3;

      final ReadWriteTable<Integer, Integer> table = mock(ReadWriteTable.class);
      when(table.putAllAsync(anyList())).thenReturn(CompletableFuture.supplyAsync(SLOW_UPDATE_SUPPLIER));

      final BatchProcessor<Integer, Integer> batchProcessor =
          createBatchProcessor(table, maxBatchSize, Integer.MAX_VALUE);

      List<CompletableFuture<Void>> futureList = new ArrayList<>();
      // One batch will be created and sent to the remote table after the for-loop.
      for (int i = 0; i < maxBatchSize; i++) {
        futureList.add(batchProcessor.processUpdateOperation(new PutOperation<>(i, i)));
      }

      for (int i = 0; i < maxBatchSize; i++) {
        Assert.assertFalse(futureList.get(i).isDone());
      }
      Assert.assertEquals(0, batchProcessor.size());

      try {
        sleep(SLOW_OPERATION_TIME_MS * 2);
      } catch (InterruptedException e) {
        // ignore
      }

      for (int i = 0; i < maxBatchSize; i++) {
        Assert.assertTrue(futureList.get(i).isDone());
      }
    }

    @Test
    public void testBatchOperationTriggeredByTimer() {
      final int maxBatchDelayMs = 100;
      final int putOperationCount = 100;

      final ReadWriteTable<Integer, Integer> table = mock(ReadWriteTable.class);
      when(table.putAllAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
      when(table.deleteAllAsync(anyList())).thenReturn(CompletableFuture.completedFuture(null));

      final BatchProcessor<Integer, Integer> batchProcessor =
          createBatchProcessor(table, Integer.MAX_VALUE, maxBatchDelayMs);

      for (int i = 0; i < putOperationCount; i++) {
        batchProcessor.processUpdateOperation(new PutOperation<>(i, i));
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

  private static BatchProcessor<Integer, Integer> createBatchProcessor(ReadWriteTable<Integer, Integer> table,
      int maxSize, int maxDelay) {
    final BatchProvider<Integer, Integer> batchProvider = new CompactBatchProvider<Integer, Integer>()
        .withMaxBatchDelay(Duration.ofMillis(maxDelay)).withMaxBatchSize(maxSize);
    final BatchHandler<Integer, Integer> batchHandler = new TableBatchHandler<>(table);
    final BatchMetrics batchMetrics = mock(BatchMetrics.class);
    return new BatchProcessor<>(batchMetrics, batchHandler, batchProvider, () -> 0, Executors.newSingleThreadScheduledExecutor());
  }
}
