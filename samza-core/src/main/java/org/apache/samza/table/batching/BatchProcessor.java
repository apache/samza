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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.samza.util.HighResolutionClock;


/**
 * Place a sequence of operations into a {@link Batch}. When a batch is not allowed to accept more
 * operations, it will handled by a {@link BatchHandler}. Meanwhile, a new {@link Batch} will be
 * created and a timer will be set for it.
 *
 * @param <K> The type of the key associated with the {@link Operation}
 * @param <V> The type of the value associated with the {@link Operation}
 */
public class BatchProcessor<K, V, U> {
  private final ScheduledExecutorService scheduledExecutorService;
  private final ReentrantLock lock = new ReentrantLock();
  private final BatchHandler<K, V, U> batchHandler;
  private final BatchProvider<K, V, U> batchProvider;
  private final BatchMetrics batchMetrics;
  private final HighResolutionClock clock;
  private Batch<K, V, U> batch;
  private ScheduledFuture<?> scheduledFuture;
  private long batchOpenTimestamp;

  /**
   * @param batchMetrics Batch metrics.
   * @param batchHandler Defines how each batch will be processed.
   * @param batchProvider The batch provider to create a batch instance.
   * @param clock A clock used to get the timestamp.
   * @param scheduledExecutorService A scheduled executor service to set timers for the managed batches.
   */
  public BatchProcessor(BatchMetrics batchMetrics, BatchHandler<K, V, U> batchHandler,
      BatchProvider<K, V, U> batchProvider, HighResolutionClock clock, ScheduledExecutorService scheduledExecutorService) {
    Preconditions.checkNotNull(batchHandler);
    Preconditions.checkNotNull(batchProvider);
    Preconditions.checkNotNull(clock);
    Preconditions.checkNotNull(scheduledExecutorService);

    this.batchHandler = batchHandler;
    this.batchProvider = batchProvider;
    this.scheduledExecutorService = scheduledExecutorService;
    this.batchMetrics = batchMetrics;
    this.clock = clock;
  }

  private CompletableFuture<Void> addOperation(Operation<K, V, U> operation) {
    if (batch == null) {
      startNewBatch();
    }
    final CompletableFuture<Void> res = batch.addOperation(operation);
    if (batch.isClosed()) {
      processBatch(true);
    }
    return res;
  }

  /**
   * @param operation The query operation to be added to the batch.
   * @return A {@link CompletableFuture} to indicate whether the operation is finished.
   */
  CompletableFuture<V> processQueryOperation(Operation<K, V, U> operation) {
    Preconditions.checkNotNull(operation);
    Preconditions.checkArgument(operation instanceof GetOperation);

    lock.lock();
    try {
      GetOperation<K, V, U> getOperation = (GetOperation<K, V, U>) operation;
      addOperation(getOperation);
      return getOperation.getCompletableFuture();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param operation The Put/Delete/Update operation to be added to the batch.
   * @return A {@link CompletableFuture} to indicate whether the operation is finished.
   */
  CompletableFuture<Void> processPutDeleteOrUpdateOperations(Operation<K, V, U> operation) {
    Preconditions.checkNotNull(operation);
    Preconditions.checkArgument(operation instanceof PutOperation
        || operation instanceof DeleteOperation
        || operation instanceof UpdateOperation);

    lock.lock();
    try {
      return addOperation(operation);
    } finally {
      lock.unlock();
    }
  }

  private void processBatch(boolean cancelTimer) {
    mayCancelTimer(cancelTimer);
    closeBatch();
    batchHandler.handle(batch);
    startNewBatch();
  }

  private void startNewBatch() {
    batch = batchProvider.getBatch();
    batchOpenTimestamp = clock.nanoTime();
    batchMetrics.incBatchCount();
    setBatchTimer(batch);
  }

  private void closeBatch() {
    batch.close();
    batchMetrics.updateBatchDuration(clock.nanoTime() - batchOpenTimestamp);
  }

  private void mayCancelTimer(boolean cancelTimer) {
    if (cancelTimer && scheduledFuture != null) {
      scheduledFuture.cancel(true);
    }
  }

  /**
   * Set a timer to close the batch when the batch is older than the max delay.
   */
  private void setBatchTimer(Batch<K, V, U> batch) {
    final long maxDelay = batch.getMaxBatchDelay().toMillis();
    if (maxDelay != Integer.MAX_VALUE) {
      scheduledFuture = scheduledExecutorService.schedule(() -> {
        lock.lock();
        try {
          processBatch(false);
        } finally {
          lock.unlock();
        }
      }, maxDelay, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Stop the processor and cancel all the pending futures.
   */
  void stop() {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true);
    }
  }

  /**
   * Get the current number of operations received.
   */
  @VisibleForTesting
  int size() {
    return batch == null ? 0 : batch.size();
  }

  /**
   * Get the latest Put/Update/Delete operation for the specified key.
   */
  @VisibleForTesting
  Operation<K, V, U> getLatestPutUpdateOrDelete(K key) {
    final Collection<Operation<K, V, U>> operations = batch.getOperations();
    final Iterator<Operation<K, V, U>> iterator = operations.iterator();
    Operation<K, V, U> lastUpdate = null;
    while (iterator.hasNext()) {
      final Operation<K, V, U> operation = iterator.next();
      if ((operation instanceof PutOperation || operation instanceof DeleteOperation ||
          operation instanceof UpdateOperation) && operation.getKey().equals(key)) {
        lastUpdate = operation;
      }
    }
    return lastUpdate;
  }
}
