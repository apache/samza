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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.AsyncReadWriteUpdateTable;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.util.HighResolutionClock;


/**
 * A wrapper of a {@link AsyncReadWriteUpdateTable} that supports batch operations.
 *
 * This batching table does not guarantee any ordering of different operation types within the batch.
 * For instance, query(Q) and put/delete(u) operations arrives in the following sequences, Q1, U1, Q2, U2,
 * it does not mean the remote data store will receive the messages in the same order. Instead,
 * the operations will be grouped by type and sent via micro batches. For this sequence, Q1 and Q2 will
 * be grouped to micro batch B1; U1 and U2 will be grouped to micro batch B2, the implementation class
 * can decide the order of the micro batches.
 *
 * Synchronized table operations (get/put/update/delete) should be used with caution for the batching feature.
 * If the table is used by a single thread, there will be at most one operation in the batch, and the
 * batch will be performed when the TTL of the batch window expires. Batching does not make sense in this scenario.
 *
 * The Batch implementation class can throw {@link BatchingNotSupportedException} if it thinks the operation is
 * not batch-able. When receiving this exception, {@link AsyncBatchingTable} will send the operation to the
 * {@link AsyncReadWriteUpdateTable}.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 * @param <U> the type of the update applied to this table
 */
public class AsyncBatchingTable<K, V, U> implements AsyncReadWriteUpdateTable<K, V, U> {
  private final AsyncReadWriteUpdateTable<K, V, U> table;
  private final String tableId;
  private final BatchProvider<K, V, U> batchProvider;
  private final ScheduledExecutorService batchTimerExecutorService;
  private BatchProcessor<K, V, U> batchProcessor;

  /**
   * @param tableId The id of the table.
   * @param table The target table that serves the batch operations.
   * @param batchProvider Batch provider to create a batch instance.
   * @param batchTimerExecutorService Executor service for batch timer.
   */
  public AsyncBatchingTable(String tableId, AsyncReadWriteUpdateTable<K, V, U> table, BatchProvider<K, V, U> batchProvider,
      ScheduledExecutorService batchTimerExecutorService) {
    Preconditions.checkNotNull(tableId);
    Preconditions.checkNotNull(table);
    Preconditions.checkNotNull(batchProvider);
    Preconditions.checkNotNull(batchTimerExecutorService);

    this.tableId = tableId;
    this.table = table;
    this.batchProvider = batchProvider;
    this.batchTimerExecutorService = batchTimerExecutorService;
  }

  @Override
  public CompletableFuture<V> getAsync(K key, Object... args) {
    try {
      return batchProcessor.processQueryOperation(new GetOperation<>(key, args));
    } catch (BatchingNotSupportedException e) {
      return table.getAsync(key, args);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys, Object... args) {
    return table.getAllAsync(keys);
  }

  @Override
  public <T> CompletableFuture<T> readAsync(int opId, Object ... args) {
    return table.readAsync(opId, args);
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value, Object... args) {
    try {
      return batchProcessor.processPutDeleteOrUpdateOperations(new PutOperation<>(key, value, args));
    } catch (BatchingNotSupportedException e) {
      return table.putAsync(key, value, args);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries, Object... args) {
    return table.putAllAsync(entries);
  }

  @Override
  public CompletableFuture<Void> updateAsync(K key, U update) {
    try {
      return batchProcessor.processPutDeleteOrUpdateOperations(new UpdateOperation<>(key, update));
    } catch (BatchingNotSupportedException e) {
      return table.updateAsync(key, update);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> updateAllAsync(List<Entry<K, U>> updates) {
    return table.updateAllAsync(updates);
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key, Object... args) {
    try {
      return batchProcessor.processPutDeleteOrUpdateOperations(new DeleteOperation<>(key, args));
    } catch (BatchingNotSupportedException e) {
      return table.deleteAsync(key, args);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys, Object... args) {
    return table.deleteAllAsync(keys);
  }

  @Override
  public void init(Context context) {
    table.init(context);
    final TableMetricsUtil metricsUtil = new TableMetricsUtil(context, this, tableId);

    createBatchProcessor(TableMetricsUtil.mayCreateHighResolutionClock(context.getJobContext().getConfig()),
        new BatchMetrics(metricsUtil));
  }

  @Override
  public <T> CompletableFuture<T> writeAsync(int opId, Object ... args) {
    return table.writeAsync(opId, args);
  }

  @Override
  public void flush() {
    table.flush();
  }

  @Override
  public void close() {
    batchProcessor.stop();
    table.close();
  }

  @VisibleForTesting
  void createBatchProcessor(HighResolutionClock clock, BatchMetrics batchMetrics) {
    batchProcessor = new BatchProcessor<>(batchMetrics, new TableBatchHandler<>(table),
        batchProvider, clock, batchTimerExecutorService);
  }

  @VisibleForTesting
  BatchProcessor<K, V, U> getBatchProcessor() {
    return batchProcessor;
  }
}
