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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.google.common.base.Preconditions;
import java.util.stream.Collectors;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.AsyncReadWriteTable;


/**
 * Defines how table performs batch operations.
 *
 * @param <K> The key type of the operation.
 * @param <V> The value type of the operation.
 */
public class TableBatchHandler<K, V> implements BatchHandler<K, V> {
  private final AsyncReadWriteTable<K, V> table;

  public TableBatchHandler(AsyncReadWriteTable<K, V> table) {
    Preconditions.checkNotNull(table);

    this.table = table;
  }

  /**
   * Define how batch get should be done.
   *
   * @param operations The batch get operations.
   * @return A CompletableFuture to represent the status of the batch operation.
   */
  CompletableFuture<?> handleBatchGet(Collection<Operation<K, V>> operations) {
    Preconditions.checkNotNull(operations);

    final List<K> gets = new ArrayList<>(operations.size());
    operations.forEach(operation -> gets.add(operation.getKey()));
    final CompletableFuture<Map<K, V>> getsFuture = gets.isEmpty() ?
        CompletableFuture.completedFuture(null) : table.getAllAsync(gets);

    getsFuture.whenComplete((map, throwable) -> {
        operations.forEach(operation -> {
            GetOperation<K, V> getOperation = (GetOperation<K, V>) operation;
            if (throwable != null) {
              getOperation.completeExceptionally(throwable);
            } else {
              getOperation.complete(map.get(operation.getKey()));
            }
          });
      });
    return getsFuture;
  }

  /**
   * Define how batch put should be done.
   *
   * @param operations The batch get operations.
   * @return A CompletableFuture to represent the status of the batch operation.
   */
  CompletableFuture<?> handleBatchPut(Collection<Operation<K, V>> operations) {
    Preconditions.checkNotNull(operations);

    final List<Entry<K, V>> puts = operations.stream()
        .map(op -> new Entry<>(op.getKey(), op.getValue()))
        .collect(Collectors.toList());

    return puts.isEmpty() ? CompletableFuture.completedFuture(null) : table.putAllAsync(puts);
  }

  /**
   * Define how batch delete should be done.
   *
   * @param operations The batch get operations.
   * @return A CompletableFuture to represent the status of the batch operation.
   */
  CompletableFuture<?> handleBatchDelete(Collection<Operation<K, V>> operations) {
    Preconditions.checkNotNull(operations);

    final List<K> deletes = operations.stream()
        .map(op -> op.getKey())
        .collect(Collectors.toList());

    return deletes.isEmpty() ? CompletableFuture.completedFuture(null) : table.deleteAllAsync(deletes);
  }

  List<Operation<K, V>> getQueryOperations(Batch<K, V> batch) {
    return batch.getOperations().stream().filter(op -> op instanceof GetOperation)
        .collect(Collectors.toList());
  }

  List<Operation<K, V>> getPutOperations(Batch<K, V> batch) {
    return batch.getOperations().stream().filter(op -> op instanceof PutOperation)
        .collect(Collectors.toList());
  }

  List<Operation<K, V>> getDeleteOperations(Batch<K, V> batch) {
    return batch.getOperations().stream().filter(op -> op instanceof DeleteOperation)
        .collect(Collectors.toList());
  }

  /**
   * Perform batch operations.
   */
  @Override
  public CompletableFuture<Void> handle(Batch<K, V> batch) {
    return CompletableFuture.allOf(
        handleBatchPut(getPutOperations(batch)),
        handleBatchDelete(getDeleteOperations(batch)),
        handleBatchGet(getQueryOperations(batch)))
        .whenComplete((val, throwable) -> {
            if (throwable != null) {
              batch.completeExceptionally(throwable);
            } else {
              batch.complete();
            }
          });
  }
}
