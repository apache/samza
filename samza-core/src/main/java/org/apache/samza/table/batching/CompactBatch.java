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

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * A newer put/delete operation will override the value of an earlier one.
 * Consecutive query operations with the same key will be combined as a single query.
 * Updates are not supported in CompactBatch.
 *
 * @param <K> The type of the key associated with the {@link Operation}
 * @param <V> The type of the value associated with the {@link Operation}
 * @param <U> The type of the update associated with the {@link Operation}
 */
class CompactBatch<K, V, U> extends AbstractBatch<K, V, U> {
  private final Map<K, Operation<K, V, U>> putsDeletes = new LinkedHashMap<>();
  private final Map<K, Operation<K, V, U>> queries = new LinkedHashMap<>();

  public CompactBatch(int maxBatchSize, Duration maxBatchDelay) {
    super(maxBatchSize, maxBatchDelay);
  }

  /**
   * @return The batch size.
   */
  @Override
  public int size() {
    return putsDeletes.size() + queries.size();
  }

  /**
   * When adding a GetOperation to the batch, mark the GetOperation completed immediately if there is
   * an PutOperation/DeleteOperation for the key, otherwise, adding the operation to a list.
   * When adding a Put/DeleteOperation, if there is a put/delete operation for the same key, the existing
   * operation will be replaced by the new one. If an update operation is received, throw a BatchingNotSupportedException
   * exception.
   */
  @Override
  public CompletableFuture<Void> addOperation(Operation<K, V, U> operation) {
    Preconditions.checkNotNull(operation);

    if (operation.getArgs() != null && operation.getArgs().length > 0) {
      throw BATCH_NOT_SUPPORTED_EXCEPTION;
    }

    if (operation instanceof GetOperation) {
      queries.putIfAbsent(operation.getKey(), operation);
    } else if (operation instanceof UpdateOperation) {
      throw new BatchingNotSupportedException("Batching not supported for Updates with CompactBatches");
    } else {
      putsDeletes.put(operation.getKey(), operation);
    }
    if (size() >= maxBatchSize) {
      close();
    }
    return completableFuture;
  }

  @Override
  public Collection<Operation<K, V, U>> getOperations() {
    return Stream.of(queries.values(), putsDeletes.values()).flatMap(Collection::stream).collect(Collectors.toList());
  }
}
