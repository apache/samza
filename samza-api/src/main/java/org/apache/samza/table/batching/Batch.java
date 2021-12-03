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
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Manages a sequence of {@link Operation}s, which will be performed as a batch.
 * A batch can be configured with a {@code maxBatchSize} and/or {@code maxBatchDelay}.
 * When the number of operations in the batch exceeds the {@code maxBatchSize}
 * or the time window exceeds the {@code maxBatchDelay}, the batch will be performed.
 *
 * @param <K> The type of the key associated with the {@link Operation}
 * @param <V> The type of the value associated with the {@link Operation}
 * @param <U> The type of the update associated with the {@link Operation}
 */
public interface Batch<K, V, U> {
  /**
   * Add an operation to the batch.
   *
   * @param operation The operation to be added.
   * @return A {@link CompletableFuture} that indicate the status of the batch.
   */
  CompletableFuture<Void> addOperation(Operation<K, V, U> operation);

  /**
   * Close the bach so that it will not accept more operations.
   */
  void close();

  /**
   * @return Whether the bach can accept more operations.
   */
  boolean isClosed();

  /**
   * @return The operations buffered by the batch.
   */
  Collection<Operation<K, V, U>> getOperations();

  /**
   * @return The batch max delay.
   */
  Duration getMaxBatchDelay();

  /**
   * @return The batch capacity
   */
  int getMaxBatchSize();

  /**
   * Change the batch status to be complete.
   */
  void complete();

  /**
   * Change the batch status to be complete with exception.
   */
  void completeExceptionally(Throwable throwable);

  /**
   * @return The number of operations in the batch.
   */
  int size();

  /**
   * @return True if the batch is empty.
   */
  default boolean isEmpty() {
    return size() == 0;
  }
}