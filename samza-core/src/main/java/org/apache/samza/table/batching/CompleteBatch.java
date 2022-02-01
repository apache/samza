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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * The complete operation sequences are stored in the batch, regardless of whether the keys are the same or not.
 *
 * @param <K> The type of the key associated with the {@link Operation}
 * @param <V> The type of the value associated with the {@link Operation}
 * @param <U> The type of the update associated with the {@link Operation}
 */
class CompleteBatch<K, V, U> extends AbstractBatch<K, V, U> {
  private final List<Operation<K, V, U>> operations = new ArrayList<>();

  public CompleteBatch(int maxBatchSize, Duration maxBatchDelay) {
    super(maxBatchSize, maxBatchDelay);
  }

  /**
   * @return The batch size.
   */
  @Override
  public int size() {
    return operations.size();
  }

  /**
   * All operations will be buffered without any compaction.
   */
  @Override
  public CompletableFuture<Void> addOperation(Operation<K, V, U> operation) {
    Preconditions.checkNotNull(operation);
    operations.add(operation);
    if (size() >= maxBatchSize) {
      close();
    }
    return completableFuture;
  }

  @Override
  public Collection<Operation<K, V, U>> getOperations() {
    return operations;
  }
}
