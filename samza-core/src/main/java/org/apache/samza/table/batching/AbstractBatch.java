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
import java.util.concurrent.CompletableFuture;


abstract class AbstractBatch<K, V, U> implements Batch<K, V, U> {
  protected final int maxBatchSize;
  protected final Duration maxBatchDelay;
  protected final CompletableFuture<Void> completableFuture;
  protected boolean closed = false;
  protected static final BatchingNotSupportedException BATCH_NOT_SUPPORTED_EXCEPTION = new BatchingNotSupportedException();

  AbstractBatch(int maxBatchSize, Duration maxBatchDelay) {
    // The max number of {@link Operation}s that the batch can hold.
    this.maxBatchSize = maxBatchSize;
    // The max time that the batch can last before being performed.
    this.maxBatchDelay = maxBatchDelay;
    completableFuture = new CompletableFuture<>();
  }

  /**
   * @return The batch capacity.
   */
  @Override
  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  /**
   * @return The batch max delay.
   */
  @Override
  public Duration getMaxBatchDelay() {
    return maxBatchDelay;
  }

  @Override
  public void complete() {
    completableFuture.complete(null);
  }

  @Override
  public void completeExceptionally(Throwable throwable) {
    completableFuture.completeExceptionally(throwable);
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}
