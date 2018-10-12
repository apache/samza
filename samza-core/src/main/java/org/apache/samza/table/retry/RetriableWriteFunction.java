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

package org.apache.samza.table.retry;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import org.apache.samza.SamzaException;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.utils.TableMetricsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import net.jodah.failsafe.RetryPolicy;

import static org.apache.samza.table.retry.FailsafeAdapter.failsafe;


/**
 * Wrapper for a {@link TableWriteFunction} instance to add common retry
 * support with a {@link TableRetryPolicy}. This wrapper is created by
 * {@link org.apache.samza.table.descriptors.remote.RemoteTableProvider} when a retry
 * policy is specified together with the {@link TableWriteFunction}.
 *
 * Actual retry mechanism is provided by the failsafe library. Retry is
 * attempted in an async way with a {@link ScheduledExecutorService}.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RetriableWriteFunction<K, V> implements TableWriteFunction<K, V> {
  private final RetryPolicy retryPolicy;
  private final TableWriteFunction<K, V> writeFn;
  private final ScheduledExecutorService retryExecutor;

  @VisibleForTesting
  RetryMetrics retryMetrics;

  public RetriableWriteFunction(TableRetryPolicy policy, TableWriteFunction<K, V> writeFn,
      ScheduledExecutorService retryExecutor)  {
    Preconditions.checkNotNull(policy);
    Preconditions.checkNotNull(writeFn);
    Preconditions.checkNotNull(retryExecutor);

    this.writeFn = writeFn;
    this.retryExecutor = retryExecutor;
    Predicate<Throwable> retryPredicate = policy.getRetryPredicate();
    policy.withRetryPredicate((ex) -> writeFn.isRetriable(ex) || retryPredicate.test(ex));
    this.retryPolicy = FailsafeAdapter.valueOf(policy);
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V record) {
    return failsafe(retryPolicy, retryMetrics, retryExecutor)
        .future(() -> writeFn.putAsync(key, record))
        .exceptionally(e -> {
            throw new SamzaException("Failed to get the record for " + key + " after retries.", e);
          });
  }

  @Override
  public CompletableFuture<Void> putAllAsync(Collection<Entry<K, V>> records) {
    return failsafe(retryPolicy, retryMetrics, retryExecutor)
        .future(() -> writeFn.putAllAsync(records))
        .exceptionally(e -> {
            throw new SamzaException("Failed to put records after retries.", e);
          });
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key) {
    return failsafe(retryPolicy, retryMetrics, retryExecutor)
        .future(() -> writeFn.deleteAsync(key))
        .exceptionally(e -> {
            throw new SamzaException("Failed to delete the record for " + key + " after retries.", e);
          });
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(Collection<K> keys) {
    return failsafe(retryPolicy, retryMetrics, retryExecutor)
        .future(() -> writeFn.deleteAllAsync(keys))
        .exceptionally(e -> {
            throw new SamzaException("Failed to delete the records for " + keys + " after retries.", e);
          });
  }

  @Override
  public boolean isRetriable(Throwable exception) {
    return writeFn.isRetriable(exception);
  }

  /**
   * Initialize retry-related metrics.
   * @param metricsUtil metrics util
   */
  public void setMetrics(TableMetricsUtil metricsUtil) {
    this.retryMetrics = new RetryMetrics("writer", metricsUtil);
  }
}
