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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.samza.SamzaException;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.utils.TableMetricsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import net.jodah.failsafe.RetryPolicy;

import static org.apache.samza.table.retry.FailsafeAdapter.failsafe;


/**
 * Wrapper for a {@link TableReadFunction} instance to add common retry
 * support with a {@link TableRetryPolicy}. This wrapper is created by
 * {@link org.apache.samza.table.remote.RemoteTableProvider} when a retry
 * policy is specified together with the {@link TableReadFunction}.
 *
 * Actual retry mechanism is provided by the failsafe library. Retry is
 * attempted in an async way with a {@link ScheduledExecutorService}.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RetriableReadFunction<K, V> implements TableReadFunction<K, V> {
  private final RetryPolicy retryPolicy;
  private final TableReadFunction<K, V> readFn;
  private final ScheduledExecutorService retryExecutor;

  @VisibleForTesting
  RetryMetrics retryMetrics;

  public RetriableReadFunction(TableRetryPolicy policy, TableReadFunction<K, V> readFn,
      ScheduledExecutorService retryExecutor) {
    Preconditions.checkNotNull(policy);
    Preconditions.checkNotNull(readFn);
    Preconditions.checkNotNull(retryExecutor);

    this.readFn = readFn;
    this.retryExecutor = retryExecutor;
    this.retryPolicy = FailsafeAdapter.valueOf((ex) -> readFn.isRetriable(ex), policy);
  }

  @Override
  public CompletableFuture<V> getAsync(K key) {
    return failsafe(retryPolicy, retryMetrics, retryExecutor)
        .future(k -> readFn.getAsync(key))
        .exceptionally(e -> {
            throw new SamzaException("Failed to get the record for " + key + " after retries.", e);
          });
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(Collection<K> keys) {
    return failsafe(retryPolicy, retryMetrics, retryExecutor)
        .future(k -> readFn.getAllAsync(keys))
        .exceptionally(e -> {
            throw new SamzaException("Failed to get the records for " + keys + " after retries.", e);
          });
  }

  @Override
  public boolean isRetriable(Throwable exception) {
    return readFn.isRetriable(exception);
  }

  /**
   * Initialize retry-related metrics
   * @param metricsUtil metrics util
   */
  public void setMetrics(TableMetricsUtil metricsUtil) {
    this.retryMetrics = new RetryMetrics("reader", metricsUtil);
  }
}
