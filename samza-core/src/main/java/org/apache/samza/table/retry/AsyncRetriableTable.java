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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import net.jodah.failsafe.RetryPolicy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.AsyncReadWriteTable;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.utils.TableMetricsUtil;

import static org.apache.samza.table.BaseReadWriteTable.Func1;
import static org.apache.samza.table.retry.FailsafeAdapter.failsafe;


/**
 * A composable asynchronous retriable table implementation that supports features
 * defined in {@link TableRetryPolicy}.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class AsyncRetriableTable<K, V> implements AsyncReadWriteTable<K, V> {

  private final String tableId;
  private final AsyncReadWriteTable<K, V> table;
  private final RetryPolicy readRetryPolicy;
  private final RetryPolicy writeRetryPolicy;
  private final ScheduledExecutorService retryExecutor;

  @VisibleForTesting
  RetryMetrics readRetryMetrics;
  @VisibleForTesting
  RetryMetrics writeRetryMetrics;

  public AsyncRetriableTable(String tableId, AsyncReadWriteTable<K, V> table,
      TableRetryPolicy readRetryPolicy, TableRetryPolicy writeRetryPolicy, ScheduledExecutorService retryExecutor,
      TableReadFunction readFn, TableWriteFunction writeFn) {

    Preconditions.checkNotNull(tableId, "null tableId");
    Preconditions.checkNotNull(table, "null table");
    Preconditions.checkNotNull(retryExecutor, "null retryExecutor");
    Preconditions.checkArgument(readRetryPolicy != null || writeRetryPolicy != null,
        "both readRetryPolicy and writeRetryPolicy are null");

    this.tableId = tableId;
    this.table = table;
    this.retryExecutor = retryExecutor;

    if (readRetryPolicy != null && readFn != null) {
      Predicate<Throwable> readRetryPredicate = readRetryPolicy.getRetryPredicate();
      readRetryPolicy.withRetryPredicate((ex) -> readFn.isRetriable(ex) || readRetryPredicate.test(ex));
      this.readRetryPolicy = FailsafeAdapter.valueOf(readRetryPolicy);
    } else {
      this.readRetryPolicy = null;
    }

    if (writeRetryPolicy != null && writeFn != null) {
      Predicate<Throwable> writeRetryPredicate = writeRetryPolicy.getRetryPredicate();
      writeRetryPolicy.withRetryPredicate((ex) -> writeFn.isRetriable(ex) || writeRetryPredicate.test(ex));
      this.writeRetryPolicy = FailsafeAdapter.valueOf(writeRetryPolicy);
    } else {
      this.writeRetryPolicy = null;
    }
  }

  @Override
  public CompletableFuture<V> getAsync(K key, Object... args) {
    return doRead(() -> table.getAsync(key, args));
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys, Object ... args) {
    return doRead(() -> table.getAllAsync(keys, args));
  }

  @Override
  public <T> CompletableFuture<T> readAsync(int opId, Object... args) {
    return doRead(() -> table.readAsync(opId, args));
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value, Object... args) {
    return doWrite(() -> table.putAsync(key, value, args));
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries, Object ... args) {
    return doWrite(() -> table.putAllAsync(entries, args));
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key, Object... args) {
    return doWrite(() -> table.deleteAsync(key, args));
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys, Object ... args) {
    return doWrite(() -> table.deleteAllAsync(keys, args));
  }

  @Override
  public <T> CompletableFuture<T> writeAsync(int opId, Object... args) {
    return doWrite(() -> table.writeAsync(opId, args));
  }

  @Override
  public void init(Context context) {
    table.init(context);
    TableMetricsUtil metricsUtil = new TableMetricsUtil(context, this, tableId);
    if (readRetryPolicy != null) {
      readRetryMetrics = new RetryMetrics("reader", metricsUtil);
    }
    if (writeRetryPolicy != null) {
      writeRetryMetrics = new RetryMetrics("writer", metricsUtil);
    }
  }

  @Override
  public void flush() {
    table.flush();
  }

  @Override
  public void close() {
    table.close();
  }

  private <T> CompletableFuture<T> doRead(Func1<T> func) {
    return readRetryPolicy != null
        ? failsafe(readRetryPolicy, readRetryMetrics, retryExecutor).getStageAsync(() ->  func.apply())
        : func.apply();
  }

  private <T> CompletableFuture<T> doWrite(Func1<T> func) {
    return writeRetryPolicy != null
        ? failsafe(writeRetryPolicy, writeRetryMetrics, retryExecutor).getStageAsync(() -> func.apply())
        : func.apply();
  }
}
