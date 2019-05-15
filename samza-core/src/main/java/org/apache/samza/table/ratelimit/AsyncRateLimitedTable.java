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
package org.apache.samza.table.ratelimit;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.apache.samza.config.MetricsConfig;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.AsyncReadWriteTable;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.utils.TableMetricsUtil;

import static org.apache.samza.table.BaseReadWriteTable.Func0;
import static org.apache.samza.table.BaseReadWriteTable.Func1;

/**
 * A composable read and/or write rate limited asynchronous table implementation
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class AsyncRateLimitedTable<K, V> implements AsyncReadWriteTable<K, V> {

  private final String tableId;
  private final AsyncReadWriteTable<K, V> table;
  private final TableRateLimiter<K, V> readRateLimiter;
  private final TableRateLimiter<K, V> writeRateLimiter;
  private final ExecutorService rateLimitingExecutor;

  public AsyncRateLimitedTable(String tableId, AsyncReadWriteTable<K, V> table, TableRateLimiter<K, V> readRateLimiter,
      TableRateLimiter<K, V> writeRateLimiter, ExecutorService rateLimitingExecutor) {
    Preconditions.checkNotNull(tableId, "null tableId");
    Preconditions.checkNotNull(table, "null table");
    Preconditions.checkNotNull(rateLimitingExecutor, "null rateLimitingExecutor");
    Preconditions.checkArgument(readRateLimiter != null || writeRateLimiter != null,
        "both readRateLimiter and writeRateLimiter are null");
    this.tableId = tableId;
    this.table = table;
    this.readRateLimiter = readRateLimiter;
    this.writeRateLimiter = writeRateLimiter;
    this.rateLimitingExecutor = rateLimitingExecutor;
  }

  @Override
  public CompletableFuture<V> getAsync(K key, Object ... args) {
    return doRead(
        () -> readRateLimiter.throttle(key, args),
        () -> table.getAsync(key, args));
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys, Object ... args) {
    return doRead(
        () -> readRateLimiter.throttle(keys, args),
        () -> table.getAllAsync(keys, args));
  }

  @Override
  public <T> CompletableFuture<T> readAsync(int opId, Object ... args) {
    return doRead(
        () -> readRateLimiter.throttle(opId, args),
        () -> table.readAsync(opId, args));
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value, Object ... args) {
    return doWrite(
        () -> writeRateLimiter.throttle(key, value, args),
        () -> table.putAsync(key, value, args));
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries, Object ... args) {
    return doWrite(
        () -> writeRateLimiter.throttleRecords(entries),
        () -> table.putAllAsync(entries, args));
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key, Object ... args) {
    return doWrite(
        () -> writeRateLimiter.throttle(key, args),
        () -> table.deleteAsync(key, args));
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys, Object ... args) {
    return doWrite(
        () -> writeRateLimiter.throttle(keys, args),
        () -> table.deleteAllAsync(keys, args));
  }

  @Override
  public <T> CompletableFuture<T> writeAsync(int opId, Object ... args) {
    return doWrite(
        () -> writeRateLimiter.throttle(opId, args),
        () -> table.writeAsync(opId, args));
  }

  @Override
  public void init(Context context) {
    table.init(context);
    MetricsConfig metricsConfig = new MetricsConfig(context.getJobContext().getConfig());
    if (metricsConfig.getMetricsTimerEnabled()) {
      TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(context, this, tableId);
      if (isReadRateLimited()) {
        readRateLimiter.setTimerMetric(tableMetricsUtil.newTimer("get-throttle-ns"));
      }
      if (isWriteRateLimited()) {
        writeRateLimiter.setTimerMetric(tableMetricsUtil.newTimer("put-throttle-ns"));
      }
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

  private boolean isReadRateLimited() {
    return readRateLimiter != null;
  }

  private boolean isWriteRateLimited() {
    return writeRateLimiter != null;
  }

  private <T> CompletableFuture<T> doRead(Func0 throttleFunc, Func1<T> func) {
    return isReadRateLimited()
        ? CompletableFuture
            .runAsync(() -> throttleFunc.apply(), rateLimitingExecutor)
            .thenCompose((r) -> func.apply())
        : func.apply();
  }

  private <T> CompletableFuture<T> doWrite(Func0 throttleFunc, Func1<T> func) {
    return isWriteRateLimited()
        ? CompletableFuture
            .runAsync(() -> throttleFunc.apply(), rateLimitingExecutor)
            .thenCompose((r) -> func.apply())
        : func.apply();
  }

}
