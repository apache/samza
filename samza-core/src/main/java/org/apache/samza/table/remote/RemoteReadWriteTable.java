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

package org.apache.samza.table.remote;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.utils.TableMetricsUtil;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.samza.table.utils.TableMetricsUtil.incCounter;
import static org.apache.samza.table.utils.TableMetricsUtil.updateTimer;


/**
 * Remote store backed read writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RemoteReadWriteTable<K, V> extends RemoteReadableTable<K, V>
    implements ReadWriteTable<K, V> {

  protected final TableWriteFunction<K, V> writeFn;
  protected final TableRateLimiter writeRateLimiter;

  public RemoteReadWriteTable(String tableId, TableReadFunction readFn, TableWriteFunction writeFn,
      TableRateLimiter<K, V> readRateLimiter, TableRateLimiter<K, V> writeRateLimiter,
      ExecutorService tableExecutor, ExecutorService callbackExecutor) {
    super(tableId, readFn, readRateLimiter, tableExecutor, callbackExecutor);
    Preconditions.checkNotNull(writeFn, "null write function");
    this.writeFn = writeFn;
    this.writeRateLimiter = writeRateLimiter;
  }

  @Override
  public void init(Context context) {
    super.init(context);
    MetricsConfig metricsConfig = new MetricsConfig(context.getJobContext().getConfig());
    if (metricsConfig.getMetricsTimerEnabled()) {
      TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(context, this, tableId);
      writeRateLimiter.setTimerMetric(tableMetricsUtil.newTimer("put-throttle-ns"));
    }
  }

  @Override
  public void put(K key, V value) {
    try {
      putAsync(key, value).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value) {
    Preconditions.checkNotNull(key);
    if (value == null) {
      return deleteAsync(key);
    }

    return execute(writeRateLimiter, key, value, writeFn::putAsync, writeMetrics.numPuts, writeMetrics.putNs)
        .exceptionally(e -> {
            throw new SamzaException("Failed to put a record with key=" + key, (Throwable) e);
          });
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    try {
      putAllAsync(entries).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> records) {
    Preconditions.checkNotNull(records);
    if (records.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    List<K> deleteKeys = records.stream()
        .filter(e -> e.getValue() == null).map(Entry::getKey).collect(Collectors.toList());

    CompletableFuture<Void> deleteFuture = deleteKeys.isEmpty()
        ? CompletableFuture.completedFuture(null) : deleteAllAsync(deleteKeys);

    List<Entry<K, V>> putRecords = records.stream()
        .filter(e -> e.getValue() != null).collect(Collectors.toList());

    // Return the combined future
    return CompletableFuture.allOf(
        deleteFuture,
        executeRecords(writeRateLimiter, putRecords, writeFn::putAllAsync, writeMetrics.numPutAlls, writeMetrics.putAllNs))
        .exceptionally(e -> {
            String strKeys = records.stream().map(r -> r.getKey().toString()).collect(Collectors.joining(","));
            throw new SamzaException(String.format("Failed to put records with keys=" + strKeys), e);
          });
  }

  @Override
  public void delete(K key) {
    try {
      deleteAsync(key).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key) {
    Preconditions.checkNotNull(key);
    return execute(writeRateLimiter, key, writeFn::deleteAsync, writeMetrics.numDeletes, writeMetrics.deleteNs)
        .exceptionally(e -> {
            throw new SamzaException(String.format("Failed to delete the record for " + key), (Throwable) e);
          });
  }

  @Override
  public void deleteAll(List<K> keys) {
    try {
      deleteAllAsync(keys).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys) {
    Preconditions.checkNotNull(keys);
    if (keys.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    return execute(writeRateLimiter, keys, writeFn::deleteAllAsync, writeMetrics.numDeleteAlls, writeMetrics.deleteAllNs)
        .exceptionally(e -> {
            throw new SamzaException(String.format("Failed to delete records for " + keys), (Throwable) e);
          });
  }

  @Override
  public void flush() {
    try {
      incCounter(writeMetrics.numFlushes);
      long startNs = System.nanoTime();
      writeFn.flush();
      updateTimer(writeMetrics.flushNs, System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = "Failed to flush remote store";
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public void close() {
    writeFn.close();
    super.close();
  }

  /**
   * Execute an async request given a table record (key+value)
   * @param rateLimiter helper for rate limiting
   * @param key key of the table record
   * @param value value of the table record
   * @param method method to be executed
   * @param timer latency metric to be updated
   * @return CompletableFuture of the operation
   */
  protected CompletableFuture<Void> execute(TableRateLimiter<K, V> rateLimiter,
      K key, V value, BiFunction<K, V, CompletableFuture<Void>> method, Counter counter, Timer timer) {
    incCounter(counter);
    final long startNs = System.nanoTime();
    CompletableFuture<Void> ioFuture = rateLimiter.isRateLimited()
        ? CompletableFuture
            .runAsync(() -> rateLimiter.throttle(key, value), tableExecutor)
            .thenCompose((r) -> method.apply(key, value))
        : method.apply(key, value);
    return completeExecution(ioFuture, startNs, timer);
  }

  /**
   * Execute an async request given a collection of table records
   * @param rateLimiter helper for rate limiting
   * @param records list of records
   * @param method method to be executed
   * @param timer latency metric to be updated
   * @return CompletableFuture of the operation
   */
  protected CompletableFuture<Void> executeRecords(TableRateLimiter<K, V> rateLimiter,
      Collection<Entry<K, V>> records, Function<Collection<Entry<K, V>>, CompletableFuture<Void>> method,
      Counter counter, Timer timer) {
    incCounter(counter);
    final long startNs = System.nanoTime();
    CompletableFuture<Void> ioFuture = rateLimiter.isRateLimited()
        ? CompletableFuture
            .runAsync(() -> rateLimiter.throttleRecords(records), tableExecutor)
            .thenCompose((r) -> method.apply(records))
        : method.apply(records);
    return completeExecution(ioFuture, startNs, timer);
  }

  @VisibleForTesting
  public TableWriteFunction<K, V> getWriteFn() {
    return writeFn;
  }

  @VisibleForTesting
  public TableRateLimiter getWriteRateLimiter() {
    return writeRateLimiter;
  }
}
