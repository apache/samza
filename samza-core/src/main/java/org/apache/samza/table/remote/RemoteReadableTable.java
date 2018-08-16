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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.utils.DefaultTableReadMetrics;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * A Samza {@link org.apache.samza.table.Table} backed by a remote data-store or service.
 * <p>
 * Many stream-processing applications require to look-up data from remote data sources eg: databases,
 * web-services, RPC systems to process messages in the stream. Such access to adjunct datasets can be
 * naturally modeled as a join between the incoming stream and a {@link RemoteReadableTable}.
 * <p>
 * Example use-cases include:
 * <ul>
 *  <li> Augmenting a stream of "page-views" with information from a database of user-profiles; </li>
 *  <li> Scoring page views with impressions services. </li>
 *  <li> A notifications-system that sends out emails may require a query to an external database to process its message. </li>
 * </ul>
 * <p>
 * A {@link RemoteReadableTable} is meant to be used with a {@link TableReadFunction} and a {@link TableWriteFunction}
 * which encapsulate the functionality of reading and writing data to the remote service. These provide a
 * pluggable means to specify I/O operations on the table. While the base implementation merely delegates to
 * these reader and writer functions, sub-classes of {@link RemoteReadableTable} may provide rich functionality like
 * caching or throttling on top of them.
 *
 * For async IO methods, requests are dispatched by a single-threaded executor after invoking the rateLimiter.
 * Optionally, an executor can be specified for invoking the future callbacks which otherwise are
 * executed on the threads of the underlying native data store client. This could be useful when
 * application might execute long-running operations upon future completions; another use case is to increase
 * throughput with more parallelism in the callback executions.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RemoteReadableTable<K, V> implements ReadableTable<K, V> {

  protected final String tableId;
  protected final Logger logger;

  protected final ExecutorService callbackExecutor;
  protected final ExecutorService tableExecutor;

  private final TableReadFunction<K, V> readFn;
  private DefaultTableReadMetrics readMetrics;

  @VisibleForTesting
  final TableRateLimiter<K, V> readRateLimiter;

  /**
   * Construct a RemoteReadableTable instance
   * @param tableId table id
   * @param readFn {@link TableReadFunction} for read operations
   * @param rateLimiter helper for rate limiting
   * @param tableExecutor executor for issuing async requests
   * @param callbackExecutor executor for invoking async callbacks
   */
  public RemoteReadableTable(String tableId, TableReadFunction<K, V> readFn,
      TableRateLimiter<K, V> rateLimiter, ExecutorService tableExecutor, ExecutorService callbackExecutor) {
    Preconditions.checkArgument(tableId != null && !tableId.isEmpty(), "invalid table id");
    Preconditions.checkNotNull(readFn, "null read function");
    this.tableId = tableId;
    this.readFn = readFn;
    this.readRateLimiter = rateLimiter;
    this.callbackExecutor = callbackExecutor;
    this.tableExecutor = tableExecutor;
    this.logger = LoggerFactory.getLogger(getClass().getName() + "-" + tableId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    readMetrics = new DefaultTableReadMetrics(containerContext, taskContext, this, tableId);
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
    readRateLimiter.setTimerMetric(tableMetricsUtil.newTimer("get-throttle-ns"));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(K key) {
    try {
      return getAsync(key).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<V> getAsync(K key) {
    Preconditions.checkNotNull(key);
    readMetrics.numGets.inc();
    return execute(readRateLimiter, key, readFn::getAsync, readMetrics.getNs)
        .exceptionally(e -> {
            throw new SamzaException("Failed to get the record for " + key, e);
          });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<K, V> getAll(List<K> keys) {
    readMetrics.numGetAlls.inc();
    try {
      return getAllAsync(keys).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys) {
    Preconditions.checkNotNull(keys);
    if (keys.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.EMPTY_MAP);
    }
    readMetrics.numGetAlls.inc();
    return execute(readRateLimiter, keys, readFn::getAllAsync, readMetrics.getAllNs)
        .handle((result, e) -> {
            if (e != null) {
              throw new SamzaException("Failed to get the records for " + keys, e);
            }
            return result;
          });
  }

  /**
   * Execute an async request given a table key
   * @param key key of the table record
   * @param method method to be executed
   * @param timer latency metric to be updated
   * @param <T> return type
   * @return CompletableFuture of the operation
   */
  protected <T> CompletableFuture<T> execute(TableRateLimiter<K, V> rateLimiter,
      K key, Function<K, CompletableFuture<T>> method, Timer timer) {
    final long startNs = System.nanoTime();
    CompletableFuture<T> ioFuture = rateLimiter.isRateLimited() ?
        CompletableFuture
            .runAsync(() -> rateLimiter.throttle(key), tableExecutor)
            .thenCompose((r) -> method.apply(key)) :
        method.apply(key);
    if (callbackExecutor != null) {
      ioFuture.thenApplyAsync(r -> {
          timer.update(System.nanoTime() - startNs);
          return r;
        }, callbackExecutor);
    } else {
      ioFuture.thenApply(r -> {
          timer.update(System.nanoTime() - startNs);
          return r;
        });
    }
    return ioFuture;
  }

  /**
   * Execute an async request given a table record (key+value)
   * @param key key of the table record
   * @param value value of the table record
   * @param method method to be executed
   * @param timer latency metric to be updated
   * @return CompletableFuture of the operation
   */
  protected CompletableFuture<Void> execute(TableRateLimiter<K, V> rateLimiter,
      K key, V value, BiFunction<K, V, CompletableFuture<Void>> method, Timer timer) {
    final long startNs = System.nanoTime();
    CompletableFuture<Void> ioFuture = rateLimiter.isRateLimited() ?
        CompletableFuture
            .runAsync(() -> rateLimiter.throttle(key, value), tableExecutor)
            .thenCompose((r) -> method.apply(key, value)) :
        method.apply(key, value);
    if (callbackExecutor != null) {
      ioFuture.thenApplyAsync(r -> {
          timer.update(System.nanoTime() - startNs);
          return r;
        }, callbackExecutor);
    } else {
      ioFuture.thenApply(r -> {
          timer.update(System.nanoTime() - startNs);
          return r;
        });
    }
    return ioFuture;
  }

  /**
   * Execute an async request given a collection of table keys
   * @param keys collection of keys
   * @param method method to be executed
   * @param timer latency metric to be updated
   * @return CompletableFuture of the operation
   */
  protected <T> CompletableFuture<T> execute(TableRateLimiter<K, V> rateLimiter,
      Collection<K> keys, Function<Collection<K>, CompletableFuture<T>> method, Timer timer) {
    final long startNs = System.nanoTime();
    CompletableFuture<T> ioFuture = rateLimiter.isRateLimited() ?
        CompletableFuture
            .runAsync(() -> rateLimiter.throttle(keys), tableExecutor)
            .thenCompose((r) -> method.apply(keys)) :
        method.apply(keys);
    if (callbackExecutor != null) {
      ioFuture.thenApplyAsync(r -> {
          timer.update(System.nanoTime() - startNs);
          return r;
        }, callbackExecutor);
    } else {
      ioFuture.thenApply(r -> {
          timer.update(System.nanoTime() - startNs);
          return r;
        });
    }
    return ioFuture;
  }

  /**
   * Execute an async request given a collection of table records
   * @param records list of records
   * @param method method to be executed
   * @param timer latency metric to be updated
   * @return CompletableFuture of the operation
   */
  protected CompletableFuture<Void> executeRecords(TableRateLimiter<K, V> rateLimiter,
      Collection<Entry<K, V>> records, Function<Collection<Entry<K, V>>, CompletableFuture<Void>> method, Timer timer) {
    final long startNs = System.nanoTime();
    CompletableFuture<Void> ioFuture;
    if (rateLimiter.isRateLimited()) {
      ioFuture = CompletableFuture
          .runAsync(() -> rateLimiter.throttleRecords(records), tableExecutor)
          .thenCompose((r) -> method.apply(records));
    } else {
      ioFuture = method.apply(records);
    }
    if (callbackExecutor != null) {
      ioFuture.thenApplyAsync(r -> {
          timer.update(System.nanoTime() - startNs);
          return r;
        }, callbackExecutor);
    } else {
      ioFuture.thenApply(r -> {
          timer.update(System.nanoTime() - startNs);
          return r;
        });
    }
    return ioFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    readFn.close();
  }
}
