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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.utils.DefaultTableWriteMetrics;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * Remote store backed read writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RemoteReadWriteTable<K, V> extends RemoteReadableTable<K, V> implements ReadWriteTable<K, V> {
  private final TableWriteFunction<K, V> writeFn;

  private DefaultTableWriteMetrics writeMetrics;

  @VisibleForTesting
  final TableRateLimiter writeRateLimiter;

  public RemoteReadWriteTable(String tableId, TableReadFunction readFn, TableWriteFunction writeFn,
      TableRateLimiter<K, V> readRateLimiter, TableRateLimiter<K, V> writeRateLimiter,
      ExecutorService tableExecutor, ExecutorService callbackExecutor) {
    super(tableId, readFn, readRateLimiter, tableExecutor, callbackExecutor);
    Preconditions.checkNotNull(writeFn, "null write function");
    this.writeFn = writeFn;
    this.writeRateLimiter = writeRateLimiter;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    super.init(containerContext, taskContext);
    writeMetrics = new DefaultTableWriteMetrics(containerContext, taskContext, this, tableId);
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
    writeRateLimiter.setTimerMetric(tableMetricsUtil.newTimer("put-throttle-ns"));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(K key, V value) {
    try {
      putAsync(key, value).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> putAsync(K key, V value) {
    Preconditions.checkNotNull(key);
    if (value == null) {
      return deleteAsync(key);
    }

    writeMetrics.numPuts.inc();
    return execute(writeRateLimiter, key, value, writeFn::putAsync, writeMetrics.putNs)
        .exceptionally(e -> {
            throw new SamzaException("Failed to put a record with key=" + key, (Throwable) e);
          });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putAll(List<Entry<K, V>> entries) {
    try {
      putAllAsync(entries).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> records) {
    Preconditions.checkNotNull(records);
    if (records.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    writeMetrics.numPutAlls.inc();

    List<K> deleteKeys = records.stream()
        .filter(e -> e.getValue() == null).map(Entry::getKey).collect(Collectors.toList());

    CompletableFuture<Void> deleteFuture = deleteKeys.isEmpty()
        ? CompletableFuture.completedFuture(null) : deleteAllAsync(deleteKeys);

    List<Entry<K, V>> putRecords = records.stream()
        .filter(e -> e.getValue() != null).collect(Collectors.toList());

    // Return the combined future
    return CompletableFuture.allOf(
        deleteFuture,
        executeRecords(writeRateLimiter, putRecords, writeFn::putAllAsync, writeMetrics.putAllNs))
        .exceptionally(e -> {
            String strKeys = records.stream().map(r -> r.getKey().toString()).collect(Collectors.joining(","));
            throw new SamzaException(String.format("Failed to put records with keys=" + strKeys), e);
          });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(K key) {
    try {
      deleteAsync(key).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> deleteAsync(K key) {
    Preconditions.checkNotNull(key);
    writeMetrics.numDeletes.inc();
    return execute(writeRateLimiter, key, writeFn::deleteAsync, writeMetrics.deleteNs)
        .exceptionally(e -> {
            throw new SamzaException(String.format("Failed to delete the record for " + key), (Throwable) e);
          });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteAll(List<K> keys) {
    try {
      deleteAllAsync(keys).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys) {
    Preconditions.checkNotNull(keys);
    if (keys.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    writeMetrics.numDeleteAlls.inc();
    return execute(writeRateLimiter, keys, writeFn::deleteAllAsync, writeMetrics.deleteAllNs)
        .exceptionally(e -> {
            throw new SamzaException(String.format("Failed to delete records for " + keys), (Throwable) e);
          });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    try {
      writeMetrics.numFlushes.inc();
      long startNs = System.nanoTime();
      writeFn.flush();
      writeMetrics.flushNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = "Failed to flush remote store";
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    writeFn.close();
    super.close();
  }
}
