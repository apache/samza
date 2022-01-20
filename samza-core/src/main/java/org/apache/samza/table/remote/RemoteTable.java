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

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.AsyncReadWriteUpdateTable;
import org.apache.samza.table.BaseReadWriteUpdateTable;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.table.RecordNotFoundException;
import org.apache.samza.table.batching.BatchProvider;
import org.apache.samza.table.batching.AsyncBatchingTable;
import org.apache.samza.table.ratelimit.AsyncRateLimitedTable;
import org.apache.samza.table.retry.AsyncRetriableTable;
import org.apache.samza.table.retry.TableRetryPolicy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.samza.table.utils.TableMetricsUtil.incCounter;
import static org.apache.samza.table.utils.TableMetricsUtil.updateTimer;


/**
 * A Samza {@link ReadWriteUpdateTable} backed by a remote data-store or service.
 * <p>
 * Many stream-processing applications require to look-up data from remote data sources eg: databases,
 * web-services, RPC systems to process messages in the stream. Such access to adjunct datasets can be
 * naturally modeled as a join between the incoming stream and a table.
 * <p>
 * Example use-cases include:
 * <ul>
 *  <li> Augmenting a stream of "page-views" with information from a database of user-profiles; </li>
 *  <li> Scoring page views with impressions services. </li>
 *  <li> A notifications-system that sends out emails may require a query to an external database to process its message. </li>
 * </ul>
 * <p>
 * A {@link RemoteTable} is meant to be used with a {@link TableReadFunction} and a {@link TableWriteFunction}
 * which encapsulate the functionality of reading and writing data to the remote service. These provide a
 * pluggable means to specify I/O operations on the table.
 *
 * For async IO methods, requests are dispatched by a single-threaded executor after invoking the rateLimiter.
 * Optionally, an executor can be specified for invoking the future callbacks which otherwise are
 * executed on the threads of the underlying native data store client. This could be useful when
 * application might execute long-running operations upon future completions; another use case is to increase
 * throughput with more parallelism in the callback executions.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <U> the type of the update in this table
 */
public final class RemoteTable<K, V, U> extends BaseReadWriteUpdateTable<K, V, U>
    implements ReadWriteUpdateTable<K, V, U>, AsyncReadWriteUpdateTable<K, V, U> {

  // Read/write functions
  protected final TableReadFunction<K, V> readFn;
  protected final TableWriteFunction<K, V, U> writeFn;
  // Rate limiting
  protected final TableRateLimiter<K, V> readRateLimiter;
  protected final TableRateLimiter<K, V> writeRateLimiter;
  protected final TableRateLimiter<K, U> updateRateLimiter;
  protected final ExecutorService rateLimitingExecutor;
  // Retries
  protected final TableRetryPolicy readRetryPolicy;
  protected final TableRetryPolicy writeRetryPolicy;
  protected final ScheduledExecutorService retryExecutor;
  // batch
  protected final BatchProvider<K, V, U> batchProvider;
  protected final ScheduledExecutorService batchExecutor;

  // Other
  protected final ExecutorService callbackExecutor;

  // The async table to delegate to
  protected final AsyncReadWriteUpdateTable<K, V, U> asyncTable;

  /**
   * Construct a RemoteTable instance
   * @param tableId table id
   * @param readFn {@link TableReadFunction} for read operations
   * @param writeFn {@link TableWriteFunction} for read operations
   * @param readRateLimiter helper for read rate limiting
   * @param writeRateLimiter helper for write rate limiting
   * @param rateLimitingExecutor executor for executing rate limiting
   * @param readRetryPolicy read retry policy
   * @param writeRetryPolicy write retry policy
   * @param retryExecutor executor for invoking retries
   * @param batchProvider batch provider to create a batch instance
   * @param batchExecutor scheduled executor for batch
   * @param callbackExecutor executor for invoking async callbacks
   */
  public RemoteTable(
      String tableId,
      TableReadFunction<K, V> readFn,
      TableWriteFunction<K, V, U> writeFn,
      TableRateLimiter<K, V> readRateLimiter,
      TableRateLimiter<K, V> writeRateLimiter,
      TableRateLimiter<K, U> updateRateLimiter,
      ExecutorService rateLimitingExecutor,
      TableRetryPolicy readRetryPolicy,
      TableRetryPolicy writeRetryPolicy,
      ScheduledExecutorService retryExecutor,
      BatchProvider<K, V, U> batchProvider,
      ScheduledExecutorService batchExecutor,
      ExecutorService callbackExecutor) {

    super(tableId);
    Preconditions.checkArgument(writeFn != null || readFn != null,
        "Must have one of TableReadFunction or TableWriteFunction");

    this.readFn = readFn;
    this.writeFn = writeFn;
    this.readRateLimiter = readRateLimiter;
    this.writeRateLimiter = writeRateLimiter;
    this.updateRateLimiter = updateRateLimiter;
    this.rateLimitingExecutor = rateLimitingExecutor;
    this.readRetryPolicy = readRetryPolicy;
    this.writeRetryPolicy = writeRetryPolicy;
    this.callbackExecutor = callbackExecutor;
    this.retryExecutor = retryExecutor;
    this.batchProvider = batchProvider;
    this.batchExecutor = batchExecutor;

    AsyncReadWriteUpdateTable table = new AsyncRemoteTable(readFn, writeFn);
    if (readRateLimiter != null || writeRateLimiter != null || updateRateLimiter != null) {
      table = new AsyncRateLimitedTable(tableId, table, readRateLimiter, writeRateLimiter, updateRateLimiter,
          rateLimitingExecutor);
    }
    if (readRetryPolicy != null || writeRetryPolicy != null) {
      table = new AsyncRetriableTable(tableId, table, readRetryPolicy, writeRetryPolicy, retryExecutor, readFn, writeFn);
    }
    if (batchProvider != null) {
      table = new AsyncBatchingTable(tableId, table, batchProvider, batchExecutor);
    }

    asyncTable = table;
  }

  @Override
  public V get(K key, Object ... args) {
    try {
      return getAsync(key, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<V> getAsync(K key, Object ... args) {
    Preconditions.checkNotNull(key, "null key");
    return instrument(() -> asyncTable.getAsync(key, args), metrics.numGets, metrics.getNs)
        .handle((result, e) -> {
          if (e != null) {
            throw new SamzaException("Failed to get the records for " + key, e);
          }
          if (result == null) {
            incCounter(metrics.numMissedLookups);
          }
          return result;
        });
  }

  @Override
  public Map<K, V> getAll(List<K> keys, Object ... args) {
    try {
      return getAllAsync(keys, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys, Object ... args) {
    Preconditions.checkNotNull(keys, "null keys");
    if (keys.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyMap());
    }
    return instrument(() -> asyncTable.getAllAsync(keys, args), metrics.numGetAlls, metrics.getAllNs)
        .handle((result, e) -> {
          if (e != null) {
            throw new SamzaException("Failed to get the records for " + keys, e);
          }
          result.values().stream().filter(Objects::isNull).forEach(v -> incCounter(metrics.numMissedLookups));
          return result;
        });
  }

  @Override
  public <T> T read(int opId, Object ... args) {
    try {
      return (T) readAsync(opId, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public <T> CompletableFuture<T> readAsync(int opId, Object ... args) {
    return instrument(() -> asyncTable.<T>readAsync(opId, args), metrics.numReads, metrics.readNs)
        .exceptionally(e -> {
          throw new SamzaException(String.format("Failed to read, opId=%d", opId), e);
        });
  }

  @Override
  public void put(K key, V value, Object ... args) {
    try {
      putAsync(key, value, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value, Object ... args) {
    Preconditions.checkNotNull(writeFn, "null write function");
    Preconditions.checkNotNull(key, "null key");
    if (value == null) {
      return deleteAsync(key, args);
    }

    return instrument(() -> asyncTable.putAsync(key, value, args), metrics.numPuts, metrics.putNs)
        .exceptionally(e -> {
          throw new SamzaException("Failed to put a record with key=" + key, e);
        });
  }

  @Override
  public void putAll(List<Entry<K, V>> entries, Object ... args) {
    try {
      putAllAsync(entries, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> records, Object ... args) {

    Preconditions.checkNotNull(writeFn, "null write function");
    Preconditions.checkNotNull(records, "null records");

    if (records.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    List<K> deleteKeys = records.stream()
        .filter(e -> e.getValue() == null).map(Entry::getKey).collect(Collectors.toList());
    List<Entry<K, V>> putRecords = records.stream()
        .filter(e -> e.getValue() != null).collect(Collectors.toList());

    CompletableFuture<Void> deleteFuture = deleteKeys.isEmpty()
        ? CompletableFuture.completedFuture(null)
        : deleteAllAsync(deleteKeys, args);

    // Return the combined future
    return CompletableFuture.allOf(
        deleteFuture,
        instrument(() -> asyncTable.putAllAsync(putRecords, args), metrics.numPutAlls, metrics.putAllNs))
        .exceptionally(e -> {
          String strKeys = records.stream().map(r -> r.getKey().toString()).collect(Collectors.joining(","));
          throw new SamzaException("Failed to put records with keys=" + strKeys, e);
        });
  }


  @Override
  public void update(K key, U update) {
    try {
      updateAsync(key, update).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public void updateAll(List<Entry<K, U>> updates) {
    try {
      updateAllAsync(updates).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> updateAsync(K key, U update) {
    Preconditions.checkNotNull(writeFn, "null write function");
    Preconditions.checkNotNull(key, "null key");
    Preconditions.checkNotNull(update, "null update");

    return instrument(() -> asyncTable.updateAsync(key, update), metrics.numUpdates,
        metrics.updateNs)
        .exceptionally(e -> {
          // rethrow RecordDoesNotExistException instead of wrapping it in SamzaException
          if (e.getCause() instanceof RecordNotFoundException) {
            throw (RecordNotFoundException) e.getCause();
          }
          throw new SamzaException("Failed to update a record with key=" + key, e);
        });
  }

  @Override
  public CompletableFuture<Void> updateAllAsync(List<Entry<K, U>> updates) {
    Preconditions.checkNotNull(writeFn, "null write function");
    Preconditions.checkNotNull(updates, "null records");

    if (updates.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return instrument(() -> asyncTable.updateAllAsync(updates), metrics.numUpdateAlls,
        metrics.updateAllNs)
        .exceptionally(e -> {
          String strKeys = updates.stream().map(r -> r.getKey().toString()).collect(Collectors.joining(","));
          throw new SamzaException("Failed to put records with keys=" + strKeys, e);
        });
  }

  @Override
  public void delete(K key, Object ... args) {
    try {
      deleteAsync(key, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key, Object ... args) {
    Preconditions.checkNotNull(writeFn, "null write function");
    Preconditions.checkNotNull(key, "null key");
    return instrument(() -> asyncTable.deleteAsync(key, args), metrics.numDeletes, metrics.deleteNs)
        .exceptionally(e -> {
          throw new SamzaException("Failed to delete the record for " + key, e);
        });
  }

  @Override
  public void deleteAll(List<K> keys, Object ... args) {
    try {
      deleteAllAsync(keys, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys, Object ... args) {
    Preconditions.checkNotNull(writeFn, "null write function");
    Preconditions.checkNotNull(keys, "null keys");
    if (keys.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    return instrument(() -> asyncTable.deleteAllAsync(keys, args), metrics.numDeleteAlls, metrics.deleteAllNs)
        .exceptionally(e -> {
          throw new SamzaException("Failed to delete records for " + keys, e);
        });
  }

  @Override
  public <T> T write(int opId, Object ... args) {
    try {
      return (T) writeAsync(opId, args).get();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public <T> CompletableFuture<T> writeAsync(int opId, Object... args) {
    return instrument(() -> asyncTable.<T>writeAsync(opId, args), metrics.numWrites, metrics.writeNs)
        .exceptionally(e -> {
          throw new SamzaException(String.format("Failed to write, opId=%d", opId), e);
        });
  }

  @Override
  public void init(Context context) {
    super.init(context);
    asyncTable.init(context);
    if (readFn != null) {
      readFn.init(context, this);
    }
    if (writeFn != null) {
      writeFn.init(context, this);
    }
  }

  @Override
  public void flush() {
    try {
      incCounter(metrics.numFlushes);
      long startNs = clock.nanoTime();
      asyncTable.flush();
      updateTimer(metrics.flushNs, clock.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = "Failed to flush remote store";
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public void close() {
    asyncTable.close();
  }

  public TableReadFunction<K, V> getReadFunction() {
    return readFn;
  }

  public TableWriteFunction<K, V, U> getWriteFunction() {
    return writeFn;
  }

  protected <T> CompletableFuture<T> instrument(Func1<T> func, Counter counter, Timer timer) {
    incCounter(counter);
    final long startNs = clock.nanoTime();
    CompletableFuture<T> ioFuture = func.apply();
    if (callbackExecutor != null) {
      ioFuture.thenApplyAsync(r -> {
        updateTimer(timer, clock.nanoTime() - startNs);
        return r;
      }, callbackExecutor);
    } else {
      ioFuture.thenApply(r -> {
        updateTimer(timer, clock.nanoTime() - startNs);
        return r;
      });
    }
    return ioFuture;
  }

  public BatchProvider<K, V, U> getBatchProvider() {
    return this.batchProvider;
  }
}
