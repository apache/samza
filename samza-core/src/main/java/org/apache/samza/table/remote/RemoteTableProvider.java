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

import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.table.batching.BatchProvider;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.retry.TableRetryPolicy;
import org.apache.samza.table.BaseTableProvider;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.util.RateLimiter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provide for remote table instances
 */
public class RemoteTableProvider extends BaseTableProvider {

  private final List<RemoteTable<?, ?, ?>> tables = new ArrayList<>();

  /**
   * Map of tableId -> executor service for async table IO and callbacks. The same executors
   * are shared by both read/write operations such that tables of the same tableId all share
   * the set same of executors globally whereas table itself is per-task.
   */
  private static Map<String, ExecutorService> rateLimitingExecutors = new ConcurrentHashMap<>();
  private static Map<String, ExecutorService> callbackExecutors = new ConcurrentHashMap<>();
  private static Map<String, ScheduledExecutorService> batchExecutors = new ConcurrentHashMap<>();
  private static ScheduledExecutorService retryExecutor;

  public RemoteTableProvider(String tableId) {
    super(tableId);
  }

  @Override
  public ReadWriteUpdateTable getTable() {

    Preconditions.checkNotNull(context, String.format("Table %s not initialized", tableId));

    JavaTableConfig tableConfig = new JavaTableConfig(context.getJobContext().getConfig());

    // Read part
    TableReadFunction readFn = deserializeObject(tableConfig, RemoteTableDescriptor.READ_FN);
    RateLimiter rateLimiter = deserializeObject(tableConfig, RemoteTableDescriptor.RATE_LIMITER);
    if (rateLimiter != null) {
      rateLimiter.init(this.context);
    }

    TableRateLimiter.CreditFunction<?, ?> readCreditFn = deserializeObject(tableConfig, RemoteTableDescriptor.READ_CREDIT_FN);
    TableRateLimiter readRateLimiter = rateLimiter != null && rateLimiter.getSupportedTags().contains(RemoteTableDescriptor.RL_READ_TAG)
        ? new TableRateLimiter(tableId, rateLimiter, readCreditFn, RemoteTableDescriptor.RL_READ_TAG)
        : null;
    TableRetryPolicy readRetryPolicy = deserializeObject(tableConfig, RemoteTableDescriptor.READ_RETRY_POLICY);

    // Write part
    // Reuse write rate limiter for update
    TableRateLimiter writeRateLimiter = null;
    TableRetryPolicy writeRetryPolicy = null;
    TableWriteFunction writeFn = deserializeObject(tableConfig, RemoteTableDescriptor.WRITE_FN);
    if (writeFn != null) {
      TableRateLimiter.CreditFunction<?, ?> writeCreditFn = deserializeObject(tableConfig, RemoteTableDescriptor.WRITE_CREDIT_FN);
      writeRateLimiter = rateLimiter != null && rateLimiter.getSupportedTags().contains(RemoteTableDescriptor.RL_WRITE_TAG)
          ? new TableRateLimiter(tableId, rateLimiter, writeCreditFn, RemoteTableDescriptor.RL_WRITE_TAG)
          : null;
      writeRetryPolicy = deserializeObject(tableConfig, RemoteTableDescriptor.WRITE_RETRY_POLICY);
    }

    if (readRetryPolicy != null || writeRetryPolicy != null) {
      retryExecutor = createRetryExecutor();
    }

    // Optional executor for future callback/completion. Shared by both read and write operations.
    int callbackPoolSize = Integer.parseInt(tableConfig.getForTable(tableId, RemoteTableDescriptor.ASYNC_CALLBACK_POOL_SIZE, "-1"));
    if (callbackPoolSize > 0) {
      callbackExecutors.computeIfAbsent(tableId, (arg) ->
          Executors.newFixedThreadPool(callbackPoolSize, (runnable) -> {
            Thread thread = new Thread(runnable);
            thread.setName("table-" + tableId + "-async-callback-pool");
            thread.setDaemon(true);
            return thread;
          }));
    }

    boolean isRateLimited = readRateLimiter != null || writeRateLimiter != null;
    if (isRateLimited) {
      rateLimitingExecutors.computeIfAbsent(tableId, (arg) ->
          Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("table-" + tableId + "-async-executor");
            thread.setDaemon(true);
            return thread;
          }));
    }

    BatchProvider batchProvider = deserializeObject(tableConfig, RemoteTableDescriptor.BATCH_PROVIDER);
    if (batchProvider != null) {
      batchExecutors.computeIfAbsent(tableId, (arg) ->
          Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("table-" + tableId + "-batch-scheduled-executor");
            thread.setDaemon(true);
            return thread;
          }));
    }

    RemoteTable table = new RemoteTable(tableId,
        readFn, writeFn,
        readRateLimiter, writeRateLimiter, writeRateLimiter, rateLimitingExecutors.get(tableId),
        readRetryPolicy, writeRetryPolicy, retryExecutor, batchProvider, batchExecutors.get(tableId),
        callbackExecutors.get(tableId));
    table.init(this.context);
    tables.add(table);
    return table;
  }

  @Override
  public void close() {
    super.close();
    tables.forEach(t -> t.close());
    rateLimitingExecutors.values().forEach(e -> e.shutdown());
    rateLimitingExecutors.clear();
    callbackExecutors.values().forEach(e -> e.shutdown());
    callbackExecutors.clear();
    batchExecutors.values().forEach(e -> e.shutdown());
    batchExecutors.clear();
  }

  private <T> T deserializeObject(JavaTableConfig tableConfig, String key) {
    String entry = tableConfig.getForTable(tableId, key, "");
    if (entry.isEmpty()) {
      return null;
    }
    return SerdeUtils.deserialize(key, entry);
  }

  private ScheduledExecutorService createRetryExecutor() {
    return Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName("table-retry-executor");
      thread.setDaemon(true);
      return thread;
    });
  }
}
