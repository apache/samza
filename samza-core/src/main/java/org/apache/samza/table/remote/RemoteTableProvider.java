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

import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.retry.RetriableReadFunction;
import org.apache.samza.table.retry.RetriableWriteFunction;
import org.apache.samza.table.retry.TableRetryPolicy;
import org.apache.samza.table.BaseTableProvider;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.table.utils.TableMetricsUtil;
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


  private final boolean readOnly;
  private final List<RemoteReadableTable<?, ?>> tables = new ArrayList<>();

  /**
   * Map of tableId -> executor service for async table IO and callbacks. The same executors
   * are shared by both read/write operations such that tables of the same tableId all share
   * the set same of executors globally whereas table itself is per-task.
   */
  private static Map<String, ExecutorService> tableExecutors = new ConcurrentHashMap<>();
  private static Map<String, ExecutorService> callbackExecutors = new ConcurrentHashMap<>();
  private static ScheduledExecutorService retryExecutor;

  public RemoteTableProvider(String tableId, Config config) {
    super(tableId, config);
    JavaTableConfig tableConfig = new JavaTableConfig(config);
    this.readOnly = tableConfig.getForTable(tableId, RemoteTableDescriptor.WRITE_FN) == null;
  }

  @Override
  public Table getTable() {
    RemoteReadableTable table;

    JavaTableConfig tableConfig = new JavaTableConfig(config);

    TableReadFunction readFn = getReadFn(tableConfig);
    RateLimiter rateLimiter = deserializeObject(tableConfig, RemoteTableDescriptor.RATE_LIMITER);
    if (rateLimiter != null) {
      rateLimiter.init(this.context);
    }
    TableRateLimiter.CreditFunction<?, ?> readCreditFn = deserializeObject(tableConfig, RemoteTableDescriptor.READ_CREDIT_FN);
    TableRateLimiter readRateLimiter = new TableRateLimiter(tableId, rateLimiter, readCreditFn, RemoteTableDescriptor.RL_READ_TAG);

    TableRateLimiter.CreditFunction<?, ?> writeCreditFn;
    TableRateLimiter writeRateLimiter = null;

    TableRetryPolicy readRetryPolicy = deserializeObject(tableConfig, RemoteTableDescriptor.READ_RETRY_POLICY);
    TableRetryPolicy writeRetryPolicy = null;

    if ((readRetryPolicy != null || writeRetryPolicy != null) && retryExecutor == null) {
      retryExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("table-retry-executor");
          thread.setDaemon(true);
          return thread;
        });
    }

    if (readRetryPolicy != null) {
      readFn = new RetriableReadFunction<>(readRetryPolicy, readFn, retryExecutor);
    }

    TableWriteFunction writeFn = getWriteFn(tableConfig);

    boolean isRateLimited = readRateLimiter.isRateLimited();
    if (!readOnly) {
      writeCreditFn = deserializeObject(tableConfig, RemoteTableDescriptor.WRITE_CREDIT_FN);
      writeRateLimiter = new TableRateLimiter(tableId, rateLimiter, writeCreditFn, RemoteTableDescriptor.RL_WRITE_TAG);
      isRateLimited |= writeRateLimiter.isRateLimited();
      writeRetryPolicy = deserializeObject(tableConfig, RemoteTableDescriptor.WRITE_RETRY_POLICY);
      if (writeRetryPolicy != null) {
        writeFn = new RetriableWriteFunction(writeRetryPolicy, writeFn, retryExecutor);
      }
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

    if (isRateLimited) {
      tableExecutors.computeIfAbsent(tableId, (arg) ->
          Executors.newSingleThreadExecutor(runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName("table-" + tableId + "-async-executor");
              thread.setDaemon(true);
              return thread;
            }));
    }

    if (readOnly) {
      table = new RemoteReadableTable(tableId, readFn, readRateLimiter,
          tableExecutors.get(tableId), callbackExecutors.get(tableId));
    } else {
      table = new RemoteReadWriteTable(tableId, readFn, writeFn, readRateLimiter,
          writeRateLimiter, tableExecutors.get(tableId), callbackExecutors.get(tableId));
    }

    TableMetricsUtil metricsUtil = new TableMetricsUtil(this.context, table, tableId);
    if (readRetryPolicy != null) {
      ((RetriableReadFunction) readFn).setMetrics(metricsUtil);
    }
    if (writeRetryPolicy != null) {
      ((RetriableWriteFunction) writeFn).setMetrics(metricsUtil);
    }

    table.init(this.context);
    tables.add(table);
    return table;
  }

  @Override
  public void close() {
    super.close();
    tables.forEach(t -> t.close());
    tableExecutors.values().forEach(e -> e.shutdown());
    callbackExecutors.values().forEach(e -> e.shutdown());
  }

  private <T> T deserializeObject(JavaTableConfig tableConfig, String key) {
    String entry = tableConfig.getForTable(tableId, key, "");
    if (entry.isEmpty()) {
      return null;
    }
    return SerdeUtils.deserialize(key, entry);
  }

  private TableReadFunction<?, ?> getReadFn(JavaTableConfig tableConfig) {
    TableReadFunction<?, ?> readFn = deserializeObject(tableConfig, RemoteTableDescriptor.READ_FN);
    if (readFn != null) {
      readFn.init(this.context);
    }
    return readFn;
  }

  private TableWriteFunction<?, ?> getWriteFn(JavaTableConfig tableConfig) {
    TableWriteFunction<?, ?> writeFn = deserializeObject(tableConfig, RemoteTableDescriptor.WRITE_FN);
    if (writeFn != null) {
      writeFn.init(this.context);
    }
    return writeFn;
  }
}

