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
package org.apache.samza.table;

import com.google.common.base.Preconditions;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.context.Context;
import org.apache.samza.table.utils.TableMetrics;
import org.apache.samza.util.HighResolutionClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for a concrete table implementation
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
abstract public class BaseReadWriteUpdateTable<K, V, U> implements ReadWriteUpdateTable<K, V, U> {

  protected final Logger logger;

  protected final String tableId;

  protected TableMetrics metrics;

  protected HighResolutionClock clock;

  /**
   * Construct an instance
   * @param tableId Id of the table
   */
  public BaseReadWriteUpdateTable(String tableId) {
    Preconditions.checkArgument(tableId != null & !tableId.isEmpty(),
        String.format("Invalid table Id: %s", tableId));
    this.tableId = tableId;
    this.logger = LoggerFactory.getLogger(getClass().getName() + "." + tableId);
  }

  @Override
  public void init(Context context) {
    MetricsConfig metricsConfig = new MetricsConfig(context.getJobContext().getConfig());
    clock = metricsConfig.getMetricsTimerEnabled()
        ? () -> System.nanoTime()
        : () -> 0L;
    metrics = new TableMetrics(context, this, tableId);
  }

  public String getTableId() {
    return tableId;
  }

  public interface Func0 {
    void apply();
  }

  public interface Func1<T> {
    CompletableFuture<T> apply();
  }
}
