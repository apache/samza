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

package org.apache.samza.table.utils;

import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.Table;
import org.apache.samza.table.caching.SupplierGauge;
import org.apache.samza.task.TaskContext;


/**
 * Utility class to generate metrics that helps to encapsulate required parameters,
 * maintains naming consistency and simplifies metrics creation API for tables.
 */
public class TableMetricsUtil {

  private final MetricsRegistry metricsRegistry;
  private final String groupName;
  private final String tableId;

  /**
   * Constructor based on container context
   *
   * @param containerContext container context
   * @param taskContext task context
   * @param table underlying table
   * @param tableId table Id
   */
  public TableMetricsUtil(SamzaContainerContext containerContext, TaskContext taskContext,
      Table table, String tableId) {

    Preconditions.checkNotNull(containerContext);
    Preconditions.checkNotNull(table);
    Preconditions.checkNotNull(tableId);

    this.metricsRegistry = taskContext == null // The table is at container level, when the task
        ? containerContext.metricsRegistry     // context passed in is null
        : taskContext.getMetricsRegistry();
    this.groupName = table.getClass().getSimpleName();
    this.tableId = tableId;
  }

  /**
   * Create a new counter by delegating to the underlying metrics registry
   * @param name name of the counter
   * @return newly created counter
   */
  public Counter newCounter(String name) {
    return metricsRegistry.newCounter(groupName, getMetricFullName(name));
  }

  /**
   * Create a new timer by delegating to the underlying metrics registry
   * @param name name of the timer
   * @return newly created timer
   */
  public Timer newTimer(String name) {
    return metricsRegistry.newTimer(groupName, getMetricFullName(name));
  }

  /**
   * Create a new gauge by delegating to the underlying metrics registry
   * @param name name of the gauge
   * @param supplier a function that supplies the value
   * @param <T> type of the value
   * @return newly created gauge
   */
  public <T> Gauge<T> newGauge(String name, Supplier<T> supplier) {
    return metricsRegistry.newGauge(groupName, new SupplierGauge(getMetricFullName(name), supplier));
  }

  private String getMetricFullName(String name) {
    return String.format("%s-%s", tableId, name);
  }

}
