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

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.Table;
import org.apache.samza.task.TaskContext;


public class DefaultTableWriteMetrics {

  public final Timer putNs;
  public final Timer putAllNs;
  public final Timer deleteNs;
  public final Timer deleteAllNs;
  public final Timer flushNs;
  public final Counter numPuts;
  public final Counter numPutAlls;
  public final Counter numDeletes;
  public final Counter numDeleteAlls;
  public final Counter numFlushes;
  public final Timer putCallbackNs;
  public final Timer deleteCallbackNs;

  /**
   * Utility class that contains the default set of write metrics.
   *
   * @param containerContext container context
   * @param taskContext task context
   * @param table underlying table
   * @param tableId table Id
   */
  public DefaultTableWriteMetrics(SamzaContainerContext containerContext, TaskContext taskContext,
      Table table, String tableId) {
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, table, tableId);
    putNs = tableMetricsUtil.newTimer("put-ns");
    putAllNs = tableMetricsUtil.newTimer("putAll-ns");
    deleteNs = tableMetricsUtil.newTimer("delete-ns");
    deleteAllNs = tableMetricsUtil.newTimer("deleteAll-ns");
    flushNs = tableMetricsUtil.newTimer("flush-ns");
    numPuts = tableMetricsUtil.newCounter("num-puts");
    numPutAlls = tableMetricsUtil.newCounter("num-putAlls");
    numDeletes = tableMetricsUtil.newCounter("num-deletes");
    numDeleteAlls = tableMetricsUtil.newCounter("num-deleteAlls");
    numFlushes = tableMetricsUtil.newCounter("num-flushes");
    putCallbackNs = tableMetricsUtil.newTimer("put-callback-ns");
    deleteCallbackNs = tableMetricsUtil.newTimer("delete-callback-ns");
  }
}
