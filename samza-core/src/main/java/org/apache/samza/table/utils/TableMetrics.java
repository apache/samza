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

import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.Table;


/**
 * Utility class that contains the default set of read metrics.
 */
public class TableMetrics {

  // Read metrics
  public final Timer getNs;
  public final Timer getAllNs;
  public final Timer readNs;
  public final Counter numGets;
  public final Counter numGetAlls;
  public final Counter numReads;
  public final Counter numMissedLookups;
  // Write metrics
  public final Counter numPuts;
  public final Timer putNs;
  public final Counter numPutAlls;
  public final Timer putAllNs;
  public final Counter numUpdates;
  public final Timer updateNs;
  public final Counter numUpdateAlls;
  public final Timer updateAllNs;
  public final Counter numDeletes;
  public final Timer deleteNs;
  public final Counter numDeleteAlls;
  public final Timer deleteAllNs;
  public final Counter numWrites;
  public final Timer writeNs;
  public final Counter numFlushes;
  public final Timer flushNs;

  /**
   * Constructor based on container and task container context
   *
   * @param context {@link Context} for this task
   * @param table underlying table
   * @param tableId table Id
   */
  public TableMetrics(Context context, Table table, String tableId) {
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(context, table, tableId);
    // Read metrics
    numGets = tableMetricsUtil.newCounter("num-gets");
    getNs = tableMetricsUtil.newTimer("get-ns");
    numGetAlls = tableMetricsUtil.newCounter("num-getAlls");
    getAllNs = tableMetricsUtil.newTimer("getAll-ns");
    numReads = tableMetricsUtil.newCounter("num-reads");
    readNs = tableMetricsUtil.newTimer("read-ns");
    numMissedLookups = tableMetricsUtil.newCounter("num-missed-lookups");
    // Write metrics
    numPuts = tableMetricsUtil.newCounter("num-puts");
    putNs = tableMetricsUtil.newTimer("put-ns");
    numPutAlls = tableMetricsUtil.newCounter("num-putAlls");
    putAllNs = tableMetricsUtil.newTimer("putAll-ns");
    numUpdates = tableMetricsUtil.newCounter("update-puts");
    updateNs = tableMetricsUtil.newTimer("update-ns");
    numUpdateAlls = tableMetricsUtil.newCounter("num-updateAlls");
    updateAllNs = tableMetricsUtil.newTimer("updateAll-ns");
    numDeletes = tableMetricsUtil.newCounter("num-deletes");
    deleteNs = tableMetricsUtil.newTimer("delete-ns");
    numDeleteAlls = tableMetricsUtil.newCounter("num-deleteAlls");
    deleteAllNs = tableMetricsUtil.newTimer("deleteAll-ns");
    numWrites = tableMetricsUtil.newCounter("num-writes");
    writeNs = tableMetricsUtil.newTimer("write-ns");
    numFlushes = tableMetricsUtil.newCounter("num-flushes");
    flushNs = tableMetricsUtil.newTimer("flush-ns");
  }
}
