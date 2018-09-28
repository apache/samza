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
public class DefaultTableReadMetrics {

  public final Timer getNs;
  public final Timer getAllNs;
  public final Counter numGets;
  public final Counter numGetAlls;
  public final Timer getCallbackNs;

  /**
   * Constructor based on container and task container context
   *
   * @param context {@link Context} for this task
   * @param table underlying table
   * @param tableId table Id
   */
  public DefaultTableReadMetrics(Context context, Table table, String tableId) {
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(context, table, tableId);
    getNs = tableMetricsUtil.newTimer("get-ns");
    getAllNs = tableMetricsUtil.newTimer("getAll-ns");
    numGets = tableMetricsUtil.newCounter("num-gets");
    numGetAlls = tableMetricsUtil.newCounter("num-getAlls");
    getCallbackNs = tableMetricsUtil.newTimer("get-callback-ns");
  }

}
