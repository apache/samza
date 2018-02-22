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
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RemoteReadableTable<K, V> implements ReadableTable<K, V> {
  static final String READ_FN = "io.readFn";
  static final String WRITE_FN = "io.writeFn";

  protected final String tableId;
  protected final Logger logger;
  protected final TableReadFunction<K, V> readFn;
  protected final String groupName;

  protected Timer getNs;
  protected Counter numGets;

  public RemoteReadableTable(String tableId, TableReadFunction<K, V> readFn) {
    Preconditions.checkArgument(tableId != null && !tableId.isEmpty(), "invalid table id");
    Preconditions.checkNotNull(readFn, "null read function");
    this.tableId = tableId;
    this.readFn = readFn;
    this.groupName = getClass().getSimpleName();
    this.logger = LoggerFactory.getLogger(groupName + tableId);
  }

  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    getNs = taskContext.getMetricsRegistry().newTimer(groupName, tableId + "-get-ns");
    numGets = taskContext.getMetricsRegistry().newCounter(groupName, tableId + "-num-gets");
  }

  @Override
  public V get(K key) {
    try {
      numGets.inc();
      long startNs = System.nanoTime();
      V result = readFn.get(key);
      getNs.update(System.nanoTime() - startNs);
      return result;
    } catch (Exception e) {
      String errMsg = String.format("Failed to get a record, key=%s", key);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    Map<K, V> result;
    try {
      result = readFn.getAll(keys);
    } catch (Exception e) {
      String errMsg = "Failed to get some records";
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }

    if (result == null) {
      String errMsg = String.format("Received null records, keys=%s", keys);
      logger.error(errMsg);
      throw new SamzaException(errMsg);
    }

    if (result.size() < keys.size()) {
      String errMsg = String.format("Received insufficient number of records (%d), keys=%s", result.size(), keys);
      logger.error(errMsg);
      throw new SamzaException(errMsg);
    }

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    readFn.close();
  }
}
