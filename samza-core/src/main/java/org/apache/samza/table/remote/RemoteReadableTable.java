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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.KV;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import static org.apache.samza.table.remote.RemoteTableDescriptor.RL_READ_TAG;


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
  protected final String tableId;
  protected final Logger logger;
  protected final TableReadFunction<K, V> readFn;
  protected final String groupName;
  protected final RateLimiter rateLimiter;
  protected final CreditFunction<K, V> readCreditFn;
  protected final boolean rateLimitReads;

  protected Timer getNs;
  protected Timer getAllNs;
  protected Timer getThrottleNs;
  protected Counter numGets;
  protected Counter numGetAlls;

  /**
   * Construct a RemoteReadableTable instance
   * @param tableId table id
   * @param readFn {@link TableReadFunction} for read operations
   * @param rateLimiter optional {@link RateLimiter} for throttling reads
   * @param readCreditFn function returning a credit to be charged for rate limiting per record
   */
  public RemoteReadableTable(String tableId, TableReadFunction<K, V> readFn, RateLimiter rateLimiter,
      CreditFunction<K, V> readCreditFn) {
    Preconditions.checkArgument(tableId != null && !tableId.isEmpty(), "invalid table id");
    Preconditions.checkNotNull(readFn, "null read function");
    this.tableId = tableId;
    this.readFn = readFn;
    this.rateLimiter = rateLimiter;
    this.readCreditFn = readCreditFn;
    this.groupName = getClass().getSimpleName();
    this.logger = LoggerFactory.getLogger(groupName + tableId);
    this.rateLimitReads = rateLimiter != null && rateLimiter.getSupportedTags().contains(RL_READ_TAG);
    logger.info("Rate limiting is {} for remote read operations", rateLimitReads ? "enabled" : "disabled");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
    getNs = tableMetricsUtil.newTimer("get-ns");
    getAllNs = tableMetricsUtil.newTimer("getAll-ns");
    getThrottleNs = tableMetricsUtil.newTimer("get-throttle-ns");
    numGets = tableMetricsUtil.newCounter("num-gets");
    numGetAlls = tableMetricsUtil.newCounter("num-getAlls");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(K key) {
    try {
      numGets.inc();
      if (rateLimitReads) {
        throttle(key, null, RL_READ_TAG, readCreditFn, getThrottleNs);
      }
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

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<K, V> getAll(List<K> keys) {
    Map<K, V> result;
    try {
      numGetAlls.inc();
      long startNs = System.nanoTime();
      result = readFn.getAll(keys);
      getAllNs.update(System.nanoTime() - startNs);
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

  /**
   * Throttle requests given a table record (key, value) with rate limiter and credit function
   * @param key key of the table record (nullable)
   * @param value value of the table record (nullable)
   * @param tag tag for rate limiter
   * @param creditFn mapper function from KV to credits to be charged
   * @param timer timer metric to track throttling delays
   */
  protected void throttle(K key, V value, String tag, CreditFunction<K, V> creditFn, Timer timer) {
    long startNs = System.nanoTime();
    int credits = (creditFn == null) ? 1 : creditFn.apply(KV.of(key, value));
    rateLimiter.acquire(Collections.singletonMap(tag, credits));
    timer.update(System.nanoTime() - startNs);
  }
}
