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

import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.utils.DefaultTableWriteMetrics;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.RateLimiter;

import com.google.common.base.Preconditions;

import static org.apache.samza.table.remote.RemoteTableDescriptor.RL_WRITE_TAG;


/**
 * Remote store backed read writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RemoteReadWriteTable<K, V> extends RemoteReadableTable<K, V> implements ReadWriteTable<K, V> {

  protected final TableWriteFunction<K, V> writeFn;
  protected final CreditFunction<K, V> writeCreditFn;
  protected final boolean rateLimitWrites;

  protected DefaultTableWriteMetrics writeMetrics;
  protected Timer putThrottleNs; // use single timer for all write operations

  public RemoteReadWriteTable(String tableId, TableReadFunction readFn, TableWriteFunction writeFn,
      RateLimiter ratelimiter, CreditFunction<K, V> readCreditFn, CreditFunction<K, V> writeCreditFn) {
    super(tableId, readFn, ratelimiter, readCreditFn);
    Preconditions.checkNotNull(writeFn, "null write function");
    this.writeFn = writeFn;
    this.writeCreditFn = writeCreditFn;
    this.rateLimitWrites = rateLimiter != null && rateLimiter.getSupportedTags().contains(RL_WRITE_TAG);
    logger.info("Rate limiting is {} for remote write operations", rateLimitWrites ? "enabled" : "disabled");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    super.init(containerContext, taskContext);
    writeMetrics = new DefaultTableWriteMetrics(containerContext, taskContext, this, tableId);
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
    putThrottleNs = tableMetricsUtil.newTimer("put-throttle-ns");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(K key, V value) {

    if (value == null) {
      delete(key);
      return;
    }

    try {
      writeMetrics.numPuts.inc();
      if (rateLimitWrites) {
        throttle(key, value, RL_WRITE_TAG, writeCreditFn, putThrottleNs);
      }
      long startNs = System.nanoTime();
      writeFn.put(key, value);
      writeMetrics.putNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = String.format("Failed to put a record, key=%s, value=%s", key, value);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putAll(List<Entry<K, V>> entries) {
    try {
      writeMetrics.numPutAlls.inc();
      long startNs = System.nanoTime();
      writeFn.putAll(entries);
      writeMetrics.putAllNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = String.format("Failed to put records: %s", entries);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(K key) {
    try {
      writeMetrics.numDeletes.inc();
      if (rateLimitWrites) {
        throttle(key, null, RL_WRITE_TAG, writeCreditFn, putThrottleNs);
      }
      long startNs = System.nanoTime();
      writeFn.delete(key);
      writeMetrics.deleteNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = String.format("Failed to delete a record, key=%s", key);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteAll(List<K> keys) {
    try {
      writeMetrics.numDeleteAlls.inc();
      writeFn.deleteAll(keys);
      long startNs = System.nanoTime();
      writeMetrics.deleteAllNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = String.format("Failed to delete records, keys=%s", keys);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    try {
      writeMetrics.numFlushes.inc();
      if (rateLimitWrites) {
        throttle(null, null, RL_WRITE_TAG, writeCreditFn, putThrottleNs);
      }
      long startNs = System.nanoTime();
      writeFn.flush();
      writeMetrics.flushNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = "Failed to flush remote store";
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    super.close();
    writeFn.close();
  }
}
