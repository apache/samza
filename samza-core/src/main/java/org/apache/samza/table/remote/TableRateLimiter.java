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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * Helper class for remote table to throttle table IO requests with the configured rate limiter.
 * For each request, the needed credits are calculated with the configured credit functions.
 * The throttle methods are overloaded to support the possible CRUD operations.
 *
 * @param <K> type of the table key
 * @param <V> type of the table record
 */
public class TableRateLimiter<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(TableRateLimiter.class);

  private final String tag;
  private final boolean rateLimited;
  private final CreditFunction<K, V> creditFn;

  @VisibleForTesting
  final RateLimiter rateLimiter;

  private Timer waitTimeMetric;

  /**
   * Function interface for providing rate limiting credits for each table record.
   * This interface allows callers to pass in lambda expressions which are otherwise
   * non-serializable as-is.
   * @param <K> the type of the key
   * @param <V> the type of the value
   */
  @InterfaceStability.Unstable
  public interface CreditFunction<K, V> extends Serializable {
    /**
     * Get the number of credits required for the {@code key} and {@code value} pair.
     * @param key table key
     * @param value table record
     * @return number of credits
     */
    int getCredits(K key, V value);
  }

  /**
   * @param tableId table id of the table to be rate limited
   * @param rateLimiter actual rate limiter instance to be used
   * @param creditFn function for deriving the credits for each request
   * @param tag tag to be used with the rate limiter
   */
  public TableRateLimiter(String tableId, RateLimiter rateLimiter, CreditFunction<K, V> creditFn, String tag) {
    this.rateLimiter = rateLimiter;
    this.creditFn = creditFn;
    this.tag = tag;
    this.rateLimited = rateLimiter != null && rateLimiter.getSupportedTags().contains(tag);
    LOG.info("Rate limiting is {} for {}", rateLimited ? "enabled" : "disabled", tableId);
  }

  /**
   * Set up waitTimeMetric metric for latency reporting due to throttling.
   * @param timer waitTimeMetric metric
   */
  public void setTimerMetric(Timer timer) {
    Preconditions.checkNotNull(timer);
    this.waitTimeMetric = timer;
  }

  int getCredits(K key, V value) {
    return (creditFn == null) ? 1 : creditFn.getCredits(key, value);
  }

  int getCredits(Collection<K> keys) {
    if (creditFn == null) {
      return keys.size();
    } else {
      return keys.stream().mapToInt(k -> creditFn.getCredits(k, null)).sum();
    }
  }

  int getEntryCredits(Collection<Entry<K, V>> entries) {
    if (creditFn == null) {
      return entries.size();
    } else {
      return entries.stream().mapToInt(e -> creditFn.getCredits(e.getKey(), e.getValue())).sum();
    }
  }

  private void throttle(int credits) {
    if (!rateLimited) {
      return;
    }

    long startNs = System.nanoTime();
    rateLimiter.acquire(Collections.singletonMap(tag, credits));
    waitTimeMetric.update(System.nanoTime() - startNs);
  }

  /**
   * Throttle a request with a key argument if necessary.
   * @param key key used for the table request
   */
  public void throttle(K key) {
    throttle(getCredits(key, null));
  }

  /**
   * Throttle a request with both the key and value arguments if necessary.
   * @param key key used for the table request
   * @param value value used for the table request
   */
  public void throttle(K key, V value) {
    throttle(getCredits(key, value));
  }

  /**
   * Throttle a request with a collection of keys as the argument if necessary.
   * @param keys collection of keys used for the table request
   */
  public void throttle(Collection<K> keys) {
    throttle(getCredits(keys));
  }

  /**
   * Throttle a request with a collection of table records as the argument if necessary.
   * @param records collection of records used for the table request
   */
  public void throttleRecords(Collection<Entry<K, V>> records) {
    throttle(getEntryCredits(records));
  }

  /**
   * @return whether rate limiting is enabled for the associated table
   */
  public boolean isRateLimited() {
    return rateLimited;
  }
}
