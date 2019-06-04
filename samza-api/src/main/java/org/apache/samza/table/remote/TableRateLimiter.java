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

  private final String tag;
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
  public interface CreditFunction<K, V> extends TablePart, Serializable {
    /**
     * Get the number of credits required for the {@code key} and {@code value} pair.
     * @param key table key
     * @param value table record
     * @param args additional arguments
     * @return number of credits
     */
    int getCredits(K key, V value, Object ... args);

    /**
     * Get the number of credits required for the {@code opId} and associated {@code args}.
     * @param opId operation Id
     * @param args additional arguments
     * @return number of credits
     */
    default int getCredits(int opId, Object ... args) {
      return 1;
    }
  }

  /**
   * @param tableId table id of the table to be rate limited
   * @param rateLimiter actual rate limiter instance to be used
   * @param creditFn function for deriving the credits for each request
   * @param tag tag to be used with the rate limiter
   */
  public TableRateLimiter(String tableId, RateLimiter rateLimiter, CreditFunction<K, V> creditFn, String tag) {
    Preconditions.checkNotNull(rateLimiter);
    Preconditions.checkArgument(rateLimiter.getSupportedTags().contains(tag),
        String.format("Rate limiter for table %s doesn't support %s", tableId, tag));
    this.rateLimiter = rateLimiter;
    this.creditFn = creditFn;
    this.tag = tag;
  }

  /**
   * Set up waitTimeMetric metric for latency reporting due to throttling.
   * @param timer waitTimeMetric metric
   */
  public void setTimerMetric(Timer timer) {
    Preconditions.checkNotNull(timer);
    this.waitTimeMetric = timer;
  }

  int getCredits(K key, V value, Object ... args) {
    return (creditFn == null) ? 1 : creditFn.getCredits(key, value, args);
  }

  int getCredits(Collection<K> keys, Object ... args) {
    if (creditFn == null) {
      return keys.size();
    } else {
      return keys.stream().mapToInt(k -> creditFn.getCredits(k, null, args)).sum();
    }
  }

  int getEntryCredits(Collection<Entry<K, V>> entries, Object ... args) {
    if (creditFn == null) {
      return entries.size();
    } else {
      return entries.stream().mapToInt(e -> creditFn.getCredits(e.getKey(), e.getValue(), args)).sum();
    }
  }

  int getCredits(int opId, Object ... args) {
    return (creditFn == null) ? 1 : creditFn.getCredits(opId, args);
  }

  private void throttle(int credits) {
    long startNs = System.nanoTime();
    rateLimiter.acquire(Collections.singletonMap(tag, credits));
    if (waitTimeMetric != null) {
      waitTimeMetric.update(System.nanoTime() - startNs);
    }
  }

  /**
   * Throttle a request with a key argument if necessary.
   * @param key key used for the table request
   * @param args additional arguments
   */
  public void throttle(K key, Object ... args) {
    throttle(getCredits(key, null, args));
  }

  /**
   * Throttle a request with both the key and value arguments if necessary.
   * @param key key used for the table request
   * @param value value used for the table request
   * @param args additional arguments
   */
  public void throttle(K key, V value, Object ... args) {
    throttle(getCredits(key, value, args));
  }

  /**
   * Throttle a request with opId and associated arguments
   * @param opId operation Id
   * @param args associated arguments
   */
  public void throttle(int opId, Object ... args) {
    throttle(getCredits(opId, args));
  }

  /**
   * Throttle a request with a collection of keys as the argument if necessary.
   * @param keys collection of keys used for the table request
   * @param args additional arguments
   */
  public void throttle(Collection<K> keys, Object ... args) {
    throttle(getCredits(keys, args));
  }

  /**
   * Throttle a request with a collection of table records as the argument if necessary.
   * @param records collection of records used for the table request
   * @param args additional arguments
   */
  public void throttleRecords(Collection<Entry<K, V>> records, Object ... args) {
    throttle(getEntryCredits(records, args));
  }
}
