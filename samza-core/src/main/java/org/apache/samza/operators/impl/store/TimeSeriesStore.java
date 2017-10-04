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
 *
 */

package org.apache.samza.operators.impl.store;

import org.apache.samza.storage.kv.ClosableIterator;

/**
 * A key-value store that allows entries to be queried and stored based on time ranges.
 *
 * Operations on the store can be invoked from multiple threads. Hence, implementations are expected to be thread-safe.
 *
 * @param <K> the type of key in the store
 * @param <V> the type of value in the store
 */
public interface TimeSeriesStore<K, V> {

  /**
   * Insert a key and the value in the store with the provided timestamp.
   *
   * @param key the key to insert
   * @param val the value to insert
   * @param timestamp the timestamp in milliseconds
   */
  void put(K key, V val, long timestamp);

  /**
   * Returns an iterator over values for the given key in the provided time-range - [{@code startTimestamp}, {@code endTimestamp})
   *
   * Values returned by the iterator are ordered by their timestamp. Values with the same timestamp are
   * returned in their order of insertion.
   *
   * <p> The iterator <b>must</b> be closed after use by calling {@link #close}. Not doing so will result in memory leaks.
   *
   * @param key the key to look up in the store
   * @param startTimestamp the start timestamp of the range, inclusive
   * @param endTimestamp the end timestamp of the range, exclusive
   * @return an iterator over the values for the given key in the provided time-range that must be closed after use
   * @throws IllegalArgumentException when startTimeStamp &gt; endTimestamp, or when either of them is negative
   */
  ClosableIterator<TimestampedValue<V>> get(K key, long startTimestamp, long endTimestamp);

  /**
   * Removes all values for this key in the given time-range.
   *
   * @param key the key to look up in the store
   * @param startTimestamp the start timestamp of the range, inclusive
   * @param endTimeStamp the end timestamp of the range, exclusive
   * @throws IllegalArgumentException when startTimeStamp &gt; endTimeStamp, or when either of them is negative
   */
  void remove(K key, long startTimestamp, long endTimeStamp);

  /**
   * Flushes this time series store, if applicable.
   */
  void flush();

  /**
   * Closes this store.
   *
   * Use this to perform final clean-ups, release acquired resources etc.
   */
  void close();
}
