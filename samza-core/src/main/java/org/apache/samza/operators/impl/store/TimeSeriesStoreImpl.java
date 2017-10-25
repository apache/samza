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
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides a view on top of a {@link KeyValueStore} that allows retrieval of entries by time ranges.
 *
 * <p>
 * A {@link TimeSeriesStoreImpl} can be backed by persistent stores like rocksDB, in-memory stores, change-logged
 * stores, cached stores (or any combination of these).
 *
 * <p>
 * Range iterators in the store return values in the order of their timestamp. Within the same key and timestamp,
 * values are returned in their order of insertion.
 *
 * <p>
 * This store has two modes of operation depending on how duplicates are handled:
 * <ol>
 *   <li>
 *     Overwrite Mode: In this mode, the store only retains the most recent value for a given key and timestamp. I.e.,Calling
 *     {@link #put} on an existing key and timestamp will overwrite the previously stored value for that key.
 *   </li>
 *   <li>
 *     Append Mode: In this mode, the store retains all previous values for a given key and timestamp. I.e., Calling {@link #put}
 *     with an existing key and timestamp will append the value to the list.
 *   </li>
 * </ol>
 * <p>
 * Implementation Notes:
 *
 * Data is serialized and organized into K-V pairs as follows:
 *  <pre>
 *    +-----------------------+------------------+------------+------------------------+-----------------------+
 *    |  serialized-key bytes |  timestamp       | version    |       sequence number  | serialized value      |
 *    |                       |                  |            |                        |                       |
 *    +-----------------------+------------------+------------+------------------------+-----------------------+
 *    +----------------------+--------8 bytes----+----1 bytes-+---------7 bytes--------+----value size----------
 *    +----------------------------------STORE KEY-------------------------------------+---STORE VAL-----------+
 *  </pre>
 * An 8 byte timestamp, a one byte version and a 7 byte sequence number are appended to the provided key and this
 * combination is used as the key in the k-v store. The provided value is stored as is.
 *
 * <p> This class is thread-safe and concurrent reads/writes are expected.
 *
 * @param <K> the type of key in the store
 * @param <V> the type of value in the store
 */
public class TimeSeriesStoreImpl<K, V> implements TimeSeriesStore<K, V> {

  private final KeyValueStore<TimeSeriesKey<K>, V> kvStore;

  /**
   * Since timestamps are at the granularity of milliseconds, multiple entries added in the same
   * millisecond are distinguished by a monotonically increasing sequence number.
   */
  private final AtomicLong seqNum = new AtomicLong();
  private final boolean appendMode;

  /**
   * Creates a {@link TimeSeriesStoreImpl}
   *
   * @param kvStore the backing kv store to use
   * @param appendMode should the store be used in appendMode
   */
  public TimeSeriesStoreImpl(KeyValueStore<TimeSeriesKey<K>, V> kvStore, boolean appendMode) {
    this.kvStore = kvStore;
    this.appendMode = appendMode;
  }

  /**
   * Creates a {@link TimeSeriesStoreImpl} in append mode.
   *
   * @param kvStore the backing kv store to use
   */
  public TimeSeriesStoreImpl(KeyValueStore<TimeSeriesKey<K>, V> kvStore) {
    this(kvStore, true);
  }

  @Override
  public void put(K key, V val, long timestamp) {
    // For append mode, values are differentiated by an unique sequence number. For overwrite mode, the sequence
    // number is always zero. This ensures that only the most recent value is retained.
    if (appendMode) {
      seqNum.getAndIncrement();
    }
    TimeSeriesKey<K> timeSeriesKey = new TimeSeriesKey<>(key, timestamp, seqNum.get());
    kvStore.put(timeSeriesKey, val);
  }

  @Override
  public ClosableIterator<TimestampedValue<V>> get(K key, long startTimestamp, long endTimestamp) {
    validateRange(startTimestamp, endTimestamp);
    TimeSeriesKey<K> fromKey = new TimeSeriesKey(key, startTimestamp, 0);
    TimeSeriesKey<K> toKey = new TimeSeriesKey(key, endTimestamp, 0);

    KeyValueIterator<TimeSeriesKey<K>, V> range = kvStore.range(fromKey, toKey);
    return new TimeSeriesStoreIterator<>(range);
  }

  @Override
  public ClosableIterator<TimestampedValue<V>> get(K key, long startTimestamp, long endTimestamp, int maxValues) {
    ClosableIterator<TimestampedValue<V>> iterator = get(key, startTimestamp, endTimestamp);
    return new BoundedClosableIterator<>(iterator, maxValues);
  }

  @Override
  public ClosableIterator<TimestampedValue<V>> get(K key, long timestamp) {
    return get(key, timestamp, timestamp + 1);
  }

  @Override
  public void remove(K key, long startTimestamp, long endTimeStamp) {
    validateRange(startTimestamp, endTimeStamp);
    TimeSeriesKey<K> fromKey = new TimeSeriesKey(key, startTimestamp, 0);
    TimeSeriesKey<K> toKey = new TimeSeriesKey(key, endTimeStamp, 0);

    List<TimeSeriesKey<K>> keysToDelete = new LinkedList<>();

    KeyValueIterator<TimeSeriesKey<K>, V> range = kvStore.range(fromKey, toKey);
    while (range.hasNext()) {
      keysToDelete.add(range.next().getKey());
    }

    kvStore.deleteAll(keysToDelete);
  }

  @Override
  public void remove(K key, long timestamp) {
    remove(key, timestamp, timestamp + 1);
  }

  @Override
  public void flush() {
    kvStore.flush();
  }

  @Override
  public void close() {
  }

  private void validateRange(long startTimestamp, long endTimestamp) throws IllegalArgumentException {
    if (startTimestamp < 0) {
      throw new IllegalArgumentException(String.format("Start timestamp :%d is less than zero", startTimestamp));
    }

    if (endTimestamp < 0) {
      throw new IllegalArgumentException(String.format("End timestamp :%d is less than zero", endTimestamp));
    }

    if (endTimestamp < startTimestamp) {
      throw new IllegalArgumentException(String.format("End timestamp :%d is less than start timestamp: %d", endTimestamp, startTimestamp));
    }
  }

  private static class TimeSeriesStoreIterator<K, V> implements ClosableIterator<TimestampedValue<V>> {

    private final KeyValueIterator<TimeSeriesKey<K>, V> wrappedIterator;

    public TimeSeriesStoreIterator(KeyValueIterator<TimeSeriesKey<K>, V> wrappedIterator) {
      this.wrappedIterator = wrappedIterator;
    }

    @Override
    public void close() {
      wrappedIterator.close();
    }

    @Override
    public boolean hasNext() {
      return wrappedIterator.hasNext();
    }

    @Override
    public TimestampedValue<V> next() {
      Entry<TimeSeriesKey<K>, V> next = wrappedIterator.next();
      return new TimestampedValue<>(next.getValue(), next.getKey().getTimestamp());
    }

    @Override
    public void remove() {
      wrappedIterator.remove();
    }
  }

  /**
   * Wraps a {@link ClosableIterator} to only return the specified number of values
   *
   * @param <T> the type of values in the iterator
   */
  private static class BoundedClosableIterator<T> implements ClosableIterator<T> {

    private final AtomicInteger currentCount = new AtomicInteger(0);
    private final ClosableIterator<T> wrappedIterator;
    private final int maxCount;

    public BoundedClosableIterator(ClosableIterator<T> wrappedIterator, int maxCount) {
      this.wrappedIterator = wrappedIterator;
      this.maxCount = maxCount;
    }

    @Override
    public boolean hasNext() {
      return wrappedIterator.hasNext() && currentCount.get() < maxCount;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      currentCount.incrementAndGet();
      return wrappedIterator.next();
    }

    @Override
    public void remove() {
      wrappedIterator.remove();
    }

    @Override
    public void close() {
      wrappedIterator.close();
    }
  }

}