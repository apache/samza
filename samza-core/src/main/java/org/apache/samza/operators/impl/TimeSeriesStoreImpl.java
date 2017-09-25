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
package org.apache.samza.operators.impl;

import org.apache.samza.storage.kv.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * A key-value store that allows entries to be queried and stored based on time ranges.
 *
 * Operations on the store can be invoked from multiple threads. So, implementations are expected to be thread-safe.
 *
 * @param <K>, the type of key in the store
 * @param <V>, the type of value in the store
 */

public class TimeSeriesStoreImpl<K, V> implements TimeSeriesStore<K, V> {

  private final KeyValueStore<TimeSeriesKey<K>, V> kvStore;

  private int seqNum = 0;
  private final Object seqNumLock = new Object();

  public TimeSeriesStoreImpl(KeyValueStore<TimeSeriesKey<K>, V> kvStore) {
    this.kvStore = kvStore;
  }

  @Override
  public void put(K key, V val, Long timeStamp) {
    TimeSeriesKey<K> timeSeriesKey = new TimeSeriesKey<>(key, timeStamp, seqNum);
    kvStore.put(timeSeriesKey, val);
  }

  @Override
  public ClosableIterator<KV<V, Long>> get(K key, Long startTimestamp, Long endTimeStamp) {
    TimeSeriesKey<K> fromKey = new TimeSeriesKey(key, startTimestamp, 0);
    TimeSeriesKey<K> toKey = new TimeSeriesKey(key, endTimeStamp, Integer.MAX_VALUE);

    KeyValueIterator<TimeSeriesKey<K>, V> range = kvStore.range(fromKey, toKey);
    return new TimeSeriesStoreIterator<>(range);
  }

  @Override
  public void remove(K key, Long startTimestamp, Long endTimeStamp) {
    TimeSeriesKey<K> fromKey = new TimeSeriesKey(key, startTimestamp, 0);
    TimeSeriesKey<K> toKey = new TimeSeriesKey(key, endTimeStamp, Integer.MAX_VALUE);

    List<TimeSeriesKey<K>> keysToDelete = new LinkedList<>();

    KeyValueIterator<TimeSeriesKey<K>, V> range = kvStore.range(fromKey, toKey);
    while (range.hasNext()) {
      keysToDelete.add(range.next().getKey());
    }

    kvStore.deleteAll(keysToDelete);

  }

  private void incSeqNum() {
    synchronized (seqNumLock) {
      // cycles the seqNum back
      seqNum = (seqNum + 1) & Integer.MAX_VALUE;
    }
  }

  private static class TimeSeriesStoreIterator<K, V> implements ClosableIterator<KV<V, Long>> {

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
    public KV<V, Long> next() {
      Entry<TimeSeriesKey<K>, V> next = wrappedIterator.next();

      KV<V, Long> kv = new KV<>(next.getValue(), next.getKey().getTimestamp());
      return kv;
    }

    @Override
    public void remove() {
      wrappedIterator.remove();
    }
  }

  public static void main(String[] args) {
    HashMap<byte[], byte[]> map = new HashMap<>();

  }
}
