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

import com.google.common.io.Files;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.ByteSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.ClosableIterator;
import org.apache.samza.storage.kv.KeyValueStoreMetrics;
import org.apache.samza.storage.kv.RocksDbKeyValueStore;
import org.apache.samza.storage.kv.SerializedKeyValueStore;
import org.apache.samza.storage.kv.SerializedKeyValueStoreMetrics;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TestTimeSeriesStoreImpl {

  @Test
  public void testGetOnTimestampBoundaries() {
    TimeSeriesStore<String, byte[]> timeSeriesStore = newTimeSeriesStore("my-store", new StringSerde("UTF-8"), true);

    // insert an entry with key "hello" at timestamps "1" and "2"
    timeSeriesStore.put("hello", "world-1".getBytes(), 1L);
    timeSeriesStore.put("hello", "world-1".getBytes(), 2L);
    timeSeriesStore.put("hello", "world-2".getBytes(), 2L);

    // read from time-range
    List<TimeSeriesValue<byte[]>> values = readStore(timeSeriesStore, "hello", 0L, 1L);
    Assert.assertEquals(values.size(), 0);

    // read from time-range [1,2) should return one entry
    values = readStore(timeSeriesStore, "hello", 1L, 2L);
    Assert.assertEquals(values.size(), 1);
    Assert.assertEquals(new String(values.get(0).getValue()), "world-1");

    // read from time-range [2,3) should return two entries
    values = readStore(timeSeriesStore, "hello", 2L, 3L);
    Assert.assertEquals(values.size(), 2);
    Assert.assertEquals(new String(values.get(0).getValue()), "world-1");
    Assert.assertEquals(values.get(0).getTimestamp(), new Long(2));

    // read from time-range [0,3) should return three entries
    values = readStore(timeSeriesStore, "hello", 0L, 3L);
    Assert.assertEquals(values.size(), 3);

    // read from time-range [2,999999) should return two entries
    values = readStore(timeSeriesStore, "hello", 2L, 999999L);
    Assert.assertEquals(values.size(), 2);

    // read from time-range [3,4) should return no entries
    values = readStore(timeSeriesStore, "hello", 3L, 4L);
    Assert.assertEquals(values.size(), 0);
  }

  @Test
  public void testGetWithNonExistentKeys() {
    TimeSeriesStore<String, byte[]> timeSeriesStore = newTimeSeriesStore("my-store", new StringSerde("UTF-8"), true);
    timeSeriesStore.put("hello", "world-1".getBytes(), 1L);

    // read from a non-existent key
    List<TimeSeriesValue<byte[]>> values = readStore(timeSeriesStore, "non-existent-key", 0, Integer.MAX_VALUE);
    Assert.assertEquals(values.size(), 0);

    // read from an existing key but out of range timestamp
    values = readStore(timeSeriesStore, "hello", 2, Integer.MAX_VALUE);
    Assert.assertEquals(values.size(), 0);
  }

  @Test
  public void testPutWithMultipleEntries() {
    TimeSeriesStore<String, byte[]> timeSeriesStore = newTimeSeriesStore("my-store", new StringSerde("UTF-8"), true);

    // insert 100 entries at timestamps "1" and "2"
    for (int i = 0; i < 100; i++) {
      timeSeriesStore.put("hello", "world-1".getBytes(), 1L);
      timeSeriesStore.put("hello", "world-2".getBytes(), 2L);
    }

    // read from time-range [0,2) should return 100 entries
    List<TimeSeriesValue<byte[]>> values = readStore(timeSeriesStore, "hello", 0L, 2L);
    Assert.assertEquals(values.size(), 100);
    values.forEach(timeSeriesValue -> {
        Assert.assertEquals(new String(timeSeriesValue.getValue()), "world-1");
      });

    // read from time-range [2,4) should return 100 entries
    values = readStore(timeSeriesStore, "hello", 2L, 4L);
    Assert.assertEquals(values.size(), 100);
    values.forEach(timeSeriesValue -> {
        Assert.assertEquals(new String(timeSeriesValue.getValue()), "world-2");
      });

    // read all entries in the store
    values = readStore(timeSeriesStore, "hello", 0L, Integer.MAX_VALUE);
    Assert.assertEquals(values.size(), 200);
  }

  @Test
  public void testGetOnTimestampBoundariesWithOverwriteMode() {
    // instantiate a store in overwrite mode
    TimeSeriesStore<String, byte[]> timeSeriesStore = newTimeSeriesStore("my-store", new StringSerde("UTF-8"), false);

    // insert an entry with key "hello" at timestamps "1" and "2"
    timeSeriesStore.put("hello", "world-1".getBytes(), 1L);
    timeSeriesStore.put("hello", "world-1".getBytes(), 2L);
    timeSeriesStore.put("hello", "world-2".getBytes(), 2L);

    // read from time-range
    List<TimeSeriesValue<byte[]>> values = readStore(timeSeriesStore, "hello", 0L, 1L);
    Assert.assertEquals(values.size(), 0);

    // read from time-range [1,2) should return one entry
    values = readStore(timeSeriesStore, "hello", 1L, 2L);
    Assert.assertEquals(values.size(), 1);
    Assert.assertEquals(new String(values.get(0).getValue()), "world-1");

    // read from time-range [2,3) should return the most recent entry
    values = readStore(timeSeriesStore, "hello", 2L, 3L);
    Assert.assertEquals(values.size(), 1);
    Assert.assertEquals(new String(values.get(0).getValue()), "world-2");
    Assert.assertEquals(values.get(0).getTimestamp(), new Long(2));

    // read from time-range [0,3) should return two entries
    values = readStore(timeSeriesStore, "hello", 0L, 3L);
    Assert.assertEquals(values.size(), 2);

    // read from time-range [2,999999) should return one entry
    values = readStore(timeSeriesStore, "hello", 2L, 999999L);
    Assert.assertEquals(values.size(), 1);

    // read from time-range [3,4) should return no entries
    values = readStore(timeSeriesStore, "hello", 3L, 4L);
    Assert.assertEquals(values.size(), 0);
  }

  @Test
  public void testDeletesInOverwriteMode() {
    // instantiate a store in overwrite mode
    TimeSeriesStore<String, byte[]> timeSeriesStore = newTimeSeriesStore("my-store", new StringSerde("UTF-8"), false);

    // insert an entry with key "hello" at timestamps "1" and "2"
    timeSeriesStore.put("hello", "world-1".getBytes(), 1L);
    timeSeriesStore.put("hello", "world-1".getBytes(), 2L);
    timeSeriesStore.put("hello", "world-2".getBytes(), 2L);

    List<TimeSeriesValue<byte[]>> values = readStore(timeSeriesStore, "hello", 1L, 3L);
    Assert.assertEquals(values.size(), 2);

    timeSeriesStore.remove("hello", 0L, 3L);
    values = readStore(timeSeriesStore, "hello", 1L, 3L);
    Assert.assertEquals(values.size(), 0);
  }

  private static <K, V> List<TimeSeriesValue<V>> readStore(TimeSeriesStore<K, V> store, K key, long startTimestamp, long endTimestamp) {
    List<TimeSeriesValue<V>> list = new ArrayList<>();
    ClosableIterator<TimeSeriesValue<V>> storeValuesIterator = store.get(key, startTimestamp, endTimestamp);

    while (storeValuesIterator.hasNext()) {
      TimeSeriesValue<V> next = storeValuesIterator.next();
      list.add(next);
    }

    storeValuesIterator.close();
    return list;
  }
  
  private static <K> TimeSeriesStore<K, byte[]> newTimeSeriesStore(String storeName, Serde<K> keySerde, boolean appendMode) {
    RocksDbKeyValueStore rocksKVStore = newRocksDbStore("someStore");
    SerializedKeyValueStore<TimeSeriesKey<K>, byte[]> kvStore = new SerializedKeyValueStore<>(rocksKVStore,
            new TimeSeriesKeySerde<>(keySerde), new ByteSerde(),
            new SerializedKeyValueStoreMetrics("", new MetricsRegistryMap()));
    return new TimeSeriesStoreImpl<>(kvStore, appendMode);
  }

  private static RocksDbKeyValueStore newRocksDbStore(String storeName) {
    File dir = Files.createTempDir();
    return new RocksDbKeyValueStore(dir,
            new Options().setCreateIfMissing(true).setCompressionType(CompressionType.SNAPPY_COMPRESSION), new MapConfig(),
            false, storeName, new WriteOptions(), new FlushOptions(),
            new KeyValueStoreMetrics(storeName, new MetricsRegistryMap()));
  }

}
