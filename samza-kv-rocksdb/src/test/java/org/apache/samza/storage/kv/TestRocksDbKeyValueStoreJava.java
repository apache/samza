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

package org.apache.samza.storage.kv;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.junit.Test;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRocksDbKeyValueStoreJava {
  @Test
  public void testIterate() throws Exception {
    Config config = new MapConfig();
    Options options = new Options();
    options.setCreateIfMissing(true);

    File dbDir = new File(System.getProperty("java.io.tmpdir") + "/dbStore" + System.currentTimeMillis());
    RocksDbKeyValueStore store = new RocksDbKeyValueStore(dbDir, options, config, false, "dbStore",
        new WriteOptions(), new FlushOptions(), new KeyValueStoreMetrics("dbStore", new MetricsRegistryMap()));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    String prefix = "prefix";
    for(int i = 0; i < 100; i++) {
      store.put(genKey(outputStream, prefix, i), genValue());
    }

    byte[] firstKey = genKey(outputStream, prefix, 0);
    byte[] lastKey = genKey(outputStream, prefix, 1000);
    KeyValueSnapshot<byte[], byte[]> snapshot = store.snapshot(firstKey, lastKey);
    // Make sure the cached Iterable won't change when new elements are added
    store.put(genKey(outputStream, prefix, 200), genValue());
    KeyValueIterator<byte[], byte[]> iterator = snapshot.iterator();
    assertTrue(Iterators.size(iterator) == 100);
    iterator.close();
    List<Integer> keys = new ArrayList<>();
    KeyValueIterator<byte[], byte[]> iterator2 = snapshot.iterator();
    while (iterator2.hasNext()) {
      Entry<byte[], byte[]> entry = iterator2.next();
      int key = Ints.fromByteArray(Arrays.copyOfRange(entry.getKey(), prefix.getBytes().length, entry.getKey().length));
      keys.add(key);
    }
    assertEquals(keys, IntStream.rangeClosed(0, 99).boxed().collect(Collectors.toList()));
    iterator2.close();

    outputStream.close();
    snapshot.close();
    store.close();
  }

  @Test
  public void testPerf() throws Exception {
    Config config = new MapConfig();
    Options options = new Options();
    options.setCreateIfMissing(true);

    File dbDir = new File(System.getProperty("java.io.tmpdir") + "/dbStore" + System.currentTimeMillis());
    RocksDbKeyValueStore store = new RocksDbKeyValueStore(dbDir, options, config, false, "dbStore",
        new WriteOptions(), new FlushOptions(), new KeyValueStoreMetrics("dbStore", new MetricsRegistryMap()));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    String prefix = "this is the key prefix";
    Random r = new Random();
    for(int i = 0; i < 100000; i++) {
      store.put(genKey(outputStream, prefix, r.nextInt()), genValue());
    }

    byte[] firstKey = genKey(outputStream, prefix, 0);
    byte[] lastKey = genKey(outputStream, prefix, Integer.MAX_VALUE);

    long start;
    KeyValueIterator iter;

    start = System.currentTimeMillis();
    iter = store.range(firstKey, lastKey);
    long rangeTime = System.currentTimeMillis() - start;
    start = System.currentTimeMillis();
    Iterators.size(iter);
    long rangeIterTime = System.currentTimeMillis() - start;
    System.out.println("range iter create time: " + rangeTime + ", iterate time: " + rangeIterTime);

    // Please comment out range query part in order to do an accurate perf test for snapshot
    start = System.currentTimeMillis();
    iter = store.snapshot(firstKey, lastKey).iterator();
    long snapshotTime = System.currentTimeMillis() - start;
    start = System.currentTimeMillis();
    Iterators.size(iter);
    long snapshotIterTime = System.currentTimeMillis() - start;
    System.out.println("snapshot iter create time: " + snapshotTime + ", iterate time: " + snapshotIterTime);
  }

  private byte[] genKey(ByteArrayOutputStream outputStream, String prefix, int i) throws Exception {
    outputStream.reset();
    outputStream.write(prefix.getBytes());
    outputStream.write(Ints.toByteArray(i));
    return outputStream.toByteArray();
  }

  private byte[] genValue() {
    int randomVal = ThreadLocalRandom.current().nextInt();
    return Ints.toByteArray(randomVal);
  }
}
