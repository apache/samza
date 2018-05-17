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

package org.apache.samza.storage.kv.inmemory;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueSnapshot;
import org.apache.samza.storage.kv.KeyValueStoreMetrics;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestInMemoryKeyValueStore {
  @Test
  public void testSnapshot() throws Exception {
    InMemoryKeyValueStore store = new InMemoryKeyValueStore(
        new KeyValueStoreMetrics("testInMemory", new MetricsRegistryMap()));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    String prefix = "prefix";
    for(int i = 0; i < 100; i++) {
      store.put(genKey(outputStream, prefix, i), genValue());
    }

    byte[] firstKey = genKey(outputStream, prefix, 0);
    byte[] lastKey = genKey(outputStream, prefix, 100);
    KeyValueSnapshot<byte[], byte[]> snapshot = store.snapshot(firstKey, lastKey);
    // Make sure the cached Iterable won't change when new elements are added
    store.put(genKey(outputStream, prefix, 200), genValue());
    assertTrue(Iterators.size(snapshot.iterator()) == 100);

    List<Integer> keys = new ArrayList<>();
    KeyValueIterator<byte[], byte[]> iter = snapshot.iterator();
    while (iter.hasNext()) {
      Entry<byte[], byte[]> entry = iter.next();
      int key = Ints.fromByteArray(Arrays.copyOfRange(entry.getKey(), prefix.getBytes().length, entry.getKey().length));
      keys.add(key);
    }
    assertEquals(keys, IntStream.rangeClosed(0, 99).boxed().collect(Collectors.toList()));

    outputStream.close();
    store.close();
  }

  private byte[] genKey(ByteArrayOutputStream outputStream, String prefix, int i) throws Exception {
    outputStream.reset();
    outputStream.write(prefix.getBytes());
    outputStream.write(Ints.toByteArray(i));
    return outputStream.toByteArray();
  }

  private byte[] genValue() {
    int randomVal = ThreadLocalRandom.current().nextInt(0, 100000);
    return Ints.toByteArray(randomVal);
  }
}
