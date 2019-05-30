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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStore;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.FlushOptions;
import org.rocksdb.WriteOptions;
import scala.Int;


/**
 * Test suite to check handling of large messages when attempting to write to the store.
 */
@RunWith(value = Parameterized.class)
public class TestLargeMessageSafeKeyValueStores {

  private String typeOfStore;
  private String storeConfig;
  private boolean dropLargeMessage;

  private static File dir = new File(System.getProperty("java.io.tmpdir"), "rocksdb-test-" + new Random().nextInt(Int.MaxValue()));
  private static Serde<String> stringSerde = new StringSerde();
  private static String storeName = "testStore";
  private static SystemStreamPartition systemStreamPartition = new SystemStreamPartition("test-system", "test-stream", new Partition(0));
  private static MetricsRegistry metricsRegistry = new MetricsRegistryMap();
  private static LoggedStoreMetrics loggedStoreMetrics = new LoggedStoreMetrics(storeName, metricsRegistry);
  private static KeyValueStoreMetrics keyValueStoreMetrics = new KeyValueStoreMetrics(storeName, metricsRegistry);
  private static SerializedKeyValueStoreMetrics serializedKeyValueStoreMetrics = new SerializedKeyValueStoreMetrics(storeName, metricsRegistry);
  private static CachedStoreMetrics cachedStoreMetrics = new CachedStoreMetrics(storeName, metricsRegistry);
  private static int maxMessageSize = 1024;
  private static int cacheSize = 1024;
  private static int batchSize = 1;

  private KeyValueStore<String, String> store = null;
  private KeyValueStore<byte[], byte[]> loggedStore = null;

  /**
   * @param typeOfStore Defines type of key-value store (Eg: "rocksdb" / "inmemory")
   * @param storeConfig Defines what order we are invoking the stores in - serde / cache-then-serde / serde-then-cache
   * @param dropLargeMessageStr Defines the value of the drop.large.message config which drops large messages if true
   */
  public TestLargeMessageSafeKeyValueStores(String typeOfStore, String storeConfig, String dropLargeMessageStr) {
    this.typeOfStore = typeOfStore;
    this.storeConfig = storeConfig;
    this.dropLargeMessage = Boolean.valueOf(dropLargeMessageStr);
  }

  @Parameterized.Parameters
  public static Collection<String[]> data() {
    return Arrays.asList(new String[][] {
        {"inmemory", "serde", "true"},
        {"inmemory", "serde", "false"},
        {"inmemory", "cache-then-serde", "true"},
        {"inmemory", "cache-then-serde", "false"},
        {"inmemory", "serde-then-cache", "false"},
        {"inmemory", "serde-then-cache", "true"},
        //RocksDB
        {"rocksdb", "serde", "true"},
        {"rocksdb", "serde", "false"},
        {"rocksdb", "cache-then-serde", "true"},
        {"rocksdb", "cache-then-serde", "false"},
        {"rocksdb", "serde-then-cache", "false"},
        {"rocksdb", "serde-then-cache", "true"}
    });
  }

  @Before
  public void setup() {

    KeyValueStore<byte[], byte[]> kvStore;
    switch (typeOfStore) {
      case "inmemory" : {
        kvStore = new InMemoryKeyValueStore(keyValueStoreMetrics);
        break;
      }
      case "rocksdb" : {
        kvStore = new RocksDbKeyValueStore(dir, new org.rocksdb.Options().setCreateIfMissing(true).setCompressionType(org.rocksdb.CompressionType.SNAPPY_COMPRESSION),
            new MapConfig(), false, storeName,
            new WriteOptions(), new FlushOptions(), keyValueStoreMetrics);
        break;
      }
      default :
        throw new IllegalArgumentException("Type of store undefined: " + typeOfStore);
    }

    MessageCollector collector = envelope -> {
      int messageLength = ((byte[]) envelope.getMessage()).length;
      if (messageLength > maxMessageSize) {
        throw new SamzaException("Logged store message size " + messageLength + " for store " + storeName
            + " was larger than the maximum allowed message size " + maxMessageSize + ".");
      }
    };
    loggedStore = new LoggedStore<>(kvStore, systemStreamPartition, collector, loggedStoreMetrics);

    switch (storeConfig) {
      case "serde" : {
        KeyValueStore<byte[], byte[]> largeMessageSafeStore =
            new LargeMessageSafeStore(loggedStore, storeName, dropLargeMessage, maxMessageSize);
        store = new SerializedKeyValueStore<>(largeMessageSafeStore, stringSerde, stringSerde,
            serializedKeyValueStoreMetrics);
        break;
      }
      case "cache-then-serde" : {
        KeyValueStore<byte[], byte[]> toBeSerializedStore = loggedStore;
        if (dropLargeMessage) {
          toBeSerializedStore = new LargeMessageSafeStore(loggedStore, storeName, dropLargeMessage, maxMessageSize);
        }
        KeyValueStore<String, String> serializedStore =
            new SerializedKeyValueStore<>(toBeSerializedStore, stringSerde, stringSerde, serializedKeyValueStoreMetrics);
        store = new CachedStore<>(serializedStore, cacheSize, batchSize, cachedStoreMetrics);
        break;
      }
      //For this case, the value of dropLargeMessage doesn't matter since we are testing the case when
      // large messages are expected and StorageConfig.EXPECT_LARGE_MESSAGES is true.
      case "serde-then-cache" : {
        KeyValueStore<byte[], byte[]> cachedStore =
            new CachedStore<>(loggedStore, cacheSize, batchSize, cachedStoreMetrics);
        KeyValueStore<byte[], byte[]> largeMessageSafeStore =
            new LargeMessageSafeStore(cachedStore, storeName, dropLargeMessage, maxMessageSize);
        store = new SerializedKeyValueStore<>(largeMessageSafeStore, stringSerde, stringSerde,
            serializedKeyValueStoreMetrics);
        break;
      }
      default :
        throw new IllegalArgumentException("Store config undefined: " + storeConfig);
    }
    store = new NullSafeKeyValueStore<>(store);
  }

  @After
  public void teardown() {
    try {
      store.close();
    } catch (SamzaException e) {
      loggedStore.close();
    }
    if (dir != null && dir.listFiles() != null) {
      for (File file : dir.listFiles())
        file.delete();
      dir.delete();
    }
  }

  @Test
  public void testLargeMessagePutFailureForLoggedStoreWithWrappedStore() {
    String key = "test";
    String largeMessage = StringUtils.repeat("a", maxMessageSize + 1);

    if (dropLargeMessage) {
      store.put(key, largeMessage);
      Assert.assertNull("The large message was stored while it shouldn't have been.", loggedStore.get(stringSerde.toBytes(key)));
    } else {
      try {
        store.put(key, largeMessage);
        Assert.fail("Failure since put() method invocation incorrectly completed.");
      } catch (SamzaException e) {
        Assert.assertNull("The large message was stored while it shouldn't have been.", loggedStore.get(stringSerde.toBytes(key)));
      }
    }
  }

  @Test
  public void testLargeMessagePutAllFailureForLoggedStoreWithWrappedStore() {
    String key = "test";
    String largeMessage = StringUtils.repeat("a", maxMessageSize + 1);
    List<Entry<String, String>> entries = new ArrayList<>();
    entries.add(new Entry<>(key, largeMessage));

    if (dropLargeMessage) {
      store.putAll(entries);
      Assert.assertNull("The large message was stored while it shouldn't have been.", loggedStore.get(stringSerde.toBytes(key)));
    } else {
      try {
        store.putAll(entries);
        Assert.fail("Failure since putAll() method invocation incorrectly completed.");
      } catch (SamzaException e) {
        Assert.assertNull("The large message was stored while it shouldn't have been.", loggedStore.get(stringSerde.toBytes(key)));
      }
    }
  }

  @Test
  public void testSmallMessagePutSuccessForLoggedStoreWithWrappedStore() {
    String key = "test";
    String smallMessage = StringUtils.repeat("a", maxMessageSize - 1);

    store.put(key, smallMessage);
    Assert.assertEquals(store.get(key), smallMessage);
  }

  @Test
  public void testSmallMessagePutAllSuccessForLoggedStoreWithWrappedStore() {
    String key = "test";
    String smallMessage = StringUtils.repeat("a", maxMessageSize - 1);

    List<Entry<String, String>> entries = new ArrayList<>();
    entries.add(new Entry<>(key, smallMessage));

    store.putAll(entries);
    Assert.assertEquals(store.get(key), smallMessage);
  }
}
