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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsVisitor;
import org.apache.samza.metrics.Timer;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.WriteOptions;


/**
 * This class is used to test whether the key value size metrics in {@link SerializedKeyValueStoreMetrics} works correctly.
 */
public class TestKeyValueSizeHistogramMetric {

  private static String storeName = "teststore";
  private static String keyPrefix = "key-size-bytes-histogram";
  private static String valuePrefix = "value-size-bytes-histogram";
  private static MetricsRegistryMap metricsRegistry = new MetricsRegistryMap();
  private static KeyValueStoreMetrics keyValueStoreMetrics = new KeyValueStoreMetrics(storeName, metricsRegistry);
  private static SerializedKeyValueStoreMetrics serializedKeyValueStoreMetrics =
      new SerializedKeyValueStoreMetrics(storeName, metricsRegistry);
  private static Serde<String> stringSerde = new StringSerde();
  private static Random random = new Random();

  private KeyValueStore<String, String> store = null;

  @Before
  public void setup() {

    Config config = new MapConfig();
    Options options = new Options();
    options.setCreateIfMissing(true);

    File dbDir = new File(System.getProperty("java.io.tmpdir") + "/dbStore" + System.currentTimeMillis());
    RocksDbKeyValueStore kvStore = new RocksDbKeyValueStore(dbDir, options, config, false, "dbStore",
        new WriteOptions(), new FlushOptions(), new KeyValueStoreMetrics("dbStore", new MetricsRegistryMap()));
    KeyValueStore<String, String> serializedStore =
        new SerializedKeyValueStore<>(kvStore, stringSerde, stringSerde, serializedKeyValueStoreMetrics);
    store = new NullSafeKeyValueStore<>(serializedStore);
  }

  /**
   * Make sure that the histograms can record the key value size and we can use a
   * {@link MetricsVisitor} to get access to the value store in the histograms
   */
  @Test
  public void testHistogramMetric() {

    List<String> keys = new ArrayList<>();
    List<String> values = new ArrayList<>();

    for (int i = 0; i < 300; i++) {
      keys.add(getRandomString());
      values.add(getRandomString());
    }

    for (int i = 0; i < keys.size(); i++) {
      store.put(keys.get(i), values.get(i));
    }

    Set<String> names = new HashSet<>();
    for (Double p : serializedKeyValueStoreMetrics.record_key_size_percentiles()) {
      names.add(storeName + "-" + keyPrefix + "_" + p);
    }

    for (Double p : serializedKeyValueStoreMetrics.record_value_size_percentiles()) {
      names.add(storeName + "-" + valuePrefix + "_" + p);
    }

    metricsRegistry.getGroups().forEach(group -> metricsRegistry.getGroup(group.toString()).forEach((name, metric) -> {
      if (names.contains(name)) {
        metric.visit(new MetricsVisitor() {
          @Override
          public void counter(Counter counter) {

          }

          @Override
          public <T> void gauge(Gauge<T> gauge) {
            Double num = (Double) gauge.getValue();
            Assert.assertNotEquals(0D, (Double) gauge.getValue(), 0.0001);
          }

          @Override
          public void timer(Timer timer) {

          }
        });
      }
    }));
  }

  private String getRandomString() {
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    int maxLength = 100;

    String generatedString = random.ints(leftLimit, rightLimit + 1)
        .limit(random.nextInt(maxLength))
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();

    return generatedString;
  }

  @After
  public void teardown() {
    try {
      store.close();
    } catch (SamzaException e) {
      e.printStackTrace();
    }
  }
}
