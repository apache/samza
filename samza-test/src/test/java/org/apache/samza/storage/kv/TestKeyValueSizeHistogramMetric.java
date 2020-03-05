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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsVisitor;
import org.apache.samza.metrics.SamzaHistogram;
import org.apache.samza.metrics.Timer;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * This class is used to test whether the key value size metrics in {@link SerializedKeyValueStoreMetrics} works correctly.
 */
public class TestKeyValueSizeHistogramMetric {

  private static String storeName = "testStore";
  private static MetricsRegistryMap metricsRegistry = new MetricsRegistryMap();
  private static KeyValueStoreMetrics keyValueStoreMetrics = new KeyValueStoreMetrics(storeName, metricsRegistry);
  private static SerializedKeyValueStoreMetrics serializedKeyValueStoreMetrics =
      new SerializedKeyValueStoreMetrics(storeName, metricsRegistry);
  private static Serde<String> stringSerde = new StringSerde();
  private static Random random = new Random();

  private KeyValueStore<String, String> store = null;

  @Before
  public void setup() {
    KeyValueStore<byte[], byte[]> kvStore = new InMemoryKeyValueStore(keyValueStoreMetrics);
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

    for (int i = 0; i < 1000; i++) {
      keys.add(getRandomString());
      values.add(getRandomString());
    }

    Collections.shuffle(keys);
    Collections.shuffle(values);

    for (int i = 0; i < keys.size(); i++) {
      store.put(keys.get(i), values.get(i));
    }

    metricsRegistry.getGroups().forEach(group -> metricsRegistry.getGroup(group.toString()).forEach((name, metric) -> {
      if (name.contains("size-bytes-histogram")) {
        metric.visit(new MetricsVisitor() {
          @Override
          public void counter(Counter counter) {

          }

          @Override
          public <T> void gauge(Gauge<T> gauge) {
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
    int maxLength = 1000;

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
