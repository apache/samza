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
package org.apache.samza.serializers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class TestMetricsSnapshotSerde {
  private static final String SERIALIZED =
      "{\"header\":{\"job-id\":\"testjobid\",\"exec-env-container-id\":\"test exec env container id\",\"samza-version\":\"samzaversion\",\"job-name\":\"test-jobName\",\"host\":\"host\",\"reset-time\":2,\"container-name\":\"samza-container-0\",\"source\":\"test source\",\"time\":1,\"version\":\"version\"},\"metrics\":{\"test\":{\"test2\":\"foo\"}}}";

  @Test
  public void testMetricsSerdeSerializeAndDeserialize() {
    Map<String, Object> metricsMap = new HashMap<>();
    metricsMap.put("test2", "foo");
    Map<String, Map<String, Object>> metricsGroupMap = new HashMap<>();
    metricsGroupMap.put("test", metricsMap);
    MetricsSnapshot snapshot = new MetricsSnapshot(metricsHeader(), Metrics.fromMap(metricsGroupMap));
    MetricsSnapshotSerde serde = new MetricsSnapshotSerde();
    byte[] bytes = serde.toBytes(snapshot);
    assertEquals(snapshot, serde.fromBytes(bytes));
  }

  /**
   * Helps for testing compatibility against older versions of code.
   */
  @Test
  public void testMetricsSerdeSerialize() {
    Map<String, Object> metricsMap = new HashMap<>();
    metricsMap.put("test2", "foo");
    Map<String, Map<String, Object>> metricsGroupMap = new HashMap<>();
    metricsGroupMap.put("test", metricsMap);
    MetricsSnapshot snapshot = new MetricsSnapshot(metricsHeader(), Metrics.fromMap(metricsGroupMap));
    MetricsSnapshotSerde serde = new MetricsSnapshotSerde();
    assertArrayEquals(SERIALIZED.getBytes(StandardCharsets.UTF_8), serde.toBytes(snapshot));
  }

  /**
   * Helps for testing compatibility against older versions of code.
   */
  @Test
  public void testMetricsSerdeDeserialize() {
    Map<String, Object> metricsMap = new HashMap<>();
    metricsMap.put("test2", "foo");
    Map<String, Map<String, Object>> metricsGroupMap = new HashMap<>();
    metricsGroupMap.put("test", metricsMap);
    MetricsSnapshot snapshot = new MetricsSnapshot(metricsHeader(), Metrics.fromMap(metricsGroupMap));
    MetricsSnapshotSerde serde = new MetricsSnapshotSerde();
    assertEquals(snapshot, serde.fromBytes(SERIALIZED.getBytes(StandardCharsets.UTF_8)));
  }

  private static MetricsHeader metricsHeader() {
    return new MetricsHeader("test-jobName", "testjobid", "samza-container-0", "test exec env container id",
        "test source", "version", "samzaversion", "host", 1L, 2L);
  }
}
