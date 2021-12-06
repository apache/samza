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
import java.util.Optional;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestMetricsSnapshotSerde {
  @Test
  public void testSerializeThenDeserialize() {
    MetricsSnapshot snapshot = metricsSnapshot(true);
    MetricsSnapshotSerde serde = new MetricsSnapshotSerde();
    byte[] bytes = serde.toBytes(snapshot);
    assertEquals(snapshot, serde.fromBytes(bytes));
  }

  @Test
  public void testSerializeThenDeserializeEmptySamzaEpochIdInHeader() {
    MetricsSnapshot snapshot = metricsSnapshot(true);
    MetricsSnapshotSerde serde = new MetricsSnapshotSerde();
    byte[] bytes = serde.toBytes(snapshot);
    assertEquals(snapshot, serde.fromBytes(bytes));
  }

  /**
   * Helps for verifying compatibility when schemas evolve.
   *
   * Maps have non-deterministic ordering when serialized, so it is difficult to check exact serialized results. It
   * isn't really necessary to check the serialized results anyways. We just need to make sure serialized data can be
   * read by old and new systems.
   */
  @Test
  public void testDeserializeRaw() {
    MetricsSnapshot snapshot = metricsSnapshot(true);
    MetricsSnapshotSerde serde = new MetricsSnapshotSerde();
    assertEquals(snapshot, serde.fromBytes(expectedSeralizedSnapshot(true, false).getBytes(StandardCharsets.UTF_8)));
    assertEquals(snapshot, serde.fromBytes(expectedSeralizedSnapshot(true, true).getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * Helps for verifying compatibility when schemas evolve.
   */
  @Test
  public void testDeserializeRawEmptySamzaEpochIdInHeader() {
    MetricsSnapshot snapshot = metricsSnapshot(false);
    MetricsSnapshotSerde serde = new MetricsSnapshotSerde();
    assertEquals(snapshot, serde.fromBytes(expectedSeralizedSnapshot(false, false).getBytes(StandardCharsets.UTF_8)));
    assertEquals(snapshot, serde.fromBytes(expectedSeralizedSnapshot(false, true).getBytes(StandardCharsets.UTF_8)));
  }

  private static String expectedSeralizedSnapshot(boolean includeSamzaEpochId,
      boolean includeExtraHeaderField) {
    String serializedSnapshot =
        "{\"header\":{\"job-id\":\"testjobid\",\"exec-env-container-id\":\"test exec env container id\",";
    if (includeSamzaEpochId) {
      serializedSnapshot += "\"samza-epoch-id\":\"epoch-123\",";
    }
    if (includeExtraHeaderField) {
      serializedSnapshot += "\"extra-header-field\":\"extra header value\",";
    }
    serializedSnapshot +=
        "\"samza-version\":\"samzaversion\",\"job-name\":\"test-jobName\",\"host\":\"host\",\"reset-time\":2,"
            + "\"container-name\":\"samza-container-0\",\"source\":\"test source\",\"time\":1,\"version\":\"version\"},"
            + "\"metrics\":{\"test\":{\"test2\":\"foo\"}}}";
    return serializedSnapshot;
  }

  private static MetricsSnapshot metricsSnapshot(boolean includeSamzaEpochId) {
    MetricsHeader metricsHeader;
    if (includeSamzaEpochId) {
      metricsHeader = new MetricsHeader("test-jobName", "testjobid", "samza-container-0", "test exec env container id",
          Optional.of("epoch-123"), "test source", "version", "samzaversion", "host", 1L, 2L);
    } else {
      metricsHeader = new MetricsHeader("test-jobName", "testjobid", "samza-container-0", "test exec env container id",
          "test source", "version", "samzaversion", "host", 1L, 2L);
    }
    Map<String, Object> metricsMap = new HashMap<>();
    metricsMap.put("test2", "foo");
    Map<String, Map<String, Object>> metricsGroupMap = new HashMap<>();
    metricsGroupMap.put("test", metricsMap);
    return new MetricsSnapshot(metricsHeader, Metrics.fromMap(metricsGroupMap));
  }
}
