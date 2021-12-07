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
package org.apache.samza.metrics.reporter;

import java.util.Map;
import java.util.Optional;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestMetricsSnapshot {
  private static final MetricsHeader METRICS_HEADER =
      new MetricsHeader("job", "id", "container", "container-id", Optional.of("epoch-123"), "source", "1.2", "3.4",
          "a.b.c", 100, 10);
  private static final Metrics METRICS =
      new Metrics(ImmutableMap.of("group0", ImmutableMap.of("a", "b"), "group1", ImmutableMap.of("c", "d")));

  @Test
  public void testGetAsMap() {
    MetricsSnapshot metricsSnapshot = new MetricsSnapshot(METRICS_HEADER, METRICS);
    Map<String, Object> expected = ImmutableMap.of("header", METRICS_HEADER.getAsMap(), "metrics", METRICS.getAsMap());
    assertEquals(expected, metricsSnapshot.getAsMap());
  }

  @Test
  public void testFromMap() {
    Map<String, Map<String, Object>> map =
        ImmutableMap.of("header", ImmutableMap.copyOf(METRICS_HEADER.getAsMap()), "metrics",
            ImmutableMap.copyOf((Map<String, Object>) (Map<?, ?>) METRICS.getAsMap()));
    MetricsSnapshot expected = new MetricsSnapshot(METRICS_HEADER, METRICS);
    assertEquals(expected, MetricsSnapshot.fromMap(map));
  }
}