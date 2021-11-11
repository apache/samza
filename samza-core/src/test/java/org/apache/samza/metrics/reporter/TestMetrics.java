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
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestMetrics {
  @Test
  public void testMetrics() {
    Map<String, Object> group0Metrics = ImmutableMap.of("int-value", 10, "boolean-value", true);
    Map<String, Object> group1Metrics =
        ImmutableMap.of("string-value", "str", "map-value", ImmutableMap.of("a", "aa", "b", "bb"));
    Map<String, Map<String, Object>> metricsMap = ImmutableMap.of("group0", group0Metrics, "group1", group1Metrics);
    Metrics metrics = new Metrics(metricsMap);
    ImmutableMap<String, Object> group0MetricsCopy = ImmutableMap.copyOf(group0Metrics);
    ImmutableMap<String, Object> group1MetricsCopy = ImmutableMap.copyOf(group1Metrics);
    assertEquals(ImmutableMap.of("group0", group0MetricsCopy, "group1", group1MetricsCopy), metrics.getAsMap());
    assertEquals(group0MetricsCopy, metrics.get("group0"));
    assertEquals(group1MetricsCopy, metrics.get("group1"));
    assertNull(metrics.get("group2"));
    assertEquals(new Integer(10), metrics.get("group0", "int-value"));
    assertEquals(Boolean.TRUE, metrics.get("group0", "boolean-value"));
    assertNull(metrics.get("group0", "key-does-not-exist"));
    assertEquals("str", metrics.get("group1", "string-value"));
    assertEquals(ImmutableMap.of("a", "aa", "b", "bb"), metrics.get("group1", "map-value"));
  }
}