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
package org.apache.samza.config;

import java.util.Collections;
import java.util.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestMetricsConfig {
  @Test
  public void testGetMetricsFactoryClass() {
    String metricsReporterName = "metricReporterName";
    String metricsReporterValue = "metrics.reporter.class";
    Config config = new MapConfig(
        ImmutableMap.of(String.format(MetricsConfig.METRICS_REPORTER_FACTORY, metricsReporterName),
            metricsReporterValue));
    assertEquals(Optional.of(metricsReporterValue),
        new MetricsConfig(config).getMetricsFactoryClass(metricsReporterName));

    assertFalse(metricsReporterValue,
        new MetricsConfig(new MapConfig()).getMetricsFactoryClass("someName").isPresent());
  }

  @Test
  public void testGetMetricsSnapshotReporterStream() {
    String metricsReporterName = "metricReporterName";
    String value = "reporter-stream";
    Config config = new MapConfig(
        ImmutableMap.of(String.format(MetricsConfig.METRICS_SNAPSHOT_REPORTER_STREAM, metricsReporterName), value));
    assertEquals(Optional.of(value), new MetricsConfig(config).getMetricsSnapshotReporterStream(metricsReporterName));

    assertFalse(value, new MetricsConfig(new MapConfig()).getMetricsSnapshotReporterStream("someName").isPresent());
  }

  @Test
  public void testGetMetricsSnapshotReporterInterval() {
    String metricsReporterName = "metricReporterName";
    String value = "10";
    Config config = new MapConfig(
        ImmutableMap.of(String.format(MetricsConfig.METRICS_SNAPSHOT_REPORTER_INTERVAL, metricsReporterName), value));
    assertEquals(Optional.of(value), new MetricsConfig(config).getMetricsSnapshotReporterInterval(metricsReporterName));

    assertFalse(value, new MetricsConfig(new MapConfig()).getMetricsSnapshotReporterInterval("someName").isPresent());
  }

  @Test
  public void testGetMetricsSnapshotReporterBlacklist() {
    String metricsReporterName = "metricReporterName";
    String value = "metric0|metric1";
    Config config = new MapConfig(
        ImmutableMap.of(String.format(MetricsConfig.METRICS_SNAPSHOT_REPORTER_BLACKLIST, metricsReporterName), value));
    assertEquals(Optional.of(value),
        new MetricsConfig(config).getMetricsSnapshotReporterBlacklist(metricsReporterName));

    assertFalse(value, new MetricsConfig(new MapConfig()).getMetricsSnapshotReporterBlacklist("someName").isPresent());
  }

  @Test
  public void testGetMetricReporterNames() {
    Config config = new MapConfig(
        ImmutableMap.of(MetricsConfig.METRICS_REPORTERS, "reporter0.class, reporter1.class, reporter2.class "));
    assertEquals(ImmutableList.of("reporter0.class", "reporter1.class", "reporter2.class"),
        new MetricsConfig(config).getMetricReporterNames());

    Config configEmptyValue = new MapConfig(ImmutableMap.of(MetricsConfig.METRICS_REPORTERS, ""));
    assertEquals(Collections.emptyList(), new MetricsConfig(configEmptyValue).getMetricReporterNames());

    assertEquals(Collections.emptyList(), new MetricsConfig(new MapConfig()).getMetricReporterNames());
  }

  @Test
  public void testGetMetricsTimerEnabled() {
    Config config = new MapConfig(ImmutableMap.of(MetricsConfig.METRICS_TIMER_ENABLED, "true"));
    assertTrue(new MetricsConfig(config).getMetricsTimerEnabled());

    config = new MapConfig(ImmutableMap.of(MetricsConfig.METRICS_TIMER_ENABLED, "false"));
    assertFalse(new MetricsConfig(config).getMetricsTimerEnabled());

    assertTrue(new MetricsConfig(new MapConfig()).getMetricsTimerEnabled());
  }

  @Test
  public void testGetMetricsTimerDebugEnabled() {
    Config config = new MapConfig(ImmutableMap.of(MetricsConfig.METRICS_TIMER_DEBUG_ENABLED, "true"));
    assertTrue(new MetricsConfig(config).getMetricsTimerDebugEnabled());

    config = new MapConfig(ImmutableMap.of(MetricsConfig.METRICS_TIMER_DEBUG_ENABLED, "false"));
    assertFalse(new MetricsConfig(config).getMetricsTimerDebugEnabled());

    assertFalse(new MetricsConfig(new MapConfig()).getMetricsTimerDebugEnabled());
  }
}