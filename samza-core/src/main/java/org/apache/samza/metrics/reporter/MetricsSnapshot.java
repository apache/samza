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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class MetricsSnapshot {
  private static final String HEADER_KEY = "header";
  private static final String METRICS_KEY = "metrics";

  private final MetricsHeader metricsHeader;
  private final Metrics metrics;

  public MetricsSnapshot(MetricsHeader metricsHeader, Metrics metrics) {
    this.metricsHeader = metricsHeader;
    this.metrics = metrics;
  }

  public MetricsHeader getHeader() {
    return metricsHeader;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public Map<String, Object> getAsMap() {
    Map<String, Object> map = new HashMap<>();
    map.put(HEADER_KEY, this.metricsHeader.getAsMap());
    map.put(METRICS_KEY, this.metrics.getAsMap());
    return map;
  }

  public static MetricsSnapshot fromMap(Map<String, Map<String, Object>> map) {
    MetricsHeader metricsHeader = MetricsHeader.fromMap(map.get(HEADER_KEY));
    Metrics metrics = Metrics.fromMap((Map<String, Map<String, Object>>) (Map<?, ?>) map.get(METRICS_KEY));
    return new MetricsSnapshot(metricsHeader, metrics);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricsSnapshot that = (MetricsSnapshot) o;
    return Objects.equals(metricsHeader, that.metricsHeader) && Objects.equals(metrics, that.metrics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricsHeader, metrics);
  }

  @Override
  public String toString() {
    return "MetricsSnapshot{" + "metricsHeader=" + metricsHeader + ", metrics=" + metrics + '}';
  }
}