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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class Metrics {
  private final Map<String, Map<String, Object>> immutableMetrics;

  public Metrics() {
    this(Collections.emptyMap());
  }

  public Metrics(Map<String, Map<String, Object>> metrics) {
    Map<String, Map<String, Object>> metricsMapCopy = new HashMap<>();
    metrics.forEach((key, value) -> metricsMapCopy.put(key, Collections.unmodifiableMap(new HashMap<>(value))));
    this.immutableMetrics = metricsMapCopy;
  }

  public <T> T get(String group, String metricName) {
    return (T) this.immutableMetrics.get(group).get(metricName);
  }

  public Map<String, Object> get(String group) {
    return this.immutableMetrics.get(group);
  }

  public Map<String, Map<String, Object>> getAsMap() {
    return Collections.unmodifiableMap(this.immutableMetrics);
  }

  public static Metrics fromMap(Map<String, Map<String, Object>> map) {
    return new Metrics(map);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Metrics that = (Metrics) o;
    return Objects.equals(immutableMetrics, that.immutableMetrics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(immutableMetrics);
  }

  @Override
  public String toString() {
    return "Metrics{" + "immutableMetrics=" + immutableMetrics + '}';
  }
}