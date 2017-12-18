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

package org.apache.samza.system.kinesis.metrics;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;


class SamzaHistogram {

  private static final List<Double> DEFAULT_HISTOGRAM_PERCENTILES = Arrays.asList(50D, 99D);
  private final MetricsRegistry registry;
  private final Histogram histogram;
  private final List<Double> percentiles;
  private final Map<Double, Gauge<Double>> gauges;

  SamzaHistogram(MetricsRegistry registry, String group, String name) {
    this(registry, group, name, DEFAULT_HISTOGRAM_PERCENTILES);
  }

  SamzaHistogram(MetricsRegistry registry, String group, String name, List<Double> percentiles) {
    this.registry = registry;
    this.histogram = new Histogram(new ExponentiallyDecayingReservoir());
    this.percentiles = percentiles;
    this.gauges = percentiles.stream()
        .filter(x -> x > 0 && x <= 100)
        .collect(
            Collectors.toMap(Function.identity(), x -> this.registry.newGauge(group, name + "_" + String.valueOf(0), 0D)));
  }

  synchronized void update(long value) {
    histogram.update(value);
    Snapshot values = histogram.getSnapshot();
    percentiles.forEach(x -> gauges.get(x).set(values.getValue(x / 100)));
  }
}
