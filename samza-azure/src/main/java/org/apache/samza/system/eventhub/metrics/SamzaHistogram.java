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

package org.apache.samza.system.eventhub.metrics;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsVisitor;
import org.apache.samza.system.eventhub.SamzaEventHubClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Creates a {@link Histogram} metric using {@link ExponentiallyDecayingReservoir}
 * Keeps a {@link Gauge} for each percentile
 */
public class SamzaHistogram {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaEventHubClientManager.class.getName());
  private static final List<Double> DEFAULT_HISTOGRAM_PERCENTILES = Arrays.asList(50D, 99D);
  private final Histogram histogram;
  private final List<Double> percentiles;
  private final Map<Double, Gauge<Double>> gauges;

  public SamzaHistogram(MetricsRegistry registry, String group, String name) {
    this(registry, group, name, DEFAULT_HISTOGRAM_PERCENTILES);
  }

  public SamzaHistogram(MetricsRegistry registry, String group, String name, List<Double> percentiles) {
    this.histogram = new Histogram(new ExponentiallyDecayingReservoir());
    this.percentiles = percentiles;
    this.gauges = this.percentiles.stream()
        .filter(x -> x > 0 && x <= 100)
        .collect(Collectors.toMap(Function.identity(),
            x -> registry.newGauge(group, new HistogramGauge(name + "_" + String.valueOf(0), 0D))));
  }

  public void update(long value) {
    histogram.update(value);
  }

  public void updateGaugeValues() {
    Snapshot values = histogram.getSnapshot();
    percentiles.forEach(x -> gauges.get(x).set(values.getValue(x / 100)));
  }

  /**
   * Custom gauge whose value is set based on the underlying Histogram
   */
  private class HistogramGauge extends Gauge<Double> {
    public HistogramGauge(String name, double value) {
      super(name, value);
    }

    public void visit(MetricsVisitor visitor) {
      updateGaugeValues();
      visitor.gauge(this);
    }
  }
}
