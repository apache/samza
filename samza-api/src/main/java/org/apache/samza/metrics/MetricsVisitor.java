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

package org.apache.samza.metrics;

/**
 * A MetricsVisitor can be used to process each metric in a {@link org.apache.samza.metrics.ReadableMetricsRegistry},
 * encapsulating the logic of what to be done with each metric in the counter and gauge methods.  This makes it easy
 * to quickly process all of the metrics in a registry.
 */
public abstract class MetricsVisitor {
  public abstract void counter(Counter counter);

  public abstract <T> void gauge(Gauge<T> gauge);

  public abstract void timer(Timer timer);

  public abstract <T> void listGauge(ListGauge<T> listGauge);

  public void visit(Metric metric) {
    // Cast for metrics of type ListGauge
    if (metric instanceof ListGauge<?>) {
      listGauge((ListGauge<?>) metric);
    } else if (metric instanceof Counter) {
      counter((Counter) metric);
    } else if (metric instanceof Gauge<?>) {
      gauge((Gauge<?>) metric);
    } else if (metric instanceof Timer) {
      timer((Timer) metric);
    }
  }
}
