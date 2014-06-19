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
 * A MetricsRegistry allows its users to create new {@link org.apache.samza.metrics.Metric}s and
 * have those metrics wired to specific metrics systems, such as JMX, provided by {@link org.apache.samza.metrics.MetricsReporter}s.
 * Those implementing Samza jobs use the MetricsRegistry to register metrics, which then handle
 * the details of getting those metrics to each defined MetricsReporter.
 *
 * Users are free to define their metrics into groups as needed for their jobs. {@link org.apache.samza.metrics.MetricsReporter}s
 * will likely use the group field to group the user-defined metrics together.
 */
public interface MetricsRegistry {
  /**
   * Create and register a new {@link org.apache.samza.metrics.Counter}
   * @param group Group for this Counter
   * @param name Name of to-be-created Counter
   * @return New Counter instance
   */
  Counter newCounter(String group, String name);

  /**
   * Register existing {@link org.apache.samza.metrics.Counter} with this registry
   * @param group Group for this Counter
   * @param counter Existing Counter to register
   * @return Counter that was registered
   */
  Counter newCounter(String group, Counter counter);

  /**
   * Create and register a new {@link org.apache.samza.metrics.Gauge}
   * @param group Group for this Gauge
   * @param name Name of to-be-created Gauge
   * @param value Initial value for the Gauge
   * @param <T> Type the Gauge will be wrapping
   * @return Gauge was created and registered
   */
  <T> Gauge<T> newGauge(String group, String name, T value);

  /**
   * Register an existing {@link org.apache.samza.metrics.Gauge}
   * @param group Group for this Gauge
   * @param value Initial value for the Gauge
   * @param <T> Type the Gauge will be wrapping
   * @return Gauge was registered
   */
  <T> Gauge<T> newGauge(String group, Gauge<T> value);
}
