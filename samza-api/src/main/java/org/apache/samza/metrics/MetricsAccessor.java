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

import java.util.Map;


/**
 * A MetricsAccessor allows users to retrieve metric values, based on group name and metric name,
 * though specific metrics system, such as JMX.
 */
public interface MetricsAccessor {
  /**
   * Get the values of a counter
   * @param group Group for the counter, e.g. org.apache.samza.container.SamzaContainerMetrics
   * @param counter Name of the counter, e.g. commit-calls
   * @return A map of counter values, keyed by type, e.g. {"samza-container-0": 100L}
   */
  Map<String, Long> getCounterValues(String group, String counter);

  /**
   * Get the values of a gauge
   * @param group Group for the gauge, e.g. org.apache.samza.container.SamzaContainerMetrics
   * @param gauge Name of the gauge, e.g. event-loop-utilization
   * @param <T> Type of the gauge value, e.g. Double
   * @return A map of gauge values, keyed by type, e.g. {"samza-container-0": 0.8}
   */
  <T> Map<String, T> getGaugeValues(String group, String gauge);

  /**
   * Get the values of a timer
   * @param group Group for the timer, e.g. org.apache.samza.container.SamzaContainerMetrics
   * @param timer Name of the timer, e.g. choose-ns
   * @return A map of timer values, keyed by type, e.g. {"samza-container-0": 10.5}
   */
  Map<String, Double> getTimerValues(String group, String timer);
}