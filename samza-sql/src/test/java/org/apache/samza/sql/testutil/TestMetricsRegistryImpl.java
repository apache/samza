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
package org.apache.samza.sql.testutil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.ListGauge;
import org.apache.samza.metrics.Timer;


/**
 * TestMetricsRegistryImpl implements the MetricRegistry interface and adds get APIs
 * for testing Translators.
 */
public class TestMetricsRegistryImpl implements org.apache.samza.metrics.MetricsRegistry {
  private Map<String, List<Counter>> counters = new HashMap<>();
  private Map<String, List<Timer>> timers = new HashMap<>();
  private Map<String, List<Gauge<?>>> gauges = new HashMap<>();
  private Map<String, List<ListGauge>> listGauges = new HashMap<>();

  @Override
  public Counter newCounter(String group, String name) {
    Counter counter = new Counter(name);
    return newCounter(group, counter);
  }

  @Override
  public Counter newCounter(String group, Counter counter) {
    if (!counters.containsKey(group)) {
      counters.put(group, new ArrayList<>());
    }
    counters.get(group).add(counter);
    return counter;
  }

  /**
   * retrieves the Map of Counters
   * @return counters
   */
  public Map<String, List<Counter>> getCounters() {
    return counters;
  }

  @Override
  public Timer newTimer(String group, String name) {
    Timer timer = new Timer(name);
    return newTimer(group, timer);
  }

  @Override
  public Timer newTimer(String group, Timer timer) {
    if (!timers.containsKey(group)) {
      timers.put(group, new ArrayList<>());
    }
    timers.get(group).add(timer);
    return timer;
  }

  /**
   * retrieves the Map of Timers
   * @return timers
   */
  public Map<String, List<Timer>> getTimers() {
    return timers;
  }

  @Override
  public <T> Gauge<T> newGauge(String group, String name, T value) {
    Gauge<T> gauge = new Gauge<>(name, value);
    return newGauge(group, gauge);
  }

  @Override
  public <T> Gauge<T> newGauge(String group, Gauge<T> gauge) {
    if (!gauges.containsKey(group)) {
      gauges.put(group, new ArrayList<>());
    }
    gauges.get(group).add(gauge);
    return gauge;
  }

  /**
   * retrieves the Map of Gauges
   * @return gauges
   */
  public Map<String, List<Gauge<?>>> getGauges() {
    return gauges;
  }

  @Override
  public ListGauge newListGauge(String group, ListGauge listGauge) {
    listGauges.putIfAbsent(group, new ArrayList());
    listGauges.get(group).add(listGauge);
    return listGauge;
  }

}
