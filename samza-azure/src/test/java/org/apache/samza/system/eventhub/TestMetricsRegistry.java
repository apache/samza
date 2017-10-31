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

package org.apache.samza.system.eventhub;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestMetricsRegistry implements MetricsRegistry {

  private Map<String, List<Counter>> counters = new HashedMap<>();
  private Map<String, List<Gauge<?>>> gauges = new HashedMap<>();

  public List<Counter> getCounters(String groupName) {
    return counters.get(groupName);
  }

  public List<Gauge<?>> getGauges(String groupName) {
    return gauges.get(groupName);
  }

  @Override
  public Counter newCounter(String group, String name) {
    if (!counters.containsKey(group)) {
      counters.put(group, new ArrayList<>());
    }
    Counter c = new Counter(name);
    counters.get(group).add(c);
    return c;
  }

  @Override
  public <T> Gauge<T> newGauge(String group, String name, T value) {
    if (!gauges.containsKey(group)) {
      gauges.put(group, new ArrayList<>());
    }

    Gauge<T> g = new Gauge<>(name, value);
    gauges.get(group).add(g);
    return g;
  }

  @Override
  public Counter newCounter(String group, Counter counter) {
    return null;
  }

  @Override
  public <T> Gauge<T> newGauge(String group, Gauge<T> value) {
    return null;
  }

  @Override
  public Timer newTimer(String group, String name) {
    return null;
  }

  @Override
  public Timer newTimer(String group, Timer timer) {
    return null;
  }
}
