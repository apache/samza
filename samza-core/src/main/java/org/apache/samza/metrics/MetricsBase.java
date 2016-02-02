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
 * A base class for metrics. The name of the class that extends the
 * base class will be used as the metric group name
 */
public abstract class MetricsBase {
  protected final MetricGroup group;

  public MetricsBase(String prefix, MetricsRegistry registry) {
    String groupName = this.getClass().getName();
    group = new MetricGroup(groupName, prefix, registry);
  }

  public MetricsBase(MetricsRegistry registry) {
    this("", registry);
  }

  public Counter newCounter(String name) {
    return group.newCounter(name);
  }

  public <T> Gauge<T> newGauge(String name, T value) {
    return group.newGauge(name, value);
  }

  public <T> Gauge<T> newGauge(String name, final MetricGroup.ValueFunction<T> valueFunc) {
    return group.newGauge(name, valueFunc);
  }

  public Timer newTimer(String name) {
    return group.newTimer(name);
  }
}
