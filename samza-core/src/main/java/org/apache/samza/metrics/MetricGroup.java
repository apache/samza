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
 * MetricGroup is a little helper class to make it easy to register and
 * manage a group of counters, gauges and timers.  It's shared between Java
 * and Scala
 */
public class MetricGroup {

  public interface ValueFunction<T> {
    T getValue();
  }

  protected final MetricsRegistry registry;
  protected final String groupName;
  protected final String prefix;

  public MetricGroup(String groupName, String prefix, MetricsRegistry registry) {
    this.groupName = groupName;
    this.registry = registry;
    this.prefix = prefix;
  }

  public Counter newCounter(String name) {
    return registry.newCounter(groupName, (prefix + name).toLowerCase());
  }

  public <T> Gauge<T> newGauge(String name, T value) {
    return registry.newGauge(groupName, new Gauge<T>((prefix + name).toLowerCase(), value));
  }

  /*
   * Specify a dynamic gauge that always returns the latest value when polled.
   * The value closure/object must be thread safe, since metrics reporters may access
   * it from another thread.
   */
  public <T> Gauge<T> newGauge(String name, final ValueFunction<T> valueFunc) {
    return registry.newGauge(groupName, new Gauge<T>((prefix + name).toLowerCase(), valueFunc.getValue()) {
      @Override
      public T getValue() {
        return valueFunc.getValue();
      }
    });
  }

  public Timer newTimer(String name) {
    return registry.newTimer(groupName, (prefix + name).toLowerCase());
  }
}
