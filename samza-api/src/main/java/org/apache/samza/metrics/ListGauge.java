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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * A {@link ListGauge} is a {@link org.apache.samza.metrics.Metric} that buffers multiple instances of a type T in a list.
 * {@link ListGauge}s are useful for maintaining, recording, or collecting values over time.
 * For example, a set of specific logging-events (e.g., errors).
 *
 * Eviction from list is either done by consuming-code using the remove APIs or by specifying an eviction policy
 * at creation time.
 *
 * All public methods are thread-safe.
 *
 */
public class ListGauge<T> implements Metric {
  private final String name;
  private final List<T> metricList;
  private ListGaugeEvictionPolicy<T> listGaugeEvictionPolicy;

  private final static int DEFAULT_POLICY_NUM_RETAIN = 60;

  /**
   * Create a new {@link ListGauge} with no auto eviction, callers can add/remove items as desired.
   * @param name Name to be assigned
   */
  public ListGauge(String name) {
    this.name = name;
    this.metricList = new ArrayList<T>(DEFAULT_POLICY_NUM_RETAIN);
    this.listGaugeEvictionPolicy = new RetainLastNPolicy<T>(this, DEFAULT_POLICY_NUM_RETAIN);
  }

  /**
   * Get the name assigned to this {@link ListGauge}
   * @return the assigned name
   */
  public String getName() {
    return this.name;
  }

  /**
   * Get the Collection of Gauge values currently in the list, used when serializing this Gauge.
   * @return the collection of gauge values
   */
  public synchronized Collection<T> getValue() {
    return Collections.unmodifiableList(this.metricList);
  }

  /**
   * Package-private method to change the eviction policy
   * @param listGaugeEvictionPolicy
   */
  synchronized void setEvictionPolicy(ListGaugeEvictionPolicy<T> listGaugeEvictionPolicy) {
    this.listGaugeEvictionPolicy = listGaugeEvictionPolicy;
  }

  /**
   * Add a gauge to the list
   * @param value The Gauge value to be added
   */
  public synchronized void add(T value) {
    this.metricList.add(value);

    // notify the policy object (if one is present), for performing any eviction that may be needed.
    // note: monitor is being held
    if (this.listGaugeEvictionPolicy != null) {
      this.listGaugeEvictionPolicy.elementAddedCallback();
    }
  }

  public synchronized boolean remove(T value) {
    return this.metricList.remove(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void visit(MetricsVisitor visitor) {
    visitor.listGauge(this);
  }
}
