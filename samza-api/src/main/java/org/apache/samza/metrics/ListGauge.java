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

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.samza.util.TimestampedValue;


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
  private final Queue<TimestampedValue<T>> elements;
  private final DefaultListGaugeEvictionPolicy<T> listGaugeEvictionPolicy;

  private final static int DEFAULT_MAX_NITEMS = 1000;
  private final static Duration DEFAULT_MAX_STALENESS = Duration.ofMinutes(60);
  private final static Duration DEFAULT_EVICTION_CHECK_PERIOD = Duration.ofMinutes(1);

  /**
   * Create a new {@link ListGauge} that auto evicts based on the given maxNumberOfItems, maxStaleness, and period parameters.
   *
   * @param name Name to be assigned
   * @param maxNumberOfItems The max number of items that can remain in the listgauge
   * @param maxStaleness The max staleness of items permitted in the listgauge
   * @param period The periodicity with which the listGauge would be checked for stale values.
   */
  public ListGauge(String name, int maxNumberOfItems, Duration maxStaleness, Duration period) {
    this.name = name;
    this.elements = new ConcurrentLinkedQueue<TimestampedValue<T>>();
    this.listGaugeEvictionPolicy =
        new DefaultListGaugeEvictionPolicy<T>(this.elements, maxNumberOfItems, maxStaleness, period);
  }

  /**
   * Create a new {@link ListGauge} that auto evicts upto a max of 100 items and a max-staleness of 60 minutes.
   * @param name Name to be assigned
   */
  public ListGauge(String name) {
    this(name, DEFAULT_MAX_NITEMS, DEFAULT_MAX_STALENESS, DEFAULT_EVICTION_CHECK_PERIOD);
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
  public Collection<T> getValues() {
    return Collections.unmodifiableList(this.elements.stream().map(x -> x.getValue()).collect(Collectors.toList()));
  }

  /**
   * Add a value to the list.
   * (Timestamp assigned to this value is the current timestamp.)
   * @param value The Gauge value to be added
   */
  public void add(T value) {
    this.elements.add(new TimestampedValue<T>(value, Instant.now().toEpochMilli()));

    // notify the policy object for performing any eviction that may be needed.
    this.listGaugeEvictionPolicy.elementAddedCallback();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void visit(MetricsVisitor visitor) {
    visitor.listGauge(this);
  }


}
