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

import java.util.Collection;
import java.util.LinkedList;


/**
 * A ListGauge is a collection of {@link org.apache.samza.metrics.Gauge}. ListGauges are useful for maintaining, recording,
 * or collecting specific Gauge values over time. It is implemented as a {@link org.apache.samza.metrics.Metric}.
 * For example, the set of recent errors that have occurred.
 *
 * This current implementation uses a size-bound-policy and holds the N most-recent Gauge objects added to the list.
 * This bound N is configurable at instantiation time.
 * TODO: Support a time-based and size-and-time-based hybrid policy.
 * TODO: Add a derived class to do compaction for errors using hash-based errorIDs and adding timestamp for errors to dedup
 * on the read path.
 *
 * All public methods are thread-safe.
 *
 */
public class ListGauge implements Metric {
  private final String name;
  private final Collection<Gauge> metricList;
  private int nItems;

  /**
   * Create a new ListGauge.
   * @param name Name to be assigned
   * @param nItems The number of items to hold in the list
   */
  public ListGauge(String name, int nItems) {
    this.name = name;
    this.metricList = new LinkedList<>();
    this.nItems = nItems;
  }

  /**
   * Get the name assigned to the ListGauge
   * @return the assigned name
   */
  public String getName() {
    return this.name;
  }

  /**
   * Add a gauge to the list
   * @param value The Gauge value to be added
   */
  public synchronized void add(Gauge value) {
    if (this.metricList.size() == nItems) {
      ((LinkedList<Gauge>) this.metricList).removeFirst();
    }

    this.metricList.add(value);
  }

  /**
   * Get the Collection of Gauge values currently in the list
   * @return the collection of gauge values
   */
  public synchronized Collection<Gauge> getValue() {
    return this.metricList;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void visit(MetricsVisitor visitor) {
    visitor.listGauge(this);
  }
}
