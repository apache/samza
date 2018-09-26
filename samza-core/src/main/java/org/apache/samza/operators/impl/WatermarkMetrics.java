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

package org.apache.samza.operators.impl;

import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsBase;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class WatermarkMetrics extends MetricsBase {
  private final Map<SystemStreamPartition, Gauge<Long>> aggregates = new ConcurrentHashMap<>();

  WatermarkMetrics(MetricsRegistry registry) {
    super("watermark-", registry);
  }

  void setAggregateTime(SystemStreamPartition systemStreamPartition, long time) {
    final Gauge<Long> aggregate = aggregates.computeIfAbsent(systemStreamPartition,
        ssp -> newGauge(String.format("%s-%s-aggr-watermark",
        ssp.getStream(), ssp.getPartition().getPartitionId()), 0L));
    aggregate.set(time);
  }
}
