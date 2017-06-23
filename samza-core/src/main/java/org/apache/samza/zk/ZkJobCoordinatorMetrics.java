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

package org.apache.samza.zk;


import org.apache.samza.metrics.MetricsBase;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.metrics.Timer;


public class ZkJobCoordinatorMetrics extends MetricsBase {

  private final ReadableMetricsRegistry metricsRegistry;

  public final Counter reads;
  public final Counter writes;
  public final Counter subscriptions;
  public final Counter zkConnectionError;
  public final Gauge<Integer> isLeader;
  public final Counter barrierCreation;
  public final Counter barrierStateChange;
  public final Counter barrierError;
  public final Timer singleBarrierRebalancingTime;

  public ZkJobCoordinatorMetrics(ReadableMetricsRegistry metricsRegistry) {
    super(metricsRegistry);
    this.metricsRegistry = metricsRegistry;
    this.reads = newCounter("reads");
    this.writes = newCounter("writes");
    this.subscriptions = newCounter("subscriptions");
    this.zkConnectionError = newCounter("zk-connection-error");
    this.isLeader = newGauge("is-leader", 0);
    this.barrierCreation = newCounter("barrier-creation");
    this.barrierStateChange = newCounter("barrier-state-change");
    this.barrierError = newCounter("barrier-error");
    this.singleBarrierRebalancingTime = newTimer("single-barrier-rebalancing-time");
  }

  public ReadableMetricsRegistry getMetricsRegistry() {
    return this.metricsRegistry;
  }

}
