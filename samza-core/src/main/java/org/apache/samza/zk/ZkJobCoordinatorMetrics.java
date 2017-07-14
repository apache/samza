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
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;


public class ZkJobCoordinatorMetrics extends MetricsBase {

  private final MetricsRegistry metricsRegistry;

  /**
   * Denotes if the processor is a leader or not
   */
  public final Gauge<Boolean> isLeader;

  /**
   * Number of times a barrier was created by the leader
   */
  public final Counter barrierCreation;

  /**
   * Number of times the barrier state changed
   */
  public final Counter barrierStateChange;

  /**
   * Number of times the barrier encountered an error while attaining consensus on the job model version
   */
  public final Counter barrierError;

  /**
   * Average time taken for all the processors to get the latest version of the job model after single
   * processor change (without the occurence of a barrier timeout)
   */
  public final Timer singleBarrierRebalancingTime;

  public ZkJobCoordinatorMetrics(MetricsRegistry metricsRegistry) {
    super(metricsRegistry);
    this.metricsRegistry = metricsRegistry;
    this.isLeader = newGauge("is-leader", false);
    this.barrierCreation = newCounter("barrier-creation");
    this.barrierStateChange = newCounter("barrier-state-change");
    this.barrierError = newCounter("barrier-error");
    this.singleBarrierRebalancingTime = newTimer("single-barrier-rebalancing-time");
  }

  public MetricsRegistry getMetricsRegistry() {
    return this.metricsRegistry;
  }

}
