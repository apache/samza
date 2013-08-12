/*
 *
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
 *
 */

package org.apache.samza.system.kafka

import org.apache.samza.metrics.{Gauge, Counter, MetricsRegistry}
import collection.mutable
import kafka.common.TopicAndPartition

private[kafka] trait BrokerProxyMetrics {
  self:BrokerProxy =>
  // TODO: Move topic-partition specific metrics out of brokerproxy, into system
  val metricsRegistry:MetricsRegistry

  def newCounter = metricsRegistry.newCounter(metricsGroup, _:String)

  val hostPort = host + ":"  + port
  val metricsGroup = "samza.kafka.brokerproxy"

  // Counters
  val reconnectCounter = newCounter("%s-Reconnects" format hostPort)



  // Gauges
  val lagGauges = mutable.Map[TopicAndPartition, Gauge[Long]]()
  def getLagGauge(tp:TopicAndPartition) = lagGauges.getOrElseUpdate(tp, metricsRegistry.newGauge[Long](metricsGroup, "%s-MessagesBehindHighWaterMark" format tp, 0l))

  val tpGauge = metricsRegistry.newGauge[Long](metricsGroup, "%s-NumberOfTopicsPartitions" format hostPort, 0)

  def tpGaugeInc = tpGauge.set(tpGauge.getValue + 1l)

  def tpGaugeDec = tpGauge.set(tpGauge.getValue - 1l)
}
