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

package org.apache.samza.metrics

import org.apache.samza.clustermanager.SamzaApplicationState
import org.apache.samza.config.Config
import org.apache.samza.config.MetricsConfig.Config2Metrics
import org.apache.samza.util.Logging
import org.apache.samza.util.MetricsReporterLoader

import scala.collection.JavaConverters._

object ContainerProcessManagerMetrics {
  val sourceName = "ApplicationMaster"
}

/**
 * Responsible for wiring up Samza's metrics. Given that Samza has a metric
 * registry, we might as well use it. This class takes Samza's application
 * master state, and converts it to metrics.
 */
class ContainerProcessManagerMetrics(
                                      val config: Config,
                                      val state: SamzaApplicationState,
                                      val registry: ReadableMetricsRegistry) extends MetricsHelper  with Logging {

  val jvm = new JvmMetrics(registry)
  val reporters = MetricsReporterLoader.getMetricsReporters(config, ContainerProcessManagerMetrics.sourceName).asScala
  reporters.values.foreach(_.register(ContainerProcessManagerMetrics.sourceName, registry))

   def start() {
    val mRunningContainers = newGauge("running-containers", () => state.runningContainers.size)
    val mNeededContainers = newGauge("needed-containers", () => state.neededContainers.get())
    val mCompletedContainers = newGauge("completed-containers", () => state.completedContainers.get())
    val mFailedContainers = newGauge("failed-containers", () => state.failedContainers.get())
    val mReleasedContainers = newGauge("released-containers", () => state.releasedContainers.get())
    val mContainers = newGauge("container-count", () => state.containerCount)
    val mRedundantNotifications = newGauge("redundant-notifications", () => state.redundantNotifications.get())
    val mJobHealthy = newGauge("job-healthy", () => if (state.jobHealthy.get()) 1 else 0)
    val mPreferredHostRequests = newGauge("preferred-host-requests", () => state.preferredHostRequests.get())
    val mAnyHostRequests = newGauge("any-host-requests", () => state.anyHostRequests.get())
    val mExpiredPreferredHostRequests = newGauge("expired-preferred-host-requests", () => state.expiredPreferredHostRequests.get())
    val mExpiredAnyHostRequests = newGauge("expired-any-host-requests", () => state.expiredAnyHostRequests.get())

    val mHostAffinityMatchPct = newGauge("host-affinity-match-pct", () => {
      val numPreferredHostRequests = state.preferredHostRequests.get()
      val numExpiredPreferredHostRequests = state.expiredPreferredHostRequests.get()
      if (numPreferredHostRequests != 0) {
            100.00 * (numPreferredHostRequests - numExpiredPreferredHostRequests) / numPreferredHostRequests
          } else {
            0L
          }
      })

     val mFailedStandbyAllocations = newGauge("failed-standby-allocations", () => state.failedStandbyAllocations.get())
     val mSuccessfulStandbyAllocations = newGauge("successful-standby-allocations", () => state.successfulStandbyAllocations.get())
     val mFailoversToAnyHost = newGauge("failovers-to-any-host", () => state.failoversToAnyHost.get())
     val mFailoversToStandby = newGauge("failovers-to-standby", () => state.failoversToStandby.get())

    jvm.start
    reporters.values.foreach(_.start)
  }

   def stop() {
    reporters.values.foreach(_.stop)
  }
}
