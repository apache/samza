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
import org.apache.samza.config.{ClusterManagerConfig, Config, MetricsConfig}
import org.apache.samza.util.Logging

/**
  * Responsible for wiring up Samza's metrics. Given that Samza has a metric
  * registry, we might as well use it. This class takes Samza's application
  * master state, and converts it to metrics.
  */
class ContainerProcessManagerMetrics(val config: Config,
  val state: SamzaApplicationState,
  val registry: ReadableMetricsRegistry) extends MetricsHelper with Logging {
  val clusterManagerConfig = new ClusterManagerConfig(config)

  val mRunningContainers = newGauge("running-containers", () => state.runningProcessors.size)
  val mNeededContainers = newGauge("needed-containers", () => state.neededProcessors.get())
  val mCompletedContainers = newGauge("completed-containers", () => state.completedProcessors.get())
  val mFailedContainers = newGauge("failed-containers", () => state.failedContainers.get())
  val mReleasedContainers = newGauge("released-containers", () => state.releasedContainers.get())
  val mContainers = newGauge("container-count", () => state.processorCount.get())
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
  val mFailoversToAnyHost = newGauge("failovers-to-any-host", () => state.failoversToAnyHost.get())
  val mFailoversToStandby = newGauge("failovers-to-standby", () => state.failoversToStandby.get())

  val mFailedContainerPlacementActions = newGauge("failed-container-placements-actions", () => state.failedContainerPlacementActions.get())

  val mContainerMemoryMb = newGauge("container-memory-mb", () => clusterManagerConfig.getContainerMemoryMb)
  val mContainerCpuCores = newGauge("container-cpu-cores", () => clusterManagerConfig.getNumCores)

  val mFaultDomainAwareContainerRequests = newGauge("fault-domain-aware-container-requests", () => state.faultDomainAwareContainerRequests.get())
  val mFaultDomainAwareContainersStarted = newGauge("fault-domain-aware-containers-started", () => state.faultDomainAwareContainersStarted.get())
  val mExpiredFaultDomainAwareContainerRequests = newGauge("expired-fault-domain-aware-container-requests", () => state.expiredFaultDomainAwareContainerRequests.get())
  val mFailedFaultDomainAwareContainerRequests = newGauge("failed-fault-domain-aware-container-requests", () => state.failedFaultDomainAwareContainerAllocations.get())

}
