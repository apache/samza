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

package org.apache.samza.job.yarn

import org.apache.samza.clustermanager.SamzaApplicationState
import org.apache.samza.config.{Config, MetricsConfig}
import org.apache.samza.util.Logging
import org.apache.samza.util.MetricsReporterLoader
import org.apache.samza.metrics.{Counter, MetricsHelper, ReadableMetricsRegistry}

import scala.collection.JavaConverters._

object SamzaAppMasterMetrics {
  val sourceName = "ApplicationMaster"
}

/**
 * Responsible for wiring up Samza's metrics. Given that Samza has a metric
 * registry, we might as well use it. This class takes Samza's application
 * master state, and converts it to metrics.
 */
class SamzaAppMasterMetrics(val config: Config,
  val state: SamzaApplicationState,
  val registry: ReadableMetricsRegistry) extends MetricsHelper with Logging {

  private val metricsConfig = new MetricsConfig(config)
  val containersFromPreviousAttempts = newGauge("container-from-previous-attempt", 0L)
  val allocatedContainersInBuffer = newGauge("allocated-containers-in-buffer", 0L)
  val reporters = MetricsReporterLoader.getMetricsReporters(metricsConfig, SamzaAppMasterMetrics.sourceName).asScala
  reporters.values.foreach(_.register(SamzaAppMasterMetrics.sourceName, registry))

  def setContainersFromPreviousAttempts(containerCount: Int) {
    containersFromPreviousAttempts.set(containerCount)
  }

  def incrementAllocatedContainersInBuffer(): Unit = {
    allocatedContainersInBuffer.set(allocatedContainersInBuffer.getValue + 1)
  }

  def decrementAllocatedContainersInBuffer(): Unit = {
    allocatedContainersInBuffer.set(allocatedContainersInBuffer.getValue - 1)
  }

  def start() {
    val mRunningContainers = newGauge("running-containers", () => state.runningProcessors.size)
    val mNeededContainers = newGauge("needed-containers", () => state.neededProcessors.get())
    val mCompletedContainers = newGauge("completed-containers", () => state.completedProcessors.get())
    val mFailedContainers = newGauge("failed-containers", () => state.failedContainers.get())
    val mReleasedContainers = newGauge("released-containers", () => state.releasedContainers.get())
    val mContainers = newGauge("container-count", () => state.processorCount.get())
    val mJobHealthy = newGauge("job-healthy", () => if (state.jobHealthy.get()) 1 else 0)

    reporters.values.foreach(_.start)
  }

  def stop() {
    reporters.values.foreach(_.stop)
  }
}
