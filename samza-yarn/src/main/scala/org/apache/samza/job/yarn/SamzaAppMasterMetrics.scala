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
import org.apache.samza.config.Config
import org.apache.samza.config.MetricsConfig.Config2Metrics
import org.apache.samza.util.Logging
import org.apache.samza.util.MetricsReporterLoader
import org.apache.samza.metrics.ReadableMetricsRegistry
import org.apache.samza.metrics.MetricsHelper

import scala.collection.JavaConverters._

object SamzaAppMasterMetrics {
  val sourceName = "ApplicationMaster"
}

/**
 * Responsible for wiring up Samza's metrics. Given that Samza has a metric
 * registry, we might as well use it. This class takes Samza's application
 * master state, and converts it to metrics.
 */
class SamzaAppMasterMetrics(
                             val config: Config,
                             val state: SamzaApplicationState,
                             val registry: ReadableMetricsRegistry) extends MetricsHelper with Logging {

  val reporters = MetricsReporterLoader.getMetricsReporters(config, SamzaAppMasterMetrics.sourceName).asScala
  reporters.values.foreach(_.register(SamzaAppMasterMetrics.sourceName, registry))

  def start() {
    val mRunningContainers = newGauge("running-containers", () => state.runningContainers.size)
    val mNeededContainers = newGauge("needed-containers", () => state.neededContainers.get())
    val mCompletedContainers = newGauge("completed-containers", () => state.completedContainers.get())
    val mFailedContainers = newGauge("failed-containers", () => state.failedContainers.get())
    val mReleasedContainers = newGauge("released-containers", () => state.releasedContainers.get())
    val mContainers = newGauge("container-count", () => state.containerCount)
    val mJobHealthy = newGauge("job-healthy", () => if (state.jobHealthy.get()) 1 else 0)

    reporters.values.foreach(_.start)
  }

  def stop() {
    reporters.values.foreach(_.stop)
  }
}
