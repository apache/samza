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
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.JvmMetrics
import org.apache.samza.config.Config
import org.apache.samza.task.TaskContext
import org.apache.samza.Partition
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.config.MetricsConfig.Config2Metrics
import org.apache.samza.metrics.MetricsReporterFactory
import org.apache.samza.util.Util
import org.apache.samza.metrics.ReadableMetricsRegistry
import org.apache.samza.util.Logging
import org.apache.samza.SamzaException
import java.util.Timer
import java.util.TimerTask
import org.apache.samza.metrics.MetricsHelper

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
  val state: SamzaAppState,
  val registry: ReadableMetricsRegistry) extends MetricsHelper with YarnAppMasterListener with Logging {

  val jvm = new JvmMetrics(registry)
  val reporters = config.getMetricReporterNames.map(reporterName => {
    val metricsFactoryClassName = config
      .getMetricsFactoryClass(reporterName)
      .getOrElse(throw new SamzaException("Metrics reporter %s missing .class config" format reporterName))

    val reporter =
      Util
        .getObj[MetricsReporterFactory](metricsFactoryClassName)
        .getMetricsReporter(reporterName, SamzaAppMasterMetrics.sourceName, config)

    reporter.register(SamzaAppMasterMetrics.sourceName, registry)
    (reporterName, reporter)
  }).toMap

  override def onInit() {
    val mRunningContainers = newGauge("running-containers", () => state.runningContainers.size)
    val mNeededContainers = newGauge("needed-containers", () => state.neededContainers.get())
    val mCompletedContainers = newGauge("completed-containers", () => state.completedContainers.get())
    val mFailedContainers = newGauge("failed-containers", () => state.failedContainers.get())
    val mReleasedContainers = newGauge("released-containers", () => state.releasedContainers.get())
    val mContainers = newGauge("container-count", () => state.containerCount)
    val mHost = newGauge("http-host", () => state.nodeHost)
    val mTrackingPort = newGauge("http-port", () => state.trackingUrl.getPort)
    val mRpcPort = newGauge("rpc-port", () => state.rpcUrl.getPort)
    val mAppAttemptId = newGauge("app-attempt-id", () => state.appAttemptId.toString)
    val mJobHealthy = newGauge("job-healthy", () => if (state.jobHealthy.get()) 1 else 0)
    val mLocalityMatchedRequests = newGauge(
      "locality-matched",
      () => {
        if (state.containerRequests.get() != 0) {
          state.matchedContainerRequests.get() / state.containerRequests.get()
        } else {
          0L
        }
      })

    jvm.start
    reporters.values.foreach(_.start)
  }

  override def onShutdown() {
    reporters.values.foreach(_.stop)
  }
}
