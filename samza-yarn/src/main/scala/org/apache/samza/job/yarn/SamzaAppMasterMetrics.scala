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
import grizzled.slf4j.Logging
import org.apache.samza.SamzaException
import java.util.Timer
import java.util.TimerTask
import org.apache.samza.task.ReadableCollector

object SamzaAppMasterMetrics {
  val metricsGroup = "samza.yarn.am"

  val sourceName = "ApplicationMaster"
}

/**
 * Responsible for wiring up Samza's metrics. Given that Samza has a metric
 * registry, we might as well use it. This class takes Samza's application
 * master state, and converts it to metrics.
 */
class SamzaAppMasterMetrics(config: Config, state: SamzaAppMasterState, registry: ReadableMetricsRegistry) extends YarnAppMasterListener with Logging {
  import SamzaAppMasterMetrics._

  val jvm = new JvmMetrics(registry)
  val mEventLoops = registry.newCounter(metricsGroup, "EventLoops")
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
    val mRunningContainers = registry.newGauge(metricsGroup, "RunningContainers", { state.runningTasks.size })
    val mNeededContainers = registry.newGauge(metricsGroup, "NeededContainers", { state.neededContainers })
    val mCompletedContainers = registry.newGauge(metricsGroup, "CompletedContainers", { state.completedTasks })
    val mFailedContainers = registry.newGauge(metricsGroup, "FailedContainers", { state.failedContainers })
    val mReleasedContainers = registry.newGauge(metricsGroup, "ReleasedContainers", { state.releasedContainers })
    val mTasks = registry.newGauge(metricsGroup, "TaskCount", { state.taskCount })
    val mHost = registry.newGauge(metricsGroup, "HttpHost", { state.nodeHost })
    val mTrackingPort = registry.newGauge(metricsGroup, "HttpPort", { state.trackingPort })
    val mRpcPort = registry.newGauge(metricsGroup, "RpcPort", { state.rpcPort })
    val mAppAttemptId = registry.newGauge(metricsGroup, "AppAttemptId", { state.appAttemptId.toString })

    reporters.values.foreach(_.start)
  }

  override def onEventLoop() {
    mEventLoops.inc
  }

  override def onShutdown() {
    reporters.values.foreach(_.stop)
  }
}
