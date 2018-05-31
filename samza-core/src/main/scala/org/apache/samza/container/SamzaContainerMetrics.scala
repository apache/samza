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

package org.apache.samza.container

import java.util

import org.apache.samza.metrics.{Gauge, ReadableMetricsRegistry, MetricsRegistryMap, MetricsHelper}

class SamzaContainerMetrics(
  val source: String = "unknown",
  val registry: ReadableMetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {

  val commits = newCounter("commit-calls")
  val windows = newCounter("window-calls")
  val timers = newCounter("timer-calls")
  val processes = newCounter("process-calls")
  val envelopes = newCounter("process-envelopes")
  val nullEnvelopes = newCounter("process-null-envelopes")
  val chooseNs = newTimer("choose-ns")
  val windowNs = newTimer("window-ns")
  val timerNs = newTimer("timer-ns")
  val processNs = newTimer("process-ns")
  val commitNs = newTimer("commit-ns")
  val blockNs = newTimer("block-ns")
  val containerStartupTime = newTimer("container-startup-time")
  val utilization = newGauge("event-loop-utilization", 0.0F)
  val diskUsageBytes = newGauge("disk-usage-bytes", 0L)
  val diskQuotaBytes = newGauge("disk-quota-bytes", Long.MaxValue)
  val executorWorkFactor = newGauge("executor-work-factor", 1.0)
  val physicalMemoryMb = newGauge[Double]("physical-memory-mb", 0.0F)

  val taskStoreRestorationMetrics: util.Map[TaskName, Gauge[Long]] = new util.HashMap[TaskName, Gauge[Long]]()

  // A string-gauge metric to capture the last 1000 exceptions at this container
  val exception = newListGauge[String]("exception", 1000)

  def addStoreRestorationGauge(taskName: TaskName, storeName: String) {
    taskStoreRestorationMetrics.put(taskName, newGauge("%s-%s-restore-time" format(taskName.toString, storeName), -1L))
  }

}
