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

import org.apache.samza.metrics.ReadableMetricsRegistry
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.MetricsHelper
import org.apache.samza.system.SystemStreamPartition

class TaskInstanceMetrics(
  val source: String = "unknown",
  val registry: ReadableMetricsRegistry = new MetricsRegistryMap,
  val prefix: String = "") extends MetricsHelper {

  val commits = newCounter("commit-calls")
  val windows = newCounter("window-calls")
  val processes = newCounter("process-calls")
  val messagesActuallyProcessed = newCounter("messages-actually-processed")
  val sends = newCounter("send-calls")
  val flushes = newCounter("flush-calls")
  val pendingMessages = newGauge("pending-messages", 0)
  val messagesInFlight = newGauge("messages-in-flight", 0)
  val asyncCallbackCompleted = newCounter("async-callback-complete-calls")
  val commitsTimedOut = newGauge("commits-timed-out", 0)
  val commitsSkipped = newGauge("commits-skipped", 0)
  val commitNs = newTimer("commit-ns")
  val lastCommitNs = newGauge("last-commit-ns", 0L)
  val commitSyncNs = newTimer("commit-sync-ns")
  val commitAsyncNs = newTimer("commit-async-ns")
  val snapshotNs = newTimer("snapshot-ns")
  val storeCheckpointNs = newTimer("store-checkpoint-ns")
  val asyncUploadNs = newTimer("async-upload-ns")
  val asyncCleanupNs = newTimer("async-cleanup-ns")

  def addOffsetGauge(systemStreamPartition: SystemStreamPartition, getValue: () => String) {
    newGauge("%s-%s-%d-offset" format (systemStreamPartition.getSystem, systemStreamPartition.getStream, systemStreamPartition.getPartition.getPartitionId), getValue)
  }

  override def getPrefix: String = prefix
}
