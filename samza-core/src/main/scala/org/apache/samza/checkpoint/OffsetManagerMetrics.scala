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

package org.apache.samza.checkpoint

import org.apache.samza.metrics._
import java.util.concurrent.ConcurrentHashMap

import org.apache.samza.system.SystemStreamPartition

class OffsetManagerMetrics(val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {

  val checkpointedOffsets = new ConcurrentHashMap[SystemStreamPartition, Gauge[String]]

  def addCheckpointedOffset(systemStreamPartition: SystemStreamPartition, checkpointedOffset: String) {
    checkpointedOffsets.put(systemStreamPartition, newGauge("%s-%s-%d-checkpointed-offset" format (systemStreamPartition.getSystem, systemStreamPartition.getStream, systemStreamPartition.getPartition.getPartitionId), checkpointedOffset))
  }
}
