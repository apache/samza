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

package org.apache.samza.system.hdfs


import org.apache.samza.metrics.{MetricsRegistry, MetricsHelper, Gauge, MetricsRegistryMap}


class HdfsSystemProducerMetrics(val systemName: String = "unknown", val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {

  /* Tracks the number of calls made to send in producer */
  val sends = newCounter("producer-sends")
  val sendSuccess = newCounter("send-success")
  val sendFailed = newCounter("send-failed")
  val sendMs = newTimer("send-ms")

  /* Tracks the number of calls made to flush in producer */
  val flushes = newCounter("flushes")
  val flushFailed = newCounter("flush-failed")
  val flushSuccess = newCounter("flush-success")
  val flushMs = newTimer("flush-ms")

  override def getPrefix = systemName + "-"

}
