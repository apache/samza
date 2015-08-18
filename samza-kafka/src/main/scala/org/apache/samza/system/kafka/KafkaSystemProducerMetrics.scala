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

package org.apache.samza.system.kafka

import org.apache.samza.metrics.{MetricsHelper, MetricsRegistry, MetricsRegistryMap}

class KafkaSystemProducerMetrics(val systemName: String = "unknown", val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {
  /* Tracks the number of calls made to send in KafkaSystemProducer */
  val sends = newCounter("producer-sends")
  /* Tracks the number of calls made to flush in KafkaSystemProducer */
  val flushes = newCounter("flushes")
  /* Tracks how long the flush call takes to complete */
  val flushNs = newTimer("flush-ns")
  /* Tracks the number of times the system producer retries a send request (due to RetriableException) */
  val retries = newCounter("producer-retries")
  /* Tracks the number of times flush operation failed */
  val flushFailed = newCounter("flush-failed")
  /* Tracks the number of send requests that was failed by the KafkaProducer (due to unrecoverable errors) */
  val sendFailed = newCounter("producer-send-failed")
  /* Tracks the number of send requests that was successfully completed by the KafkaProducer */
  val sendSuccess = newCounter("producer-send-success")

  override def getPrefix = systemName + "-"
}
