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

package org.apache.samza.storage.kv

import org.apache.samza.metrics.{MetricsHelper, MetricsRegistry, MetricsRegistryMap, SamzaHistogram}


class SerializedKeyValueStoreMetrics(
  val storeName: String = "unknown",
  val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {

  val gets = newCounter("gets")
  val ranges = newCounter("ranges")
  val alls = newCounter("alls")
  val puts = newCounter("puts")
  val deletes = newCounter("deletes")
  val flushes = newCounter("flushes")
  val bytesSerialized = newCounter("bytes-serialized")
  val bytesDeserialized = newCounter("bytes-deserialized")
  val maxRecordSizeBytes = newGauge("max-record-size-bytes", 0L)
  val record_key_size_percentiles = java.util.Arrays.asList(10D, 50D, 90D, 99D)
  val record_value_size_percentiles = java.util.Arrays.asList(10D, 50D, 90D, 99D)
  val recordKeySizeBytes = newHistogram("key-size-bytes-histogram",record_key_size_percentiles )
  val recordValueSizeBytes = newHistogram("value-size-bytes-histogram", record_value_size_percentiles)

  override def getPrefix = storeName + "-"
}