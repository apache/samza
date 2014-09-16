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

package org.apache.samza.serializers

import java.util.HashMap
import java.util.Map

import org.apache.samza.metrics.reporter.MetricsSnapshot
import org.apache.samza.metrics.reporter.MetricsHeader
import org.apache.samza.metrics.reporter.Metrics
import org.junit.Assert._
import org.junit.Ignore
import org.junit.Test

class TestMetricsSnapshotSerde {
  @Ignore
  @Test
  def testMetricsSerdeShouldSerializeAndDeserializeAMetric {
    val header = new MetricsHeader("test", "testjobid", "task", "test", "version", "samzaversion", "host", 1L, 2L)
    val metricsMap = new HashMap[String, Object]()
    metricsMap.put("test2", "foo")
    val metricsGroupMap = new HashMap[String, Map[String, Object]]()
    metricsGroupMap.put("test", metricsMap)
    val metrics = Metrics.fromMap(metricsGroupMap)
    val snapshot = new MetricsSnapshot(header, metrics)
    val serde = new MetricsSnapshotSerde()
    val bytes = serde.toBytes(snapshot)
    assertTrue(serde.fromBytes(bytes).equals(metrics))
  }
}
