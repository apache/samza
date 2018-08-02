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

package org.apache.samza.metrics.reporter

import java.util.{Collections, HashMap, Map}
import scala.collection.JavaConverters._

object Metrics {
  def fromMap(map: Map[String, Map[String, Object]]): Metrics = {
    new Metrics(map)
  }
}

/**
 * Immutable metrics snapshot.
 */
class Metrics(metrics: Map[String, Map[String, Object]]) {
  val immutableMetrics = new HashMap[String, Map[String, Object]]

  for ((groupKey, groupValue) <- metrics.asScala) {
    val immutableMetricGroup = new HashMap[String, Object]

    for ((metricKey, metricValue) <- groupValue.asScala) {
      immutableMetricGroup.put(metricKey, metricValue)
    }

    immutableMetrics.put(groupKey, Collections.unmodifiableMap(immutableMetricGroup))
  }

  def get[T](group: String, metricName: String) =
    immutableMetrics.get(group).get(metricName).asInstanceOf[T]

  def get(group: String) = immutableMetrics.get(group)

  def getAsMap(): Map[String, Map[String, Object]] = Collections.unmodifiableMap(immutableMetrics)

  // default constructor to enable deserialization by MetricsSnapshotSerdeV2
  def this() {
    this(new HashMap[String, Map[String, Object]]())
  }
}
