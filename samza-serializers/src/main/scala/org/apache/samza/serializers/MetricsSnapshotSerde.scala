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
import org.apache.samza.config.Config
import org.codehaus.jackson.map.ObjectMapper
import java.util.Map
import java.nio.ByteBuffer
import org.apache.samza.metrics.reporter.MetricsSnapshot

class MetricsSnapshotSerde extends Serde[MetricsSnapshot] {
  val jsonMapper = new ObjectMapper

  def toBytes(obj: MetricsSnapshot) = jsonMapper
    .writeValueAsString(obj.getAsMap)
    .getBytes("UTF-8")

  def fromBytes(bytes: Array[Byte]) = {
    val metricMap = jsonMapper.readValue(bytes, classOf[java.util.Map[String, java.util.Map[String, Object]]])
    MetricsSnapshot.fromMap(metricMap)
  }
}

class MetricsSnapshotSerdeFactory extends SerdeFactory[MetricsSnapshot] {
  def getSerde(name: String, config: Config) = new MetricsSnapshotSerde
}
