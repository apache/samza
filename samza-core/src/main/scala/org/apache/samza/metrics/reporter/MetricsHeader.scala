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

import java.util.HashMap
import java.util.Map
import scala.beans.BeanProperty

object MetricsHeader {
  def fromMap(map: Map[String, Object]): MetricsHeader = {
    new MetricsHeader(
      map.get("job-name").toString,
      map.get("job-id").toString,
      map.get("container-name").toString,
      map.get("container-id").toString,
      map.get("source").toString,
      map.get("version").toString,
      map.get("samza-version").toString,
      map.get("host").toString,
      map.get("time").asInstanceOf[Number].longValue,
      map.get("reset-time").asInstanceOf[Number].longValue)
  }
}

/**
 * Immutable metric header snapshot.
 */
class MetricsHeader(
  @BeanProperty val jobName: String,
  @BeanProperty val jobId: String,
  @BeanProperty val containerName: String,
  @BeanProperty val containerId: String,
  @BeanProperty val source: String,
  @BeanProperty val version: String,
  @BeanProperty val samzaVersion: String,
  @BeanProperty val host: String,
  @BeanProperty val time: Long,
  @BeanProperty val resetTime: Long) {

  def getAsMap: Map[String, Object] = {
    val map = new HashMap[String, Object]
    map.put("job-name", jobName)
    map.put("job-id", jobId)
    map.put("container-name", containerName)
    map.put("container-id", containerId)
    map.put("source", source)
    map.put("version", version)
    map.put("samza-version", samzaVersion)
    map.put("host", host)
    map.put("time", time: java.lang.Long)
    map.put("reset-time", resetTime: java.lang.Long)
    map
  }
}
