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

package org.apache.samza.util

import org.apache.samza.container.TaskName
import org.codehaus.jackson.map.ObjectMapper
import org.apache.samza.system.SystemStreamPartition
import org.codehaus.jackson.`type`.TypeReference
import java.util
import scala.collection.JavaConversions._
import org.apache.samza.Partition
import scala.reflect.BeanProperty
import org.apache.samza.config.MapConfig
import org.apache.samza.container.TaskNamesToSystemStreamPartitions

/**
 * Working with Jackson and JSON in Scala is tricky. These helper methods are
 * used to convert objects back and forth in SamzaContainer, and the
 * JobServlet.
 */
object JsonHelpers {
  // Jackson really hates Scala's classes, so we need to wrap up the SSP in a 
  // form Jackson will take.
  class SSPWrapper(@BeanProperty var partition: java.lang.Integer = null,
    @BeanProperty var Stream: java.lang.String = null,
    @BeanProperty var System: java.lang.String = null) {
    def this() { this(null, null, null) }
    def this(ssp: SystemStreamPartition) { this(ssp.getPartition.getPartitionId, ssp.getSystemStream.getStream, ssp.getSystemStream.getSystem) }
  }

  def convertSystemStreamPartitionSet(sspTaskNames: java.util.Map[TaskName, java.util.Set[SystemStreamPartition]]): util.HashMap[TaskName, util.ArrayList[SSPWrapper]] = {
    val map = new util.HashMap[TaskName, util.ArrayList[SSPWrapper]]()
    for ((key, ssps) <- sspTaskNames) {
      val al = new util.ArrayList[SSPWrapper](ssps.size)
      for (ssp <- ssps) { al.add(new SSPWrapper(ssp)) }
      map.put(key, al)
    }
    map
  }

  def convertTaskNameToChangeLogPartitionMapping(mapping: Map[TaskName, Int]): util.HashMap[TaskName, java.lang.Integer] = {
    val javaMap = new util.HashMap[TaskName, java.lang.Integer]()
    mapping.foreach(kv => javaMap.put(kv._1, Integer.valueOf(kv._2)))
    javaMap
  }

  def deserializeCoordinatorBody(body: String) = new ObjectMapper().readValue(body, new TypeReference[util.HashMap[String, Object]] {}).asInstanceOf[util.HashMap[String, Object]]

  def convertCoordinatorConfig(config: util.Map[String, String]) = new MapConfig(config)

  def convertCoordinatorTaskNameChangelogPartitions(taskNameToChangelogMapping: util.Map[String, java.lang.Integer]) = {
    taskNameToChangelogMapping.map {
      case (taskName, changelogPartitionId) =>
        (new TaskName(taskName), changelogPartitionId.toInt)
    }.toMap
  }

  // First key is containerId, second key is TaskName, third key is 
  // [system|stream|partition].
  def convertCoordinatorSSPTaskNames(containers: util.Map[String, util.Map[String, util.List[util.Map[String, Object]]]]): Map[Int, TaskNamesToSystemStreamPartitions] = {
    containers.map {
      case (containerId, tasks) => {
        containerId.toInt -> new TaskNamesToSystemStreamPartitions(tasks.map {
          case (taskName, ssps) => {
            new TaskName(taskName) -> ssps.map {
              case (sspMap) => new SystemStreamPartition(
                sspMap.get("system").toString,
                sspMap.get("stream").toString,
                new Partition(sspMap.get("partition").toString.toInt))
            }.toSet
          }
        }.toMap)
      }
    }.toMap
  }
}