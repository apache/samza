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

package org.apache.samza.coordinator.server

import org.apache.samza.config.Config
import java.util.HashMap
import org.apache.samza.container.TaskNamesToSystemStreamPartitions
import org.apache.samza.util.JsonHelpers
import org.apache.samza.container.TaskName
import org.apache.samza.util.Logging

object JobServlet {
  val CONFIG = "config"
  val CONTAINERS = "containers"
  val TASK_CHANGELOG_MAPPING = "task-changelog-mappings"
}

class JobServlet(
  config: Config,
  containerToTaskMapping: Map[Int, TaskNamesToSystemStreamPartitions],
  taskToChangelogMapping: Map[TaskName, Int]) extends ServletBase with Logging {
  import JobServlet._
  import JsonHelpers._

  val javaSafeContainerToTaskMapping = buildTasksToSSPs
  val javaSafeTaskToChangelogMappings = convertTaskNameToChangeLogPartitionMapping(taskToChangelogMapping)
  val jsonMap = buildJsonMap

  debug("Built JSON map: %s" format jsonMap)

  protected def getObjectToWrite() = {
    jsonMap
  }

  private def buildTasksToSSPs = {
    val map = new HashMap[java.lang.Integer, java.util.HashMap[TaskName, java.util.ArrayList[SSPWrapper]]]
    containerToTaskMapping.foreach {
      case (containerId, taskNameToSSPs) =>
        map.put(Integer.valueOf(containerId), convertSystemStreamPartitionSet(taskNameToSSPs.getJavaFriendlyType))
    }
    map
  }

  private def buildJsonMap = {
    val map = new HashMap[String, Object]()
    map.put(CONFIG, config)
    map.put(CONTAINERS, javaSafeContainerToTaskMapping)
    map.put(TASK_CHANGELOG_MAPPING, javaSafeTaskToChangelogMappings)
    map
  }
}
