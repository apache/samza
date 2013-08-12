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

package org.apache.samza.webapp

import org.scalatra._
import scalate.ScalateSupport
import org.apache.samza.config.Config
import org.apache.samza.job.yarn.SamzaAppMasterState
import org.apache.samza.job.yarn.ClientHelper
import org.apache.samza.metrics._
import scala.collection.JavaConversions._
import scala.collection.immutable.TreeMap
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.codehaus.jackson.map.ObjectMapper
import java.util.HashMap

class ApplicationMasterRestServlet(config: Config, state: SamzaAppMasterState, registry: ReadableMetricsRegistry) extends ScalatraServlet with ScalateSupport {
  val yarnConfig = new YarnConfiguration
  val client = new ClientHelper(yarnConfig)
  val jsonMapper = new ObjectMapper()

  before() {
    contentType = "application/json"
  }

  get("/metrics") {
    val metricMap = new HashMap[String, java.util.Map[String, Object]]

    // build metric map
    registry.getGroups.foreach(group => {
      val groupMap = new HashMap[String, Object]

      registry.getGroup(group).foreach {
        case (name, metric) =>
          metric.visit(new MetricsVisitor() {
            def counter(counter: Counter) =
              groupMap.put(counter.getName, counter.getCount: java.lang.Long)

            def gauge[T](gauge: Gauge[T]) =
              groupMap.put(gauge.getName, gauge.getValue.asInstanceOf[java.lang.Object])
          })
      }

      metricMap.put(group, groupMap)
    })

    jsonMapper.writeValueAsString(metricMap)
  }

  get("/task-context") {
    // sick of fighting with scala.. just using java map for now
    val contextMap = new HashMap[String, Object]

    contextMap.put("task-id", state.taskId: java.lang.Integer)
    contextMap.put("name", state.containerId.toString)

    jsonMapper.writeValueAsString(contextMap)
  }

  get("/am") {
    val containers = new HashMap[String, HashMap[String, Object]]

    state.runningTasks.values.foreach(c => {
      val containerIdStr = c.getId.toString
      val containerMap = new HashMap[String, Object]
      val taskId = state.runningTasks.filter { case (_, container) => container.getId.toString.equals(containerIdStr) }.keys.head
      var partitions = new java.util.ArrayList(state.taskPartitions.get(taskId).get)

      containerMap.put("yarn-address", c.getNodeHttpAddress)
      containerMap.put("partitions", partitions)
      containerMap.put("task-id", taskId.toString)
      containers.put(containerIdStr, containerMap)
    })

    val status = Map[String, Object](
      "app-attempt-id" -> state.appAttemptId.toString,
      "container-id" -> state.containerId.toString,
      "containers" -> containers,
      "host" -> "%s:%s".format(state.nodeHost, state.rpcPort))

    jsonMapper.writeValueAsString(new HashMap[String, Object](status))
  }

  get("/am/kill") {
    client.kill(state.appAttemptId.getApplicationId)
  }
}
