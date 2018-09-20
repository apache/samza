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

import java.{lang, util}

import org.apache.samza.clustermanager.SamzaApplicationState
import org.scalatra._
import scalate.ScalateSupport
import org.apache.samza.config.Config
import org.apache.samza.job.yarn.{ClientHelper, YarnAppState}
import org.apache.samza.metrics._

import scala.collection.JavaConverters._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import java.util.HashMap

import org.apache.samza.serializers.model.SamzaObjectMapper
import org.codehaus.jackson.map.ObjectMapper

object ApplicationMasterRestServlet {
  def getMetrics(jsonMapper: ObjectMapper, metricsRegistry: ReadableMetricsRegistry) = {
    val metricMap = new HashMap[String, util.Map[String, Object]]

    // build metric map
    metricsRegistry.getGroups.asScala.foreach(group => {
      val groupMap = new HashMap[String, Object]

      metricsRegistry.getGroup(group).asScala.foreach {
        case (name, metric) =>
          metric.visit(new MetricsVisitor() {
            def listGauge[T](listGauge: ListGauge[T]) =
              groupMap.put(name, listGauge.getValues)

            def counter(counter: Counter) =
              groupMap.put(counter.getName, counter.getCount: lang.Long)

            def gauge[T](gauge: Gauge[T]) =
              groupMap.put(gauge.getName, gauge.getValue.asInstanceOf[Object])

            def timer(timer: Timer) =
              groupMap.put(timer.getName, timer.getSnapshot().getAverage: lang.Double)
          })
      }

      metricMap.put(group, groupMap)
    })

    jsonMapper.writeValueAsString(metricMap)
  }

  def getTaskContext(jsonMapper: ObjectMapper, state: YarnAppState) = {
    // sick of fighting with scala.. just using java map for now
    val contextMap = new HashMap[String, Object]

    contextMap.put("task-id", state.taskId: Integer)
    contextMap.put("name", state.amContainerId.toString)

    jsonMapper.writeValueAsString(contextMap)
  }

  def getAmState(jsonMapper: ObjectMapper, samzaAppState: SamzaApplicationState, state: YarnAppState) = {
    val containers = new HashMap[String, util.HashMap[String, Object]]

    state.runningYarnContainers.asScala.foreach {
      case (containerId, container) =>
        val yarnContainerId = container.id.toString
        val containerMap = new HashMap[String, Object]
        val taskModels = samzaAppState.jobModelManager.jobModel.getContainers.get(containerId).getTasks
        containerMap.put("yarn-address", container.nodeHttpAddress)
        containerMap.put("start-time", container.startTime.toString)
        containerMap.put("up-time", container.upTime.toString)
        containerMap.put("task-models", taskModels)
        containerMap.put("container-id", containerId.toString)
        containers.put(yarnContainerId, containerMap)
    }

    val status = Map[String, Object](
      "app-attempt-id" -> state.appAttemptId.toString,
      "container-id" -> state.amContainerId.toString,
      "containers" -> containers,
      "host" -> "%s:%s".format(state.nodeHost, state.rpcUrl.getPort))

    jsonMapper.writeValueAsString(new HashMap[String, Object](status.asJava))
  }

  def getConfig(jsonMapper: ObjectMapper, samzaConfig: Config) = {
    jsonMapper.writeValueAsString(new HashMap[String, Object](samzaConfig.sanitize))
  }
}

/**
  * Defines the Scalatra routes for the servlet.
  */
class ApplicationMasterRestServlet(samzaConfig: Config, samzaAppState: SamzaApplicationState, state: YarnAppState, registry: ReadableMetricsRegistry) extends ScalatraServlet with ScalateSupport {
  val yarnConfig = new YarnConfiguration
  val client = new ClientHelper(yarnConfig)
  val jsonMapper = SamzaObjectMapper.getObjectMapper

  before() {
    contentType = "application/json"
  }

  get("/metrics") {
    ApplicationMasterRestServlet.getMetrics(jsonMapper, registry)
  }



  get("/task-context") {
    ApplicationMasterRestServlet.getTaskContext(jsonMapper, state)
  }



  get("/am") {
    ApplicationMasterRestServlet.getAmState(jsonMapper, samzaAppState, state)
  }



  get("/config") {
    ApplicationMasterRestServlet.getConfig(jsonMapper, samzaConfig)
  }
}
