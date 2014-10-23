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

package org.apache.samza.job.yarn

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ Container, ContainerStatus, NodeReport }
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.samza.config.MapConfig
import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.config.YarnConfig
import org.apache.samza.config.YarnConfig.Config2Yarn
import org.apache.samza.config.serializers.JsonConfigSerializer
import org.apache.samza.job.yarn.SamzaAppMasterTaskManager.DEFAULT_CONTAINER_MEM
import org.apache.samza.job.yarn.SamzaAppMasterTaskManager.DEFAULT_CPU_CORES
import org.apache.samza.metrics.JmxServer
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.util.hadoop.HttpFileSystem

import org.apache.samza.util.Logging

/**
 * When YARN executes an application master, it needs a bash command to
 * execute. For Samza, YARN will execute this main method when starting Samza's
 * YARN application master.
 *
 * <br/><br/>
 *
 * The main method gets all of the environment variables (passed by Samza's
 * YARN client, and YARN itself), and wires up everything to run Samza's
 * application master.
 */
object SamzaAppMaster extends Logging with AMRMClientAsync.CallbackHandler {
  val DEFAULT_POLL_INTERVAL_MS: Int = 1000
  var listeners: List[YarnAppMasterListener] = null
  var storedException: Throwable = null

  def main(args: Array[String]) {
    val containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString)
    info("got container id: %s" format containerIdStr)
    val containerId = ConverterUtils.toContainerId(containerIdStr)
    val applicationAttemptId = containerId.getApplicationAttemptId
    info("got app attempt id: %s" format applicationAttemptId)
    val nodeHostString = System.getenv(ApplicationConstants.Environment.NM_HOST.toString)
    info("got node manager host: %s" format nodeHostString)
    val nodePortString = System.getenv(ApplicationConstants.Environment.NM_PORT.toString)
    info("got node manager port: %s" format nodePortString)
    val nodeHttpPortString = System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.toString)
    info("got node manager http port: %s" format nodeHttpPortString)
    val config = new MapConfig(JsonConfigSerializer.fromJson(System.getenv(ShellCommandConfig.ENV_CONFIG)))
    info("got config: %s" format config)
    val hConfig = new YarnConfiguration
    hConfig.set("fs.http.impl", classOf[HttpFileSystem].getName)
    val interval = config.getAMPollIntervalMs.getOrElse(DEFAULT_POLL_INTERVAL_MS)
    val amClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](interval, this)
    val clientHelper = new ClientHelper(hConfig)
    val registry = new MetricsRegistryMap
    val containerMem = config.getContainerMaxMemoryMb.getOrElse(DEFAULT_CONTAINER_MEM)
    val containerCpu = config.getContainerMaxCpuCores.getOrElse(DEFAULT_CPU_CORES)
    val jmxServer = if (new YarnConfig(config).getJmxServerEnabled) Some(new JmxServer()) else None

    try {
      // wire up all of the yarn event listeners
      val state = new SamzaAppMasterState(-1, containerId, nodeHostString, nodePortString.toInt, nodeHttpPortString.toInt)
      val service = new SamzaAppMasterService(config, state, registry, clientHelper)
      val lifecycle = new SamzaAppMasterLifecycle(containerMem, containerCpu, state, amClient)
      val metrics = new SamzaAppMasterMetrics(config, state, registry)
      val am = new SamzaAppMasterTaskManager({ System.currentTimeMillis }, config, state, amClient, hConfig)
      listeners = List(state, service, lifecycle, metrics, am)
      run(amClient, listeners, hConfig, interval)
    } finally {
      // jmxServer has to be stopped or will prevent process from exiting.
      if (jmxServer.isDefined) {
        jmxServer.get.stop
      }
    }
  }

  def run(amClient: AMRMClientAsync[ContainerRequest], listeners: List[YarnAppMasterListener], hConfig: YarnConfiguration, interval: Int): Unit = {
    try {
      amClient.init(hConfig)
      amClient.start
      listeners.foreach(_.onInit)
      var isShutdown: Boolean = false
      // have the loop to prevent the process from exiting until the job is to shutdown or error occurs on amClient
      while (!isShutdown && !listeners.map(_.shouldShutdown).reduceLeft(_ || _) && storedException == null) {
        try {
          Thread.sleep(interval)
        } catch {
          case e: InterruptedException => {
            isShutdown = true
            info("got interrupt in app master thread, so shutting down")
          }
        }
      }
    } finally {
      // listeners has to be stopped
      listeners.foreach(listener => try {
        listener.onShutdown
      } catch {
        case e: Exception => warn("Listener %s failed to shutdown." format listener, e)
      })
      // amClient has to be stopped
      amClient.stop
    }
  }

  override def onContainersCompleted(statuses: java.util.List[ContainerStatus]): Unit =
    statuses.foreach(containerStatus => listeners.foreach(_.onContainerCompleted(containerStatus)))

  override def onContainersAllocated(containers: java.util.List[Container]): Unit =
    containers.foreach(container => listeners.foreach(_.onContainerAllocated(container)))

  override def onShutdownRequest: Unit = listeners.foreach(_.onReboot)

  override def onNodesUpdated(updatedNodes: java.util.List[NodeReport]): Unit = Unit

  // TODO need to think about meaningful SAMZA's progress
  override def getProgress: Float = 0.0F

  override def onError(e: Throwable): Unit = {
    error("Error occured in amClient's callback", e)
    storedException = e
  }

}
