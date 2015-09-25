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
import org.apache.samza.config.Config
import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.YarnConfig
import org.apache.samza.metrics.JmxServer
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.util.hadoop.HttpFileSystem
import org.apache.samza.util.Logging
import org.apache.samza.serializers.model.SamzaObjectMapper
import org.apache.samza.coordinator.JobCoordinator
import org.apache.samza.SamzaException

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
    putMDC("containerName", "samza-application-master")
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
    val coordinatorSystemConfig = new MapConfig(SamzaObjectMapper.getObjectMapper.readValue(System.getenv(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG), classOf[Config]))
    info("got coordinator system config: %s" format coordinatorSystemConfig)
    val registry = new MetricsRegistryMap
    val jobCoordinator = JobCoordinator(coordinatorSystemConfig, registry)
    val config = jobCoordinator.jobModel.getConfig
    val yarnConfig = new YarnConfig(config)
    info("got config: %s" format coordinatorSystemConfig)
    putMDC("jobName", config.getName.getOrElse(throw new SamzaException("can not find the job name")))
    putMDC("jobId", config.getJobId.getOrElse("1"))
    val hConfig = new YarnConfiguration
    hConfig.set("fs.http.impl", classOf[HttpFileSystem].getName)
    val interval = yarnConfig.getAMPollIntervalMs
    val amClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](interval, this)
    val clientHelper = new ClientHelper(hConfig)
    val containerMem = yarnConfig.getContainerMaxMemoryMb
    val containerCpu = yarnConfig.getContainerMaxCpuCores
    val jmxServer = if (yarnConfig.getJmxServerEnabled) Some(new JmxServer()) else None

    try {
      // wire up all of the yarn event listeners
      val state = new SamzaAppState(jobCoordinator, -1, containerId, nodeHostString, nodePortString.toInt, nodeHttpPortString.toInt)

      if (jmxServer.isDefined) {
        state.jmxUrl = jmxServer.get.getJmxUrl
        state.jmxTunnelingUrl = jmxServer.get.getTunnelingJmxUrl
      }

      val service = new SamzaAppMasterService(config, state, registry, clientHelper)
      val lifecycle = new SamzaAppMasterLifecycle(containerMem, containerCpu, state, amClient)
      val metrics = new SamzaAppMasterMetrics(config, state, registry)
      val taskManager = new SamzaTaskManager(config, state, amClient, hConfig)

      listeners =  List(service, lifecycle, metrics, taskManager)
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
