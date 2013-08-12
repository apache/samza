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
import org.apache.samza.config.MapConfig
import org.apache.samza.config.YarnConfig
import org.apache.samza.config.serializers.JsonConfigSerializer
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import scala.collection.JavaConversions._
import org.apache.samza.metrics.{ JmxServer, MetricsRegistryMap }
import grizzled.slf4j.Logging
import org.apache.hadoop.yarn.client.AMRMClientImpl
import org.apache.samza.config.YarnConfig._
import org.apache.samza.job.yarn.SamzaAppMasterTaskManager._

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
object SamzaAppMaster extends Logging {
  def main(args: Array[String]) {
    val containerIdStr = System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV)
    info("got container id: %s" format containerIdStr)
    val containerId = ConverterUtils.toContainerId(containerIdStr)
    val applicationAttemptId = containerId.getApplicationAttemptId
    info("got app attempt id: %s" format applicationAttemptId)
    val nodeHostString = System.getenv(ApplicationConstants.NM_HOST_ENV)
    info("got node manager host: %s" format nodeHostString)
    val nodePortString = System.getenv(ApplicationConstants.NM_PORT_ENV)
    info("got node manager port: %s" format nodePortString)
    val nodeHttpPortString = System.getenv(ApplicationConstants.NM_HTTP_PORT_ENV)
    info("got node manager http port: %s" format nodeHttpPortString)
    val config = new MapConfig(JsonConfigSerializer.fromJson(System.getenv(YarnConfig.ENV_CONFIG)))
    info("got config: %s" format config)
    val hConfig = new YarnConfiguration
    hConfig.set("fs.http.impl", "samza.util.hadoop.HttpFileSystem")
    val amClient = new AMRMClientImpl(applicationAttemptId)
    val clientHelper = new ClientHelper(hConfig)
    val registry = new MetricsRegistryMap
    val containerMem = config.getContainerMaxMemoryMb.getOrElse(DEFAULT_CONTAINER_MEM)
    val containerCpu = config.getContainerMaxCpuCores.getOrElse(DEFAULT_CPU_CORES)
    val jmxServer = if (new YarnConfig(config).getJmxServerEnabled) Some(new JmxServer()) else None

    try {
      // wire up all of the yarn event listeners
      val state = new SamzaAppMasterState(-1, containerId, nodeHostString, nodePortString.toInt, nodeHttpPortString.toInt)
      val service = new SamzaAppMasterService(config, state, registry, clientHelper)
      val lifecycle = new SamzaAppMasterLifecycle(containerMem, containerCpu, state, amClient, hConfig)
      val metrics = new SamzaAppMasterMetrics(config, state, registry)
      val am = new SamzaAppMasterTaskManager({ System.currentTimeMillis }, config, state, amClient, hConfig)

      // run the app master
      new YarnAppMaster(List(state, service, lifecycle, metrics, am), amClient).run
    } finally {
      // jmxServer has to be stopped or will prevent process from exiting.
      if (jmxServer.isDefined) {
        jmxServer.get.stop
      }
    }
  }
}
