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

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.samza.SamzaException
import org.apache.samza.util.Logging

/**
 * Responsible for managing the lifecycle of the application master. Mostly,
 * this means registering and unregistering with the RM, and shutting down
 * when the RM tells us to Reboot.
 */
class SamzaAppMasterLifecycle(containerMem: Int, containerCpu: Int, state: SamzaAppState, amClient: AMRMClientAsync[ContainerRequest]) extends YarnAppMasterListener with Logging {
  var validResourceRequest = true
  var shutdownMessage: String = null

  override def onInit() {
    val host = state.nodeHost
    val response = amClient.registerApplicationMaster(host, state.rpcUrl.getPort, "%s:%d" format (host, state.trackingUrl.getPort))

    // validate that the YARN cluster can handle our container resource requirements
    val maxCapability = response.getMaximumResourceCapability
    val maxMem = maxCapability.getMemory
    val maxCpu = maxCapability.getVirtualCores

    info("Got AM register response. The YARN RM supports container requests with max-mem: %s, max-cpu: %s" format (maxMem, maxCpu))

    if (containerMem > maxMem || containerCpu > maxCpu) {
      shutdownMessage = "The YARN cluster is unable to run your job due to unsatisfiable resource requirements. You asked for mem: %s, and cpu: %s." format (containerMem, containerCpu)
      error(shutdownMessage)
      validResourceRequest = false
      state.status = FinalApplicationStatus.FAILED
      state.jobHealthy.set(false)
    }
  }

  override def onReboot() {
    throw new SamzaException("Received a reboot signal from the RM, so throwing an exception to reboot the AM.")
  }

  override def onShutdown() {
    info("Shutting down.")
    amClient.unregisterApplicationMaster(state.status, shutdownMessage, null)
  }

  override def shouldShutdown = !validResourceRequest
}
