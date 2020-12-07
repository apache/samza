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

import java.io.IOException
import java.util
import java.util.HashMap

import org.apache.hadoop.yarn.api.records.{Container, ContainerId, FinalApplicationStatus}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.exceptions.{InvalidApplicationMasterRequestException, YarnException}
import org.apache.samza.SamzaException
import org.apache.samza.clustermanager.{SamzaApplicationState, SamzaResource}
import SamzaApplicationState.SamzaAppStatus
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

/**
 * Responsible for managing the lifecycle of the Yarn application master. Mostly,
 * this means registering and unregistering with the RM, and shutting down
 * when the RM tells us to Reboot.
 */
//This class is used in the refactored code path as called by run-jc.sh
class SamzaYarnAppMasterLifecycle(containerMem: Int, containerCpu: Int, samzaAppState: SamzaApplicationState, state: YarnAppState, amClient: AMRMClientAsync[ContainerRequest],
  isApplicationMasterHighAvailabilityEnabled: Boolean) extends Logging {
  var validResourceRequest = true
  var shutdownMessage: String = null
  var webApp: SamzaYarnAppMasterService = null
  def onInit(): util.Set[ContainerId] = {
    val host = state.nodeHost
    val response = amClient.registerApplicationMaster(host, state.rpcUrl.getPort, "%s:%d" format (host, state.trackingUrl.getPort))

    // validate that the YARN cluster can handle our container resource requirements
    val maxCapability = response.getMaximumResourceCapability
    val maxMem = maxCapability.getMemory
    val maxCpu = maxCapability.getVirtualCores
    val previousAttemptContainers = new util.HashSet[ContainerId]()
    if (isApplicationMasterHighAvailabilityEnabled) {
      val yarnIdToprocIdMap = new HashMap[String, String]()
      samzaAppState.processorToExecutionId.asScala foreach { entry => yarnIdToprocIdMap.put(entry._2, entry._1) }
      response.getContainersFromPreviousAttempts.asScala foreach { (ctr: Container) =>
        val samzaProcId = yarnIdToprocIdMap.get(ctr.getId.toString)
        info("Received container from previous attempt with samza processor id %s and yarn container id %s" format(samzaProcId, ctr.getId.toString))
        samzaAppState.pendingProcessors.put(samzaProcId,
          new SamzaResource(ctr.getResource.getVirtualCores, ctr.getResource.getMemory, ctr.getNodeId.getHost, ctr.getId.toString))
        state.pendingProcessors.put(samzaProcId, new YarnContainer(ctr))
        previousAttemptContainers.add(ctr.getId)
      }
    }
    info("Got AM register response. The YARN RM supports container requests with max-mem: %s, max-cpu: %s" format (maxMem, maxCpu))

    if (containerMem > maxMem || containerCpu > maxCpu) {
      shutdownMessage = "The YARN cluster is unable to run your job due to unsatisfiable resource requirements. You asked for mem: %s, and cpu: %s." format (containerMem, containerCpu)
      error(shutdownMessage)
      validResourceRequest = false
      samzaAppState.status = SamzaAppStatus.FAILED;
      samzaAppState.jobHealthy.set(false)
    }
    previousAttemptContainers
  }

  def onReboot() {
    throw new SamzaException("Received a reboot signal from the RM, so throwing an exception to reboot the AM.")
  }

  def onShutdown(samzaAppStatus: SamzaAppStatus) {
    val yarnStatus: FinalApplicationStatus = getStatus(samzaAppStatus)
    info("Shutting down SamzaAppStatus: " + samzaAppStatus + " yarn status: " + yarnStatus)
    //The value of state.status is set to either SUCCEEDED or FAILED for errors we catch and handle - like container failures
    //All other AM failures (errors in callbacks/connection failures after retries/token expirations) should not unregister the AM,
    //allowing the RM to restart it (potentially on a different host)
    if(samzaAppStatus != SamzaAppStatus.UNDEFINED) {
      info("Unregistering AM from the RM.")
      try {
        amClient.unregisterApplicationMaster(yarnStatus, shutdownMessage, null)
        info("Unregister complete.")
      } catch {
        case ex: InvalidApplicationMasterRequestException =>
          // Once the NM dies, the corresponding app attempt ID is removed from the RM cache so that the RM can spin up a new AM and its containers.
          // Hence, this throws InvalidApplicationMasterRequestException since that AM is unregistered with the RM already.
          info("Removed application attempt from RM cache because the AM died. Unregister complete.")
        case ex @ (_ : YarnException | _ : IOException) =>
          error("Caught an exception while trying to unregister AM. Trying to stop other components.", ex)
      }
    }
    else {
      info("Not unregistering AM from the RM. This will enable RM retries")
    }
  }

  def getStatus(samzaAppStatus: SamzaAppStatus): FinalApplicationStatus = {
    if (samzaAppStatus == SamzaAppStatus.FAILED)
       return FinalApplicationStatus.FAILED
    if(samzaAppStatus == SamzaAppStatus.SUCCEEDED)
       return FinalApplicationStatus.SUCCEEDED

   return FinalApplicationStatus.UNDEFINED
  }


  def shouldShutdown = !validResourceRequest
}
