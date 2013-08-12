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

import java.util.Collections

import scala.collection.JavaConversions._
import scala.collection.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.api.ClientRMProtocol
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records

import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus.New
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.job.ApplicationStatus.SuccessfulFinish
import org.apache.samza.job.ApplicationStatus.UnsuccessfulFinish

import grizzled.slf4j.Logging
import org.apache.samza.SamzaException

/**
 * Client helper class required to submit an application master start script to the resource manager. Also
 * allows us to forcefully shut-down the application master which in-turn will shut-down the corresponding
 * container and its processes.
 */
class ClientHelper(conf: Configuration) extends Logging {
  val rpc = YarnRPC.create(conf)
  val rmAddress = NetUtils.createSocketAddr(conf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS))
  info("trying to connect to RM %s" format rmAddress)
  val applicationsManager = rpc.getProxy(classOf[ClientRMProtocol], rmAddress, conf).asInstanceOf[ClientRMProtocol]
  var appId: Option[ApplicationId] = None

  /**
   * Generate an application and submit it to the resource manager to start an application master
   */
  def submitApplication(packagePath: Path, memoryMb: Int, cpuCore: Int, user: UserGroupInformation, cmds: List[String], env: Option[Map[String, String]], name: Option[String]): Option[ApplicationId] = {
    val newAppRequest = Records.newRecord(classOf[GetNewApplicationRequest])
    val newAppResponse = applicationsManager.getNewApplication(newAppRequest)
    var mem = memoryMb
    var cpu = cpuCore

    // If we are asking for memory less than the minimum required, bump it
    if (mem < newAppResponse.getMinimumResourceCapability().getMemory()) {
      val min = newAppResponse.getMinimumResourceCapability().getMemory()
      warn("requesting %s megs of memory, which is less than minimum capability of %s, so using minimum" format (mem, min))
      mem = min
    }

    // If we are asking for memory more than the max allowed, shout out
    if (mem > newAppResponse.getMaximumResourceCapability().getMemory()) {
      throw new SamzaException("You're asking for more memory (%s) than is allowed by YARN: %s" format
        (mem, newAppResponse.getMaximumResourceCapability().getMemory()))
    }

    // if we are asking for cpu less than the minimum required, bump it
    if (cpu < newAppResponse.getMinimumResourceCapability().getVirtualCores()) {
      val min = newAppResponse.getMinimumResourceCapability.getVirtualCores()
      warn("requesting %s virtual cores of cpu, which is less than minimum capability of %s, so using minimum" format (cpu, min))
      cpu = min
    }

    // If we are asking for cpu more than the max allowed, shout out
    if (cpu > newAppResponse.getMaximumResourceCapability().getVirtualCores()) {
      throw new SamzaException("You're asking for more CPU (%s) than is allowed by YARN: %s" format
        (cpu, newAppResponse.getMaximumResourceCapability().getVirtualCores()))
    }

    appId = Some(newAppResponse.getApplicationId)

    info("preparing to request resources for app id %s" format appId.get)

    val appCtx = Records.newRecord(classOf[ApplicationSubmissionContext])
    val containerCtx = Records.newRecord(classOf[ContainerLaunchContext])
    val resource = Records.newRecord(classOf[Resource])
    val submitAppRequest = Records.newRecord(classOf[SubmitApplicationRequest])
    val packageResource = Records.newRecord(classOf[LocalResource])

    name match {
      case Some(name) => { appCtx.setApplicationName(name) }
      case None => { appCtx.setApplicationName(appId.toString) }
    }

    env match {
      case Some(env) => {
        containerCtx.setEnvironment(env)
        info("set environment variables to %s for %s" format (env, appId.get))
      }
      case None => None
    }

    // set the local package so that the containers and app master are provisioned with it
    val packageUrl = ConverterUtils.getYarnUrlFromPath(packagePath)
    val fileStatus = packagePath.getFileSystem(conf).getFileStatus(packagePath)

    packageResource.setResource(packageUrl)
    info("set package url to %s for %s" format (packageUrl, appId.get))
    packageResource.setSize(fileStatus.getLen)
    info("set package size to %s for %s" format (fileStatus.getLen, appId.get))
    packageResource.setTimestamp(fileStatus.getModificationTime)
    packageResource.setType(LocalResourceType.ARCHIVE)
    packageResource.setVisibility(LocalResourceVisibility.APPLICATION)

    resource.setMemory(mem)
    info("set memory request to %s for %s" format (mem, appId.get))
    resource.setVirtualCores(cpu)
    info("set cpu core request to %s for %s" format (cpu, appId.get))
    containerCtx.setResource(resource)
    containerCtx.setCommands(cmds.toList)
    info("set command to %s for %s" format (cmds, appId.get))
    containerCtx.setLocalResources(Collections.singletonMap("__package", packageResource))
    appCtx.setApplicationId(appId.get)
    info("set app ID to %s" format (user, appId.get))
    appCtx.setUser(user.getShortUserName)
    info("set user to %s for %s" format (user, appId.get))
    appCtx.setAMContainerSpec(containerCtx)
    submitAppRequest.setApplicationSubmissionContext(appCtx)
    info("submitting application request for %s" format appId.get)
    applicationsManager.submitApplication(submitAppRequest)
    appId
  }

  def status(appId: ApplicationId): Option[ApplicationStatus] = {
    val statusRequest = Records.newRecord(classOf[GetApplicationReportRequest])
    statusRequest.setApplicationId(appId)
    val statusResponse = applicationsManager.getApplicationReport(statusRequest)
    convertState(statusResponse.getApplicationReport)
  }

  def kill(appId: ApplicationId) {
    val killRequest = Records.newRecord(classOf[KillApplicationRequest])
    killRequest.setApplicationId(appId)
    applicationsManager.forceKillApplication(killRequest)
  }

  def getApplicationMaster(appId: ApplicationId): Option[ApplicationReport] = {
    val getAppsReq = Records.newRecord(classOf[GetAllApplicationsRequest])
    val getAppsRsp = applicationsManager.getAllApplications(getAppsReq)

    getAppsRsp.getApplicationList.filter(appRep => appId.equals(appRep.getApplicationId())).headOption
  }

  def getApplicationMasters(status: Option[ApplicationStatus]): List[ApplicationReport] = {
    val getAppsReq = Records.newRecord(classOf[GetAllApplicationsRequest])
    val getAppsRsp = applicationsManager.getAllApplications(getAppsReq)

    status match {
      case Some(status) => getAppsRsp.getApplicationList
        .filter(appRep => status.equals(convertState(appRep).get)).toList
      case None => getAppsRsp.getApplicationList.toList
    }
  }

  private def convertState(appReport: ApplicationReport): Option[ApplicationStatus] = {
    (appReport.getYarnApplicationState(), appReport.getFinalApplicationStatus()) match {
      case (YarnApplicationState.FINISHED, FinalApplicationStatus.SUCCEEDED) => Some(SuccessfulFinish)
      case (YarnApplicationState.KILLED, _) | (YarnApplicationState.FAILED, _) => Some(UnsuccessfulFinish)
      case (YarnApplicationState.NEW, _) | (YarnApplicationState.SUBMITTED, _) => Some(New)
      case _ => Some(Running)
    }
  }
}
