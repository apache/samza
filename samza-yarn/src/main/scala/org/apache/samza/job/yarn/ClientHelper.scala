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

import scala.collection.JavaConversions._
import scala.collection.Map
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records
import org.apache.samza.SamzaException
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus.New
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.job.ApplicationStatus.SuccessfulFinish
import org.apache.samza.job.ApplicationStatus.UnsuccessfulFinish
import org.apache.samza.util.Logging
import java.util.Collections

object ClientHelper {
  val applicationType = "Samza"
}

/**
 * Client helper class required to submit an application master start script to the resource manager. Also
 * allows us to forcefully shut-down the application master which in-turn will shut-down the corresponding
 * container and its processes.
 */
class ClientHelper(conf: Configuration) extends Logging {
  val yarnClient = YarnClient.createYarnClient
  info("trying to connect to RM %s" format conf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS))
  yarnClient.init(conf);
  yarnClient.start
  var appId: Option[ApplicationId] = None

  /**
   * Generate an application and submit it to the resource manager to start an application master
   */
  def submitApplication(packagePath: Path, memoryMb: Int, cpuCore: Int, cmds: List[String], env: Option[Map[String, String]], name: Option[String], queueName: Option[String]): Option[ApplicationId] = {
    val app = yarnClient.createApplication
    val newAppResponse = app.getNewApplicationResponse
    var mem = memoryMb
    var cpu = cpuCore

    // If we are asking for memory more than the max allowed, shout out
    if (mem > newAppResponse.getMaximumResourceCapability().getMemory()) {
      throw new SamzaException("You're asking for more memory (%s) than is allowed by YARN: %s" format
        (mem, newAppResponse.getMaximumResourceCapability().getMemory()))
    }

    // If we are asking for cpu more than the max allowed, shout out
    if (cpu > newAppResponse.getMaximumResourceCapability().getVirtualCores()) {
      throw new SamzaException("You're asking for more CPU (%s) than is allowed by YARN: %s" format
        (cpu, newAppResponse.getMaximumResourceCapability().getVirtualCores()))
    }

    appId = Some(newAppResponse.getApplicationId)

    info("preparing to request resources for app id %s" format appId.get)

    val appCtx = app.getApplicationSubmissionContext
    val containerCtx = Records.newRecord(classOf[ContainerLaunchContext])
    val resource = Records.newRecord(classOf[Resource])
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

    queueName match {
      case Some(queueName) => {
        appCtx.setQueue(queueName)
        info("set yarn queue name to %s" format queueName)
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
    appCtx.setResource(resource)
    containerCtx.setCommands(cmds.toList)
    info("set command to %s for %s" format (cmds, appId.get))
    containerCtx.setLocalResources(Collections.singletonMap("__package", packageResource))
    appCtx.setApplicationId(appId.get)
    info("set app ID to %s" format appId.get)
    appCtx.setAMContainerSpec(containerCtx)
    appCtx.setApplicationType(ClientHelper.applicationType)
    info("submitting application request for %s" format appId.get)
    yarnClient.submitApplication(appCtx)
    appId
  }

  def status(appId: ApplicationId): Option[ApplicationStatus] = {
    val statusResponse = yarnClient.getApplicationReport(appId)
    convertState(statusResponse.getYarnApplicationState, statusResponse.getFinalApplicationStatus)
  }

  def kill(appId: ApplicationId) {
    yarnClient.killApplication(appId)
  }

  def getApplicationMaster(appId: ApplicationId): Option[ApplicationReport] = {
    yarnClient
      .getApplications
      .filter(appRep => appId.equals(appRep.getApplicationId()))
      .headOption
  }

  def getApplicationMasters(status: Option[ApplicationStatus]): List[ApplicationReport] = {
    val getAppsRsp = yarnClient.getApplications

    status match {
      case Some(status) => getAppsRsp
        .filter(appRep => status.equals(convertState(appRep.getYarnApplicationState, appRep.getFinalApplicationStatus).get))
        .toList
      case None => getAppsRsp.toList
    }
  }

  private def convertState(state: YarnApplicationState, status: FinalApplicationStatus): Option[ApplicationStatus] = {
    (state, status) match {
      case (YarnApplicationState.FINISHED, FinalApplicationStatus.SUCCEEDED) => Some(SuccessfulFinish)
      case (YarnApplicationState.KILLED, _) | (YarnApplicationState.FAILED, _) => Some(UnsuccessfulFinish)
      case (YarnApplicationState.NEW, _) | (YarnApplicationState.SUBMITTED, _) => Some(New)
      case _ => Some(Running)
    }
  }
}
