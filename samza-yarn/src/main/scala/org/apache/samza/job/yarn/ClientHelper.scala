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

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.samza.config.{Config, JobConfig, YarnConfig}
import org.apache.samza.coordinator.stream.CoordinatorStreamWriter
import org.apache.samza.coordinator.stream.messages.SetConfig

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.HashMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
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
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.fs.FileSystem
import org.apache.samza.SamzaException
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus.New
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.job.ApplicationStatus.SuccessfulFinish
import org.apache.samza.job.ApplicationStatus.UnsuccessfulFinish
import org.apache.samza.util.Logging
import java.io.IOException
import java.nio.ByteBuffer

import org.apache.http.impl.client.HttpClientBuilder
import org.apache.samza.webapp.ApplicationMasterRestClient

object ClientHelper {
  val applicationType = "Samza"

  val CREDENTIALS_FILE = "credentials"

  val SOURCE = "yarn"
}

/**
 * Client helper class required to submit an application master start script to the resource manager. Also
 * allows us to forcefully shut-down the application master which in-turn will shut-down the corresponding
 * container and its processes.
 */
class ClientHelper(conf: Configuration) extends Logging {
  val yarnClient = createYarnClient

  private[yarn] def createYarnClient() = {
    val yarnClient = YarnClient.createYarnClient
    info("trying to connect to RM %s" format conf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS))
    yarnClient.init(conf)
    yarnClient.start
    yarnClient
  }

  private[yarn] def createAmClient(applicationReport: ApplicationReport) = {
    val trackingUrl = applicationReport.getTrackingUrl
    val rpcPort = applicationReport.getRpcPort

    new ApplicationMasterRestClient(HttpClientBuilder.create.build, trackingUrl, rpcPort)
  }

  var jobContext: JobContext = null

  /**
   * Generate an application and submit it to the resource manager to start an application master
   */
  def submitApplication(config: Config, cmds: List[String], env: Option[Map[String, String]], name: Option[String]): Option[ApplicationId] = {
    val app = yarnClient.createApplication
    val newAppResponse = app.getNewApplicationResponse

    val yarnConfig = new YarnConfig(config)

    val packagePath = new Path(yarnConfig.getPackagePath)
    val mem = yarnConfig.getAMContainerMaxMemoryMb
    val cpu = yarnConfig.getAMContainerMaxCpuCores
    val queueName = Option(yarnConfig.getQueueName)
    val appMasterLabel = Option(yarnConfig.getAMContainerLabel)

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

    jobContext = new JobContext
    jobContext.setAppId(newAppResponse.getApplicationId)
    val appId = jobContext.getAppId

    info("preparing to request resources for app id %s" format appId.get)

    val appCtx = app.getApplicationSubmissionContext
    val containerCtx = Records.newRecord(classOf[ContainerLaunchContext])
    val resource = Records.newRecord(classOf[Resource])
    val packageResource = Records.newRecord(classOf[LocalResource])

    name match {
      case Some(name) => { appCtx.setApplicationName(name) }
      case None => { appCtx.setApplicationName(appId.get.toString) }
    }

    appMasterLabel match {
      case Some(label) => {
        appCtx.setNodeLabelExpression(label)
        info("set yarn node label expression to %s" format queueName)
      }
      case None =>
    }

    queueName match {
      case Some(queueName) => {
        appCtx.setQueue(queueName)
        info("set yarn queue name to %s" format queueName)
      }
      case None =>
    }

    // TODO: remove the customized approach for package resource and use the common one.
    // But keep it now for backward compatibility.
    // set the local package so that the containers and app master are provisioned with it
    val packageUrl = ConverterUtils.getYarnUrlFromPath(packagePath)
    val fs = packagePath.getFileSystem(conf)
    val fileStatus = fs.getFileStatus(packagePath)

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
    containerCtx.setCommands(cmds.asJava)
    info("set command to %s for %s" format (cmds, appId.get))

    appCtx.setApplicationId(appId.get)
    info("set app ID to %s" format appId.get)

    val localResources: HashMap[String, LocalResource] = HashMap[String, LocalResource]()
    localResources += "__package" -> packageResource


    // include the resources from the universal resource configurations
    try {
      val resourceMapper = new LocalizerResourceMapper(new LocalizerResourceConfig(config), new YarnConfiguration(conf))
      localResources ++= resourceMapper.getResourceMap.asScala
    } catch {
      case e: LocalizerResourceException => {
        throw new SamzaException("Exception during resource mapping from config. ", e)
      }
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      validateJobConfig(config)

      setupSecurityToken(fs, containerCtx)
      info("set security token for %s" format appId.get)

      val amLocalResources = setupAMLocalResources(fs, Option(yarnConfig.getYarnKerberosPrincipal), Option(yarnConfig.getYarnKerberosKeytab))
      localResources ++= amLocalResources

      val securityYarnConfig  = getSecurityYarnConfig
      val coordinatorStreamWriter: CoordinatorStreamWriter = new CoordinatorStreamWriter(config)
      coordinatorStreamWriter.start()

      securityYarnConfig.foreach {
        case (key: String, value: String) =>
          coordinatorStreamWriter.sendMessage(SetConfig.TYPE, key, value)
      }
      coordinatorStreamWriter.stop()
    }

    // prepare all local resources for localizer
    info("localResources is: %s" format localResources)
    containerCtx.setLocalResources(localResources.asJava)
    info("set local resources on application master for %s" format appId.get)

    env match {
      case Some(env) => {
        containerCtx.setEnvironment(env.asJava)
        info("set environment variables to %s for %s" format (env, appId.get))
      }
      case None =>
    }

    appCtx.setAMContainerSpec(containerCtx)
    appCtx.setApplicationType(ClientHelper.applicationType)
    info("submitting application request for %s" format appId.get)
    yarnClient.submitApplication(appCtx)
    appId
  }

  /**
    * Gets the list of Yarn [[org.apache.hadoop.yarn.api.records.ApplicationId]]
    * corresponding to the specified appName and are "active".
    * <p>
    * In this context, "active" means that the application is starting or running
    * and is not in any terminated state.
    * <p>
    * In Samza, an appName should be unique and there should only be one active
    * applicationId for a given appName, but this can be violated in unusual cases
    * like while troubleshooting a new application. So, this method returns as many
    * active application ids as it finds.
    *
    * @param appName the app name as found in the Name column in the Yarn application list.
    * @return        the active application ids.
    */
  def getActiveApplicationIds(appName: String): List[ApplicationId] = {
    val applicationReports = yarnClient.getApplications

    applicationReports
      .asScala
        .filter(applicationReport => isActiveApplication(applicationReport)
          && appName.equals(applicationReport.getName))
        .map(applicationReport => applicationReport.getApplicationId)
        .toList
  }

  def getPreviousApplicationIds(appName: String): List[ApplicationId] = {
    val applicationReports = yarnClient.getApplications

    applicationReports
      .asScala
      .filter(applicationReport => (!(isActiveApplication(applicationReport))
        && appName.equals(applicationReport.getName)))
      .map(applicationReport => applicationReport.getApplicationId)
      .toList
  }

  def status(appId: ApplicationId): Option[ApplicationStatus] = {
    val statusResponse = yarnClient.getApplicationReport(appId)
    info("Got state: %s, final status: %s".format(statusResponse.getYarnApplicationState, statusResponse.getFinalApplicationStatus))
    toAppStatus(statusResponse)
  }

  def kill(appId: ApplicationId) {
    yarnClient.killApplication(appId)
  }

  def getApplicationMaster(appId: ApplicationId): Option[ApplicationReport] = {
    yarnClient
      .getApplications
      .asScala
      .find(appRep => appId.equals(appRep.getApplicationId))
  }

  def getApplicationMasters(status: Option[ApplicationStatus]): List[ApplicationReport] = {
    val getAppsRsp = yarnClient.getApplications

    status match {
      case Some(status) => getAppsRsp
        .asScala
        .filter(appRep => status.equals(toAppStatus(appRep).get))
        .toList
      case None => getAppsRsp.asScala.toList
    }
  }

  private def isActiveApplication(applicationReport: ApplicationReport): Boolean = {
    (Running.equals(toAppStatus(applicationReport).get)
    || New.equals(toAppStatus(applicationReport).get))
  }

  def toAppStatus(applicationReport: ApplicationReport): Option[ApplicationStatus] = {
    val state = applicationReport.getYarnApplicationState
    val status = applicationReport.getFinalApplicationStatus
    (state, status) match {
      case (YarnApplicationState.FINISHED, FinalApplicationStatus.SUCCEEDED) | (YarnApplicationState.KILLED, FinalApplicationStatus.KILLED) => Some(SuccessfulFinish)
      case (YarnApplicationState.KILLED, _) | (YarnApplicationState.FAILED, _) | (YarnApplicationState.FINISHED, _) =>
        val diagnostics = applicationReport.getDiagnostics
        if (StringUtils.isEmpty(diagnostics)) {
          Some(UnsuccessfulFinish)
        } else {
          Some(ApplicationStatus.unsuccessfulFinish(new SamzaException(diagnostics)))
        }
      case (YarnApplicationState.NEW, _) | (YarnApplicationState.SUBMITTED, _) => Some(New)
      case _ =>
        if (allContainersRunning(applicationReport)) {
          Some(Running)
        } else {
          Some(New)
        }
    }
  }

  def allContainersRunning(applicationReport: ApplicationReport): Boolean = {
    val amClient: ApplicationMasterRestClient = createAmClient(applicationReport)
    try {
      val metrics = amClient.getMetrics
      val neededContainers = Integer.parseInt(metrics.get("needed-containers").toString)
      if (neededContainers == 0) {
        true
      } else {
        false
      }
    } finally {
      amClient.close()
    }
  }

  private[yarn] def validateJobConfig(config: Config) = {
    import org.apache.samza.config.JobConfig.Config2Job

    if (config.getSecurityManagerFactory.isEmpty) {
      throw new SamzaException(s"Job config ${JobConfig.JOB_SECURITY_MANAGER_FACTORY} not found. This config must be set for a secure cluster")
    }
  }

  private def setupSecurityToken(fs: FileSystem, amContainer: ContainerLaunchContext): Unit = {
    info("security is enabled")
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
    if (tokenRenewer == null || tokenRenewer.length() == 0) {
      throw new IOException(
        "Can't get Master Kerberos principal for the RM to use as renewer");
    }

    val tokens =
      fs.addDelegationTokens(tokenRenewer, credentials)
    tokens.foreach { token => info("Got dt for " + fs.getUri() + "; " + token) }
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    amContainer.setTokens(ByteBuffer.wrap(dob.getData))
  }

  private[yarn] def setupAMLocalResources(fs: FileSystem, principal: Option[String], keytab: Option[String]) = {
    if (principal.isEmpty || keytab.isEmpty) {
      throw new SamzaException("You need to set both %s and %s on a secure cluster" format(YarnConfig.YARN_KERBEROS_PRINCIPAL, YarnConfig
        .YARN_KERBEROS_KEYTAB))
    }

    val localResources = HashMap[String, LocalResource]()

    // create application staging dir
    val created = YarnJobUtil.createStagingDir(jobContext, fs)
    created match {
      case Some(appStagingDir) =>
        jobContext.setAppStagingDir(appStagingDir)
        val destFilePath = addLocalFile(fs, keytab.get, appStagingDir)
        localResources.put(destFilePath.getName(), getLocalResource(fs, destFilePath, LocalResourceType.FILE))
        localResources.toMap
      case None => throw new SamzaException(s"Failed to create staging directory for %s" format jobContext.getAppId)
    }
  }

  private def addLocalFile(fs: FileSystem, localFile: String, destDirPath: Path) = {
    val srcFilePath = new Path(localFile)
    val destFilePath = new Path(destDirPath, srcFilePath.getName)
    fs.copyFromLocalFile(srcFilePath, destFilePath)
    val localFilePermission = FsPermission.createImmutable(Integer.parseInt("400", 8).toShort)
    fs.setPermission(destFilePath, localFilePermission)
    destFilePath
  }

  private def getLocalResource(fs: FileSystem, destFilePath: Path, resourceType: LocalResourceType) = {
    val localResource = Records.newRecord(classOf[LocalResource])
    val fileStatus = fs.getFileStatus(destFilePath)
    localResource.setResource(ConverterUtils.getYarnUrlFromPath(destFilePath))
    localResource.setSize(fileStatus.getLen())
    localResource.setTimestamp(fileStatus.getModificationTime())
    localResource.setType(resourceType)
    localResource.setVisibility(LocalResourceVisibility.APPLICATION)
    localResource
  }

  private[yarn] def getSecurityYarnConfig = {
    val stagingDir = jobContext.getAppStagingDir.get
    val credentialsFile = new Path(stagingDir, ClientHelper.CREDENTIALS_FILE)
    val jobConfigs = HashMap[String, String]()

    jobConfigs += YarnConfig.YARN_CREDENTIALS_FILE -> credentialsFile.toString
    jobConfigs += YarnConfig.YARN_JOB_STAGING_DIRECTORY -> stagingDir.toString

    jobConfigs.toMap
  }

  /**
    * Cleanup application staging directory.
    */
  def cleanupStagingDir(): Unit = {
    if (jobContext != null) {
      YarnJobUtil.cleanupStagingDir(jobContext, FileSystem.get(conf))
    }
  }
}
