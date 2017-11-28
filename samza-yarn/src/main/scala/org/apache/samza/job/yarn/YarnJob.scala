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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.{Config, JobConfig, ShellCommandConfig, YarnConfig}
import org.apache.samza.job.ApplicationStatus.{Running, SuccessfulFinish, UnsuccessfulFinish}
import org.apache.samza.job.{ApplicationStatus, StreamJob}
import org.apache.samza.serializers.model.SamzaObjectMapper
import org.apache.samza.util.Util
import org.slf4j.LoggerFactory

/**
 * Starts the application manager
 */
class YarnJob(config: Config, hadoopConfig: Configuration) extends StreamJob {

  val client = new ClientHelper(hadoopConfig)
  var appId: Option[ApplicationId] = None
  val yarnConfig = new YarnConfig(config)
  val logger = LoggerFactory.getLogger(this.getClass)

  def submit: YarnJob = {
    try {
      val cmdExec = buildAmCmd()

      appId = client.submitApplication(
        config,
        List(
          // we need something like this:
          //"export SAMZA_LOG_DIR=%s && ln -sfn %s logs && exec <fwk_path>/bin/run-am.sh 1>logs/%s 2>logs/%s"

          "export SAMZA_LOG_DIR=%s && ln -sfn %s logs && exec %s 1>logs/%s 2>logs/%s"
            format (ApplicationConstants.LOG_DIR_EXPANSION_VAR, ApplicationConstants.LOG_DIR_EXPANSION_VAR,
            cmdExec, ApplicationConstants.STDOUT, ApplicationConstants.STDERR)),
        Some({
          val coordinatorSystemConfig = Util.buildCoordinatorStreamConfig(config)
          val envMap = Map(
            ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG -> Util.envVarEscape(SamzaObjectMapper.getObjectMapper.writeValueAsString
            (coordinatorSystemConfig)),
            ShellCommandConfig.ENV_JAVA_OPTS -> Util.envVarEscape(yarnConfig.getAmOpts))
          val amJavaHome = yarnConfig.getAMJavaHome
          val envMapWithJavaHome = if (amJavaHome == null) {
            envMap
          } else {
            envMap + (ShellCommandConfig.ENV_JAVA_HOME -> amJavaHome)
          }
          envMapWithJavaHome
        }),
        Some("%s_%s" format(config.getName.get, config.getJobId.getOrElse(1)))
      )
    } catch {
      case e: Throwable =>
        client.cleanupStagingDir
        throw e
    }

    this
  }

  def buildAmCmd() =  {
    // figure out if we have framework is deployed into a separate location
    val fwkPath = config.get(JobConfig.SAMZA_FWK_PATH, "")
    var fwkVersion = config.get(JobConfig.SAMZA_FWK_VERSION)
    if (fwkVersion == null || fwkVersion.isEmpty()) {
      fwkVersion = "STABLE"
    }
    logger.info("Inside YarnJob: fwk_path is %s, ver is %s use it directly " format(fwkPath, fwkVersion))

    var cmdExec = "./__package/bin/run-jc.sh" // default location

    if (!fwkPath.isEmpty()) {
      // if we have framework installed as a separate package - use it
      cmdExec = fwkPath + "/" + fwkVersion + "/bin/run-jc.sh"

      logger.info("Using FWK path: " + "export SAMZA_LOG_DIR=%s && ln -sfn %s logs && exec %s 1>logs/%s 2>logs/%s".
             format(ApplicationConstants.LOG_DIR_EXPANSION_VAR, ApplicationConstants.LOG_DIR_EXPANSION_VAR, cmdExec,
                    ApplicationConstants.STDOUT, ApplicationConstants.STDERR))

    }
    cmdExec
  }


  def waitForFinish(timeoutMs: Long): ApplicationStatus = {
    val startTimeMs = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      Option(getStatus) match {
        case Some(s) => if (SuccessfulFinish.equals(s) || UnsuccessfulFinish.equals(s))
          client.cleanupStagingDir
          return s
        case None =>
      }

      Thread.sleep(1000)
    }

    getStatus
  }

  def waitForStatus(status: ApplicationStatus, timeoutMs: Long): ApplicationStatus = {
    val startTimeMs = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      Option(getStatus) match {
        case Some(s) => if (status.equals(s)) return status
        case None => null
      }

      Thread.sleep(1000)
    }

    getStatus
  }

  def getStatus: ApplicationStatus = {
    getAppId match {
      case Some(appId) =>
        logger.info("Getting status for applicationId %s" format appId)
        client.status(appId).getOrElse(null)
      case None =>
        logger.info("Unable to report status because no applicationId could be found.")
        ApplicationStatus.SuccessfulFinish
    }
  }

  def kill: YarnJob = {
    // getAppId only returns one appID. Run multiple times to kill dupes (erroneous case)
    getAppId match {
      case Some(appId) =>
        try {
          logger.info("Killing applicationId {}", appId)
          client.kill(appId)
        } finally {
          client.cleanupStagingDir
        }
      case None =>
    }
    this
  }

  private def getAppId: Option[ApplicationId] = {
    appId match {
      case Some(applicationId) =>
       appId
      case None =>
        // Get by name
        config.getName match {
          case Some(jobName) =>
            val applicationName = "%s_%s" format(jobName, config.getJobId.getOrElse(1))
            logger.info("Fetching status from YARN for application name %s" format applicationName)
            val applicationIds = client.getActiveApplicationIds(applicationName)

            if (applicationIds.nonEmpty) {
              // Only return latest one, because there should only be one.
              logger.info("Matching active ids: " + applicationIds.sorted.reverse.toString())
              applicationIds.sorted.reverse.headOption
            } else {
              // Couldn't find an active applicationID. Use one the latest finished ID.
              val pastApplicationIds = client.getPreviousApplicationIds(applicationName)
              // Don't log because there could be many, many previous app IDs for an application.
              pastApplicationIds.sorted.reverse.headOption  // Get latest
            }

          case None =>
            None
        }
    }
  }
}
