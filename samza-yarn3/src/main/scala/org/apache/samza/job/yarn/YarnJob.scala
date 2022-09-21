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

import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.samza.SamzaException
import org.apache.samza.config.{Config, JobConfig, ShellCommandConfig, YarnConfig}
import org.apache.samza.job.ApplicationStatus.{SuccessfulFinish, UnsuccessfulFinish}
import org.apache.samza.job.{ApplicationStatus, StreamJob}
import org.apache.samza.serializers.model.SamzaObjectMapper
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals
import org.apache.samza.util.{CoordinatorStreamUtil, Logging, Util}

/**
 * Starts the application manager
 */
class YarnJob(config: Config, hadoopConfig: Configuration) extends StreamJob with Logging {

  val client = new ClientHelper(hadoopConfig)
  var appId: Option[ApplicationId] = None
  val yarnConfig = new YarnConfig(config)

  def submit: YarnJob = {
    try {
      val jobConfig = new JobConfig(config)
      val cmdExec = "./__package/" + jobConfig.getCoordinatorExecuteCommand
      val environment = YarnJob.buildEnvironment(config, this.yarnConfig, jobConfig)

      appId = client.submitApplication(
        config,
        List(
          "export SAMZA_LOG_DIR=%s && ln -sfn %s logs && exec %s 1>logs/%s 2>logs/%s"
            format (ApplicationConstants.LOG_DIR_EXPANSION_VAR, ApplicationConstants.LOG_DIR_EXPANSION_VAR,
            cmdExec, ApplicationConstants.STDOUT, ApplicationConstants.STDERR)),
        Some(environment),
        Some("%s_%s" format(jobConfig.getName.get, jobConfig.getJobId))
      )
    } catch {
      case e: Throwable =>
        logger.error("Exception submitting yarn job.", e )
        try {
          // try to clean up. this may throw an exception depending on how far into launching the job we got.
          // we don't want to mask the original problem by throwing this.
          client.cleanupStagingDir
        } catch {
          case ce: Throwable => logger.warn("Exception cleaning Staging Directory after failed launch attempt.", ce)
        } finally {
          throw e
        }
    }

    this
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
        client.status(appId).getOrElse(
          throw new SamzaException("No status was determined for applicationId %s" format appId))
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
        val jobConfig = new JobConfig(config)
        JavaOptionals.toRichOptional(jobConfig.getName).toOption match {
          case Some(jobName) =>
            val applicationName = "%s_%s" format(jobName, jobConfig.getJobId)
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

object YarnJob extends Logging {
  /**
    * Build the environment variable map for the job coordinator execution.
    * Passing multiple separate config objects so that they can be reused for other logic.
    */
  @VisibleForTesting
  private[yarn] def buildEnvironment(config: Config, yarnConfig: YarnConfig,
    jobConfig: JobConfig): Map[String, String] = {
    val envMapBuilder = Map.newBuilder[String, String]
    if (jobConfig.getConfigLoaderFactory.isPresent) {
      envMapBuilder += ShellCommandConfig.ENV_SUBMISSION_CONFIG ->
        Util.envVarEscape(SamzaObjectMapper.getObjectMapper.writeValueAsString(config))
    } else {
      // TODO SAMZA-2432: Clean this up once SAMZA-2405 is completed when legacy flow is removed.
      val coordinatorSystemConfig = CoordinatorStreamUtil.buildCoordinatorStreamConfig(config)
      envMapBuilder += ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG ->
        Util.envVarEscape(SamzaObjectMapper.getObjectMapper.writeValueAsString(coordinatorSystemConfig))
    }
    envMapBuilder += ShellCommandConfig.ENV_JAVA_OPTS -> Util.envVarEscape(yarnConfig.getAmOpts)
    Option.apply(yarnConfig.getAMJavaHome).foreach {
      amJavaHome => envMapBuilder += ShellCommandConfig.ENV_JAVA_HOME -> amJavaHome
    }
    envMapBuilder += ShellCommandConfig.ENV_ADDITIONAL_CLASSPATH_DIR ->
      Util.envVarEscape(config.get(ShellCommandConfig.ADDITIONAL_CLASSPATH_DIR, ""))
    envMapBuilder.result()
  }
}
