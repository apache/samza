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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig
import org.apache.samza.util.Util
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.job.StreamJob
import org.apache.samza.job.ApplicationStatus.SuccessfulFinish
import org.apache.samza.job.ApplicationStatus.UnsuccessfulFinish
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.YarnConfig
import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.SamzaException
import org.apache.samza.serializers.model.SamzaObjectMapper
import org.apache.samza.config.JobConfig.Config2Job
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import org.apache.samza.config.MapConfig
import org.apache.samza.config.ConfigException
import org.apache.samza.config.SystemConfig



/**
 * Starts the application manager
 */
class YarnJob(config: Config, hadoopConfig: Configuration) extends StreamJob {

  val client = new ClientHelper(hadoopConfig)
  var appId: Option[ApplicationId] = None
  val yarnConfig = new YarnConfig(config)
  val logger = LoggerFactory.getLogger(this.getClass)

  def submit: YarnJob = {

    val cmdExec = buildAmCmd()

    appId = client.submitApplication(
      new Path(yarnConfig.getPackagePath),
      yarnConfig.getAMContainerMaxMemoryMb,
      1,
      List(
       // we need something like this:
       //"export SAMZA_LOG_DIR=%s && ln -sfn %s logs && exec <fwk_path>/bin/run-am.sh 1>logs/%s 2>logs/%s"

        "export SAMZA_LOG_DIR=%s && ln -sfn %s logs && exec %s 1>logs/%s 2>logs/%s"
           format (ApplicationConstants.LOG_DIR_EXPANSION_VAR, ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                   cmdExec, ApplicationConstants.STDOUT, ApplicationConstants.STDERR)),

      Some({
        val coordinatorSystemConfig = Util.buildCoordinatorStreamConfig(config)
        val envMap = Map(
          ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG -> Util.envVarEscape(SamzaObjectMapper.getObjectMapper.writeValueAsString(coordinatorSystemConfig)),
          ShellCommandConfig.ENV_JAVA_OPTS -> Util.envVarEscape(yarnConfig.getAmOpts))
        val amJavaHome = yarnConfig.getAMJavaHome
        val envMapWithJavaHome = if(amJavaHome == null) {
          envMap
        } else {
          envMap + (ShellCommandConfig.ENV_JAVA_HOME -> amJavaHome)
        }
        envMapWithJavaHome
      }),
      Some("%s_%s" format (config.getName.get, config.getJobId.getOrElse(1))),
      Option(yarnConfig.getQueueName))
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

    var cmdExec = "./__package/bin/run-am.sh" // default location

    if (!fwkPath.isEmpty()) {
      // if we have framework installed as a separate package - use it
      cmdExec = fwkPath + "/" + fwkVersion + "/bin/run-am.sh"

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
        case Some(s) => if (SuccessfulFinish.equals(s) || UnsuccessfulFinish.equals(s)) return s
        case None => null
      }

      Thread.sleep(1000)
    }

    Running
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

    Running
  }

  def getStatus: ApplicationStatus = {
    appId match {
      case Some(appId) => client.status(appId).getOrElse(null)
      case None => null
    }
  }

  def kill: YarnJob = {
    appId match {
      case Some(appId) => client.kill(appId)
      case None => None
    }
    this
  }
}
