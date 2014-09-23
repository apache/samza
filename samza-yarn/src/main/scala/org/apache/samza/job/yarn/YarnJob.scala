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
import org.apache.samza.util.Util
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.job.StreamJob
import org.apache.samza.job.ApplicationStatus.SuccessfulFinish
import org.apache.samza.job.ApplicationStatus.UnsuccessfulFinish
import org.apache.samza.config.serializers.JsonConfigSerializer
import org.apache.samza.config.YarnConfig.Config2Yarn
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.YarnConfig
import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.SamzaException


object YarnJob {
  val DEFAULT_AM_CONTAINER_MEM = 1024
}

/**
 * Starts the application manager
 */
class YarnJob(config: Config, hadoopConfig: Configuration) extends StreamJob {
  import YarnJob._

  val client = new ClientHelper(hadoopConfig)
  var appId: Option[ApplicationId] = None

  def submit: YarnJob = {
    appId = client.submitApplication(
      new Path(config.getPackagePath.getOrElse(throw new SamzaException("No YARN package path defined in config."))),
      config.getAMContainerMaxMemoryMb.getOrElse(DEFAULT_AM_CONTAINER_MEM),
      1,
      List(
        "export SAMZA_LOG_DIR=%s && ln -sfn %s logs && exec ./__package/bin/run-am.sh 1>logs/%s 2>logs/%s"
          format (ApplicationConstants.LOG_DIR_EXPANSION_VAR, ApplicationConstants.LOG_DIR_EXPANSION_VAR, ApplicationConstants.STDOUT, ApplicationConstants.STDERR)),
      Some(Map(
        ShellCommandConfig.ENV_CONFIG -> Util.envVarEscape(JsonConfigSerializer.toJson(config)),
        ShellCommandConfig.ENV_CONTAINER_NAME -> Util.envVarEscape("application-master"),
        ShellCommandConfig.ENV_JAVA_OPTS -> Util.envVarEscape(config.getAmOpts.getOrElse("")),
        ShellCommandConfig.ENV_JAVA_HOME -> Util.envVarEscape(config.getAMJavaHome.getOrElse("")))),
      Some("%s_%s" format (config.getName.get, config.getJobId.getOrElse(1))))

    this
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
