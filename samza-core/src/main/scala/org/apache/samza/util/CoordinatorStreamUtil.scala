/*
 *
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
 *
 */

package org.apache.samza.util

import org.apache.samza.SamzaException
import org.apache.samza.config._
import org.apache.samza.system.{SystemFactory, SystemStream}
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals

import scala.collection.immutable.Map
import scala.collection.JavaConverters._

object CoordinatorStreamUtil {
  /**
    * Given a job's full config object, build a subset config which includes
    * only the job name, job id, and system config for the coordinator stream.
    */
  def buildCoordinatorStreamConfig(config: Config) = {
    val (jobName, jobId) = getJobNameAndId(config)
    // Build a map with just the system config and job.name/job.id. This is what's required to start the JobCoordinator.
    val map = config.subset(SystemConfig.SYSTEM_ID_PREFIX format config.getCoordinatorSystemName, false).asScala ++
      Map[String, String](
        JobConfig.JOB_NAME -> jobName,
        JobConfig.JOB_ID -> jobId,
        JobConfig.JOB_COORDINATOR_SYSTEM -> config.getCoordinatorSystemName,
        JobConfig.MONITOR_PARTITION_CHANGE_FREQUENCY_MS -> String.valueOf(config.getMonitorPartitionChangeFrequency))
    new MapConfig(map.asJava)
  }

  /**
    * Get the coordinator system stream from the configuration
    * @param config
    * @return
    */
  def getCoordinatorSystemStream(config: Config) = {
    val systemName = config.getCoordinatorSystemName
    val (jobName, jobId) = getJobNameAndId(config)
    val streamName = getCoordinatorStreamName(jobName, jobId)
    new SystemStream(systemName, streamName)
  }

  /**
    * Get the coordinator system factory from the configuration
    * @param config
    * @return
    */
  def getCoordinatorSystemFactory(config: Config) = {
    val systemName = config.getCoordinatorSystemName
    val systemConfig = new SystemConfig(config)
    val systemFactoryClassName = JavaOptionals.toRichOptional(systemConfig.getSystemFactory(systemName)).toOption
      .getOrElse(throw new SamzaException("Missing configuration: " + SystemConfig.SYSTEM_FACTORY_FORMAT format systemName))
    Util.getObj(systemFactoryClassName, classOf[SystemFactory])
  }

  /**
    * Generates a coordinator stream name based on the job name and job id
    * for the job. The format of the stream name will be:
    * &#95;&#95;samza_coordinator_&lt;JOBNAME&gt;_&lt;JOBID&gt;.
    */
  def getCoordinatorStreamName(jobName: String, jobId: String) = {
    "__samza_coordinator_%s_%s" format (jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"))
  }

  /**
    * Get a job's name and ID given a config. Job ID is defaulted to 1 if not
    * defined in the config, and job name must be defined in config.
    *
    * @return A tuple of (jobName, jobId)
    */
  private def getJobNameAndId(config: Config) = {
    (config.getName.getOrElse(throw new ConfigException("Missing required config: job.name")),
      config.getJobId)
  }
}
