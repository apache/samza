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

package org.apache.samza.config


import java.io.File

import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.util.Logging

object JobConfig {
  // job config constants
  val STREAM_JOB_FACTORY_CLASS = "job.factory.class" // streaming.job_factory_class

  /**
   * job.config.rewriters is a CSV list of config rewriter names. Each name is determined
   * by the %s value in job.config.rewriter.%s.class. For example, if you define
   * job.config.rewriter.some-regex.class=org.apache.samza.config.RegExTopicGenerator,
   * then the rewriter config would be set to job.config.rewriters = some-regex.
   */
  val CONFIG_REWRITERS = "job.config.rewriters" // streaming.job_config_rewriters
  val CONFIG_REWRITER_CLASS = "job.config.rewriter.%s.class" // streaming.job_config_rewriter_class - regex, system, config
  val JOB_NAME = "job.name" // streaming.job_name
  val JOB_ID = "job.id" // streaming.job_id
  val SAMZA_FWK_PATH = "samza.fwk.path"
  val SAMZA_FWK_VERSION = "samza.fwk.version"
  val JOB_COORDINATOR_SYSTEM = "job.coordinator.system"
  val JOB_DEFAULT_SYSTEM = "job.default.system"
  val JOB_CONTAINER_COUNT = "job.container.count"
  val JOB_CONTAINER_THREAD_POOL_SIZE = "job.container.thread.pool.size"
  val JOB_CONTAINER_SINGLE_THREAD_MODE = "job.container.single.thread.mode"
  val JOB_INTERMEDIATE_STREAM_PARTITIONS = "job.intermediate.stream.partitions"
  val JOB_DEBOUNCE_TIME_MS = "job.debounce.time.ms"
  val DEFAULT_DEBOUNCE_TIME_MS = 20000

  val SSP_GROUPER_FACTORY = "job.systemstreampartition.grouper.factory"

  val SSP_MATCHER_CLASS = "job.systemstreampartition.matcher.class"

  val SSP_MATCHER_CLASS_REGEX = "org.apache.samza.system.RegexSystemStreamPartitionMatcher"

  val SSP_MATCHER_CLASS_RANGE = "org.apache.samza.system.RangeSystemStreamPartitionMatcher"

  val SSP_MATCHER_CONFIG_REGEX = "job.systemstreampartition.matcher.config.regex"

  val SSP_MATCHER_CONFIG_RANGES = "job.systemstreampartition.matcher.config.ranges"

  val SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX = "job.systemstreampartition.matcher.config.job.factory.regex"

  val DEFAULT_SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX = "org\\.apache\\.samza\\.job\\.local(.*ProcessJobFactory|.*ThreadJobFactory)"

  // number of partitions in the checkpoint stream should be 1. But sometimes,
  // if a stream was created(automatically) with the wrong number of partitions(default number of partitions
  // for new streams), there is no easy fix for the user (topic deletion or reducing of number of partitions
  // is not yet supported, and auto-creation of the topics cannot be always easily tuned off).
  // So we add a setting that allows for the job to continue even though number of partitions is not 1.
  val JOB_FAIL_CHECKPOINT_VALIDATION = "job.checkpoint.validation.enabled"
  val MONITOR_PARTITION_CHANGE = "job.coordinator.monitor-partition-change"
  val MONITOR_PARTITION_CHANGE_FREQUENCY_MS = "job.coordinator.monitor-partition-change.frequency.ms"
  val DEFAULT_MONITOR_PARTITION_CHANGE_FREQUENCY_MS = 300000
  val JOB_SECURITY_MANAGER_FACTORY = "job.security.manager.factory"

  // Processor Config Constants
  val PROCESSOR_ID = "processor.id"
  val PROCESSOR_LIST = "processor.list"

  // Represents the store path for non-changelog stores.
  val JOB_NON_LOGGED_STORE_PATH = "job.non-logged.store.path"

  // Represents the store path for stores with changelog enabled. Typically the stores are not cleaned up
  // across application restarts
  val JOB_LOGGED_STORE_PATH = "job.logged.store.path"

  implicit def Config2Job(config: Config) = new JobConfig(config)

  /**
   * reads the config to figure out if split deployment is enabled
   * and fwk directory is setup
   * @return fwk + "/" + version
   */
  def getFwkPath (conf: Config) = {
    var fwkPath = conf.get(JobConfig.SAMZA_FWK_PATH, "")
    var fwkVersion = conf.get(JobConfig.SAMZA_FWK_VERSION)
    if (fwkVersion == null || fwkVersion.isEmpty()) {
      fwkVersion = "STABLE"
    }
    if (! fwkPath.isEmpty()) {
      fwkPath = fwkPath + File.separator  + fwkVersion
    }
    fwkPath
  }
}

class JobConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  def getName = getOption(JobConfig.JOB_NAME)

  def getCoordinatorSystemName = {
    val system = getCoordinatorSystemNameOrNull
    if (system == null) {
      throw new ConfigException("Missing job.coordinator.system configuration. Cannot proceed with job execution.")
    }
    system
  }

  /**
    * Gets the System to use for reading/writing the coordinator stream. Uses the following precedence.
    *
    * 1. If job.coordinator.system is defined, that value is used.
    * 2. If job.default.system is defined, that value is used.
    * 3. None
    */
  def getCoordinatorSystemNameOrNull =  getOption(JobConfig.JOB_COORDINATOR_SYSTEM).getOrElse(getDefaultSystem.orNull)

  def getDefaultSystem = getOption(JobConfig.JOB_DEFAULT_SYSTEM)

  def getContainerCount = {
    getOption(JobConfig.JOB_CONTAINER_COUNT) match {
      case Some(count) => count.toInt
      case _ =>
        // To maintain backwards compatibility, honor yarn.container.count for now.
        // TODO get rid of this in a future release.
        getOption("yarn.container.count") match {
          case Some(count) =>
            warn("Configuration 'yarn.container.count' is deprecated. Please use %s." format JobConfig.JOB_CONTAINER_COUNT)
            count.toInt
          case _ => 1
        }
    }
  }

  def getMonitorPartitionChangeFrequency = getInt(
    JobConfig.MONITOR_PARTITION_CHANGE_FREQUENCY_MS,
    JobConfig.DEFAULT_MONITOR_PARTITION_CHANGE_FREQUENCY_MS)

  def getStreamJobFactoryClass = getOption(JobConfig.STREAM_JOB_FACTORY_CLASS)

  def getJobId = getOption(JobConfig.JOB_ID)

  def failOnCheckpointValidation = { getBoolean(JobConfig.JOB_FAIL_CHECKPOINT_VALIDATION, true) }

  def getConfigRewriters = getOption(JobConfig.CONFIG_REWRITERS)

  def getConfigRewriterClass(name: String) = getOption(JobConfig.CONFIG_REWRITER_CLASS format name)

  def getSystemStreamPartitionGrouperFactory = getOption(JobConfig.SSP_GROUPER_FACTORY).getOrElse(classOf[GroupByPartitionFactory].getCanonicalName)

  def getSecurityManagerFactory = getOption(JobConfig.JOB_SECURITY_MANAGER_FACTORY)

  def getSSPMatcherClass = getOption(JobConfig.SSP_MATCHER_CLASS)

  def getSSPMatcherConfigRegex = getExcept(JobConfig.SSP_MATCHER_CONFIG_REGEX)

  def getSSPMatcherConfigRanges = getExcept(JobConfig.SSP_MATCHER_CONFIG_RANGES)

  def getSSPMatcherConfigJobFactoryRegex = getOrElse(JobConfig.SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX, JobConfig.DEFAULT_SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX)

  def getThreadPoolSize = getOption(JobConfig.JOB_CONTAINER_THREAD_POOL_SIZE) match {
    case Some(size) => size.toInt
    case _ => 0
  }

  def getSingleThreadMode = getOption(JobConfig.JOB_CONTAINER_SINGLE_THREAD_MODE) match {
    case Some(mode) => mode.toBoolean
    case _ => false
  }

  def getDebounceTimeMs = getInt(JobConfig.JOB_DEBOUNCE_TIME_MS, JobConfig.DEFAULT_DEBOUNCE_TIME_MS)

  def getNonLoggedStorePath = getOption(JobConfig.JOB_NON_LOGGED_STORE_PATH)

  def getLoggedStorePath = getOption(JobConfig.JOB_LOGGED_STORE_PATH)
}
