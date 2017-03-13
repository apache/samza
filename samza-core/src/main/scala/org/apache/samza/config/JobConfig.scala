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
import org.apache.samza.system.{RegexSystemStreamPartitionMatcher, SystemStreamPartitionMatcher}
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
  val jOB_CONTAINER_THREAD_POOL_SIZE = "job.container.thread.pool.size"
  val JOB_CONTAINER_SINGLE_THREAD_MODE = "job.container.single.thread.mode"
  val JOB_REPLICATION_FACTOR = "job.coordinator.replication.factor"
  val JOB_SEGMENT_BYTES = "job.coordinator.segment.bytes"
  val SSP_GROUPER_FACTORY = "job.systemstreampartition.grouper.factory"

  val SSP_MATCHER_CLASS = "job.systemstreampartition.matcher.class";

  val SSP_MATCHER_CLASS_REGEX = "org.apache.samza.system.RegexSystemStreamPartitionMatcher"

  val SSP_MATCHER_CLASS_RANGE = "org.apache.samza.system.RangeSystemStreamPartitionMatcher"

  val SSP_MATCHER_CONFIG_REGEX = "job.systemstreampartition.matcher.config.regex";

  val SSP_MATCHER_CONFIG_RANGES = "job.systemstreampartition.matcher.config.ranges";

  val SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX = "job.systemstreampartition.matcher.config.job.factory.regex";

  val DEFAULT_SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX = "org\\.apache\\.samza\\.job\\.local(.*ProcessJobFactory|.*ThreadJobFactory)";

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

  def getCoordinatorSystemName = getOption(JobConfig.JOB_COORDINATOR_SYSTEM).getOrElse(
      throw new ConfigException("Missing job.coordinator.system configuration. Cannot proceed with job execution."))

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

  def getMonitorPartitionChange = getBoolean(JobConfig.MONITOR_PARTITION_CHANGE, false)

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

  val CHECKPOINT_SEGMENT_BYTES = "task.checkpoint.segment.bytes"
  val CHECKPOINT_REPLICATION_FACTOR = "task.checkpoint.replication.factor"

  def getCoordinatorReplicationFactor = getOption(JobConfig.JOB_REPLICATION_FACTOR) match {
    case Some(rplFactor) => rplFactor
    case _ =>
      getOption(CHECKPOINT_REPLICATION_FACTOR) match {
        case Some(rplFactor) =>
          info("%s was not found. Using %s=%s for coordinator stream" format (JobConfig.JOB_REPLICATION_FACTOR, CHECKPOINT_REPLICATION_FACTOR, rplFactor))
          rplFactor
        case _ => "3"
      }
  }

  def getCoordinatorSegmentBytes = getOption(JobConfig.JOB_SEGMENT_BYTES) match {
    case Some(segBytes) => segBytes
    case _ =>
      getOption(CHECKPOINT_SEGMENT_BYTES) match {
        case Some(segBytes) =>
          info("%s was not found. Using %s=%s for coordinator stream" format (JobConfig.JOB_SEGMENT_BYTES, CHECKPOINT_SEGMENT_BYTES, segBytes))
          segBytes
        case _ => "26214400"
      }
  }

  def getSSPMatcherClass = getOption(JobConfig.SSP_MATCHER_CLASS)

  def getSSPMatcherConfigRegex = getExcept(JobConfig.SSP_MATCHER_CONFIG_REGEX)

  def getSSPMatcherConfigRanges = getExcept(JobConfig.SSP_MATCHER_CONFIG_RANGES)

  def getSSPMatcherConfigJobFactoryRegex = getOrElse(JobConfig.SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX, JobConfig.DEFAULT_SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX)

  def getThreadPoolSize = getOption(JobConfig.jOB_CONTAINER_THREAD_POOL_SIZE) match {
    case Some(size) => size.toInt
    case _ => 0
  }

  def getSingleThreadMode = getOption(JobConfig.JOB_CONTAINER_SINGLE_THREAD_MODE) match {
    case Some(mode) => mode.toBoolean
    case _ => false
  }
}
