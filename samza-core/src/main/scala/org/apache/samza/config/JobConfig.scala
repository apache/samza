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

import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.util.Logging
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory

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
  val JOB_COORDINATOR_SYSTEM = "job.coordinator.system"
  val JOB_CONTAINER_COUNT = "job.container.count"
  val JOB_REPLICATION_FACTOR = "job.coordinator.replication.factor"
  val JOB_SEGMENT_BYTES = "job.coordinator.segment.bytes"
  val SSP_GROUPER_FACTORY = "job.systemstreampartition.grouper.factory"

  implicit def Config2Job(config: Config) = new JobConfig(config)
}

class JobConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  def getName = getOption(JobConfig.JOB_NAME)

  def getCoordinatorSystemName = getOption(JobConfig.JOB_COORDINATOR_SYSTEM).getOrElse(
      throw new ConfigException("Missing job.coordinator.system configuration. Cannot proceed with job execution."))

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

  def getStreamJobFactoryClass = getOption(JobConfig.STREAM_JOB_FACTORY_CLASS)

  def getJobId = getOption(JobConfig.JOB_ID)

  def getConfigRewriters = getOption(JobConfig.CONFIG_REWRITERS)

  def getConfigRewriterClass(name: String) = getOption(JobConfig.CONFIG_REWRITER_CLASS format name)

  def getSystemStreamPartitionGrouperFactory = getOption(JobConfig.SSP_GROUPER_FACTORY).getOrElse(classOf[GroupByPartitionFactory].getCanonicalName)

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
}
