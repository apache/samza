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

package org.apache.samza.coordinator

import org.apache.samza.config.Config
import org.apache.samza.job.model.JobModel
import org.apache.samza.SamzaException
import org.apache.samza.container.grouper.task.GroupByContainerCount
import org.apache.samza.util.Util
import org.apache.samza.checkpoint.CheckpointManagerFactory
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory
import java.util
import org.apache.samza.container.TaskName
import org.apache.samza.util.Logging
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.config.StorageConfig.Config2Storage
import scala.collection.JavaConversions._
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.Partition
import org.apache.samza.job.model.TaskModel
import org.apache.samza.system.StreamMetadataCache
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.serializers.model.SamzaObjectMapper
import java.net.URL
import org.apache.samza.system.SystemFactory
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.checkpoint.CheckpointManager
import org.apache.samza.coordinator.server.JobServlet

object JobCoordinator extends Logging {
  /**
   * Build a JobCoordinator using a Samza job's configuration.
   */
  def apply(config: Config, containerCount: Int) = {
    val jobModel = buildJobModel(config, containerCount)
    val server = new HttpServer
    server.addServlet("/*", new JobServlet(jobModel))
    new JobCoordinator(jobModel, server)
  }

  /**
   * Gets a CheckpointManager from the configuration.
   */
  def getCheckpointManager(config: Config) = {
    config.getCheckpointManagerFactory match {
      case Some(checkpointFactoryClassName) =>
        Util
          .getObj[CheckpointManagerFactory](checkpointFactoryClassName)
          .getCheckpointManager(config, new MetricsRegistryMap)
      case _ =>
        if (!config.getStoreNames.isEmpty) {
          throw new SamzaException("Storage factories configured, but no checkpoint manager has been specified.  " +
            "Unable to start job as there would be no place to store changelog partition mapping.")
        }
        null
    }
  }

  /**
   * For each input stream specified in config, exactly determine its
   * partitions, returning a set of SystemStreamPartitions containing them all.
   */
  def getInputStreamPartitions(config: Config) = {
    val inputSystemStreams = config.getInputStreams
    val systemNames = config.getSystemNames.toSet

    // Map the name of each system to the corresponding SystemAdmin
    val systemAdmins = systemNames.map(systemName => {
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
      systemName -> systemFactory.getAdmin(systemName, config)
    }).toMap

    // Get the set of partitions for each SystemStream from the stream metadata
    new StreamMetadataCache(systemAdmins)
      .getStreamMetadata(inputSystemStreams)
      .flatMap {
        case (systemStream, metadata) =>
          metadata
            .getSystemStreamPartitionMetadata
            .keys
            .map(new SystemStreamPartition(systemStream, _))
      }.toSet
  }

  /**
   * Gets a SystemStreamPartitionGrouper object from the configuration.
   */
  def getSystemStreamPartitionGrouper(config: Config) = {
    val factoryString = config.getSystemStreamPartitionGrouperFactory
    val factory = Util.getObj[SystemStreamPartitionGrouperFactory](factoryString)
    factory.getSystemStreamPartitionGrouper(config)
  }

  /**
   * Build a full Samza job model using the job configuration.
   */
  def buildJobModel(config: Config, containerCount: Int) = {
    // TODO containerCount should go away when we generalize the job coordinator, 
    // and have a non-yarn-specific way of specifying container count.
    val checkpointManager = getCheckpointManager(config)
    val allSystemStreamPartitions = getInputStreamPartitions(config)
    val grouper = getSystemStreamPartitionGrouper(config)
    val previousChangelogeMapping = if (checkpointManager != null) {
      checkpointManager.start
      checkpointManager.readChangeLogPartitionMapping
    } else {
      new util.HashMap[TaskName, java.lang.Integer]()
    }
    var maxChangelogPartitionId = previousChangelogeMapping
      .values
      .map(_.toInt)
      .toList
      .sorted
      .lastOption
      .getOrElse(-1)

    // Assign all SystemStreamPartitions to TaskNames.
    val taskModels = {
      val groups = grouper.group(allSystemStreamPartitions)
      info("SystemStreamPartitionGrouper " + grouper + " has grouped the SystemStreamPartitions into the following taskNames:")
      groups
        .map {
          case (taskName, systemStreamPartitions) =>
            val changelogPartition = Option(previousChangelogeMapping.get(taskName)) match {
              case Some(changelogPartitionId) => new Partition(changelogPartitionId)
              case _ =>
                // If we've never seen this TaskName before, then assign it a 
                // new changelog.
                maxChangelogPartitionId += 1
                info("New task %s is being assigned changelog partition %s." format (taskName, maxChangelogPartitionId))
                new Partition(maxChangelogPartitionId)
            }
            new TaskModel(taskName, systemStreamPartitions, changelogPartition)
        }
        .toSet
    }

    // Save the changelog mapping back to the checkpoint manager.
    if (checkpointManager != null) {
      // newChangelogMapping is the merging of all current task:changelog 
      // assignments with whatever we had before (previousChangelogeMapping).
      // We must persist legacy changelog assignments so that 
      // maxChangelogPartitionId always has the absolute max, not the current 
      // max (in case the task with the highest changelog partition mapping 
      // disappears.
      val newChangelogMapping = taskModels.map(taskModel => {
        taskModel.getTaskName -> Integer.valueOf(taskModel.getChangelogPartition.getPartitionId)
      }).toMap ++ previousChangelogeMapping
      info("Saving task-to-changelog partition mapping: %s" format newChangelogMapping)
      checkpointManager.writeChangeLogPartitionMapping(newChangelogMapping)
      checkpointManager.stop
    }

    // Here is where we should put in a pluggable option for the 
    // SSPTaskNameGrouper for locality, load-balancing, etc.
    val containerGrouper = new GroupByContainerCount(containerCount)
    val containerModels = containerGrouper
      .group(taskModels)
      .map { case (containerModel) => Integer.valueOf(containerModel.getContainerId) -> containerModel }
      .toMap

    new JobModel(config, containerModels)
  }
}

/**
 * <p>JobCoordinator is responsible for managing the lifecycle of a Samza job
 * once it's been started. This includes starting and stopping containers,
 * managing configuration, etc.</p>
 *
 * <p>Any new cluster manager that's integrated with Samza (YARN, Mesos, etc)
 * must integrate with the job coordinator.</p>
 *
 * <p>This class' API is currently unstable, and likely to change. The
 * coordinator's responsibility is simply to propagate the job model, and HTTP
 * server right now.</p>
 */
class JobCoordinator(
  /**
   * The data model that describes the Samza job's containers and tasks.
   */
  val jobModel: JobModel,

  /**
   * HTTP server used to serve a Samza job's container model to SamzaContainers when they start up.
   */
  val server: HttpServer) extends Logging {

  debug("Got job model: %s." format jobModel)

  def start {
    debug("Starting HTTP server.")
    server.start
    info("Startd HTTP server: %s" format server.getUrl)
  }

  def stop {
    debug("Stopping HTTP server.")
    server.stop
    info("Stopped HTTP server.")
  }
}