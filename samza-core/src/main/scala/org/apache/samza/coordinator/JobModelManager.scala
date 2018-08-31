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


import java.util
import java.util.concurrent.atomic.AtomicReference

import org.apache.samza.config._
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.Config
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory
import org.apache.samza.container.grouper.task.BalancingTaskNameGrouper
import org.apache.samza.container.grouper.task.TaskNameGrouperFactory
import org.apache.samza.container.LocalityManager
import org.apache.samza.container.TaskName
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.coordinator.server.JobServlet
import org.apache.samza.coordinator.stream.CoordinatorStreamManager
import org.apache.samza.job.model.JobModel
import org.apache.samza.job.model.TaskModel
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system._
import org.apache.samza.util.Logging
import org.apache.samza.util.Util
import org.apache.samza.Partition

import scala.collection.JavaConverters._

/**
 * Helper companion object that is responsible for wiring up a JobModelManager
 * given a Config object.
 */
object JobModelManager extends Logging {

  val SOURCE = "JobModelManager"
  /**
   * a volatile value to store the current instantiated <code>JobModelManager</code>
   */
  @volatile var currentJobModelManager: JobModelManager = null
  val jobModelRef: AtomicReference[JobModel] = new AtomicReference[JobModel]()

  /**
   * Does the following actions for a job.
   * a) Reads the jobModel from coordinator stream using the job's configuration.
   * b) Recomputes changelog partition mapping based on jobModel and job's configuration.
   * c) Builds JobModelManager using the jobModel read from coordinator stream.
   * @param coordinatorStreamManager Coordinator stream manager.
   * @param changelogPartitionMapping The changelog partition-to-task mapping.
   * @return JobModelManager
   */
  def apply(coordinatorStreamManager: CoordinatorStreamManager, changelogPartitionMapping: util.Map[TaskName, Integer]) = {
    val config = coordinatorStreamManager.getConfig
    val localityManager = new LocalityManager(config, new MetricsRegistryMap())

    // Map the name of each system to the corresponding SystemAdmin
    val systemAdmins = new SystemAdmins(config)
    val streamMetadataCache = new StreamMetadataCache(systemAdmins, 0)

    val containerCount = new JobConfig(config).getContainerCount
    val processorList = List.range(0, containerCount).map(c => c.toString)

    systemAdmins.start()
    val jobModelManager = getJobModelManager(config, changelogPartitionMapping, localityManager, streamMetadataCache, processorList.asJava)
    systemAdmins.stop()

    jobModelManager
  }

  /**
   * Build a JobModelManager using a Samza job's configuration.
   */
  private def getJobModelManager(config: Config,
                                changeLogMapping: util.Map[TaskName, Integer],
                                localityManager: LocalityManager,
                                streamMetadataCache: StreamMetadataCache,
                                containerIds: java.util.List[String]) = {
    val jobModel: JobModel = readJobModel(config, changeLogMapping, localityManager, streamMetadataCache, containerIds)
    jobModelRef.set(jobModel)

    val server = new HttpServer
    server.addServlet("/", new JobServlet(jobModelRef))
    currentJobModelManager = new JobModelManager(jobModel, server, localityManager)
    currentJobModelManager
  }

  /**
   * For each input stream specified in config, exactly determine its
   * partitions, returning a set of SystemStreamPartitions containing them all.
   */
  private def getInputStreamPartitions(config: Config, streamMetadataCache: StreamMetadataCache) = {
    val inputSystemStreams = config.getInputStreams

    // Get the set of partitions for each SystemStream from the stream metadata
    streamMetadataCache
      .getStreamMetadata(inputSystemStreams, true)
      .flatMap {
        case (systemStream, metadata) =>
          metadata
            .getSystemStreamPartitionMetadata
            .asScala
            .keys
            .map(new SystemStreamPartition(systemStream, _))
      }.toSet
  }

  private def getMatchedInputStreamPartitions(config: Config, streamMetadataCache: StreamMetadataCache): Set[SystemStreamPartition] = {
    val allSystemStreamPartitions = getInputStreamPartitions(config, streamMetadataCache)
    config.getSSPMatcherClass match {
      case Some(s) => {
        val jfr = config.getSSPMatcherConfigJobFactoryRegex.r
        config.getStreamJobFactoryClass match {
          case Some(jfr(_*)) => {
            info("before match: allSystemStreamPartitions.size = %s" format (allSystemStreamPartitions.size))
            val sspMatcher = Util.getObj(s, classOf[SystemStreamPartitionMatcher])
            val matchedPartitions = sspMatcher.filter(allSystemStreamPartitions.asJava, config).asScala.toSet
            // Usually a small set hence ok to log at info level
            info("after match: matchedPartitions = %s" format (matchedPartitions))
            matchedPartitions
          }
          case _ => allSystemStreamPartitions
        }
      }
      case _ => allSystemStreamPartitions
    }
  }

  /**
   * Gets a SystemStreamPartitionGrouper object from the configuration.
   */
  private def getSystemStreamPartitionGrouper(config: Config) = {
    val factoryString = config.getSystemStreamPartitionGrouperFactory
    val factory = Util.getObj(factoryString, classOf[SystemStreamPartitionGrouperFactory])
    factory.getSystemStreamPartitionGrouper(config)
  }

  /**
   * The function reads the latest checkpoint from the underlying coordinator stream and
   * builds a new JobModel.
   */
  def readJobModel(config: Config,
                   changeLogPartitionMapping: util.Map[TaskName, Integer],
                   localityManager: LocalityManager,
                   streamMetadataCache: StreamMetadataCache,
                   containerIds: java.util.List[String]): JobModel = {
    // Do grouping to fetch TaskName to SSP mapping
    val allSystemStreamPartitions = getMatchedInputStreamPartitions(config, streamMetadataCache)

    // processor list is required by some of the groupers. So, let's pass them as part of the config.
    // Copy the config and add the processor list to the config copy.
    val configMap = new util.HashMap[String, String](config)
    configMap.put(JobConfig.PROCESSOR_LIST, String.join(",", containerIds))
    val grouper = getSystemStreamPartitionGrouper(new MapConfig(configMap))

    val groups = grouper.group(allSystemStreamPartitions.asJava)
    info("SystemStreamPartitionGrouper %s has grouped the SystemStreamPartitions into %d tasks with the following taskNames: %s" format(grouper, groups.size(), groups.keySet()))

    val isHostAffinityEnabled = new ClusterManagerConfig(config).getHostAffinityEnabled

    // If no mappings are present(first time the job is running) we return -1, this will allow 0 to be the first change
    // mapping.
    var maxChangelogPartitionId = changeLogPartitionMapping.asScala.values.map(_.toInt).toList.sorted.lastOption.getOrElse(-1)
    // Sort the groups prior to assigning the changelog mapping so that the mapping is reproducible and intuitive
    val sortedGroups = new util.TreeMap[TaskName, util.Set[SystemStreamPartition]](groups)

    // Assign all SystemStreamPartitions to TaskNames.
    val taskModels = {
      sortedGroups.asScala.map { case (taskName, systemStreamPartitions) =>
        val changelogPartition = Option(changeLogPartitionMapping.get(taskName)) match {
          case Some(changelogPartitionId) => new Partition(changelogPartitionId)
          case _ =>
            // If we've never seen this TaskName before, then assign it a
            // new changelog.
            maxChangelogPartitionId += 1
            info("New task %s is being assigned changelog partition %s." format(taskName, maxChangelogPartitionId))
            new Partition(maxChangelogPartitionId)
        }
        new TaskModel(taskName, systemStreamPartitions, changelogPartition)
      }.toSet
    }

    // Here is where we should put in a pluggable option for the
    // SSPTaskNameGrouper for locality, load-balancing, etc.
    val containerGrouperFactory = Util.getObj(config.getTaskNameGrouperFactory, classOf[TaskNameGrouperFactory])
    val containerGrouper = containerGrouperFactory.build(config)
    val containerModels = {
      containerGrouper match {
        case grouper: BalancingTaskNameGrouper if isHostAffinityEnabled => grouper.balance(taskModels.asJava, localityManager)
        case _ => containerGrouper.group(taskModels.asJava, containerIds)
      }
    }
    val containerMap = containerModels.asScala.map { case (containerModel) => containerModel.getProcessorId -> containerModel }.toMap

    if (isHostAffinityEnabled) {
      new JobModel(config, containerMap.asJava, localityManager)
    } else {
      new JobModel(config, containerMap.asJava)
    }
  }

  private def getSystemNames(config: Config) = config.getSystemNames.toSet
}

/**
 * <p>JobModelManager is responsible for managing the lifecycle of a Samza job
 * once it's been started. This includes starting and stopping containers,
 * managing configuration, etc.</p>
 *
 * <p>Any new cluster manager that's integrated with Samza (YARN, Mesos, etc)
 * must integrate with the job coordinator.</p>
 *
 * <p>This class' API is currently unstable, and likely to change. The
 * responsibility is simply to propagate the job model, and HTTP
 * server right now.</p>
 */
class JobModelManager(
  /**
   * The data model that describes the Samza job's containers and tasks.
   */
  val jobModel: JobModel,

  /**
   * HTTP server used to serve a Samza job's container model to SamzaContainers when they start up.
   */
  val server: HttpServer = null,

  /**
   * LocalityManager employed to read and write container and task locality information to metadata store.
   */
  val localityManager: LocalityManager = null) extends Logging {

  debug("Got job model: %s." format jobModel)

  def start {
    if (server != null) {
      debug("Starting HTTP server.")
      server.start
      info("Started HTTP server: %s" format server.getUrl)
    }
  }

  def stop {
    if (server != null) {
      debug("Stopping HTTP server.")
      server.stop
      info("Stopped HTTP server.")
      if (localityManager != null) {
        info("Stopping localityManager")
        localityManager.close()
        info("Stopped localityManager")
      }
    }
  }
}
