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

import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.{Config, StorageConfig}
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory
import org.apache.samza.container.grouper.task.{BalancingTaskNameGrouper, TaskNameGrouperFactory}
import org.apache.samza.container.{LocalityManager, TaskName}
import org.apache.samza.coordinator.server.{HttpServer, JobServlet}
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory
import org.apache.samza.job.model.{JobModel, TaskModel}
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.storage.ChangelogPartitionManager
import org.apache.samza.system.{ExtendedSystemAdmin, StreamMetadataCache, SystemFactory, SystemStreamPartition, SystemStreamPartitionMatcher}
import org.apache.samza.util.{Logging, Util}
import org.apache.samza.{Partition, SamzaException}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


/**
 * Helper companion object that is responsible for wiring up a JobCoordinator
 * given a Config object.
 */
object JobModelManager extends Logging {

  /**
   * a volatile value to store the current instantiated <code>JobCoordinator</code>
   */
  @volatile var currentJobModelManager: JobModelManager = null
  val jobModelRef: AtomicReference[JobModel] = new AtomicReference[JobModel]()

  /**
   * @param coordinatorSystemConfig A config object that contains job.name,
   * job.id, and all system.&lt;job-coordinator-system-name&gt;.*
   * configuration. The method will use this config to read all configuration
   * from the coordinator stream, and instantiate a JobCoordinator.
   */
  def apply(coordinatorSystemConfig: Config, metricsRegistryMap: MetricsRegistryMap): JobModelManager = {
    val coordinatorStreamSystemFactory: CoordinatorStreamSystemFactory = new CoordinatorStreamSystemFactory()
    val coordinatorSystemConsumer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(coordinatorSystemConfig, metricsRegistryMap)
    val coordinatorSystemProducer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(coordinatorSystemConfig, metricsRegistryMap)
    info("Registering coordinator system stream.")
    coordinatorSystemConsumer.register
    debug("Starting coordinator system stream.")
    coordinatorSystemConsumer.start
    debug("Bootstrapping coordinator system stream.")
    coordinatorSystemConsumer.bootstrap
    val source = "Job-coordinator"
    coordinatorSystemProducer.register(source)
    info("Registering coordinator system stream producer.")
    val config = coordinatorSystemConsumer.getConfig
    info("Got config: %s" format config)
    val changelogManager = new ChangelogPartitionManager(coordinatorSystemProducer, coordinatorSystemConsumer, source)
    val localityManager = new LocalityManager(coordinatorSystemProducer, coordinatorSystemConsumer)

    val systemNames = getSystemNames(config)

    // Map the name of each system to the corresponding SystemAdmin
    val systemAdmins = systemNames.map(systemName => {
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
      systemName -> systemFactory.getAdmin(systemName, config)
    }).toMap

    val streamMetadataCache = new StreamMetadataCache(systemAdmins = systemAdmins, cacheTTLms = 0)
    var streamPartitionCountMonitor: StreamPartitionCountMonitor = null
    if (config.getMonitorPartitionChange) {
      val extendedSystemAdmins = systemAdmins.filter{
                                                      case (systemName, systemAdmin) => systemAdmin.isInstanceOf[ExtendedSystemAdmin]
                                                    }
      val inputStreamsToMonitor = config.getInputStreams.filter(systemStream => extendedSystemAdmins.containsKey(systemStream.getSystem))
      if (inputStreamsToMonitor.nonEmpty) {
        streamPartitionCountMonitor = new StreamPartitionCountMonitor(
          setAsJavaSet(inputStreamsToMonitor),
          streamMetadataCache,
          metricsRegistryMap,
          config.getMonitorPartitionChangeFrequency)
      }
    }

    val jobCoordinator = getJobCoordinator(config, changelogManager, localityManager, streamMetadataCache, streamPartitionCountMonitor)
    createChangeLogStreams(config, jobCoordinator.jobModel.maxChangeLogStreamPartitions)

    jobCoordinator
  }

  def apply(coordinatorSystemConfig: Config): JobModelManager = apply(coordinatorSystemConfig, new MetricsRegistryMap())

  /**
   * Build a JobCoordinator using a Samza job's configuration.
   */
  def getJobCoordinator(config: Config,
                        changelogManager: ChangelogPartitionManager,
                        localityManager: LocalityManager,
                        streamMetadataCache: StreamMetadataCache,
                        streamPartitionCountMonitor: StreamPartitionCountMonitor) = {
    val jobModel: JobModel = initializeJobModel(config, changelogManager, localityManager, streamMetadataCache)
    jobModelRef.set(jobModel)

    val server = new HttpServer
    server.addServlet("/*", new JobServlet(jobModelRef))
    currentJobModelManager = new JobModelManager(jobModel, server, streamPartitionCountMonitor)
    currentJobModelManager
  }

  /**
   * For each input stream specified in config, exactly determine its
   * partitions, returning a set of SystemStreamPartitions containing them all.
   */
  def getInputStreamPartitions(config: Config, streamMetadataCache: StreamMetadataCache) = {
    val inputSystemStreams = config.getInputStreams

    // Get the set of partitions for each SystemStream from the stream metadata
    streamMetadataCache
      .getStreamMetadata(inputSystemStreams, true)
      .flatMap {
        case (systemStream, metadata) =>
          metadata
            .getSystemStreamPartitionMetadata
            .keys
            .map(new SystemStreamPartition(systemStream, _))
      }.toSet
  }

  def getMatchedInputStreamPartitions(config: Config, streamMetadataCache: StreamMetadataCache) : Set[SystemStreamPartition] = {
    val allSystemStreamPartitions = getInputStreamPartitions(config, streamMetadataCache)
    config.getSSPMatcherClass match {
      case Some(s) => {
        val jfr = config.getSSPMatcherConfigJobFactoryRegex.r
        config.getStreamJobFactoryClass match {
          case Some(jfr(_*)) => {
            info("before match: allSystemStreamPartitions.size = %s" format (allSystemStreamPartitions.size))
            val sspMatcher = Util.getObj[SystemStreamPartitionMatcher](s)
            val matchedPartitions = sspMatcher.filter(allSystemStreamPartitions, config).asScala.toSet
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
  def getSystemStreamPartitionGrouper(config: Config) = {
    val factoryString = config.getSystemStreamPartitionGrouperFactory
    val factory = Util.getObj[SystemStreamPartitionGrouperFactory](factoryString)
    factory.getSystemStreamPartitionGrouper(config)
  }

  /**
   * The method intializes the jobModel and returns it to the caller.
   * Note: refreshJobModel can be used as a lambda for JobModel generation in the future.
   */
  private def initializeJobModel(config: Config,
                                 changelogManager: ChangelogPartitionManager,
                                 localityManager: LocalityManager,
                                 streamMetadataCache: StreamMetadataCache): JobModel = {
    // Do grouping to fetch TaskName to SSP mapping
    val allSystemStreamPartitions = getMatchedInputStreamPartitions(config, streamMetadataCache)
    val grouper = getSystemStreamPartitionGrouper(config)
    val groups = grouper.group(allSystemStreamPartitions)
    info("SystemStreamPartitionGrouper %s has grouped the SystemStreamPartitions into %d tasks with the following taskNames: %s" format(grouper, groups.size(), groups.keySet()))

    // Initialize the ChangelogPartitionManager and the CheckpointManager
    val previousChangelogMapping = if (changelogManager != null)
    {
      changelogManager.start()
      changelogManager.readChangeLogPartitionMapping()
    }
    else
    {
      new util.HashMap[TaskName, Integer]()
    }
    // We don't need to start() localityManager as they share the same instances with checkpoint and changelog managers.
    // TODO: This code will go away with refactoring - SAMZA-678

    if (localityManager != null) {
      localityManager.start()
    }

    // Generate the jobModel
    def jobModelGenerator(): JobModel = refreshJobModel(config,
                                                        allSystemStreamPartitions,
                                                        groups,
                                                        previousChangelogMapping,
                                                        localityManager)

    val jobModel = jobModelGenerator()

    // Save the changelog mapping back to the ChangelogPartitionmanager
    if (changelogManager != null)
    {
      // newChangelogMapping is the merging of all current task:changelog
      // assignments with whatever we had before (previousChangelogMapping).
      // We must persist legacy changelog assignments so that
      // maxChangelogPartitionId always has the absolute max, not the current
      // max (in case the task with the highest changelog partition mapping
      // disappears.
      val newChangelogMapping = jobModel.getContainers.flatMap(_._2.getTasks).map{case (taskName,taskModel) => {
                                                 taskName -> Integer.valueOf(taskModel.getChangelogPartition.getPartitionId)
                                               }}.toMap ++ previousChangelogMapping
      info("Saving task-to-changelog partition mapping: %s" format newChangelogMapping)
      changelogManager.writeChangeLogPartitionMapping(newChangelogMapping)
    }

    jobModel
  }

  /**
   * Build a full Samza job model. The function reads the latest checkpoint from the underlying coordinator stream and
   * builds a new JobModel.
   * Note: This method no longer needs to be thread safe because HTTP request from a container no longer triggers a jobmodel
   * refresh. Hence, there is no need for synchronization as before.
   */
  private def refreshJobModel(config: Config,
                              allSystemStreamPartitions: util.Set[SystemStreamPartition],
                              groups: util.Map[TaskName, util.Set[SystemStreamPartition]],
                              previousChangelogMapping: util.Map[TaskName, Integer],
                              localityManager: LocalityManager): JobModel = {

    // If no mappings are present(first time the job is running) we return -1, this will allow 0 to be the first change
    // mapping.
    var maxChangelogPartitionId = previousChangelogMapping.values.map(_.toInt).toList.sorted.lastOption.getOrElse(-1)
    // Sort the groups prior to assigning the changelog mapping so that the mapping is reproducible and intuitive
    val sortedGroups = new util.TreeMap[TaskName, util.Set[SystemStreamPartition]](groups)

    // Assign all SystemStreamPartitions to TaskNames.
    val taskModels = {
      sortedGroups.map { case (taskName, systemStreamPartitions) =>
        val changelogPartition = Option(previousChangelogMapping.get(taskName)) match {
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
    val containerGrouperFactory = Util.getObj[TaskNameGrouperFactory](config.getTaskNameGrouperFactory)
    val containerGrouper = containerGrouperFactory.build(config)
    val containerModels = {
      if (containerGrouper.isInstanceOf[BalancingTaskNameGrouper])
        containerGrouper.asInstanceOf[BalancingTaskNameGrouper].balance(taskModels, localityManager)
      else
        containerGrouper.group(taskModels)
    }
    val containerMap = asScalaSet(containerModels).map { case (containerModel) => Integer.valueOf(containerModel.getContainerId) -> containerModel }.toMap

    new JobModel(config, containerMap, localityManager)
  }

  private def createChangeLogStreams(config: StorageConfig, changeLogPartitions: Int) {
    val changeLogSystemStreams = config
      .getStoreNames
      .filter(config.getChangelogStream(_).isDefined)
      .map(name => (name, config.getChangelogStream(name).get)).toMap
      .mapValues(Util.getSystemStreamFromNames(_))

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      val systemAdmin = Util.getObj[SystemFactory](config
        .getSystemFactory(systemStream.getSystem)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemStream.getSystem))
        ).getAdmin(systemStream.getSystem, config)

      systemAdmin.createChangelogStream(systemStream.getStream, changeLogPartitions)
    }
  }

  private def getSystemNames(config: Config) = config.getSystemNames.toSet

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
class JobModelManager(
  /**
   * The data model that describes the Samza job's containers and tasks.
   */
  val jobModel: JobModel,

  /**
   * HTTP server used to serve a Samza job's container model to SamzaContainers when they start up.
   */
  val server: HttpServer = null,
  val streamPartitionCountMonitor: StreamPartitionCountMonitor = null) extends Logging {

  debug("Got job model: %s." format jobModel)

  def start {
    if (server != null) {
      debug("Starting HTTP server.")
      server.start
      if (streamPartitionCountMonitor != null) {
        debug("Starting Stream Partition Count Monitor..")
        streamPartitionCountMonitor.start()
      }
      info("Started HTTP server: %s" format server.getUrl)
    }
  }

  def stop {
    if (server != null) {
      debug("Stopping HTTP server.")
      if (streamPartitionCountMonitor != null) {
        debug("Stopping Stream Partition Count Monitor..")
        streamPartitionCountMonitor.stop()
      }
      server.stop
      info("Stopped HTTP server.")
    }
  }
}
