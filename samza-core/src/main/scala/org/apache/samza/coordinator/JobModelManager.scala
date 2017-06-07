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
import java.lang.{NoSuchMethodException, NoSuchMethodError}
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.Config
import org.apache.samza.config.StorageConfig
import org.apache.samza.container.grouper.stream.{SystemStreamPartitionAssignmentManager, SystemStreamPartitionGrouperFactory}
import org.apache.samza.container.grouper.task.BalancingTaskNameGrouper
import org.apache.samza.container.grouper.task.TaskNameGrouperFactory
import org.apache.samza.container.LocalityManager
import org.apache.samza.container.TaskName
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.coordinator.server.JobServlet
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer
import org.apache.samza.job.model.JobModel
import org.apache.samza.job.model.TaskModel
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.storage.ChangelogPartitionManager
import org.apache.samza.system.ExtendedSystemAdmin
import org.apache.samza.system.StreamMetadataCache
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.SystemStreamPartitionMatcher
import org.apache.samza.system.SystemAdmin
import org.apache.samza.system.StreamSpec
import org.apache.samza.util.Logging
import org.apache.samza.util.Util
import org.apache.samza.Partition
import org.apache.samza.SamzaException

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
   * b) Creates changeLogStream for task stores if it does not exists.
   * c) Recomputes changelog partition mapping based on jobModel and job's configuration
   * and writes it to the coordinator stream.
   * d) Builds JobModelManager using the jobModel read from coordinator stream.
   * @param coordinatorSystemConfig A config object that contains job.name
   *                                job.id, and all system.&lt;job-coordinator-system-name&gt;.*
   *                                configuration. The method will use this config to read all configuration
   *                                from the coordinator stream, and instantiate a JobModelManager.
   */
  def apply(coordinatorSystemConfig: Config, metricsRegistryMap: MetricsRegistryMap): JobModelManager = {
    val coordinatorStreamSystemFactory: CoordinatorStreamSystemFactory = new CoordinatorStreamSystemFactory()
    val coordinatorSystemConsumer: CoordinatorStreamSystemConsumer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(coordinatorSystemConfig, metricsRegistryMap)
    val coordinatorSystemProducer: CoordinatorStreamSystemProducer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(coordinatorSystemConfig, metricsRegistryMap)
    info("Registering coordinator system stream consumer.")
    coordinatorSystemConsumer.register
    debug("Starting coordinator system stream consumer.")
    coordinatorSystemConsumer.start
    debug("Bootstrapping coordinator system stream consumer.")
    coordinatorSystemConsumer.bootstrap
    info("Registering coordinator system stream producer.")
    coordinatorSystemProducer.register(SOURCE)

    val config = coordinatorSystemConsumer.getConfig
    info("Got config: %s" format config)
    val changelogManager = new ChangelogPartitionManager(coordinatorSystemProducer, coordinatorSystemConsumer, SOURCE)
    changelogManager.start()

    val localityManager = new LocalityManager(coordinatorSystemProducer, coordinatorSystemConsumer)
    // We don't need to start() localityManager as they share the same instances with checkpoint and changelog managers.
    // TODO: This code will go away with refactoring - SAMZA-678
    localityManager.start()

    val sspAssignmentManager = new SystemStreamPartitionAssignmentManager(coordinatorSystemProducer, coordinatorSystemConsumer)
    sspAssignmentManager.start()

    // Map the name of each system to the corresponding SystemAdmin
    val systemAdmins = getSystemAdmins(config)

    val streamMetadataCache = new StreamMetadataCache(systemAdmins = systemAdmins, cacheTTLms = 0)
    var streamPartitionCountMonitor: StreamPartitionCountMonitor = null
    if (config.getMonitorPartitionChange) {
      val extendedSystemAdmins = systemAdmins.filter{
        case (systemName, systemAdmin) => systemAdmin.isInstanceOf[ExtendedSystemAdmin]
      }
      val inputStreamsToMonitor = config.getInputStreams.filter(systemStream => extendedSystemAdmins.contains(systemStream.getSystem))
      if (inputStreamsToMonitor.nonEmpty) {
        streamPartitionCountMonitor = new StreamPartitionCountMonitor(
          inputStreamsToMonitor.asJava,
          streamMetadataCache,
          metricsRegistryMap,
          config)
      }
    }
    val previousChangelogPartitionMapping = changelogManager.readChangeLogPartitionMapping()
    val previousSSPTaskAssignment = sspAssignmentManager.readSSPTaskAssignment()
    val jobModelManager = getJobModelManager(config, previousChangelogPartitionMapping, previousSSPTaskAssignment,
      localityManager, streamMetadataCache, streamPartitionCountMonitor, null)
    val jobModel = jobModelManager.jobModel
    // Save the changelog mapping back to the ChangelogPartitionmanager
    // newChangelogPartitionMapping is the merging of all current task:changelog
    // assignments with whatever we had before (previousChangelogPartitionMapping).
    // We must persist legacy changelog assignments so that
    // maxChangelogPartitionId always has the absolute max, not the current
    // max (in case the task with the highest changelog partition mapping
    // disappears.
    val newChangelogPartitionMapping = jobModel.getContainers.asScala.flatMap(_._2.getTasks.asScala).map{case (taskName,taskModel) => {
      taskName -> Integer.valueOf(taskModel.getChangelogPartition.getPartitionId)
    }}.toMap ++ previousChangelogPartitionMapping.asScala
    info("Saving task-to-changelog partition mapping: %s" format newChangelogPartitionMapping)
    changelogManager.writeChangeLogPartitionMapping(newChangelogPartitionMapping.asJava)

    val newSSPTaskAssignment = jobModel.getContainers.asScala.values.flatMap(_.getTasks.values().asScala).flatMap(taskModel =>
      taskModel.getSystemStreamPartitions.asScala.map(ssp => ssp -> taskModel.getTaskName.getTaskName)
    ).toMap
    info("Saving ssp-to-task mapping: %s" format newSSPTaskAssignment)
    sspAssignmentManager.writeSSPTaskAssignment(newSSPTaskAssignment.asJava)

    createChangeLogStreams(config, jobModel.maxChangeLogStreamPartitions)
    createAccessLogStreams(config, jobModel.maxChangeLogStreamPartitions)

    jobModelManager
  }
  def apply(coordinatorSystemConfig: Config): JobModelManager = apply(coordinatorSystemConfig, new MetricsRegistryMap())

  /**
   * Build a JobModelManager using a Samza job's configuration.
   */
  private def getJobModelManager(config: Config,
                                 changeLogMapping: util.Map[TaskName, Integer],
                                 previousSSPTaskAssignment: util.Map[SystemStreamPartition, String],
                                 localityManager: LocalityManager,
                                 streamMetadataCache: StreamMetadataCache,
                                 streamPartitionCountMonitor: StreamPartitionCountMonitor,
                                 containerIds: java.util.List[String]) = {
    val jobModel: JobModel = readJobModel(config, changeLogMapping, previousSSPTaskAssignment,
      localityManager, streamMetadataCache, containerIds)
    jobModelRef.set(jobModel)

    val server = new HttpServer
    server.addServlet("/", new JobServlet(jobModelRef))
    currentJobModelManager = new JobModelManager(jobModel, server, streamPartitionCountMonitor)
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
            val sspMatcher = Util.getObj[SystemStreamPartitionMatcher](s)
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
    val factory = Util.getObj[SystemStreamPartitionGrouperFactory](factoryString)
    factory.getSystemStreamPartitionGrouper(config)
  }

  /**
   * The function reads the latest checkpoint from the underlying coordinator stream and
   * builds a new JobModel.
   */
  def readJobModel(config: Config,
                   changeLogPartitionMapping: util.Map[TaskName, Integer],
                   previousSSPTaskAssignment: util.Map[SystemStreamPartition, String],
                   localityManager: LocalityManager,
                   streamMetadataCache: StreamMetadataCache,
                   containerIds: java.util.List[String]): JobModel = {
    // Do grouping to fetch TaskName to SSP mapping
    val allSystemStreamPartitions = getMatchedInputStreamPartitions(config, streamMetadataCache)
    val grouper = getSystemStreamPartitionGrouper(config)

    val groups = try {
      grouper.group(previousSSPTaskAssignment, allSystemStreamPartitions.asJava)
    } catch {
      case e @ ( _ : NoSuchMethodException | _ : NoSuchMethodError) =>
        grouper.group(allSystemStreamPartitions.asJava)
    }

    info("SystemStreamPartitionGrouper %s has grouped the SystemStreamPartitions into %d tasks with the following taskNames: %s" format(grouper, groups.size(), groups.keySet()))

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
    val containerGrouperFactory = Util.getObj[TaskNameGrouperFactory](config.getTaskNameGrouperFactory)
    val containerGrouper = containerGrouperFactory.build(config)
    val containerModels = {
      containerGrouper match {
        case grouper: BalancingTaskNameGrouper => grouper.balance(taskModels.asJava, localityManager)
        case _ => containerGrouper.group(taskModels.asJava, containerIds)
      }
    }
    val containerMap = containerModels.asScala.map { case (containerModel) => containerModel.getProcessorId -> containerModel }.toMap

    new JobModel(config, containerMap.asJava, localityManager)
  }

  /**
   * Instantiates the system admins based upon the system factory class available in {@param config}.
   * @param config contains adequate information to instantiate the SystemAdmin.
   * @return a map of SystemName(String) to the instantiated SystemAdmin.
   */
  def getSystemAdmins(config: Config) : Map[String, SystemAdmin] = {
    val systemNames = getSystemNames(config)
    // Map the name of each system to the corresponding SystemAdmin
    val systemAdmins = systemNames.map(systemName => {
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
      systemName -> systemFactory.getAdmin(systemName, config)
    }).toMap
    systemAdmins
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

  private def createAccessLogStreams(config: StorageConfig, changeLogPartitions: Int): Unit = {
    val changeLogSystemStreams = config
      .getStoreNames
      .filter(config.getChangelogStream(_).isDefined)
      .map(name => (name, config.getChangelogStream(name).get)).toMap
      .mapValues(Util.getSystemStreamFromNames(_))

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      val accessLog = config.getAccessLogEnabled(storeName)
      if (accessLog) {
        val systemAdmin = Util.getObj[SystemFactory](config
          .getSystemFactory(systemStream.getSystem)
          .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemStream.getSystem))
        ).getAdmin(systemStream.getSystem, config)

        val accessLogSpec = new StreamSpec(config.getAccessLogStream(systemStream.getStream),
          config.getAccessLogStream(systemStream.getStream), systemStream.getSystem, changeLogPartitions)
        systemAdmin.createStream(accessLogSpec)
      }
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
