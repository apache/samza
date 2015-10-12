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


import org.apache.samza.config.StorageConfig
import org.apache.samza.job.model.{JobModel, TaskModel}
import org.apache.samza.config.Config
import org.apache.samza.SamzaException
import org.apache.samza.container.grouper.task.TaskNameGrouperFactory
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory
import java.util
import org.apache.samza.container.{LocalityManager, TaskName}
import org.apache.samza.storage.ChangelogPartitionManager
import org.apache.samza.util.Logging
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.util.Util
import scala.collection.JavaConversions._
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.Partition
import org.apache.samza.system.StreamMetadataCache
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.SystemFactory
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.apache.samza.coordinator.server.JobServlet
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory

/**
 * Helper companion object that is responsible for wiring up a JobCoordinator
 * given a Config object.
 */
object JobCoordinator extends Logging {

  /**
   * a volatile value to store the current instantiated <code>JobCoordinator</code>
   */
  @volatile var currentJobCoordinator: JobCoordinator = null

  /**
   * @param coordinatorSystemConfig A config object that contains job.name,
   * job.id, and all system.&lt;job-coordinator-system-name&gt;.*
   * configuration. The method will use this config to read all configuration
   * from the coordinator stream, and instantiate a JobCoordinator.
   */
  def apply(coordinatorSystemConfig: Config, metricsRegistryMap: MetricsRegistryMap): JobCoordinator = {
    val coordinatorStreamSystemFactory: CoordinatorStreamSystemFactory = new CoordinatorStreamSystemFactory()
    val coordinatorSystemConsumer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(coordinatorSystemConfig, metricsRegistryMap)
    val coordinatorSystemProducer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(coordinatorSystemConfig, metricsRegistryMap)
    info("Registering coordinator system stream.")
    coordinatorSystemConsumer.register
    debug("Starting coordinator system stream.")
    coordinatorSystemConsumer.start
    debug("Bootstrapping coordinator system stream.")
    coordinatorSystemConsumer.bootstrap
    val config = coordinatorSystemConsumer.getConfig
    info("Got config: %s" format config)
    val changelogManager = new ChangelogPartitionManager(coordinatorSystemProducer, coordinatorSystemConsumer, "Job-coordinator")
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

    val streamMetadataCache = new StreamMetadataCache(systemAdmins)

    val jobCoordinator = getJobCoordinator(config, changelogManager, localityManager, streamMetadataCache)
    createChangeLogStreams(config, jobCoordinator.jobModel.maxChangeLogStreamPartitions, streamMetadataCache)

    jobCoordinator
  }

  def apply(coordinatorSystemConfig: Config): JobCoordinator = apply(coordinatorSystemConfig, new MetricsRegistryMap())

  /**
   * Build a JobCoordinator using a Samza job's configuration.
   */
  def getJobCoordinator(config: Config,
                        changelogManager: ChangelogPartitionManager,
                        localityManager: LocalityManager,
                        streamMetadataCache: StreamMetadataCache) = {
    val jobModelGenerator = initializeJobModel(config, changelogManager, localityManager, streamMetadataCache)
    val server = new HttpServer
    server.addServlet("/*", new JobServlet(jobModelGenerator))
    currentJobCoordinator = new JobCoordinator(jobModelGenerator(), server)
    currentJobCoordinator
  }

  /**
   * For each input stream specified in config, exactly determine its
   * partitions, returning a set of SystemStreamPartitions containing them all.
   */
  def getInputStreamPartitions(config: Config, streamMetadataCache: StreamMetadataCache) = {
    val inputSystemStreams = config.getInputStreams

    // Get the set of partitions for each SystemStream from the stream metadata
    streamMetadataCache
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
   * The method intializes the jobModel and creates a JobModel generator which can be used to generate new JobModels
   * which catchup with the latest content from the coordinator stream.
   */
  private def initializeJobModel(config: Config,
                                 changelogManager: ChangelogPartitionManager,
                                 localityManager: LocalityManager,
                                 streamMetadataCache: StreamMetadataCache): () => JobModel = {


    // Do grouping to fetch TaskName to SSP mapping
    val allSystemStreamPartitions = getInputStreamPartitions(config, streamMetadataCache)
    val grouper = getSystemStreamPartitionGrouper(config)
    info("SystemStreamPartitionGrouper " + grouper + " has grouped the SystemStreamPartitions into the following taskNames:")
    val groups = grouper.group(allSystemStreamPartitions)

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

    localityManager.start()

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
    // Return a jobModelGenerator lambda that can be used to refresh the job model
    jobModelGenerator
  }

  /**
   * Build a full Samza job model. The function reads the latest checkpoint from the underlying coordinator stream and
   * builds a new JobModel.
   * This method needs to be thread safe, the reason being, for every HTTP request from a container, this method is called
   * and underlying it uses the same instance of coordinator stream producer and coordinator stream consumer.
   */
  private def refreshJobModel(config: Config,
                              allSystemStreamPartitions: util.Set[SystemStreamPartition],
                              groups: util.Map[TaskName, util.Set[SystemStreamPartition]],
                              previousChangelogMapping: util.Map[TaskName, Integer],
                              localityManager: LocalityManager): JobModel = {
    this.synchronized
    {
      // If no mappings are present(first time the job is running) we return -1, this will allow 0 to be the first change
      // mapping.
      var maxChangelogPartitionId = previousChangelogMapping.values.map(_.toInt).toList.sorted.lastOption.getOrElse(-1)

      // Assign all SystemStreamPartitions to TaskNames.
      val taskModels =
      {
        groups.map
                { case (taskName, systemStreamPartitions) =>
                  val changelogPartition = Option(previousChangelogMapping.get(taskName)) match
                  {
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
      val containerModels = asScalaSet(containerGrouper.group(setAsJavaSet(taskModels))).map
              { case (containerModel) => Integer.valueOf(containerModel.getContainerId) -> containerModel }.toMap

      new JobModel(config, containerModels, localityManager)
    }
  }

  private def createChangeLogStreams(config: StorageConfig, changeLogPartitions: Int, streamMetadataCache: StreamMetadataCache) {
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

    val changeLogMetadata = streamMetadataCache.getStreamMetadata(changeLogSystemStreams.values.toSet)

    info("Got change log stream metadata: %s" format changeLogMetadata)
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
class JobCoordinator(
  /**
   * The data model that describes the Samza job's containers and tasks.
   */
  val jobModel: JobModel,

  /**
   * HTTP server used to serve a Samza job's container model to SamzaContainers when they start up.
   */
  val server: HttpServer = null) extends Logging {

  debug("Got job model: %s." format jobModel)

  def start {
    if (server != null) {
      debug("Starting HTTP server.")
      server.start
      info("Startd HTTP server: %s" format server.getUrl)
    }
  }

  def stop {
    if (server != null) {
      debug("Stopping HTTP server.")
      server.stop
      info("Stopped HTTP server.")
    }
  }
}
