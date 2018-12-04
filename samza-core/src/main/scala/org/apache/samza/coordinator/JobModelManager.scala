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
import org.apache.samza.container.grouper.task._
import org.apache.samza.container.LocalityManager
import org.apache.samza.container.TaskName
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.coordinator.server.JobServlet
import org.apache.samza.job.model.ContainerModel
import org.apache.samza.job.model.JobModel
import org.apache.samza.job.model.TaskModel
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system._
import org.apache.samza.util.Logging
import org.apache.samza.util.Util
import org.apache.samza.Partition
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping
import org.apache.samza.runtime.LocationId

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
 * Helper companion object that is responsible for wiring up a JobModelManager
 * given a Config object.
 */
object JobModelManager extends Logging {

  /**
   * a volatile value to store the current instantiated <code>JobModelManager</code>
   */
  @volatile var currentJobModelManager: JobModelManager = _
  val jobModelRef: AtomicReference[JobModel] = new AtomicReference[JobModel]()

  /**
   * Currently used only in the ApplicationMaster for yarn deployment model.
   * Does the following:
   * a) Reads the jobModel from coordinator stream using the job's configuration.
   * b) Recomputes the changelog partition mapping based on jobModel and job's configuration.
   * c) Builds JobModelManager using the jobModel read from coordinator stream.
   * @param config config from the coordinator stream.
   * @param changelogPartitionMapping changelog partition-to-task mapping of the samza job.
   * @param metricsRegistry the registry for reporting metrics.
   * @return the instantiated {@see JobModelManager}.
   */
  def apply(config: Config, changelogPartitionMapping: util.Map[TaskName, Integer], metricsRegistry: MetricsRegistry = new MetricsRegistryMap()): JobModelManager = {
    val localityManager = new LocalityManager(config, metricsRegistry)
    val taskAssignmentManager = new TaskAssignmentManager(config, metricsRegistry)
    val systemAdmins = new SystemAdmins(config)
    try {
      systemAdmins.start()
      val streamMetadataCache = new StreamMetadataCache(systemAdmins, 0)
      val applicationMetadataProvider: ApplicationMetadataProvider = getApplicationMetadataProvider(config, localityManager, taskAssignmentManager)

      val jobModel: JobModel = readJobModel(config, changelogPartitionMapping, streamMetadataCache, applicationMetadataProvider)
      jobModelRef.set(new JobModel(jobModel.getConfig, jobModel.getContainers, localityManager))

      updateTaskAssignments(jobModel, taskAssignmentManager, applicationMetadataProvider)

      val server = new HttpServer
      server.addServlet("/", new JobServlet(jobModelRef))

      currentJobModelManager = new JobModelManager(jobModel, server, localityManager)
      currentJobModelManager
    } finally {
      taskAssignmentManager.close()
      systemAdmins.stop()
      // Not closing localityManager, since {@code ClusterBasedJobCoordinator} uses it to read container locality through {@code JobModel}.
    }
  }

  /**
    * Builds the {@see ApplicationMetadataProviderImpl} for the samza job.
    * @param config represents the configurations defined by the user.
    * @param localityManager provides the processor to host mapping persisted to the metadata store.
    * @param taskAssignmentManager provides the processor to task assignments persisted to the metadata store.
    * @return the instantiated {@see ApplicationMetadataProviderImpl}.
    */
  private def getApplicationMetadataProvider(config: Config, localityManager: LocalityManager, taskAssignmentManager: TaskAssignmentManager) = {
    val processorLocality: util.Map[String, LocationId] = getProcessorLocality(config, localityManager)
    val taskAssignment: util.Map[String, String] = taskAssignmentManager.readTaskAssignment()
    val taskNameToProcessorId: util.Map[TaskName, String] = new util.HashMap[TaskName, String]()
    for ((taskName, processorId) <- taskAssignment) {
      taskNameToProcessorId.put(new TaskName(taskName), processorId)
    }

    val taskLocality:util.Map[TaskName, LocationId] = new util.HashMap[TaskName, LocationId]()
    for ((taskName, processorId) <- taskAssignment) {
      if (processorLocality.containsKey(processorId)) {
        taskLocality.put(new TaskName(taskName), processorLocality.get(processorId))
      }
    }
    val applicationMetadataProvider: ApplicationMetadataProvider = new ApplicationMetadataProviderImpl(processorLocality, taskLocality, new util.HashMap[TaskName, util.List[SystemStreamPartition]](), taskNameToProcessorId)
    applicationMetadataProvider
  }

  /**
    * Retrieves and returns the processor locality of a samza job using provided {@see Config} and {@see LocalityManager}.
    * @param config provides the configurations defined by the user. Required to connect to the storage layer.
    * @param localityManager provides the processor to host mapping persisted to the metadata store.
    * @return the processor locality.
    */
  private def getProcessorLocality(config: Config, localityManager: LocalityManager) = {
    val containerToLocationId: util.Map[String, LocationId] = new util.HashMap[String, LocationId]()
    val existingContainerLocality = localityManager.readContainerLocality()

    for (containerId <- 0 to config.getContainerCount) {
      val localityMapping = existingContainerLocality.get(containerId.toString)
      // To handle the case when the container count is increased between two different runs of a samza-yarn job,
      // set the locality of newly added containers to any_host.
      var locationId: LocationId = new LocationId("ANY_HOST")
      if (localityMapping != null && localityMapping.containsKey(SetContainerHostMapping.HOST_KEY)) {
        locationId = new LocationId(localityMapping.get(SetContainerHostMapping.HOST_KEY))
      }
      containerToLocationId.put(containerId.toString, locationId)
    }

    containerToLocationId
  }

  /**
    * This method does the following:
    * 1. Deletes the existing task assignments if the partition-task grouping has changed from the previous run of the job.
    * 2. Saves the newly generated task assignments to the storage layer through the {@param TaskAssignementManager}.
    *
    * @param jobModel              represents the {@see JobModel} of the samza job.
    * @param taskAssignmentManager required to persist the processor to task assignments to the storage layer.
    * @param applicationMetadataProvider        the application metadata provider used to generate the JobModel.
    *
    */
  private def updateTaskAssignments(jobModel: JobModel, taskAssignmentManager: TaskAssignmentManager, applicationMetadataProvider: ApplicationMetadataProvider): Unit = {
    val taskNames: util.Set[String] = new util.HashSet[String]()
    for (container <- jobModel.getContainers.values()) {
      for (taskModel <- container.getTasks.values()) {
        taskNames.add(taskModel.getTaskName.getTaskName)
      }
    }
    val taskToContainerId = applicationMetadataProvider.getPreviousTaskToProcessorAssignment
    if (taskNames.size() != taskToContainerId.size()) {
      warn("Current task count {} does not match saved task count {}. Stateful jobs may observe misalignment of keys!",
           taskNames.size(), taskToContainerId.size())
      // If the tasks changed, then the partition-task grouping is also likely changed and we can't handle that
      // without a much more complicated mapping. Further, the partition count may have changed, which means
      // input message keys are likely reshuffled w.r.t. partitions, so the local state may not contain necessary
      // data associated with the incoming keys. Warn the user and default to grouper
      // In this scenario the tasks may have been reduced, so we need to delete all the existing messages
      taskAssignmentManager.deleteTaskContainerMappings(taskNames)
    }

    for (container <- jobModel.getContainers.values()) {
      for (taskName <- container.getTasks.keySet) {
        taskAssignmentManager.writeTaskContainerMapping(taskName.getTaskName, container.getId)
      }
    }
  }

  /**
    * Computes the input system stream partitions of a samza job using the provided {@param config}
    * and {@param streamMetadataCache}.
    * @param config the configuration of the job.
    * @param streamMetadataCache to query the partition metadata of the input streams.
    * @return the input {@see SystemStreamPartition} of the samza job.
    */
  private def getInputStreamPartitions(config: Config, streamMetadataCache: StreamMetadataCache): Set[SystemStreamPartition] = {
    val inputSystemStreams = config.getInputStreams

    // Get the set of partitions for each SystemStream from the stream metadata
    streamMetadataCache
      .getStreamMetadata(inputSystemStreams, partitionsMetadataOnly = true)
      .flatMap {
        case (systemStream, metadata) =>
          metadata
            .getSystemStreamPartitionMetadata
            .asScala
            .keys
            .map(new SystemStreamPartition(systemStream, _))
      }.toSet
  }

  /**
    * Builds the input {@see SystemStreamPartition} based upon the {@param config} defined by the user.
    * @param config configuration to fetch the metadata of the input streams.
    * @param streamMetadataCache required to query the partition metadata of the input streams.
    * @return the input SystemStreamPartitions of the job.
    */
  private def getMatchedInputStreamPartitions(config: Config, streamMetadataCache: StreamMetadataCache): Set[SystemStreamPartition] = {
    val allSystemStreamPartitions = getInputStreamPartitions(config, streamMetadataCache)
    config.getSSPMatcherClass match {
      case Some(s) =>
        val jfr = config.getSSPMatcherConfigJobFactoryRegex.r
        config.getStreamJobFactoryClass match {
          case Some(jfr(_*)) =>
            info("before match: allSystemStreamPartitions.size = %s" format allSystemStreamPartitions.size)
            val sspMatcher = Util.getObj(s, classOf[SystemStreamPartitionMatcher])
            val matchedPartitions = sspMatcher.filter(allSystemStreamPartitions.asJava, config).asScala.toSet
            // Usually a small set hence ok to log at info level
            info("after match: matchedPartitions = %s" format matchedPartitions)
            matchedPartitions
          case _ => allSystemStreamPartitions
        }
      case _ => allSystemStreamPartitions
    }
  }

  /**
    * Finds the {@see SystemStreamPartitionGrouperFactory} from the {@param config}. Instantiates the  {@see SystemStreamPartitionGrouper}
    * object through the factory.
    * @param config the configuration of the samza job.
    * @return the instantiated {@see SystemStreamPartitionGrouper}.
    */
  private def getSystemStreamPartitionGrouper(config: Config) = {
    val factoryString = config.getSystemStreamPartitionGrouperFactory
    val factory = Util.getObj(factoryString, classOf[SystemStreamPartitionGrouperFactory])
    factory.getSystemStreamPartitionGrouper(config)
  }


  /**
    * Does the following:
    * 1. Fetches metadata of the input streams defined in configuration through {@param streamMetadataCache}.
    * 2. Applies the {@see SystemStreamPartitionGrouper}, {@see TaskNameGrouper} defined in the configuration
    * to build the {@see JobModel}.
    * @param config the configuration of the job.
    * @param changeLogPartitionMapping the task to changelog partition mapping of the job.
    * @param streamMetadataCache the cache that holds the partition metadata of the input streams.
    * @param applicationMetadataProvider the metadata provider of the application.
    * @return the built {@see JobModel}.
    */
  def readJobModel(config: Config,
                   changeLogPartitionMapping: util.Map[TaskName, Integer],
                   streamMetadataCache: StreamMetadataCache,
                   applicationMetadataProvider: ApplicationMetadataProvider): JobModel = {
    // Do grouping to fetch TaskName to SSP mapping
    val allSystemStreamPartitions = getMatchedInputStreamPartitions(config, streamMetadataCache)

    // processor list is required by some of the groupers. So, let's pass them as part of the config.
    // Copy the config and add the processor list to the config copy.
    val configMap = new util.HashMap[String, String](config)
    configMap.put(JobConfig.PROCESSOR_LIST, String.join(",", applicationMetadataProvider.getProcessorLocality.keySet()))
    val grouper = getSystemStreamPartitionGrouper(new MapConfig(configMap))

    val groups = grouper.group(allSystemStreamPartitions)
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
    var containerModels: util.Set[ContainerModel] = null
    if(isHostAffinityEnabled) {
      containerModels = containerGrouper.group(taskModels, applicationMetadataProvider)
    } else {
      containerModels = containerGrouper.group(taskModels, new util.ArrayList[String](applicationMetadataProvider.getProcessorLocality.keySet()))
    }
    val containerMap = containerModels.asScala.map(containerModel => containerModel.getId -> containerModel).toMap

    new JobModel(config, containerMap.asJava)
  }

  private def getSystemNames(config: Config) = config.getSystemNames().toSet
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

  def start() {
    if (server != null) {
      debug("Starting HTTP server.")
      server.start
      info("Started HTTP server: %s" format server.getUrl)
    }
  }

  def stop() {
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
