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

import org.apache.samza.Partition
import org.apache.samza.config._
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.Config
import org.apache.samza.container.grouper.stream.{SSPGrouperProxy, SystemStreamPartitionGrouperFactory}
import org.apache.samza.container.grouper.task._
import org.apache.samza.container.{LocalityManager, TaskName}
import org.apache.samza.coordinator.server.{HttpServer, JobServlet}
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping
import org.apache.samza.job.model.{ContainerModel, JobModel, TaskMode, TaskModel}
import org.apache.samza.metrics.{MetricsRegistry, MetricsRegistryMap}
import org.apache.samza.runtime.LocationId
import org.apache.samza.system._
import org.apache.samza.util.{Logging, Util}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

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
    val taskPartitionAssignmentManager = new TaskPartitionAssignmentManager(config, metricsRegistry)
    val systemAdmins = new SystemAdmins(config)
    try {
      systemAdmins.start()
      val streamMetadataCache = new StreamMetadataCache(systemAdmins, 0)
      val grouperMetadata: GrouperMetadata = getGrouperMetadata(config, localityManager, taskAssignmentManager, taskPartitionAssignmentManager)

      val jobModel: JobModel = readJobModel(config, changelogPartitionMapping, streamMetadataCache, grouperMetadata)
      jobModelRef.set(new JobModel(jobModel.getConfig, jobModel.getContainers, localityManager))

      updateTaskAssignments(jobModel, taskAssignmentManager, taskPartitionAssignmentManager, grouperMetadata)

      val server = new HttpServer
      server.addServlet("/", new JobServlet(jobModelRef))

      currentJobModelManager = new JobModelManager(jobModelRef.get(), server, localityManager)
      currentJobModelManager
    } finally {
      taskPartitionAssignmentManager.close()
      taskAssignmentManager.close()
      systemAdmins.stop()
      // Not closing localityManager, since {@code ClusterBasedJobCoordinator} uses it to read container locality through {@code JobModel}.
    }
  }

  /**
    * Builds the {@see GrouperMetadataImpl} for the samza job.
    * @param config represents the configurations defined by the user.
    * @param localityManager provides the processor to host mapping persisted to the metadata store.
    * @param taskAssignmentManager provides the processor to task assignments persisted to the metadata store.
    * @param taskPartitionAssignmentManager provides the task to partition assignments persisted to the metadata store.
    * @return the instantiated {@see GrouperMetadata}.
    */
  def getGrouperMetadata(config: Config, localityManager: LocalityManager, taskAssignmentManager: TaskAssignmentManager, taskPartitionAssignmentManager: TaskPartitionAssignmentManager) = {
    val processorLocality: util.Map[String, LocationId] = getProcessorLocality(config, localityManager)
    val taskModes: util.Map[TaskName, TaskMode] = taskAssignmentManager.readTaskModes()

    // We read the taskAssignment only for ActiveTasks
    val taskAssignment: util.Map[String, String] = taskAssignmentManager.readTaskAssignment().
      filterKeys(taskName => taskModes.get(new TaskName(taskName)).eq(TaskMode.Active))


    val taskNameToProcessorId: util.Map[TaskName, String] = new util.HashMap[TaskName, String]()
    for ((taskName, processorId) <- taskAssignment) {
      taskNameToProcessorId.put(new TaskName(taskName), processorId)
    }

    val taskLocality: util.Map[TaskName, LocationId] = new util.HashMap[TaskName, LocationId]()
    for ((taskName, processorId) <- taskAssignment) {
      if (processorLocality.containsKey(processorId)) {
        taskLocality.put(new TaskName(taskName), processorLocality.get(processorId))
      }
    }

    val sspToTaskMapping: util.Map[SystemStreamPartition, util.List[String]] = taskPartitionAssignmentManager.readTaskPartitionAssignments()
    val taskPartitionAssignments: util.Map[TaskName, util.List[SystemStreamPartition]] = new util.HashMap[TaskName, util.List[SystemStreamPartition]]()

    // Task to partition assignments is stored as {@see SystemStreamPartition} to list of {@see TaskName} in
    // coordinator stream. This is done due to the 1 MB value size limit in a kafka topic. Conversion to
    // taskName to SystemStreamPartitions is done here to wire-in the data to {@see JobModel}.
    sspToTaskMapping foreach { case (systemStreamPartition: SystemStreamPartition, taskNames: util.List[String]) =>
      for (task <- taskNames) {
        val taskName: TaskName = new TaskName(task)

        // We read the partition assignments only for active-tasks
        if (taskModes.get(taskName).eq(TaskMode.Active)) {
          if (!taskPartitionAssignments.containsKey(taskName)) {
            taskPartitionAssignments.put(taskName, new util.ArrayList[SystemStreamPartition]())
          }
          taskPartitionAssignments.get(taskName).add(systemStreamPartition)
        }
      }
    }
    new GrouperMetadataImpl(processorLocality, taskLocality, taskPartitionAssignments, taskNameToProcessorId)
  }

  /**
    * Retrieves and returns the processor locality of a samza job using provided {@see Config} and {@see LocalityManager}.
    * @param config provides the configurations defined by the user. Required to connect to the storage layer.
    * @param localityManager provides the processor to host mapping persisted to the metadata store.
    * @return the processor locality.
    */
  def getProcessorLocality(config: Config, localityManager: LocalityManager) = {
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
    * @param taskAssignmentManager required to persist the processor to task assignments to the metadata store.
    * @param taskPartitionAssignmentManager required to persist the task to partition assignments to the metadata store.
    * @param grouperMetadata       provides the historical metadata of the samza application.
    */
  def updateTaskAssignments(jobModel: JobModel,
                            taskAssignmentManager: TaskAssignmentManager,
                            taskPartitionAssignmentManager: TaskPartitionAssignmentManager,
                            grouperMetadata: GrouperMetadata): Unit = {
    info("Storing the task assignments into metadata store.")
    val activeTaskNames: util.Set[String] = new util.HashSet[String]()
    val standbyTaskNames: util.Set[String] = new util.HashSet[String]()
    val systemStreamPartitions: util.Set[SystemStreamPartition] = new util.HashSet[SystemStreamPartition]()
    for (container <- jobModel.getContainers.values()) {
      for (taskModel <- container.getTasks.values()) {
        if(taskModel.getTaskMode.eq(TaskMode.Active)) {
          activeTaskNames.add(taskModel.getTaskName.getTaskName)
        }

        if(taskModel.getTaskMode.eq(TaskMode.Standby)) {
          standbyTaskNames.add(taskModel.getTaskName.getTaskName)
        }
        systemStreamPartitions.addAll(taskModel.getSystemStreamPartitions)
      }
    }

    val previousTaskToContainerId = grouperMetadata.getPreviousTaskToProcessorAssignment
    if (activeTaskNames.size() != previousTaskToContainerId.size()) {
      warn("Current task count %s does not match saved task count %s. Stateful jobs may observe misalignment of keys!"
        format (activeTaskNames.size(), previousTaskToContainerId.size()))
      // If the tasks changed, then the partition-task grouping is also likely changed and we can't handle that
      // without a much more complicated mapping. Further, the partition count may have changed, which means
      // input message keys are likely reshuffled w.r.t. partitions, so the local state may not contain necessary
      // data associated with the incoming keys. Warn the user and default to grouper
      // In this scenario the tasks may have been reduced, so we need to delete all the existing messages
      taskAssignmentManager.deleteTaskContainerMappings(previousTaskToContainerId.keys.map(taskName => taskName.getTaskName).asJava)
      taskPartitionAssignmentManager.delete(systemStreamPartitions)
    }

    // if the set of standby tasks has changed, e.g., when the replication-factor changed, or the active-tasks-set has
    // changed, we log a warning and delete the existing mapping for these tasks
    val previousStandbyTasks = taskAssignmentManager.readTaskModes().filter(x => x._2.eq(TaskMode.Standby))
    if(standbyTaskNames.asScala.eq(previousStandbyTasks.keySet)) {
      info("The set of standby tasks has changed, current standby tasks %s, previous standby tasks %s" format (standbyTaskNames, previousStandbyTasks.keySet))
      taskAssignmentManager.deleteTaskContainerMappings(previousStandbyTasks.map(x => x._1.getTaskName).asJava)
    }

    // Task to partition assignments is stored as {@see SystemStreamPartition} to list of {@see TaskName} in
    // coordinator stream. This is done due to the 1 MB value size limit in a kafka topic. Conversion to
    // taskName to SystemStreamPartitions is done here to wire-in the data to {@see JobModel}.
    val sspToTaskNameMap: util.Map[SystemStreamPartition, util.List[String]] = new util.HashMap[SystemStreamPartition, util.List[String]]()

    for (container <- jobModel.getContainers.values()) {
      for ((taskName, taskModel) <- container.getTasks) {
        info ("Storing task: %s and container ID: %s into metadata store" format(taskName.getTaskName, container.getId))
        taskAssignmentManager.writeTaskContainerMapping(taskName.getTaskName, container.getId, container.getTasks.get(taskName).getTaskMode)
        for (partition <- taskModel.getSystemStreamPartitions) {
          if (!sspToTaskNameMap.containsKey(partition)) {
            sspToTaskNameMap.put(partition, new util.ArrayList[String]())
          }
          sspToTaskNameMap.get(partition).add(taskName.getTaskName)
        }
      }
    }

    for ((ssp, taskNames) <- sspToTaskNameMap) {
      info ("Storing ssp: %s and task: %s into metadata store" format(ssp, taskNames))
      taskPartitionAssignmentManager.writeTaskPartitionAssignment(ssp, taskNames)
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
    * @param grouperMetadata provides the historical metadata of the application.
    * @return the built {@see JobModel}.
    */
  def readJobModel(config: Config,
                   changeLogPartitionMapping: util.Map[TaskName, Integer],
                   streamMetadataCache: StreamMetadataCache,
                   grouperMetadata: GrouperMetadata): JobModel = {
    // Do grouping to fetch TaskName to SSP mapping
    val allSystemStreamPartitions = getMatchedInputStreamPartitions(config, streamMetadataCache)

    // processor list is required by some of the groupers. So, let's pass them as part of the config.
    // Copy the config and add the processor list to the config copy.
    val configMap = new util.HashMap[String, String](config)
    configMap.put(JobConfig.PROCESSOR_LIST, String.join(",", grouperMetadata.getProcessorLocality.keySet()))
    val grouper = getSystemStreamPartitionGrouper(new MapConfig(configMap))

    val isHostAffinityEnabled = new ClusterManagerConfig(config).getHostAffinityEnabled

    val groups: util.Map[TaskName, util.Set[SystemStreamPartition]] = if (isHostAffinityEnabled) {
      val sspGrouperProxy: SSPGrouperProxy =  new SSPGrouperProxy(config, grouper)
      sspGrouperProxy.group(allSystemStreamPartitions, grouperMetadata)
    } else {
      grouper.group(allSystemStreamPartitions)
    }
    info("SystemStreamPartitionGrouper %s has grouped the SystemStreamPartitions into %d tasks with the following taskNames: %s" format(grouper, groups.size(), groups))

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
    val standbyTasksEnabled = new JobConfig(config).getStandbyTasksEnabled
    val standbyTaskReplicationFactor = new JobConfig(config).getStandbyTaskReplicationFactor
    val taskNameGrouperProxy = new TaskNameGrouperProxy(containerGrouperFactory.build(config), standbyTasksEnabled, standbyTaskReplicationFactor)
    var containerModels: util.Set[ContainerModel] = null
    if(isHostAffinityEnabled) {
      containerModels = taskNameGrouperProxy.group(taskModels, grouperMetadata)
    } else {
      containerModels = taskNameGrouperProxy.group(taskModels, new util.ArrayList[String](grouperMetadata.getProcessorLocality.keySet()))
    }

    var containerMap = containerModels.asScala.map(containerModel => containerModel.getId -> containerModel).toMap
    new JobModel(config, containerMap.asJava)
  }
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
