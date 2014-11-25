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

package org.apache.samza.container

import java.io.File

import org.apache.samza.Partition
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.{CheckpointManagerFactory, OffsetManager}
import org.apache.samza.config.Config
import org.apache.samza.config.MetricsConfig.Config2Metrics
import org.apache.samza.config.SerializerConfig.Config2Serializer
import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.config.StorageConfig.Config2Storage
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.serializers.JsonConfigSerializer
import org.apache.samza.job.ShellCommandBuilder
import org.apache.samza.metrics.JmxServer
import org.apache.samza.metrics.JvmMetrics
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.MetricsReporter
import org.apache.samza.metrics.MetricsReporterFactory
import org.apache.samza.serializers.SerdeFactory
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.storage.StorageEngineFactory
import org.apache.samza.storage.TaskStorageManager
import org.apache.samza.system.StreamMetadataCache
import org.apache.samza.system.SystemConsumers
import org.apache.samza.system.SystemConsumersMetrics
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.SystemProducers
import org.apache.samza.system.SystemProducersMetrics
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.chooser.DefaultChooser
import org.apache.samza.system.chooser.MessageChooserFactory
import org.apache.samza.system.chooser.RoundRobinChooserFactory
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskInstanceCollector
import org.apache.samza.task.TaskLifecycleListener
import org.apache.samza.task.TaskLifecycleListenerFactory
import org.apache.samza.util.Logging
import org.apache.samza.util.Util

import scala.collection.JavaConversions._

object SamzaContainer extends Logging {

  /**
   * Get the decompressed parameter value for the given parameter (if necessary)
   * @param param The parameter which is to be decompressed (if necessary)
   * @return A valid parameter value
   */
  def getParameter(param: String, isCompressed: Boolean) : String = {
    if (isCompressed) {
      info("Compression is ON !")
      val decomressedParam = Util.decompress(param)
      info("Got param = " + decomressedParam)
      decomressedParam
    } else {
      info("Parameter is uncompressed. Using it as-is")
      param
    }
  }

  def main(args: Array[String]) {
    safeMain()
  }

  def safeMain(jmxServer: JmxServer = new JmxServer) {
    // Break out the main method to make the JmxServer injectable so we can
    // validate that we don't leak JMX non-daemon threads if we have an
    // exception in the main method.
    try {
      val containerName = System.getenv(ShellCommandConfig.ENV_CONTAINER_NAME)

      /**
       * If the compressed option is enabled in config, de-compress the 'ENV_CONFIG' and 'ENV_SYSTEM_STREAMS'
       * properties. Note: This is a temporary workaround to reduce the size of the config and hence size
       * of the environment variable(s) exported while starting a Samza container (SAMZA-337)
       */
      val isCompressed = System.getenv(ShellCommandConfig.ENV_COMPRESS_CONFIG).equals("TRUE")
      val configStr = getParameter(System.getenv(ShellCommandConfig.ENV_CONFIG), isCompressed)
      val config = JsonConfigSerializer.fromJson(configStr)
      val sspTaskNames = getTaskNameToSystemStreamPartition(getParameter(System.getenv(ShellCommandConfig.ENV_SYSTEM_STREAMS), isCompressed))
      val taskNameToChangeLogPartitionMapping = getTaskNameToChangeLogPartitionMapping(getParameter(System.getenv(ShellCommandConfig.ENV_TASK_NAME_TO_CHANGELOG_PARTITION_MAPPING), isCompressed))

      SamzaContainer(containerName, sspTaskNames, taskNameToChangeLogPartitionMapping, config).run
    } finally {
      jmxServer.stop
    }
  }

  def getTaskNameToSystemStreamPartition(SSPTaskNamesJSON: String) = {
    // Convert into a standard Java map
    val sspTaskNamesAsJava: Map[TaskName, Set[SystemStreamPartition]] = ShellCommandBuilder.deserializeSystemStreamPartitionSetFromJSON(SSPTaskNamesJSON)

    // From that map build the TaskNamesToSystemStreamPartitions
    val sspTaskNames = TaskNamesToSystemStreamPartitions(sspTaskNamesAsJava)

    if (sspTaskNames.isEmpty) {
      throw new SamzaException("No SystemStreamPartitions for this task. Can't run a task without SystemStreamPartition assignments.")
    }

    sspTaskNames
  }

  def getTaskNameToChangeLogPartitionMapping(taskNameToChangeLogPartitionMappingJSON: String) = {
    // Convert that mapping into a Map
    val taskNameToChangeLogPartitionMapping = ShellCommandBuilder.deserializeTaskNameToChangeLogPartitionMapping(taskNameToChangeLogPartitionMappingJSON).map(kv => kv._1 -> Integer.valueOf(kv._2))

    taskNameToChangeLogPartitionMapping
  }

  def apply(containerName: String, sspTaskNames: TaskNamesToSystemStreamPartitions, taskNameToChangeLogPartitionMapping: Map[TaskName, java.lang.Integer], config: Config) = {
    val containerPID = Util.getContainerPID

    info("Setting up Samza container: %s" format containerName)
    info("Using SystemStreamPartition taskNames %s" format sspTaskNames)
    info("Samza container PID: %s" format containerPID)
    info("Using configuration: %s" format config)

    val registry = new MetricsRegistryMap(containerName)
    val samzaContainerMetrics = new SamzaContainerMetrics(containerName, registry)
    val systemProducersMetrics = new SystemProducersMetrics(registry)
    val systemConsumersMetrics = new SystemConsumersMetrics(registry)

    val inputSystems = sspTaskNames.getAllSystems()

    val systemNames = config.getSystemNames

    info("Got system names: %s" format systemNames)

    val serdeStreams = systemNames.foldLeft(Set[SystemStream]())(_ ++ config.getSerdeStreams(_))

    debug("Got serde streams: %s" format serdeStreams)

    val serdeNames = config.getSerdeNames

    info("Got serde names: %s" format serdeNames)

    val systemFactories = systemNames.map(systemName => {
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      (systemName, Util.getObj[SystemFactory](systemFactoryClassName))
    }).toMap

    val systemAdmins = systemNames
      .map(systemName => (systemName, systemFactories(systemName).getAdmin(systemName, config)))
      .toMap

    info("Got system factories: %s" format systemFactories.keys)

    val streamMetadataCache = new StreamMetadataCache(systemAdmins)
    val inputStreamMetadata = streamMetadataCache.getStreamMetadata(sspTaskNames.getAllSystemStreams)

    info("Got input stream metadata: %s" format inputStreamMetadata)

    val consumers = inputSystems
      .map(systemName => {
        val systemFactory = systemFactories(systemName)

        try {
          (systemName, systemFactory.getConsumer(systemName, config, samzaContainerMetrics.registry))
        } catch {
          case e: Exception =>
            info("Failed to create a consumer for %s, so skipping." format systemName)
            debug("Exception detail:", e)
            (systemName, null)
        }
      })
      .filter(_._2 != null)
      .toMap

    info("Got system consumers: %s" format consumers.keys)

    val producers = systemFactories
      .map {
        case (systemName, systemFactory) =>
          try {
            (systemName, systemFactory.getProducer(systemName, config, samzaContainerMetrics.registry))
          } catch {
            case e: Exception =>
              info("Failed to create a producer for %s, so skipping." format systemName)
              debug("Exception detail:", e)
              (systemName, null)
          }
      }
      .filter(_._2 != null)
      .toMap

    info("Got system producers: %s" format producers.keys)

    val serdes = serdeNames.map(serdeName => {
      val serdeClassName = config
        .getSerdeClass(serdeName)
        .getOrElse(throw new SamzaException("No class defined for serde: %s." format serdeName))

      val serde = Util.getObj[SerdeFactory[Object]](serdeClassName)
        .getSerde(serdeName, config)

      (serdeName, serde)
    }).toMap

    info("Got serdes: %s" format serdes.keys)

    /*
     * A Helper function to build a Map[String, Serde] (systemName -> Serde) for systems defined in the config. This is useful to build both key and message serde maps.
     */
    val buildSystemSerdeMap = (getSerdeName: (String) => Option[String]) => {
      systemNames
        .filter(getSerdeName(_).isDefined)
        .map(systemName => {
          val serdeName = getSerdeName(systemName).get
          val serde = serdes.getOrElse(serdeName, throw new SamzaException("Serde %s for system %s does not exist in configuration." format (serdeName, systemName)))
          (systemName, serde)
        }).toMap
    }

    /*
     * A Helper function to build a Map[SystemStream, Serde] for streams defined in the config. This is useful to build both key and message serde maps.
     */
    val buildSystemStreamSerdeMap = (getSerdeName: (SystemStream) => Option[String]) => {
      (serdeStreams ++ sspTaskNames.getAllSSPs())
        .filter(systemStream => getSerdeName(systemStream).isDefined)
        .map(systemStream => {
          val serdeName = getSerdeName(systemStream).get
          val serde = serdes.getOrElse(serdeName, throw new SamzaException("Serde %s for system %s does not exist in configuration." format (serdeName, systemStream)))
          (systemStream, serde)
        }).toMap
    }

    val systemKeySerdes = buildSystemSerdeMap((systemName: String) => config.getSystemKeySerde(systemName))

    debug("Got system key serdes: %s" format systemKeySerdes)

    val systemMessageSerdes = buildSystemSerdeMap((systemName: String) => config.getSystemMsgSerde(systemName))

    debug("Got system message serdes: %s" format systemMessageSerdes)

    val systemStreamKeySerdes = buildSystemStreamSerdeMap((systemStream: SystemStream) => config.getStreamKeySerde(systemStream))

    debug("Got system stream key serdes: %s" format systemStreamKeySerdes)

    val systemStreamMessageSerdes = buildSystemStreamSerdeMap((systemStream: SystemStream) => config.getStreamMsgSerde(systemStream))

    debug("Got system stream message serdes: %s" format systemStreamMessageSerdes)

    val changeLogSystemStreams = config
      .getStoreNames
      .filter(config.getChangelogStream(_).isDefined)
      .map(name => (name, config.getChangelogStream(name).get)).toMap
      .mapValues(Util.getSystemStreamFromNames(_))

    info("Got change log system streams: %s" format changeLogSystemStreams)

    val changeLogMetadata = streamMetadataCache.getStreamMetadata(changeLogSystemStreams.values.toSet)

    info("Got change log stream metadata: %s" format changeLogMetadata)

    val serdeManager = new SerdeManager(
      serdes = serdes,
      systemKeySerdes = systemKeySerdes,
      systemMessageSerdes = systemMessageSerdes,
      systemStreamKeySerdes = systemStreamKeySerdes,
      systemStreamMessageSerdes = systemStreamMessageSerdes,
      changeLogSystemStreams = changeLogSystemStreams.values.toSet)

    info("Setting up JVM metrics.")

    val jvm = new JvmMetrics(samzaContainerMetrics.registry)

    info("Setting up message chooser.")

    val chooserFactoryClassName = config.getMessageChooserClass.getOrElse(classOf[RoundRobinChooserFactory].getName)

    val chooserFactory = Util.getObj[MessageChooserFactory](chooserFactoryClassName)

    val chooser = DefaultChooser(inputStreamMetadata, chooserFactory, config, samzaContainerMetrics.registry)

    info("Setting up metrics reporters.")

    val reporters = config.getMetricReporterNames.map(reporterName => {
      val metricsFactoryClassName = config
        .getMetricsFactoryClass(reporterName)
        .getOrElse(throw new SamzaException("Metrics reporter %s missing .class config" format reporterName))

      val reporter =
        Util
          .getObj[MetricsReporterFactory](metricsFactoryClassName)
          .getMetricsReporter(reporterName, containerName, config)
      (reporterName, reporter)
    }).toMap

    info("Got metrics reporters: %s" format reporters.keys)

    val checkpointManager = config.getCheckpointManagerFactory match {
      case Some(checkpointFactoryClassName) if (!checkpointFactoryClassName.isEmpty) =>
        Util
          .getObj[CheckpointManagerFactory](checkpointFactoryClassName)
          .getCheckpointManager(config, samzaContainerMetrics.registry)
      case _ => null
    }

    info("Got checkpoint manager: %s" format checkpointManager)

    val offsetManager = OffsetManager(inputStreamMetadata, config, checkpointManager, systemAdmins)

    info("Got offset manager: %s" format offsetManager)

    val dropDeserializationError = config.getDropDeserialization match {
      case Some(dropError) => dropError.toBoolean
      case _ => false
    }

    val dropSerializationError = config.getDropSerialization match {
      case Some(dropError) => dropError.toBoolean
      case _ => false
    }

    val pollIntervalMs = config
      .getPollIntervalMs
      .getOrElse(SystemConsumers.DEFAULT_POLL_INTERVAL_MS.toString)
      .toInt

    val consumerMultiplexer = new SystemConsumers(
      chooser = chooser,
      consumers = consumers,
      serdeManager = serdeManager,
      metrics = systemConsumersMetrics,
      dropDeserializationError = dropDeserializationError,
      pollIntervalMs = pollIntervalMs)

    val producerMultiplexer = new SystemProducers(
      producers = producers,
      serdeManager = serdeManager,
      metrics = systemProducersMetrics,
      dropSerializationError = dropSerializationError)

    val listeners = config.getLifecycleListeners match {
      case Some(listeners) => {
        listeners.split(",").map(listenerName => {
          info("Loading lifecycle listener: %s" format listenerName)

          val listenerClassName = config.getLifecycleListenerClass(listenerName).getOrElse(throw new SamzaException("Referencing missing listener %s in config" format listenerName))

          Util.getObj[TaskLifecycleListenerFactory](listenerClassName)
            .getLifecyleListener(listenerName, config)
        }).toList
      }
      case _ => {
        info("No lifecycle listeners found")

        List[TaskLifecycleListener]()
      }
    }

    // TODO not sure how we should make this config based, or not. Kind of
    // strange, since it has some dynamic directories when used with YARN.
    val storeBaseDir = new File(System.getProperty("user.dir"), "state")

    info("Got storage engine base directory: %s" format storeBaseDir)

    val storageEngineFactories = config
      .getStoreNames
      .map(storeName => {
        val storageFactoryClassName = config
          .getStorageFactoryClassName(storeName)
          .getOrElse(throw new SamzaException("Missing storage factory for %s." format storeName))
        (storeName, Util.getObj[StorageEngineFactory[Object, Object]](storageFactoryClassName))
      }).toMap

    info("Got storage engines: %s" format storageEngineFactories.keys)

    val taskClassName = config
      .getTaskClass
      .getOrElse(throw new SamzaException("No task class defined in configuration."))

    info("Got stream task class: %s" format taskClassName)

    val taskWindowMs = config.getWindowMs.getOrElse(-1L)

    info("Got window milliseconds: %s" format taskWindowMs)

    val taskCommitMs = config.getCommitMs.getOrElse(60000L)

    info("Got commit milliseconds: %s" format taskCommitMs)

    // Wire up all task-instance-level (unshared) objects.

    val taskNames = sspTaskNames.keys.toSet

    val containerContext = new SamzaContainerContext(containerName, config, taskNames)

    val taskInstances: Map[TaskName, TaskInstance] = taskNames.map(taskName => {
      debug("Setting up task instance: %s" format taskName)

      val task = Util.getObj[StreamTask](taskClassName)

      val taskInstanceMetrics = new TaskInstanceMetrics("TaskName-%s" format taskName)

      val collector = new TaskInstanceCollector(producerMultiplexer, taskInstanceMetrics)

      val storeConsumers = changeLogSystemStreams
        .map {
          case (storeName, changeLogSystemStream) =>
            val systemConsumer = systemFactories
              .getOrElse(changeLogSystemStream.getSystem, throw new SamzaException("Changelog system %s for store %s does not exist in the config." format (changeLogSystemStream, storeName)))
              .getConsumer(changeLogSystemStream.getSystem, config, taskInstanceMetrics.registry)
            (storeName, systemConsumer)
        }.toMap

      info("Got store consumers: %s" format storeConsumers)

      val partitionForThisTaskName = new Partition(taskNameToChangeLogPartitionMapping(taskName))

      val taskStores = storageEngineFactories
        .map {
          case (storeName, storageEngineFactory) =>
            val changeLogSystemStreamPartition = if (changeLogSystemStreams.contains(storeName)) {
              new SystemStreamPartition(changeLogSystemStreams(storeName), partitionForThisTaskName)
            } else {
              null
            }
            val keySerde = config.getStorageKeySerde(storeName) match {
              case Some(keySerde) => serdes(keySerde)
              case _ => null
            }
            val msgSerde = config.getStorageMsgSerde(storeName) match {
              case Some(msgSerde) => serdes(msgSerde)
              case _ => null
            }
            val storePartitionDir = TaskStorageManager.getStorePartitionDir(storeBaseDir, storeName, taskName)
            val storageEngine = storageEngineFactory.getStorageEngine(
              storeName,
              storePartitionDir,
              keySerde,
              msgSerde,
              collector,
              taskInstanceMetrics.registry,
              changeLogSystemStreamPartition,
              containerContext)
            (storeName, storageEngine)
        }

      info("Got task stores: %s" format taskStores)

      val changeLogOldestOffsets = getChangeLogOldestOffsetsForPartition(partitionForThisTaskName, changeLogMetadata)

      info("Assigning oldest change log offsets for taskName %s: %s" format (taskName, changeLogOldestOffsets))

      val storageManager = new TaskStorageManager(
        taskName = taskName,
        taskStores = taskStores,
        storeConsumers = storeConsumers,
        changeLogSystemStreams = changeLogSystemStreams,
        changeLogOldestOffsets = changeLogOldestOffsets,
        storeBaseDir = storeBaseDir,
        partitionForThisTaskName)

      val systemStreamPartitions: Set[SystemStreamPartition] = sspTaskNames.getOrElse(taskName, throw new SamzaException("Can't find taskName " + taskName + " in map of SystemStreamPartitions: " + sspTaskNames))

      info("Retrieved SystemStreamPartitions " + systemStreamPartitions + " for " + taskName)

      val taskInstance = new TaskInstance(
        task = task,
        taskName = taskName,
        config = config,
        metrics = taskInstanceMetrics,
        consumerMultiplexer = consumerMultiplexer,
        collector = collector,
        offsetManager = offsetManager,
        storageManager = storageManager,
        reporters = reporters,
        listeners = listeners,
        systemStreamPartitions = systemStreamPartitions,
        exceptionHandler = TaskInstanceExceptionHandler(taskInstanceMetrics, config))

      (taskName, taskInstance)
    }).toMap

    val runLoop = new RunLoop(
      taskInstances = taskInstances,
      consumerMultiplexer = consumerMultiplexer,
      metrics = samzaContainerMetrics,
      windowMs = taskWindowMs,
      commitMs = taskCommitMs)

    info("Samza container setup complete.")

    new SamzaContainer(
      taskInstances = taskInstances,
      runLoop = runLoop,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      offsetManager = offsetManager,
      metrics = samzaContainerMetrics,
      reporters = reporters,
      jvm = jvm)
  }

  /**
   * Builds a map from SystemStreamPartition to oldest offset for changelogs.
   */
  def getChangeLogOldestOffsetsForPartition(partition: Partition, inputStreamMetadata: Map[SystemStream, SystemStreamMetadata]): Map[SystemStream, String] = {
    inputStreamMetadata
      .mapValues(_.getSystemStreamPartitionMetadata.get(partition))
      .filter(_._2 != null)
      .mapValues(_.getOldestOffset)
  }
}

class SamzaContainer(
  taskInstances: Map[TaskName, TaskInstance],
  runLoop: RunLoop,
  consumerMultiplexer: SystemConsumers,
  producerMultiplexer: SystemProducers,
  metrics: SamzaContainerMetrics,
  offsetManager: OffsetManager = new OffsetManager,
  reporters: Map[String, MetricsReporter] = Map(),
  jvm: JvmMetrics = null) extends Runnable with Logging {

  def run {
    try {
      info("Starting container.")

      startMetrics
      startOffsetManager
      startStores
      startTask
      startProducers
      startConsumers

      info("Entering run loop.")
      runLoop.run
    } catch {
      case e: Exception =>
        error("Caught exception in process loop.", e)
        throw e
    } finally {
      info("Shutting down.")

      shutdownConsumers
      shutdownProducers
      shutdownTask
      shutdownStores
      shutdownOffsetManager
      shutdownMetrics

      info("Shutdown complete.")
    }
  }

  def startMetrics {
    info("Registering task instances with metrics.")

    taskInstances.values.foreach(_.registerMetrics)

    info("Starting JVM metrics.")

    if (jvm != null) {
      jvm.start
    }

    info("Starting metrics reporters.")

    reporters.values.foreach(reporter => {
      reporter.register(metrics.source, metrics.registry)
      reporter.start
    })
  }

  def startOffsetManager {
    info("Registering task instances with offsets.")

    taskInstances.values.foreach(_.registerOffsets)

    info("Starting offset manager.")

    offsetManager.start
  }

  def startStores {
    info("Starting task instance stores.")

    taskInstances.values.foreach(_.startStores)
  }

  def startTask {
    info("Initializing stream tasks.")

    taskInstances.values.foreach(_.initTask)
  }

  def startProducers {
    info("Registering task instances with producers.")

    taskInstances.values.foreach(_.registerProducers)

    info("Starting producer multiplexer.")

    producerMultiplexer.start
  }

  def startConsumers {
    info("Registering task instances with consumers.")

    taskInstances.values.foreach(_.registerConsumers)

    info("Starting consumer multiplexer.")

    consumerMultiplexer.start
  }

  def shutdownConsumers {
    info("Shutting down consumer multiplexer.")

    consumerMultiplexer.stop
  }

  def shutdownProducers {
    info("Shutting down producer multiplexer.")

    producerMultiplexer.stop
  }

  def shutdownTask {
    info("Shutting down task instance stream tasks.")

    taskInstances.values.foreach(_.shutdownTask)
  }

  def shutdownStores {
    info("Shutting down task instance stores.")

    taskInstances.values.foreach(_.shutdownStores)
  }

  def shutdownOffsetManager {
    info("Shutting down offset manager.")

    offsetManager.stop
  }

  def shutdownMetrics {
    info("Shutting down metrics reporters.")

    reporters.values.foreach(_.stop)

    if (jvm != null) {
      info("Shutting down JVM metrics.")

      jvm.stop
    }
  }
}
