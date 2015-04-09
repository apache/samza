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
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.{ CheckpointManagerFactory, OffsetManager }
import org.apache.samza.config.Config
import org.apache.samza.config.MetricsConfig.Config2Metrics
import org.apache.samza.config.SerializerConfig.Config2Serializer
import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.config.StorageConfig.Config2Storage
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
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
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.chooser.DefaultChooser
import org.apache.samza.system.chooser.MessageChooserFactory
import org.apache.samza.system.chooser.RoundRobinChooserFactory
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskInstanceCollector
import org.apache.samza.util.Logging
import org.apache.samza.util.Util
import scala.collection.JavaConversions._
import java.net.URL
import org.apache.samza.job.model.{TaskModel, ContainerModel, JobModel}
import org.apache.samza.serializers.model.SamzaObjectMapper
import org.apache.samza.config.JobConfig.Config2Job
import java.lang.Thread.UncaughtExceptionHandler
import org.apache.samza.serializers._
import org.apache.samza.checkpoint.OffsetManagerMetrics

object SamzaContainer extends Logging {
  def main(args: Array[String]) {
    safeMain(() => new JmxServer, new SamzaContainerExceptionHandler(() => System.exit(1)))
  }

  def safeMain(
    newJmxServer: () => JmxServer,
    exceptionHandler: UncaughtExceptionHandler = null) {
    if (exceptionHandler != null) {
      Thread.setDefaultUncaughtExceptionHandler(exceptionHandler)
    }
    putMDC("containerName", "samza-container-" + System.getenv(ShellCommandConfig.ENV_CONTAINER_ID))
    // Break out the main method to make the JmxServer injectable so we can
    // validate that we don't leak JMX non-daemon threads if we have an
    // exception in the main method.
    val containerId = System.getenv(ShellCommandConfig.ENV_CONTAINER_ID).toInt
    logger.info("Got container ID: %s" format containerId)
    val coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL)
    logger.info("Got coordinator URL: %s" format coordinatorUrl)
    val jobModel = readJobModel(coordinatorUrl)
    val containerModel = jobModel.getContainers()(containerId.toInt)
    val config = jobModel.getConfig
    putMDC("jobName", config.getName.getOrElse(throw new SamzaException("can not find the job name")))
    putMDC("jobId", config.getJobId.getOrElse("1"))
    var jmxServer: JmxServer = null

    try {
      jmxServer = newJmxServer()
      SamzaContainer(containerModel, config).run
    } finally {
      if (jmxServer != null) {
        jmxServer.stop
      }
    }
  }

  /**
   * Fetches config, task:SSP assignments, and task:changelog partition
   * assignments, and returns objects to be used for SamzaContainer's
   * constructor.
   */
  def readJobModel(url: String) = {
    info("Fetching configuration from: %s" format url)
    SamzaObjectMapper
      .getObjectMapper
      .readValue(Util.read(new URL(url)), classOf[JobModel])
  }

  /**
   * A helper function which returns system's default serde according to the
   * serde name. If not found, throw exception.
   */
  def defaultSerdesFromSerdeName(serdeName: String, exceptionSystemName: String, config: Config) = {
    info("looking for default serdes")
    def getSerde(serdeFactory: String) = {
      Util.getObj[SerdeFactory[Object]](serdeFactory).getSerde(serdeName, config)
    }
    val serde = serdeName match {
      case "byte" => getSerde(classOf[ByteSerdeFactory].getCanonicalName)
      case "bytebuffer" => getSerde(classOf[ByteBufferSerdeFactory].getCanonicalName)
      case "integer" => getSerde(classOf[IntegerSerdeFactory].getCanonicalName)
      case "json" => getSerde(classOf[JsonSerdeFactory].getCanonicalName)
      case "long" => getSerde(classOf[LongSerdeFactory].getCanonicalName)
      case "serializable" => getSerde(classOf[SerializableSerdeFactory[java.io.Serializable]].getCanonicalName)
      case "string" => getSerde(classOf[StringSerdeFactory].getCanonicalName)
      case _ => throw new SamzaException("Serde %s for system %s does not exist in configuration." format (serdeName, exceptionSystemName))
    }
    info("use default serde %s for %s" format (serde, serdeName))
    serde
  }

  def apply(containerModel: ContainerModel, config: Config) = {
    val containerId = containerModel.getContainerId
    val containerName = "samza-container-%s" format containerId
    val containerPID = Util.getContainerPID

    info("Setting up Samza container: %s" format containerName)
    info("Samza container PID: %s" format containerPID)
    info("Using configuration: %s" format config)
    info("Using container model: %s" format containerModel)

    val registry = new MetricsRegistryMap(containerName)
    val samzaContainerMetrics = new SamzaContainerMetrics(containerName, registry)
    val systemProducersMetrics = new SystemProducersMetrics(registry)
    val systemConsumersMetrics = new SystemConsumersMetrics(registry)
    val offsetManagerMetrics = new OffsetManagerMetrics(registry)

    val inputSystemStreamPartitions = containerModel
      .getTasks
      .values
      .flatMap(_.getSystemStreamPartitions)
      .toSet

    val inputSystemStreams = inputSystemStreamPartitions
      .map(_.getSystemStream)
      .toSet

    val inputSystems = inputSystemStreams
      .map(_.getSystem)
      .toSet

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
    val inputStreamMetadata = streamMetadataCache.getStreamMetadata(inputSystemStreams)

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
          val serde = serdes.getOrElse(serdeName, defaultSerdesFromSerdeName(serdeName, systemName, config))
          (systemName, serde)
        }).toMap
    }

    /*
     * A Helper function to build a Map[SystemStream, Serde] for streams defined in the config. This is useful to build both key and message serde maps.
     */
    val buildSystemStreamSerdeMap = (getSerdeName: (SystemStream) => Option[String]) => {
      (serdeStreams ++ inputSystemStreamPartitions)
        .filter(systemStream => getSerdeName(systemStream).isDefined)
        .map(systemStream => {
          val serdeName = getSerdeName(systemStream).get
          val serde = serdes.getOrElse(serdeName, defaultSerdesFromSerdeName(serdeName, systemStream.toString, config))
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

    val offsetManager = OffsetManager(inputStreamMetadata, config, checkpointManager, systemAdmins, offsetManagerMetrics)

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

    val taskShutdownMs = config.getShutdownMs.getOrElse(5000L)

    info("Got shutdown timeout milliseconds: %s" format taskShutdownMs)

    // Wire up all task-instance-level (unshared) objects.

    val taskNames = containerModel
      .getTasks
      .values
      .map(_.getTaskName)
      .toSet

    val containerContext = new SamzaContainerContext(containerId, config, taskNames)

    // compute the number of partitions necessary for the change log stream creation.
    // Increment by 1 because partition starts from 0, but we need the absolute count,
    // this value is used for change log topic creation.
    val maxChangeLogStreamPartitions = containerModel.getTasks.values
            .max(Ordering.by { task:TaskModel => task.getChangelogPartition.getPartitionId })
            .getChangelogPartition.getPartitionId + 1

    val taskInstances: Map[TaskName, TaskInstance] = containerModel.getTasks.values.map(taskModel => {
      debug("Setting up task instance: %s" format taskModel)

      val taskName = taskModel.getTaskName

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

      val taskStores = storageEngineFactories
        .map {
          case (storeName, storageEngineFactory) =>
            val changeLogSystemStreamPartition = if (changeLogSystemStreams.contains(storeName)) {
              new SystemStreamPartition(changeLogSystemStreams(storeName), taskModel.getChangelogPartition)
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

      val storageManager = new TaskStorageManager(
        taskName = taskName,
        taskStores = taskStores,
        storeConsumers = storeConsumers,
        changeLogSystemStreams = changeLogSystemStreams,
        maxChangeLogStreamPartitions,
        streamMetadataCache = streamMetadataCache,
        storeBaseDir = storeBaseDir,
        partition = taskModel.getChangelogPartition,
        systemAdmins = systemAdmins)

      val systemStreamPartitions = taskModel
        .getSystemStreamPartitions
        .toSet

      info("Retrieved SystemStreamPartitions " + systemStreamPartitions + " for " + taskName)

      val taskInstance = new TaskInstance(
        task = task,
        taskName = taskName,
        config = config,
        metrics = taskInstanceMetrics,
        consumerMultiplexer = consumerMultiplexer,
        collector = collector,
        containerContext = containerContext,
        offsetManager = offsetManager,
        storageManager = storageManager,
        reporters = reporters,
        systemStreamPartitions = systemStreamPartitions,
        exceptionHandler = TaskInstanceExceptionHandler(taskInstanceMetrics, config))

      (taskName, taskInstance)
    }).toMap

    val runLoop = new RunLoop(
      taskInstances = taskInstances,
      consumerMultiplexer = consumerMultiplexer,
      metrics = samzaContainerMetrics,
      windowMs = taskWindowMs,
      commitMs = taskCommitMs,
      shutdownMs = taskShutdownMs)

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
      startProducers
      startTask
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
      shutdownTask
      shutdownProducers
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
