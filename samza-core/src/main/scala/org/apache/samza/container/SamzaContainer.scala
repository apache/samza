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
import java.lang.management.ManagementFactory
import java.lang.reflect.InvocationTargetException
import java.net.{URL, UnknownHostException}
import java.nio.file.Path
import java.time.Duration
import java.util
import java.util.Base64
import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, TimeUnit}

import com.google.common.annotations.VisibleForTesting
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.samza.checkpoint.{CheckpointListener, CheckpointManagerFactory, OffsetManager, OffsetManagerMetrics}
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.MetricsConfig.Config2Metrics
import org.apache.samza.config.SerializerConfig.Config2Serializer
import org.apache.samza.config.StorageConfig.Config2Storage
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config._
import org.apache.samza.container.disk.DiskSpaceMonitor.Listener
import org.apache.samza.container.disk.{DiskQuotaPolicyFactory, DiskSpaceMonitor, NoThrottlingDiskQuotaPolicyFactory, PollingScanDiskSpaceMonitor}
import org.apache.samza.container.host.{StatisticsMonitorImpl, SystemMemoryStatistics, SystemStatisticsMonitor}
import org.apache.samza.job.model.{ContainerModel, JobModel}
import org.apache.samza.metrics.{JmxServer, JvmMetrics, MetricsRegistryMap, MetricsReporter}
import org.apache.samza.serializers._
import org.apache.samza.serializers.model.SamzaObjectMapper
import org.apache.samza.storage._
import org.apache.samza.system._
import org.apache.samza.system.chooser.{DefaultChooser, MessageChooserFactory, RoundRobinChooserFactory}
import org.apache.samza.table.TableManager
import org.apache.samza.table.utils.SerdeUtils
import org.apache.samza.task._
import org.apache.samza.util.{Util, _}
import org.apache.samza.{SamzaContainerStatus, SamzaException}

import scala.collection.JavaConverters._

object SamzaContainer extends Logging {
  val DEFAULT_READ_JOBMODEL_DELAY_MS = 100
  val DISK_POLL_INTERVAL_KEY = "container.disk.poll.interval.ms"

  /**
   * Fetches config, task:SSP assignments, and task:changelog partition
   * assignments, and returns objects to be used for SamzaContainer's
   * constructor.
   */
  def readJobModel(url: String, initialDelayMs: Int = scala.util.Random.nextInt(DEFAULT_READ_JOBMODEL_DELAY_MS) + 1) = {
    info("Fetching configuration from: %s" format url)
    SamzaObjectMapper
      .getObjectMapper
      .readValue(
        HttpUtil.read(
          url = new URL(url),
          retryBackoff = new ExponentialSleepStrategy(initialDelayMs = initialDelayMs)),
        classOf[JobModel])
  }

  // TODO: SAMZA-1701 SamzaContainer should not contain any logic related to store directories
  def getNonLoggedStorageBaseDir(config: Config, defaultStoreBaseDir: File) = {
    config.getNonLoggedStorePath match {
      case Some(nonLoggedStorePath) =>
        new File(nonLoggedStorePath)
      case None =>
        defaultStoreBaseDir
    }
  }

  // TODO: SAMZA-1701 SamzaContainer should not contain any logic related to store directories
  def getLoggedStorageBaseDir(config: Config, defaultStoreBaseDir: File) = {
    val defaultLoggedStorageBaseDir = config.getLoggedStorePath match {
      case Some(durableStorePath) =>
        new File(durableStorePath)
      case None =>
        defaultStoreBaseDir
    }

    var loggedStorageBaseDir:File = null
    if(System.getenv(ShellCommandConfig.ENV_LOGGED_STORE_BASE_DIR) != null) {
      val jobNameAndId = (
        config.getName.getOrElse(throw new ConfigException("Missing required config: job.name")),
        config.getJobId
      )

      loggedStorageBaseDir = new File(System.getenv(ShellCommandConfig.ENV_LOGGED_STORE_BASE_DIR)
        + File.separator + jobNameAndId._1 + "-" + jobNameAndId._2)
    } else {
      if (config.getLoggedStorePath.isEmpty) {
        warn("No override was provided for logged store base directory. This disables local state re-use on " +
          "application restart. If you want to enable this feature, set LOGGED_STORE_BASE_DIR as an environment " +
          "variable in all machines running the Samza container or configure job.logged.store.base.dir for your application")
      }

      loggedStorageBaseDir = defaultLoggedStorageBaseDir
    }

    loggedStorageBaseDir
  }

  def apply(
    containerId: String,
    jobModel: JobModel,
    config: Config,
    customReporters: Map[String, MetricsReporter] = Map[String, MetricsReporter](),
    taskFactory: TaskFactory[_]) = {
    val containerModel = jobModel.getContainers.get(containerId)
    val containerName = "samza-container-%s" format containerId
    val maxChangeLogStreamPartitions = jobModel.maxChangeLogStreamPartitions

    val containerPID = ManagementFactory.getRuntimeMXBean().getName()

    info("Setting up Samza container: %s" format containerName)

    startupLog("Samza container PID: %s" format containerPID)
    println("Container PID: %s" format containerPID)
    startupLog("Using configuration: %s" format config)
    startupLog("Using container model: %s" format containerModel)

    val registry = new MetricsRegistryMap(containerName)
    val samzaContainerMetrics = new SamzaContainerMetrics(containerName, registry)
    val systemProducersMetrics = new SystemProducersMetrics(registry)
    val systemConsumersMetrics = new SystemConsumersMetrics(registry)
    val offsetManagerMetrics = new OffsetManagerMetrics(registry)
    val clock = if (config.getMetricsTimerEnabled) {
      new HighResolutionClock {
        override def nanoTime(): Long = System.nanoTime()
      }
    } else {
      new HighResolutionClock {
        override def nanoTime(): Long = 0L
      }
    }

    val inputSystemStreamPartitions = containerModel
      .getTasks
      .values
      .asScala
      .flatMap(_.getSystemStreamPartitions.asScala)
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

    info("Got serde streams: %s" format serdeStreams)

    val systemFactories = systemNames.map(systemName => {
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      (systemName, Util.getObj(systemFactoryClassName, classOf[SystemFactory]))
    }).toMap
    info("Got system factories: %s" format systemFactories.keys)

    val systemAdmins = new SystemAdmins(config)
    info("Got system admins: %s" format systemAdmins.getSystemNames)

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
            error("Failed to create a consumer for %s, so skipping." format systemName, e)
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
              error("Failed to create a producer for %s, so skipping." format systemName, e)
              (systemName, null)
          }
      }
      .filter(_._2 != null)

    info("Got system producers: %s" format producers.keys)

    val serdesFromFactories = config.getSerdeNames.map(serdeName => {
      val serdeClassName = config
        .getSerdeClass(serdeName)
        .getOrElse(SerializerConfig.getSerdeFactoryName(serdeName))

      val serde = Util.getObj(serdeClassName, classOf[SerdeFactory[Object]])
        .getSerde(serdeName, config)

      (serdeName, serde)
    }).toMap
    info("Got serdes from factories: %s" format serdesFromFactories.keys)

    val serializableSerde = new SerializableSerde[Serde[Object]]()
    val serdesFromSerializedInstances = config.subset(SerializerConfig.SERIALIZER_PREFIX format "").asScala
        .filter { case (key, value) => key.endsWith(SerializerConfig.SERIALIZED_INSTANCE_SUFFIX) }
        .flatMap { case (key, value) =>
          val serdeName = key.replace(SerializerConfig.SERIALIZED_INSTANCE_SUFFIX, "")
          debug(s"Trying to deserialize serde instance for $serdeName")
          try {
            val bytes = Base64.getDecoder.decode(value)
            val serdeInstance = serializableSerde.fromBytes(bytes)
            debug(s"Returning serialized instance for $serdeName")
            Some((serdeName, serdeInstance))
          } catch {
            case e: Exception =>
              warn(s"Ignoring invalid serialized instance for $serdeName: $value", e)
              None
          }
        }
    info("Got serdes from serialized instances: %s" format serdesFromSerializedInstances.keys)

    val serdes = serdesFromFactories ++ serdesFromSerializedInstances

    /*
     * A Helper function to build a Map[String, Serde] (systemName -> Serde) for systems defined
     * in the config. This is useful to build both key and message serde maps.
     */
    val buildSystemSerdeMap = (getSerdeName: (String) => Option[String]) => {
      systemNames
        .filter(systemName => getSerdeName(systemName).isDefined)
        .flatMap(systemName => {
          val serdeName = getSerdeName(systemName).get
          val serde = serdes.getOrElse(serdeName,
            throw new SamzaException("buildSystemSerdeMap: No class defined for serde: %s." format serdeName))

          // this shouldn't happen since system level serdes can't be set programmatically using the high level
          // API, but adding this for safety.
          Option(serde)
            .filter(!_.isInstanceOf[NoOpSerde[Any]])
            .map(serde => (systemName, serde))
        }).toMap
    }

    /*
     * A Helper function to build a Map[SystemStream, Serde] for streams defined in the config.
     * This is useful to build both key and message serde maps.
     */
    val buildSystemStreamSerdeMap = (getSerdeName: (SystemStream) => Option[String]) => {
      (serdeStreams ++ inputSystemStreamPartitions)
        .filter(systemStream => getSerdeName(systemStream).isDefined)
        .flatMap(systemStream => {
          val serdeName = getSerdeName(systemStream).get
          val serde = serdes.getOrElse(serdeName,
            throw new SamzaException("buildSystemStreamSerdeMap: No serde found for name: %s." format serdeName))

          // respect explicitly set no-op serdes in high level API
          Option(serde)
            .filter(!_.isInstanceOf[NoOpSerde[Any]])
            .map(serde => (systemStream, serde))
        }).toMap
    }

    val systemKeySerdes = buildSystemSerdeMap(systemName => config.getSystemKeySerde(systemName))

    debug("Got system key serdes: %s" format systemKeySerdes)

    val systemMessageSerdes = buildSystemSerdeMap(systemName => config.getSystemMsgSerde(systemName))

    debug("Got system message serdes: %s" format systemMessageSerdes)

    val systemStreamKeySerdes = buildSystemStreamSerdeMap(systemStream => config.getStreamKeySerde(systemStream))

    debug("Got system stream key serdes: %s" format systemStreamKeySerdes)

    val systemStreamMessageSerdes = buildSystemStreamSerdeMap(systemStream => config.getStreamMsgSerde(systemStream))

    debug("Got system stream message serdes: %s" format systemStreamMessageSerdes)

    val changeLogSystemStreams = config
      .getStoreNames
      .filter(config.getChangelogStream(_).isDefined)
      .map(name => (name, config.getChangelogStream(name).get)).toMap
      .mapValues(StreamUtil.getSystemStreamFromNames(_))

    info("Got change log system streams: %s" format changeLogSystemStreams)

    /*
     * This keeps track of the changelog SSPs that are associated with the whole container. This is used so that we can
     * prefetch the metadata about the all of the changelog SSPs associated with the container whenever we need the
     * metadata about some of the changelog SSPs.
     * An example use case is when Samza writes offset files for stores ({@link TaskStorageManager}). Each task is
     * responsible for its own offset file, but if we can do prefetching, then most tasks will already have cached
     * metadata by the time they need the offset metadata.
     * Note: By using all changelog streams to build the sspsToPrefetch, any fetches done for persisted stores will
     * include the ssps for non-persisted stores, so this is slightly suboptimal. However, this does not increase the
     * actual number of calls to the {@link SystemAdmin}, and we can decouple this logic from the per-task objects (e.g.
     * {@link TaskStorageManager}).
     */
    val changelogSSPMetadataCache = new SSPMetadataCache(systemAdmins,
      Duration.ofSeconds(5),
      SystemClock.instance,
      getChangelogSSPsForContainer(containerModel, changeLogSystemStreams).asJava)

    val intermediateStreams = config
      .getStreamIds
      .filter(config.getIsIntermediateStream(_))
      .toList

    info("Got intermediate streams: %s" format intermediateStreams)

    val sideInputStoresToSystemStreams = config.getStoreNames
      .map { storeName => (storeName, config.getSideInputs(storeName)) }
      .filter { case (storeName, sideInputs) => sideInputs.nonEmpty }
      .map { case (storeName, sideInputs) => (storeName, sideInputs.map(StreamUtil.getSystemStreamFromNameOrId(config, _))) }
      .toMap

    info("Got side input store system streams: %s" format sideInputStoresToSystemStreams)

    val controlMessageKeySerdes = intermediateStreams
      .flatMap(streamId => {
        val systemStream = config.streamIdToSystemStream(streamId)
        systemStreamKeySerdes.get(systemStream)
                .orElse(systemKeySerdes.get(systemStream.getSystem))
                .map(serde => (systemStream, new StringSerde("UTF-8")))
      }).toMap

    val intermediateStreamMessageSerdes = intermediateStreams
      .flatMap(streamId => {
        val systemStream = config.streamIdToSystemStream(streamId)
        systemStreamMessageSerdes.get(systemStream)
                .orElse(systemMessageSerdes.get(systemStream.getSystem))
                .map(serde => (systemStream, new IntermediateMessageSerde(serde)))
      }).toMap

    val serdeManager = new SerdeManager(
      serdes = serdes,
      systemKeySerdes = systemKeySerdes,
      systemMessageSerdes = systemMessageSerdes,
      systemStreamKeySerdes = systemStreamKeySerdes,
      systemStreamMessageSerdes = systemStreamMessageSerdes,
      changeLogSystemStreams = changeLogSystemStreams.values.toSet,
      controlMessageKeySerdes = controlMessageKeySerdes,
      intermediateMessageSerdes = intermediateStreamMessageSerdes)

    info("Setting up JVM metrics.")

    val jvm = new JvmMetrics(samzaContainerMetrics.registry)

    info("Setting up message chooser.")

    val chooserFactoryClassName = config.getMessageChooserClass.getOrElse(classOf[RoundRobinChooserFactory].getName)

    val chooserFactory = Util.getObj(chooserFactoryClassName, classOf[MessageChooserFactory])

    val chooser = DefaultChooser(inputStreamMetadata, chooserFactory, config, samzaContainerMetrics.registry, systemAdmins)

    info("Setting up metrics reporters.")

    val reporters = MetricsReporterLoader.getMetricsReporters(config, containerName).asScala.toMap ++ customReporters

    info("Got metrics reporters: %s" format reporters.keys)

    val securityManager = config.getSecurityManagerFactory match {
      case Some(securityManagerFactoryClassName) =>
        Util
          .getObj(securityManagerFactoryClassName, classOf[SecurityManagerFactory])
          .getSecurityManager(config)
      case _ => null
    }
    info("Got security manager: %s" format securityManager)

    val checkpointManager = config.getCheckpointManagerFactory()
      .filterNot(_.isEmpty)
      .map(Util.getObj(_, classOf[CheckpointManagerFactory])
        .getCheckpointManager(config, samzaContainerMetrics.registry))
      .orNull
    info("Got checkpoint manager: %s" format checkpointManager)

    // create a map of consumers with callbacks to pass to the OffsetManager
    val checkpointListeners = consumers.filter(_._2.isInstanceOf[CheckpointListener])
      .map { case (system, consumer) => (system, consumer.asInstanceOf[CheckpointListener])}
    info("Got checkpointListeners : %s" format checkpointListeners)

    val offsetManager = OffsetManager(inputStreamMetadata, config, checkpointManager, systemAdmins, checkpointListeners, offsetManagerMetrics)
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
      pollIntervalMs = pollIntervalMs,
      clock = () => clock.nanoTime())

    val producerMultiplexer = new SystemProducers(
      producers = producers,
      serdeManager = serdeManager,
      metrics = systemProducersMetrics,
      dropSerializationError = dropSerializationError)

    val storageEngineFactories = config
      .getStoreNames
      .map(storeName => {
        val storageFactoryClassName = config
          .getStorageFactoryClassName(storeName)
          .getOrElse(throw new SamzaException("Missing storage factory for %s." format storeName))
        (storeName, Util.getObj(storageFactoryClassName, classOf[StorageEngineFactory[Object, Object]]))
      }).toMap

    info("Got storage engines: %s" format storageEngineFactories.keys)

    val singleThreadMode = config.getSingleThreadMode
    info("Got single thread mode: " + singleThreadMode)

    val threadPoolSize = config.getThreadPoolSize
    info("Got thread pool size: " + threadPoolSize)


    val taskThreadPool = if (!singleThreadMode && threadPoolSize > 0) {
      Executors.newFixedThreadPool(threadPoolSize,
        new ThreadFactoryBuilder().setNameFormat("Samza Container Thread-%d").build())
    } else {
      null
    }


    val finalTaskFactory = TaskFactoryUtil.finalizeTaskFactory(
      taskFactory,
      singleThreadMode,
      taskThreadPool)

    // Wire up all task-instance-level (unshared) objects.
    val taskNames = containerModel
      .getTasks
      .values
      .asScala
      .map(_.getTaskName)
      .toSet
    val containerContext = new SamzaContainerContext(containerId, config, taskNames.asJava, samzaContainerMetrics.registry)


    val storeWatchPaths = new util.HashSet[Path]()

    val timerExecutor = Executors.newSingleThreadScheduledExecutor

    val taskInstances: Map[TaskName, TaskInstance] = containerModel.getTasks.values.asScala.map(taskModel => {
      debug("Setting up task instance: %s" format taskModel)

      val taskName = taskModel.getTaskName

      val task = finalTaskFactory match {
        case tf: AsyncStreamTaskFactory => tf.asInstanceOf[AsyncStreamTaskFactory].createInstance()
        case tf: StreamTaskFactory => tf.asInstanceOf[StreamTaskFactory].createInstance()
      }

      val taskInstanceMetrics = new TaskInstanceMetrics("TaskName-%s" format taskName)

      val collector = new TaskInstanceCollector(producerMultiplexer, taskInstanceMetrics)

      val storeConsumers = changeLogSystemStreams
        .map {
          case (storeName, changeLogSystemStream) =>
            val systemConsumer = systemFactories
              .getOrElse(changeLogSystemStream.getSystem,
                throw new SamzaException("Changelog system %s for store %s does not " +
                  "exist in the config." format (changeLogSystemStream, storeName)))
              .getConsumer(changeLogSystemStream.getSystem, config, taskInstanceMetrics.registry)
            samzaContainerMetrics.addStoreRestorationGauge(taskName, storeName)
            (storeName, systemConsumer)
        }

      info("Got store consumers: %s" format storeConsumers)

      val defaultStoreBaseDir = new File(System.getProperty("user.dir"), "state")
      info("Got default storage engine base directory: %s" format defaultStoreBaseDir)

      val nonLoggedStorageBaseDir = getNonLoggedStorageBaseDir(config, defaultStoreBaseDir)
      info("Got base directory for non logged data stores: %s" format nonLoggedStorageBaseDir)

      val loggedStorageBaseDir = getLoggedStorageBaseDir(config, defaultStoreBaseDir)
      info("Got base directory for logged data stores: %s" format loggedStorageBaseDir)

      val taskStores = storageEngineFactories
        .map {
          case (storeName, storageEngineFactory) =>
            val changeLogSystemStreamPartition = if (changeLogSystemStreams.contains(storeName)) {
              new SystemStreamPartition(changeLogSystemStreams(storeName), taskModel.getChangelogPartition)
            } else {
              null
            }

            val keySerde = config.getStorageKeySerde(storeName) match {
              case Some(keySerde) => serdes.getOrElse(keySerde,
                throw new SamzaException("StorageKeySerde: No class defined for serde: %s." format keySerde))
              case _ => null
            }

            val msgSerde = config.getStorageMsgSerde(storeName) match {
              case Some(msgSerde) => serdes.getOrElse(msgSerde,
                throw new SamzaException("StorageMsgSerde: No class defined for serde: %s." format msgSerde))
              case _ => null
            }

            val storeDir = if (changeLogSystemStreamPartition != null) {
              TaskStorageManager.getStorePartitionDir(loggedStorageBaseDir, storeName, taskName)
            } else {
              TaskStorageManager.getStorePartitionDir(nonLoggedStorageBaseDir, storeName, taskName)
            }

            storeWatchPaths.add(storeDir.toPath)

            val storageEngine = storageEngineFactory.getStorageEngine(
              storeName,
              storeDir,
              keySerde,
              msgSerde,
              collector,
              taskInstanceMetrics.registry,
              changeLogSystemStreamPartition,
              containerContext)
            (storeName, storageEngine)
        }

      info("Got task stores: %s" format taskStores)

      val taskSSPs = taskModel.getSystemStreamPartitions.asScala.toSet
      info("Got task SSPs: %s" format taskSSPs)

      val (sideInputStores, nonSideInputStores) =
        taskStores.partition { case (storeName, _) => sideInputStoresToSystemStreams.contains(storeName)}

      val sideInputStoresToSSPs = sideInputStoresToSystemStreams.mapValues(sideInputSystemStreams =>
        taskSSPs.filter(ssp => sideInputSystemStreams.contains(ssp.getSystemStream)).asJava)

      val taskSideInputSSPs = sideInputStoresToSSPs.values.flatMap(_.asScala).toSet

      info ("Got task side input SSPs: %s" format taskSideInputSSPs)

      val sideInputStoresToProcessor = sideInputStores.keys.map(storeName => {
          // serialized instances takes precedence over the factory configuration.
          config.getSideInputsProcessorSerializedInstance(storeName).map(serializedInstance =>
              (storeName, SerdeUtils.deserialize("Side Inputs Processor", serializedInstance)))
            .orElse(config.getSideInputsProcessorFactory(storeName).map(factoryClassName =>
              (storeName, Util.getObj(factoryClassName, classOf[SideInputsProcessorFactory])
                .getSideInputsProcessor(config, taskInstanceMetrics.registry))))
            .get
        }).toMap

      val storageManager = new TaskStorageManager(
        taskName = taskName,
        taskStores = nonSideInputStores,
        storeConsumers = storeConsumers,
        changeLogSystemStreams = changeLogSystemStreams,
        maxChangeLogStreamPartitions,
        streamMetadataCache = streamMetadataCache,
        sspMetadataCache = changelogSSPMetadataCache,
        nonLoggedStoreBaseDir = nonLoggedStorageBaseDir,
        loggedStoreBaseDir = loggedStorageBaseDir,
        partition = taskModel.getChangelogPartition,
        systemAdmins = systemAdmins,
        new StorageConfig(config).getChangeLogDeleteRetentionsInMs,
        new SystemClock)

      var sideInputStorageManager: TaskSideInputStorageManager = null
      if (sideInputStores.nonEmpty) {
        sideInputStorageManager = new TaskSideInputStorageManager(
          taskName,
          streamMetadataCache,
          loggedStorageBaseDir.getPath,
          sideInputStores.asJava,
          sideInputStoresToProcessor.asJava,
          sideInputStoresToSSPs.asJava,
          systemAdmins,
          config,
          new SystemClock)
      }

      val tableManager = new TableManager(config, serdes.asJava)

      info("Got table manager")

      def createTaskInstance(task: Any): TaskInstance = new TaskInstance(
          task = task,
          taskName = taskName,
          config = config,
          metrics = taskInstanceMetrics,
          systemAdmins = systemAdmins,
          consumerMultiplexer = consumerMultiplexer,
          collector = collector,
          containerContext = containerContext,
          offsetManager = offsetManager,
          storageManager = storageManager,
          tableManager = tableManager,
          reporters = reporters,
          systemStreamPartitions = taskSSPs,
          exceptionHandler = TaskInstanceExceptionHandler(taskInstanceMetrics, config),
          jobModel = jobModel,
          streamMetadataCache = streamMetadataCache,
          timerExecutor = timerExecutor,
          sideInputSSPs = taskSideInputSSPs,
          sideInputStorageManager = sideInputStorageManager)

      val taskInstance = createTaskInstance(task)

      (taskName, taskInstance)
    }).toMap

    val maxThrottlingDelayMs = config.getLong("container.disk.quota.delay.max.ms", TimeUnit.SECONDS.toMillis(1))

    val runLoop = RunLoopFactory.createRunLoop(
      taskInstances,
      consumerMultiplexer,
      taskThreadPool,
      maxThrottlingDelayMs,
      samzaContainerMetrics,
      config,
      clock)

    val memoryStatisticsMonitor : SystemStatisticsMonitor = new StatisticsMonitorImpl()
    memoryStatisticsMonitor.registerListener(new SystemStatisticsMonitor.Listener {
      override def onUpdate(sample: SystemMemoryStatistics): Unit = {
        val physicalMemoryBytes : Long = sample.getPhysicalMemoryBytes
        val physicalMemoryMb : Double = physicalMemoryBytes / (1024.0 * 1024.0)
        logger.debug("Container physical memory utilization (mb): " + physicalMemoryMb)
        samzaContainerMetrics.physicalMemoryMb.set(physicalMemoryMb)
      }
    })

    val diskQuotaBytes = config.getLong("container.disk.quota.bytes", Long.MaxValue)
    samzaContainerMetrics.diskQuotaBytes.set(diskQuotaBytes)

    val diskQuotaPolicyFactoryString = config.get("container.disk.quota.policy.factory",
      classOf[NoThrottlingDiskQuotaPolicyFactory].getName)
    val diskQuotaPolicyFactory = Util.getObj(diskQuotaPolicyFactoryString, classOf[DiskQuotaPolicyFactory])
    val diskQuotaPolicy = diskQuotaPolicyFactory.create(config)

    var diskSpaceMonitor: DiskSpaceMonitor = null
    val diskPollMillis = config.getInt(DISK_POLL_INTERVAL_KEY, 0)
    if (diskPollMillis != 0) {
      diskSpaceMonitor = new PollingScanDiskSpaceMonitor(storeWatchPaths, diskPollMillis)
      diskSpaceMonitor.registerListener(new Listener {
        override def onUpdate(diskUsageBytes: Long): Unit = {
          val newWorkRate = diskQuotaPolicy.apply(1.0 - (diskUsageBytes.toDouble / diskQuotaBytes))
          runLoop.asInstanceOf[Throttleable].setWorkFactor(newWorkRate)
          samzaContainerMetrics.executorWorkFactor.set(runLoop.asInstanceOf[Throttleable].getWorkFactor)
          samzaContainerMetrics.diskUsageBytes.set(diskUsageBytes)
        }
      })

      info("Initialized disk space monitor watch paths to: %s" format storeWatchPaths)
    } else {
      info(s"Disk quotas disabled because polling interval is not set ($DISK_POLL_INTERVAL_KEY)")
    }
    info("Samza container setup complete.")

    new SamzaContainer(
      containerContext = containerContext,
      taskInstances = taskInstances,
      runLoop = runLoop,
      systemAdmins = systemAdmins,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      offsetManager = offsetManager,
      securityManager = securityManager,
      metrics = samzaContainerMetrics,
      reporters = reporters,
      jvm = jvm,
      diskSpaceMonitor = diskSpaceMonitor,
      hostStatisticsMonitor = memoryStatisticsMonitor,
      taskThreadPool = taskThreadPool,
      timerExecutor = timerExecutor)
  }


  /**
    * Builds the set of SSPs for all changelogs on this container.
    */
  @VisibleForTesting
  private[container] def getChangelogSSPsForContainer(containerModel: ContainerModel,
    changeLogSystemStreams: Map[String, SystemStream]): Set[SystemStreamPartition] = {
    containerModel.getTasks.values().asScala
      .map(taskModel => taskModel.getChangelogPartition)
      .flatMap(changelogPartition => changeLogSystemStreams.map { case (_, systemStream) =>
        new SystemStreamPartition(systemStream, changelogPartition) })
      .toSet
  }
}

class SamzaContainer(
  containerContext: SamzaContainerContext,
  taskInstances: Map[TaskName, TaskInstance],
  runLoop: Runnable,
  systemAdmins: SystemAdmins,
  consumerMultiplexer: SystemConsumers,
  producerMultiplexer: SystemProducers,
  metrics: SamzaContainerMetrics,
  diskSpaceMonitor: DiskSpaceMonitor = null,
  hostStatisticsMonitor: SystemStatisticsMonitor = null,
  offsetManager: OffsetManager = new OffsetManager,
  localityManager: LocalityManager = null,
  securityManager: SecurityManager = null,
  reporters: Map[String, MetricsReporter] = Map(),
  jvm: JvmMetrics = null,
  taskThreadPool: ExecutorService = null,
  timerExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor) extends Runnable with Logging {

  val shutdownMs = containerContext.config.getShutdownMs.getOrElse(TaskConfigJava.DEFAULT_TASK_SHUTDOWN_MS)
  var shutdownHookThread: Thread = null
  var jmxServer: JmxServer = null
  val isAutoCommitEnabled = containerContext.config.isAutoCommitEnabled

  @volatile private var status = SamzaContainerStatus.NOT_STARTED
  private var exceptionSeen: Throwable = null
  private var containerListener: SamzaContainerListener = null

  def getStatus(): SamzaContainerStatus = status

  def getTaskInstances() = taskInstances

  def setContainerListener(listener: SamzaContainerListener): Unit = {
    containerListener = listener
  }

  def hasStopped(): Boolean = status == SamzaContainerStatus.STOPPED || status == SamzaContainerStatus.FAILED

  def run {
    try {
      info("Starting container.")

      if (containerListener != null) {
        containerListener.beforeStart()
      }

      val startTime = System.nanoTime()
      status = SamzaContainerStatus.STARTING

      jmxServer = new JmxServer()

      startMetrics
      startDiagnostics
      startAdmins
      startOffsetManager
      storeContainerLocality
      startStores
      startTableManager
      startDiskSpaceMonitor
      startHostStatisticsMonitor
      startProducers
      startTask
      startConsumers
      startSecurityManger

      addShutdownHook
      info("Entering run loop.")
      status = SamzaContainerStatus.STARTED
      if (containerListener != null) {
        containerListener.afterStart()
      }
      metrics.containerStartupTime.update(System.nanoTime() - startTime)
      runLoop.run
    } catch {
      case e: Throwable =>
        if (status.equals(SamzaContainerStatus.STARTED)) {
          error("Caught exception/error in run loop.", e)
        } else {
          error("Caught exception/error while initializing container.", e)
        }
        status = SamzaContainerStatus.FAILED
        exceptionSeen = e
    }

    try {
      info("Shutting down SamzaContainer.")
      removeShutdownHook

      jmxServer.stop

      shutdownConsumers
      shutdownTask
      shutdownTableManager
      shutdownStores
      shutdownDiskSpaceMonitor
      shutdownHostStatisticsMonitor
      shutdownProducers
      shutdownOffsetManager
      shutdownMetrics
      shutdownSecurityManger
      shutdownAdmins

      if (!status.equals(SamzaContainerStatus.FAILED)) {
        status = SamzaContainerStatus.STOPPED
      }

      info("Shutdown complete.")
    } catch {
      case e: Throwable =>
        error("Caught exception/error while shutting down container.", e)
        if (exceptionSeen == null) {
          exceptionSeen = e
        }
        status = SamzaContainerStatus.FAILED
    }

    status match {
      case SamzaContainerStatus.STOPPED =>
        if (containerListener != null) {
          containerListener.afterStop()
        }
      case SamzaContainerStatus.FAILED =>
        if (containerListener != null) {
          containerListener.afterFailure(exceptionSeen)
        }
    }
  }

  /**
   * <p>
   *   Asynchronously shuts down this [[SamzaContainer]]
   * </p>
   * <br>
   * <b>Implementation</b>: Stops the [[RunLoop]], which will eventually transition the container from
   * [[SamzaContainerStatus.STARTED]] to either [[SamzaContainerStatus.STOPPED]] or [[SamzaContainerStatus.FAILED]]].
   * Based on the final `status`, [[SamzaContainerListener#afterStop()]] or
    * [[SamzaContainerListener#afterFailure(Throwable]] will be invoked respectively.
   *
   * @throws SamzaException, Thrown when the container has already been stopped or failed
   */
  def shutdown(): Unit = {
    if (status == SamzaContainerStatus.STOPPED || status == SamzaContainerStatus.FAILED) {
      throw new IllegalContainerStateException("Cannot shutdown a container with status " + status)
    }
    shutdownRunLoop()
  }

  // Shutdown Runloop
  def shutdownRunLoop() = {
    runLoop match {
      case runLoop: RunLoop => runLoop.shutdown
      case asyncRunLoop: AsyncRunLoop => asyncRunLoop.shutdown()
    }
  }

  def startDiskSpaceMonitor: Unit = {
    if (diskSpaceMonitor != null) {
      info("Starting disk space monitor")
      diskSpaceMonitor.start()
    }
  }

  def startHostStatisticsMonitor: Unit = {
    if (hostStatisticsMonitor != null) {
      info("Starting host statistics monitor")
      hostStatisticsMonitor.start()
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

  def startDiagnostics {
    if (containerContext.config.getDiagnosticsEnabled) {
      info("Starting diagnostics.")

      try {
        val diagnosticsAppender = Class.forName(containerContext.config.getDiagnosticsAppenderClass).
          getDeclaredConstructor(classOf[SamzaContainerMetrics]).newInstance(this.metrics);
      }
      catch {
        case e@(_: ClassNotFoundException | _: InstantiationException | _: InvocationTargetException) => {
          error("Failed to instantiate diagnostic appender", e)
          throw new ConfigException("Failed to instantiate diagnostic appender class " +
            containerContext.config.getDiagnosticsAppenderClass, e)
        }
      }
    }
  }

  def startOffsetManager {
    info("Registering task instances with offsets.")

    taskInstances.values.foreach(_.registerOffsets)

    info("Starting offset manager.")

    offsetManager.start
  }

  def storeContainerLocality {
    val isHostAffinityEnabled: Boolean = new ClusterManagerConfig(containerContext.config).getHostAffinityEnabled
    if (isHostAffinityEnabled) {
      val localityManager: LocalityManager = new LocalityManager(containerContext.config, containerContext.metricsRegistry)
      val containerName = "SamzaContainer-" + String.valueOf(containerContext.id)
      info("Registering %s with metadata store" format containerName)
      try {
        val hostInet = Util.getLocalHost
        val jmxUrl = if (jmxServer != null) jmxServer.getJmxUrl else ""
        val jmxTunnelingUrl = if (jmxServer != null) jmxServer.getTunnelingJmxUrl else ""
        info("Writing container locality and JMX address to metadata store")
        localityManager.writeContainerToHostMapping(containerContext.id, hostInet.getHostName)
      } catch {
        case uhe: UnknownHostException =>
          warn("Received UnknownHostException when persisting locality info for container %s: " +
            "%s" format (containerContext.id, uhe.getMessage))  //No-op
        case unknownException: Throwable =>
          warn("Received an exception when persisting locality info for container %s: " +
            "%s" format (containerContext.id, unknownException.getMessage))
      } finally {
        info("Shutting down locality manager.")
        localityManager.close()
      }
    }
  }

  def startStores {
    taskInstances.values.foreach(taskInstance => {
      val startTime = System.currentTimeMillis()
      info("Starting stores in task instance %s" format taskInstance.taskName)
      taskInstance.startStores
      // Measuring the time to restore the stores
      val timeToRestore = System.currentTimeMillis() - startTime
      val taskGauge = metrics.taskStoreRestorationMetrics.asScala.getOrElse(taskInstance.taskName, null)
      if (taskGauge != null) {
        taskGauge.set(timeToRestore)
      }
    })
  }

  def startTableManager: Unit = {
    taskInstances.values.foreach(taskInstance => {
      info("Starting table manager in task instance %s" format taskInstance.taskName)
      taskInstance.startTableManager
    })
  }

  def startTask {
    info("Initializing stream tasks.")

    taskInstances.values.foreach(_.initTask)
  }

  def startAdmins {
    info("Starting admin multiplexer.")

    systemAdmins.start
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

  def startSecurityManger {
    if (securityManager != null) {
      info("Starting security manager.")

      securityManager.start
    }
  }

  def addShutdownHook {
    val runLoopThread = Thread.currentThread()
    shutdownHookThread = new Thread("Samza Container Shutdown Hook Thread") {
      override def run() = {
        info("Shutting down, will wait up to %s ms." format shutdownMs)
        shutdownRunLoop()  //TODO: Pull out shutdown hook to LocalContainerRunner or SP
        try {
          runLoopThread.join(shutdownMs)
        } catch {
          case e: Throwable => // Ignore to avoid deadlock with uncaughtExceptionHandler. See SAMZA-1220
            error("Did not shut down within %s ms, exiting." format shutdownMs, e)
        }
        if (!runLoopThread.isAlive) {
          info("Shutdown complete")
        } else {
          error("Did not shut down within %s ms, exiting." format shutdownMs)
          Util.logThreadDump("Thread dump from Samza Container Shutdown Hook.")
        }
      }
    }
    Runtime.getRuntime().addShutdownHook(shutdownHookThread)
  }

  def removeShutdownHook = {
    try {
      if (shutdownHookThread != null) {
        Runtime.getRuntime.removeShutdownHook(shutdownHookThread)
      }
    } catch {
      case e: IllegalStateException => {
        // Thrown when then JVM is already shutting down, so safe to ignore.
      }
    }
  }

  def shutdownConsumers {
    info("Shutting down consumer multiplexer.")

    consumerMultiplexer.stop
  }

  def shutdownAdmins {
    info("Shutting down admin multiplexer.")

    systemAdmins.stop
  }


  def shutdownProducers {
    info("Shutting down producer multiplexer.")

    producerMultiplexer.stop
  }

  def shutdownTask {
    info("Shutting down task instance stream tasks.")

    if (taskThreadPool != null) {
      info("Shutting down task thread pool")
      try {
        taskThreadPool.shutdown()
        if(taskThreadPool.awaitTermination(shutdownMs, TimeUnit.MILLISECONDS)) {
          taskThreadPool.shutdownNow()
        }
      } catch {
        case e: Exception => error(e.getMessage, e)
      }
    }

    if (timerExecutor != null) {
      info("Shutting down timer executor")
      try {
        timerExecutor.shutdown()
        if (timerExecutor.awaitTermination(shutdownMs, TimeUnit.MILLISECONDS)) {
          timerExecutor.shutdownNow()
        }
      } catch {
        case e: Exception => error("Ignoring exception shutting down timer executor", e)
      }
    }

    if (isAutoCommitEnabled) {
      info("Committing offsets for all task instances")
      taskInstances.values.foreach(_.commit)
    }

    taskInstances.values.foreach(_.shutdownTask)
  }

  def shutdownStores {
    info("Shutting down task instance stores.")

    taskInstances.values.foreach(_.shutdownStores)
  }

  def shutdownTableManager: Unit = {
    info("Shutting down task instance table manager.")

    taskInstances.values.foreach(_.shutdownTableManager)
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

  def shutdownSecurityManger: Unit = {
    if (securityManager != null) {
      info("Shutting down security manager.")

      securityManager.stop
    }
  }

  def shutdownDiskSpaceMonitor: Unit = {
    if (diskSpaceMonitor != null) {
      info("Shutting down disk space monitor.")
      diskSpaceMonitor.stop()
    }
  }

  def shutdownHostStatisticsMonitor: Unit = {
    if (hostStatisticsMonitor != null) {
      info("Shutting down host statistics monitor.")
      hostStatisticsMonitor.stop()
    }
  }
}

/**
 * Exception thrown when the SamzaContainer tries to transition to an illegal state.
 * {@link SamzaContainerStatus} has more details on the state transitions.
 *
 * @param s String, Message associated with the exception
 * @param t Throwable, Wrapped error/exception thrown, if any.
 */
class IllegalContainerStateException(s: String, t: Throwable) extends SamzaException(s, t) {
  def this(s: String) = this(s, null)
}
