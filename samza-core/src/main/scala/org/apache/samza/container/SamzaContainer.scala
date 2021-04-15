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
import java.net.{URL, UnknownHostException}
import java.nio.file.Path
import java.time.Duration
import java.util
import java.util.{Base64, Optional}
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}
import java.util.function.Consumer
import com.google.common.annotations.VisibleForTesting
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.samza.checkpoint.{CheckpointListener, OffsetManager, OffsetManagerMetrics}
import org.apache.samza.config.{StreamConfig, _}
import org.apache.samza.container.disk.DiskSpaceMonitor.Listener
import org.apache.samza.container.disk.{DiskQuotaPolicyFactory, DiskSpaceMonitor, NoThrottlingDiskQuotaPolicyFactory, PollingScanDiskSpaceMonitor}
import org.apache.samza.container.host.{StatisticsMonitorImpl, SystemMemoryStatistics, SystemStatisticsMonitor}
import org.apache.samza.context._
import org.apache.samza.diagnostics.DiagnosticsManager
import org.apache.samza.job.model.{ContainerModel, JobModel, TaskMode}
import org.apache.samza.metrics.{JmxServer, JvmMetrics, MetricsRegistry, MetricsRegistryMap, MetricsReporter}
import org.apache.samza.serializers._
import org.apache.samza.serializers.model.SamzaObjectMapper
import org.apache.samza.startpoint.StartpointManager
import org.apache.samza.storage._
import org.apache.samza.system._
import org.apache.samza.system.chooser.{DefaultChooser, MessageChooserFactory}
import org.apache.samza.table.TableManager
import org.apache.samza.task._
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals
import org.apache.samza.util.{Util, _}
import org.apache.samza.SamzaException
import org.apache.samza.clustermanager.StandbyTaskUtil

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

  /**
    * If a base-directory was NOT explicitly provided in config, a default base directory is returned.
    */
  def getNonLoggedStorageBaseDir(jobConfig: JobConfig, defaultStoreBaseDir: File) = {
    JavaOptionals.toRichOptional(jobConfig.getNonLoggedStorePath).toOption match {
      case Some(nonLoggedStorePath) =>
        new File(nonLoggedStorePath)
      case None =>
        defaultStoreBaseDir
    }
  }

  /**
    * If a base-directory was NOT explicitly provided in config or via an environment variable
    * (see ShellCommandConfig.ENV_LOGGED_STORE_BASE_DIR), then a default base directory is returned.
    */
  def getLoggedStorageBaseDir(jobConfig: JobConfig, defaultStoreBaseDir: File) = {
    val defaultLoggedStorageBaseDir = JavaOptionals.toRichOptional(jobConfig.getLoggedStorePath).toOption match {
      case Some(durableStorePath) =>
        new File(durableStorePath)
      case None =>
        defaultStoreBaseDir
    }

    var loggedStorageBaseDir:File = null
    if (System.getenv(ShellCommandConfig.ENV_LOGGED_STORE_BASE_DIR) != null) {
      val jobNameAndId = (
        JavaOptionals.toRichOptional(jobConfig.getName).toOption
          .getOrElse(throw new ConfigException("Missing required config: job.name")),
        jobConfig.getJobId
      )

      loggedStorageBaseDir = new File(System.getenv(ShellCommandConfig.ENV_LOGGED_STORE_BASE_DIR)
        + File.separator + jobNameAndId._1 + "-" + jobNameAndId._2)
    } else {
      if (!jobConfig.getLoggedStorePath.isPresent) {
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
    customReporters: Map[String, MetricsReporter] = Map[String, MetricsReporter](),
    taskFactory: TaskFactory[_],
    jobContext: JobContext,
    applicationContainerContextFactoryOption: Option[ApplicationContainerContextFactory[ApplicationContainerContext]],
    applicationTaskContextFactoryOption: Option[ApplicationTaskContextFactory[ApplicationTaskContext]],
    externalContextOption: Option[ExternalContext],
    localityManager: LocalityManager = null,
    startpointManager: StartpointManager = null,
    diagnosticsManager: Option[DiagnosticsManager] = Option.empty) = {
    val config = if (StandbyTaskUtil.isStandbyContainer(containerId)) {
      // standby containers will need to continually poll checkpoint messages
      val newConfig = new util.HashMap[String, String](jobContext.getConfig)
      newConfig.put(TaskConfig.INTERNAL_CHECKPOINT_MANAGER_CONSUMER_STOP_AFTER_FIRST_READ, java.lang.Boolean.FALSE.toString)
      new MapConfig(newConfig)
    } else {
      jobContext.getConfig
    }
    val jobConfig = new JobConfig(config)
    val systemConfig = new SystemConfig(config)
    val containerModel = jobModel.getContainers.get(containerId)
    val containerName = "samza-container-%s" format containerId

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
    val metricsConfig = new MetricsConfig(config)
    val clock = if (metricsConfig.getMetricsTimerEnabled) {
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

    val storageConfig = new StorageConfig(config)
    val sideInputStoresToSystemStreams = storageConfig.getStoreNames.asScala
      .map { storeName => (storeName, storageConfig.getSideInputs(storeName).asScala) }
      .filter { case (storeName, sideInputs) => sideInputs.nonEmpty }
      .map { case (storeName, sideInputs) => (storeName, sideInputs.map(StreamUtil.getSystemStreamFromNameOrId(config, _))) }
      .toMap

    val sideInputSystemStreams = sideInputStoresToSystemStreams.values.flatMap(sideInputs => sideInputs.toStream).toSet

    info("Got side input store system streams: %s" format sideInputSystemStreams)

    val inputSystemStreams = inputSystemStreamPartitions
      .map(_.getSystemStream)
      .toSet.diff(sideInputSystemStreams)

    val inputSystems = inputSystemStreams
      .map(_.getSystem)
      .toSet

    val systemNames = systemConfig.getSystemNames.asScala

    info("Got system names: %s" format systemNames)

    val streamConfig = new StreamConfig(config)
    val serdeStreams = systemNames.foldLeft(Set[SystemStream]())(_ ++ streamConfig.getSerdeStreams(_).asScala)

    info("Got serde streams: %s" format serdeStreams)

    val systemFactories = systemNames.map(systemName => {
      val systemFactoryClassName = JavaOptionals.toRichOptional(systemConfig.getSystemFactory(systemName)).toOption
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      (systemName, ReflectionUtil.getObj(systemFactoryClassName, classOf[SystemFactory]))
    }).toMap
    info("Got system factories: %s" format systemFactories.keys)

    val systemAdmins = new SystemAdmins(config, this.getClass.getSimpleName)
    info("Got system admins: %s" format systemAdmins.getSystemNames)

    val streamMetadataCache = new StreamMetadataCache(systemAdmins)
    val inputStreamMetadata = streamMetadataCache.getStreamMetadata(inputSystemStreams)

    info("Got input stream metadata: %s" format inputStreamMetadata)

    val consumers = inputSystems
      .map(systemName => {
        val systemFactory = systemFactories(systemName)

        try {
          (systemName, systemFactory.getConsumer(systemName, config, samzaContainerMetrics.registry, this.getClass.getSimpleName))
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
            (systemName, systemFactory.getProducer(systemName, config, samzaContainerMetrics.registry, this.getClass.getSimpleName))
          } catch {
            case e: Exception =>
              error("Failed to create a producer for %s, so skipping." format systemName, e)
              (systemName, null)
          }
      }
      .filter(_._2 != null)

    info("Got system producers: %s" format producers.keys)

    val serializerConfig = new SerializerConfig(config)
    val serdesFromFactories = serializerConfig.getSerdeNames.asScala.map(serdeName => {
      val serdeClassName = JavaOptionals.toRichOptional(serializerConfig.getSerdeFactoryClass(serdeName)).toOption
        .getOrElse(SerializerConfig.getPredefinedSerdeFactoryName(serdeName))
      val serde = ReflectionUtil.getObj(serdeClassName, classOf[SerdeFactory[Object]])
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
    val buildSystemStreamSerdeMap = (getSerdeName: (SystemStream) => Optional[String]) => {
      (serdeStreams ++ inputSystemStreamPartitions)
        .filter(systemStream => getSerdeName(systemStream).isPresent)
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

    val systemKeySerdes = buildSystemSerdeMap(systemName =>
      JavaOptionals.toRichOptional(systemConfig.getSystemKeySerde(systemName)).toOption)

    debug("Got system key serdes: %s" format systemKeySerdes)

    val systemMessageSerdes = buildSystemSerdeMap(systemName =>
      JavaOptionals.toRichOptional(systemConfig.getSystemMsgSerde(systemName)).toOption)

    debug("Got system message serdes: %s" format systemMessageSerdes)

    val systemStreamKeySerdes = buildSystemStreamSerdeMap(systemStream => streamConfig.getStreamKeySerde(systemStream))

    debug("Got system stream key serdes: %s" format systemStreamKeySerdes)

    val systemStreamMessageSerdes = buildSystemStreamSerdeMap(systemStream => streamConfig.getStreamMsgSerde(systemStream))

    debug("Got system stream message serdes: %s" format systemStreamMessageSerdes)

    val storeChangelogs = storageConfig.getStoreChangelogs

    info("Got change log system streams: %s" format storeChangelogs)

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
      getChangelogSSPsForContainer(containerModel, storeChangelogs).asJava)

    val intermediateStreams = streamConfig
      .getStreamIds()
      .asScala
      .filter((streamId:String) => streamConfig.getIsIntermediateStream(streamId))
      .toList

    info("Got intermediate streams: %s" format intermediateStreams)

    val controlMessageKeySerdes = intermediateStreams
      .flatMap(streamId => {
        val systemStream = streamConfig.streamIdToSystemStream(streamId)
        systemStreamKeySerdes.get(systemStream)
                .orElse(systemKeySerdes.get(systemStream.getSystem))
                .map(serde => (systemStream, new StringSerde("UTF-8")))
      }).toMap

    val intermediateStreamMessageSerdes = intermediateStreams
      .flatMap(streamId => {
        val systemStream = streamConfig.streamIdToSystemStream(streamId)
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
      changeLogSystemStreams = storeChangelogs.asScala.values.toSet,
      controlMessageKeySerdes = controlMessageKeySerdes,
      intermediateMessageSerdes = intermediateStreamMessageSerdes)

    info("Setting up JVM metrics.")

    val jvm = new JvmMetrics(samzaContainerMetrics.registry)

    info("Setting up message chooser.")

    val taskConfig = new TaskConfig(config)
    val chooserFactoryClassName = taskConfig.getMessageChooserClass

    val chooserFactory = ReflectionUtil.getObj(chooserFactoryClassName, classOf[MessageChooserFactory])

    val chooser = DefaultChooser(inputStreamMetadata, chooserFactory, config, samzaContainerMetrics.registry, systemAdmins)

    info("Setting up metrics reporters.")

    val reporters =
      MetricsReporterLoader.getMetricsReporters(metricsConfig, containerName).asScala.toMap ++ customReporters

    info("Got metrics reporters: %s" format reporters.keys)

    val securityManager = JavaOptionals.toRichOptional(jobConfig.getSecurityManagerFactory).toOption match {
      case Some(securityManagerFactoryClassName) =>
        ReflectionUtil.getObj(securityManagerFactoryClassName, classOf[SecurityManagerFactory])
          .getSecurityManager(config)
      case _ => null
    }
    info("Got security manager: %s" format securityManager)

    val checkpointManager = taskConfig.getCheckpointManager(samzaContainerMetrics.registry).orElse(null)
    info("Got checkpoint manager: %s" format checkpointManager)

    // create a map of consumers with callbacks to pass to the OffsetManager
    val checkpointListeners = consumers.filter(_._2.isInstanceOf[CheckpointListener])
      .map { case (system, consumer) => (system, consumer.asInstanceOf[CheckpointListener])}
    info("Got checkpointListeners : %s" format checkpointListeners)

    val offsetManager = OffsetManager(inputStreamMetadata, config, checkpointManager, startpointManager, systemAdmins, checkpointListeners, offsetManagerMetrics)
    info("Got offset manager: %s" format offsetManager)

    val dropDeserializationError = taskConfig.getDropDeserializationErrors
    val dropSerializationError = taskConfig.getDropSerializationErrors

    val pollIntervalMs = taskConfig.getPollIntervalMs

    val consumerMultiplexer = new SystemConsumers(
      chooser = chooser,
      consumers = consumers,
      systemAdmins = systemAdmins,
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

    val storageEngineFactories = storageConfig
      .getStoreNames.asScala
      .map(storeName => {
        val storageFactoryClassName =
          JavaOptionals.toRichOptional(storageConfig.getStorageFactoryClassName(storeName)).toOption
          .getOrElse(throw new SamzaException("Missing storage factory for %s." format storeName))
        (storeName,
          ReflectionUtil.getObj(storageFactoryClassName, classOf[StorageEngineFactory[Object, Object]]))
      }).toMap

    info("Got storage engines: %s" format storageEngineFactories.keys)

    val threadPoolSize = jobConfig.getThreadPoolSize
    info("Got thread pool size: " + threadPoolSize)

    val taskThreadPool = if (threadPoolSize > 0) {
      Executors.newFixedThreadPool(threadPoolSize,
        new ThreadFactoryBuilder().setNameFormat("Samza Container Thread-%d").build())
    } else {
      null
    }

    val finalTaskFactory = TaskFactoryUtil.finalizeTaskFactory(
      taskFactory,
      taskThreadPool)

    // executor for performing async commit operations for a task.
    // TODO BLOCKER pmaheshw tune thread pool sizing.
    val commitThreadPoolSize = Math.min(containerModel.getTasks.size() * 2, taskConfig.getCommitMaxThreadPoolSize)
    val commitThreadPool = Executors.newFixedThreadPool(commitThreadPoolSize,
      new ThreadFactoryBuilder().setNameFormat("Samza Task Commit Thread-%d").setDaemon(true).build())

    val taskModels = containerModel.getTasks.values.asScala
    val containerContext = new ContainerContextImpl(containerModel, samzaContainerMetrics.registry)
    val applicationContainerContextOption = applicationContainerContextFactoryOption
      .map(_.create(externalContextOption.orNull, jobContext, containerContext))

    val storeWatchPaths = new util.HashSet[Path]()

    val timerExecutor = Executors.newSingleThreadScheduledExecutor

    val taskInstanceMetrics: Map[TaskName, TaskInstanceMetrics] = taskModels.map(taskModel => {
      (taskModel.getTaskName, new TaskInstanceMetrics("TaskName-%s" format taskModel.getTaskName))
    }).toMap

    val taskCollectors : Map[TaskName, TaskInstanceCollector] = taskModels.map(taskModel => {
      (taskModel.getTaskName, new TaskInstanceCollector(producerMultiplexer, taskInstanceMetrics.get(taskModel.getTaskName).get))
    }).toMap

    val defaultStoreBaseDir = new File(System.getProperty("user.dir"), "state")
    info("Got default storage engine base directory: %s" format defaultStoreBaseDir)

    val nonLoggedStorageBaseDir = getNonLoggedStorageBaseDir(jobConfig, defaultStoreBaseDir)
    info("Got base directory for non logged data stores: %s" format nonLoggedStorageBaseDir)

    val loggedStorageBaseDir = getLoggedStorageBaseDir(jobConfig, defaultStoreBaseDir)
    info("Got base directory for logged data stores: %s" format loggedStorageBaseDir)

    // TODO dchen should we enforce restore factories to be subset of backup factories?
    val stateStorageBackendRestoreFactory = ReflectionUtil
      .getObj(storageConfig.getStateBackendRestoreFactory(), classOf[StateBackendFactory])

    val containerStorageManager = new ContainerStorageManager(
      checkpointManager,
      containerModel,
      streamMetadataCache,
      systemAdmins,
      storeChangelogs,
      sideInputStoresToSystemStreams.mapValues(systemStreamSet => systemStreamSet.toSet.asJava).asJava,
      storageEngineFactories.asJava,
      systemFactories.asJava,
      serdes.asJava,
      config,
      taskInstanceMetrics.asJava,
      samzaContainerMetrics,
      jobContext,
      containerContext,
      stateStorageBackendRestoreFactory,
      taskCollectors.asJava,
      loggedStorageBaseDir,
      nonLoggedStorageBaseDir,
      serdeManager,
      new SystemClock)

    storeWatchPaths.addAll(containerStorageManager.getStoreDirectoryPaths)

    val stateStorageBackendBackupFactories = storageConfig.getStateBackendBackupFactories.asScala.map(
      ReflectionUtil.getObj(_, classOf[StateBackendFactory])
    )

    // Create taskInstances
    val taskInstances: Map[TaskName, TaskInstance] = taskModels
      .filter(taskModel => taskModel.getTaskMode.eq(TaskMode.Active)).map(taskModel => {
      debug("Setting up task instance: %s" format taskModel)

      val taskName = taskModel.getTaskName

      val task = finalTaskFactory match {
        case tf: AsyncStreamTaskFactory => tf.asInstanceOf[AsyncStreamTaskFactory].createInstance()
        case tf: StreamTaskFactory => tf.asInstanceOf[StreamTaskFactory].createInstance()
      }

      val taskSSPs = taskModel.getSystemStreamPartitions.asScala.toSet
      info("Got task SSPs: %s" format taskSSPs)

      val sideInputStoresToSSPs = sideInputStoresToSystemStreams.mapValues(sideInputSystemStreams =>
        taskSSPs.filter(ssp => sideInputSystemStreams.contains(ssp.getSystemStream)).asJava)

      val taskSideInputSSPs = sideInputStoresToSSPs.values.flatMap(_.asScala).toSet
      info ("Got task side input SSPs: %s" format taskSideInputSSPs)

      val taskBackupManagerMap = new util.HashMap[String, TaskBackupManager]()
      stateStorageBackendBackupFactories.asJava.forEach(new Consumer[StateBackendFactory] {
        override def accept(factory: StateBackendFactory): Unit = {
          val taskMetricsRegistry =
            if (taskInstanceMetrics.contains(taskName) &&
              taskInstanceMetrics.get(taskName).isDefined) taskInstanceMetrics.get(taskName).get.registry
            else new MetricsRegistryMap
          val taskBackupManager = factory.getBackupManager(jobModel, containerModel,
            taskModel, commitThreadPool, taskMetricsRegistry, config, new SystemClock)
          taskBackupManagerMap.put(factory.getClass.getName, taskBackupManager)
        }
      })

      val commitManager = new TaskStorageCommitManager(taskName, taskBackupManagerMap,
        containerStorageManager, storeChangelogs, taskModel.getChangelogPartition, checkpointManager, config,
        commitThreadPool, new StorageManagerUtil, loggedStorageBaseDir)

      val tableManager = new TableManager(config)

      info("Got table manager")

      def createTaskInstance(task: Any): TaskInstance = new TaskInstance(
          task = task,
          taskModel = taskModel,
          metrics = taskInstanceMetrics.get(taskName).get,
          systemAdmins = systemAdmins,
          consumerMultiplexer = consumerMultiplexer,
          collector = taskCollectors.get(taskName).get,
          offsetManager = offsetManager,
          commitManager = commitManager,
          containerStorageManager = containerStorageManager,
          tableManager = tableManager,
          systemStreamPartitions = (taskSSPs -- taskSideInputSSPs).asJava,
          exceptionHandler = TaskInstanceExceptionHandler(taskInstanceMetrics.get(taskName).get, taskConfig),
          jobModel = jobModel,
          streamMetadataCache = streamMetadataCache,
          inputStreamMetadata = inputStreamMetadata,
          timerExecutor = timerExecutor,
          commitThreadPool = commitThreadPool,
          jobContext = jobContext,
          containerContext = containerContext,
          applicationContainerContextOption = applicationContainerContextOption,
          applicationTaskContextFactoryOption = applicationTaskContextFactoryOption,
          externalContextOption = externalContextOption)

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
      taskConfig,
      clock)

    val containerMemoryMb : Int = new ClusterManagerConfig(config).getContainerMemoryMb

    val hostStatisticsMonitor : SystemStatisticsMonitor = new StatisticsMonitorImpl()
    hostStatisticsMonitor.registerListener(new SystemStatisticsMonitor.Listener {
      override def onUpdate(sample: SystemMemoryStatistics): Unit = {
        val physicalMemoryBytes : Long = sample.getPhysicalMemoryBytes
        val physicalMemoryMb : Float = physicalMemoryBytes / (1024.0F * 1024.0F)
        val memoryUtilization : Float = physicalMemoryMb.toFloat / containerMemoryMb
        logger.debug("Container physical memory utilization (mb): " + physicalMemoryMb)
        logger.debug("Container physical memory utilization: " + memoryUtilization)
        samzaContainerMetrics.physicalMemoryMb.set(physicalMemoryMb)
        samzaContainerMetrics.physicalMemoryUtilization.set(memoryUtilization)

        var containerThreadPoolSize : Long = 0
        var containerActiveThreads : Long = 0
        if (taskThreadPool != null) {
          containerThreadPoolSize = taskThreadPool.asInstanceOf[ThreadPoolExecutor].getPoolSize
          containerActiveThreads = taskThreadPool.asInstanceOf[ThreadPoolExecutor].getActiveCount
        }
        samzaContainerMetrics.containerThreadPoolSize.set(containerThreadPoolSize)
        samzaContainerMetrics.containerActiveThreads.set(containerActiveThreads)
      }
    })

    val diskQuotaBytes = config.getLong("container.disk.quota.bytes", Long.MaxValue)
    samzaContainerMetrics.diskQuotaBytes.set(diskQuotaBytes)

    val diskQuotaPolicyFactoryString = config.get("container.disk.quota.policy.factory",
      classOf[NoThrottlingDiskQuotaPolicyFactory].getName)
    val diskQuotaPolicyFactory = ReflectionUtil.getObj(diskQuotaPolicyFactoryString, classOf[DiskQuotaPolicyFactory])
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
      config = config,
      taskInstances = taskInstances,
      taskInstanceMetrics = taskInstanceMetrics,
      runLoop = runLoop,
      systemAdmins = systemAdmins,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      localityManager = localityManager,
      offsetManager = offsetManager,
      securityManager = securityManager,
      metrics = samzaContainerMetrics,
      reporters = reporters,
      jvm = jvm,
      diskSpaceMonitor = diskSpaceMonitor,
      hostStatisticsMonitor = hostStatisticsMonitor,
      taskThreadPool = taskThreadPool,
      commitThreadPool = commitThreadPool,
      timerExecutor = timerExecutor,
      containerContext = containerContext,
      applicationContainerContextOption = applicationContainerContextOption,
      externalContextOption = externalContextOption,
      containerStorageManager = containerStorageManager,
      diagnosticsManager = diagnosticsManager)
  }

  /**
    * Builds the set of SSPs for all changelogs on this container.
    */
  @VisibleForTesting
  private[container] def getChangelogSSPsForContainer(containerModel: ContainerModel,
    changeLogSystemStreams: util.Map[String, SystemStream]): Set[SystemStreamPartition] = {
    containerModel.getTasks.values().asScala
      .map(taskModel => taskModel.getChangelogPartition)
      .flatMap(changelogPartition => changeLogSystemStreams.asScala.map { case (_, systemStream) =>
        new SystemStreamPartition(systemStream, changelogPartition) })
      .toSet
  }
}

class SamzaContainer(
  config: Config,
  taskInstances: Map[TaskName, TaskInstance],
  taskInstanceMetrics: Map[TaskName, TaskInstanceMetrics],
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
  commitThreadPool: ExecutorService = null,
  timerExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor,
  containerContext: ContainerContext,
  applicationContainerContextOption: Option[ApplicationContainerContext],
  externalContextOption: Option[ExternalContext],
  containerStorageManager: ContainerStorageManager,
  diagnosticsManager: Option[DiagnosticsManager] = Option.empty) extends Runnable with Logging {

  private val jobConfig = new JobConfig(config)
  private val taskConfig = new TaskConfig(config)
  val shutdownMs: Long = taskConfig.getShutdownMs
  var jmxServer: JmxServer = null

  @volatile private var status = SamzaContainerStatus.NOT_STARTED

  @volatile private var standbyContainerShutdownLatch = new CountDownLatch(1);

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
      if (jobConfig.getJMXEnabled) {
        jmxServer = new JmxServer()
      }
      applicationContainerContextOption.foreach(_.start)

      startMetrics
      startDiagnostics
      startAdmins
      startOffsetManager
      storeContainerLocality
      // TODO HIGH pmaheshw SAMZA-2338: since store restore needs to trim changelog messages,
      // need to start changelog producers before the stores, but stop them after stores.
      startProducers
      startStores
      startTableManager
      startDiskSpaceMonitor
      startHostStatisticsMonitor
      startTask
      startConsumers
      startSecurityManger

      info("Entering run loop.")
      status = SamzaContainerStatus.STARTED
      if (containerListener != null) {
        containerListener.afterStart()
      }
      metrics.containerStartupTime.set((System.nanoTime() - startTime)/1000000)
      if (taskInstances.size > 0)
        runLoop.run
      else
        standbyContainerShutdownLatch.await() // Standby containers do not spin runLoop, instead they wait on signal to invoke shutdown
    } catch {
      case e: InterruptedException =>
        /*
         * We don't want to categorize interrupts as failure since the only place the container thread gets interrupted within
         * our code inside stream processor is during the following two scenarios
         *    1. During a re-balance, if the container has not started or hasn't reported start status to StreamProcessor.
         *       Subsequently stream processor attempts to interrupt the container thread before proceeding to join the barrier
         *       to agree on the new work assignment.
         *    2. During shutdown signals to stream processor (external or internal), the stream processor signals the container to
         *       shutdown and waits for `task.shutdown.ms` before forcefully shutting down the container executor service which in
         *       turn interrupts the container thread.
         *
         * In the both of these scenarios, the failure cause is either captured externally (timing out scenario) or internally
         * (failed attempt to shut down the container). The act of interrupting the container thread is an explicit intent to shutdown
         * the container since it is not capable of reacting to shutdown signals in all scenarios.
         *
         */
        if (status.equals(SamzaContainerStatus.STARTED)) {
          warn("Received an interrupt in run loop.", e)
        } else {
          warn("Received an interrupt during initialization.", e)
        }
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
      if (jmxServer != null) {
        jmxServer.stop
      }

      shutdownConsumers
      shutdownTask
      shutdownTableManager
      shutdownStores
      shutdownDiskSpaceMonitor
      shutdownHostStatisticsMonitor
      shutdownProducers
      shutdownOffsetManager
      shutdownDiagnostics
      shutdownMetrics
      shutdownSecurityManger
      shutdownAdmins

      applicationContainerContextOption.foreach(_.stop)

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
    if (status == SamzaContainerStatus.FAILED || status == SamzaContainerStatus.STOPPED) {
      warn("Shutdown is no-op since the container is already in state: " + status)
      return
    }

    if (taskInstances.size > 0)
      shutdownRunLoop()
    else
      standbyContainerShutdownLatch.countDown // Countdown the latch so standby container can invoke a shutdown sequence
  }

  // Shutdown Runloop
  def shutdownRunLoop() = {
    runLoop.asInstanceOf[RunLoop].shutdown
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
    info("Registering task instance metrics.")
    reporters.values.foreach(reporter =>
      taskInstanceMetrics.values.foreach(taskMetrics => reporter.register(taskMetrics.source, taskMetrics.registry)))

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
    if (diagnosticsManager.isDefined) {
      info("Starting diagnostics manager.")
      diagnosticsManager.get.start()
    }
  }

  def startOffsetManager {
    info("Registering task instances with offsets.")

    taskInstances.values.foreach(_.registerOffsets)

    info("Starting offset manager.")

    offsetManager.start
  }

  def storeContainerLocality {
    val isHostAffinityEnabled: Boolean = new ClusterManagerConfig(config).getHostAffinityEnabled
    if (isHostAffinityEnabled && localityManager != null) {
      val containerId = containerContext.getContainerModel.getId
      val containerName = "SamzaContainer-" + containerId
      info("Registering %s with metadata store" format containerName)
      try {
        val hostInet = Util.getLocalHost
        info("Writing container locality to metadata store")
        localityManager.writeContainerToHostMapping(containerId, hostInet.getHostName)
      } catch {
        case uhe: UnknownHostException =>
          warn("Received UnknownHostException when persisting locality info for container: " +
            "%s" format (containerId), uhe)  //No-op
        case unknownException: Throwable =>
          warn("Received an exception when persisting locality info for container %s: " +
            "%s" format (containerId), unknownException)
      } finally {
        info("Shutting down locality manager.")
        localityManager.close()
      }
    }
  }

  def startStores {
    info("Starting container storage manager.")
    containerStorageManager.start()
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

    if (taskInstances.size > 0) {
      info("Starting consumer multiplexer.")
      consumerMultiplexer.start
    }
  }

  def startSecurityManger {
    if (securityManager != null) {
      info("Starting security manager.")

      securityManager.start
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

    if (commitThreadPool != null) {
      info("Shutting down task commit thread pool")
      try {
        commitThreadPool.shutdown()
        if(commitThreadPool.awaitTermination(shutdownMs, TimeUnit.MILLISECONDS)) {
          commitThreadPool.shutdownNow()
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

    taskInstances.values.foreach(_.shutdownTask)
  }

  def shutdownStores {
    info("Shutting down container storage manager.")
    containerStorageManager.shutdown()
  }

  def shutdownTableManager: Unit = {
    info("Shutting down task instance table manager.")

    taskInstances.values.foreach(_.shutdownTableManager)
  }

  def shutdownOffsetManager {
    info("Shutting down offset manager.")

    offsetManager.stop
  }

  def shutdownDiagnostics {
    if (diagnosticsManager.isDefined) {
      info("Shutting down diagnostics manager.")
      diagnosticsManager.get.stop()
    }
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