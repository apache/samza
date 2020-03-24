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
package org.apache.samza.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.clustermanager.StandbyTaskUtil;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.RunLoopTask;
import org.apache.samza.container.RunLoop;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.executors.KeyBasedExecutorService;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeManager;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.system.SystemConsumersMetrics;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.chooser.DefaultChooser;
import org.apache.samza.system.chooser.MessageChooser;
import org.apache.samza.system.chooser.RoundRobinChooserFactory;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.task.ReadableCoordinator;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCallbackFactory;
import org.apache.samza.task.TaskInstanceCollector;
import org.apache.samza.util.Clock;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.ScalaJavaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;


/**
 *  ContainerStorageManager is a per-container object that manages
 *  the restore of per-task partitions.
 *
 *  It is responsible for
 *  a) performing all container-level actions for restore such as, initializing and shutting down
 *  taskStorage managers, starting, registering and stopping consumers, etc.
 *
 *  b) performing individual task stores' restores in parallel.
 *
 *  and
 *  c) restoring sideInputs.
 *  It provides bootstrap semantics for sideinputs -- the main thread is blocked until
 *  all sideInputSSPs have not caught up. Side input store flushes are not in sync with task-commit, although
 *  they happen at the same frequency.
 *  In case, where a user explicitly requests a task-commit, it will not include committing sideInputs.
 */
public class ContainerStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerStorageManager.class);
  private static final String RESTORE_THREAD_NAME = "Samza Restore Thread-%d";
  private static final String SIDEINPUTS_READ_THREAD_NAME = "SideInputs Read Thread";
  private static final String SIDEINPUTS_FLUSH_THREAD_NAME = "SideInputs Flush Thread";
  private static final String SIDEINPUTS_METRICS_PREFIX = "side-inputs-";
  // We use a prefix to differentiate the SystemConsumersMetrics for sideInputs from the ones in SamzaContainer

  private static final int SIDE_INPUT_READ_THREAD_TIMEOUT_SECONDS = 10; // Timeout with which sideinput read thread checks for exceptions
  private static final Duration SIDE_INPUT_FLUSH_TIMEOUT = Duration.ofMinutes(1); // Period with which sideinputs are flushed


  /** Maps containing relevant per-task objects */
  private final Map<TaskName, Map<String, StorageEngine>> taskStores;
  private final Map<TaskName, TaskRestoreManager> taskRestoreManagers;
  private final Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics;
  private final Map<TaskName, TaskInstanceCollector> taskInstanceCollectors;

  private final Map<String, SystemConsumer> storeConsumers; // Mapping from store name to SystemConsumers
  private final Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories; // Map of storageEngineFactories indexed by store name
  private final Map<String, SystemStream> changelogSystemStreams; // Map of changelog system-streams indexed by store name
  private final Map<String, Serde<Object>> serdes; // Map of Serde objects indexed by serde name (specified in config)
  private final SystemAdmins systemAdmins;

  private final StreamMetadataCache streamMetadataCache;
  private final SSPMetadataCache sspMetadataCache;
  private final SamzaContainerMetrics samzaContainerMetrics;

  private final CheckpointManager checkpointManager;
  /* Parameters required to re-create taskStores post-restoration */
  private final ContainerModel containerModel;
  private final JobContext jobContext;
  private final ContainerContext containerContext;

  private final File loggedStoreBaseDirectory;
  private final File nonLoggedStoreBaseDirectory;
  private final Set<Path> storeDirectoryPaths; // the set of store directory paths, used by SamzaContainer to initialize its disk-space-monitor

  private final int parallelRestoreThreadPoolSize;
  private final int maxChangeLogStreamPartitions; // The partition count of each changelog-stream topic. This is used for validating changelog streams before restoring.

  /* Sideinput related parameters */
  // side inputs indexed first by task, then store name
  private final Map<TaskName, Map<String, Set<SystemStreamPartition>>> taskSideInputStoreSSPs;
  private final Map<SystemStreamPartition, Object> sideInputSSPLocks;
  private final Map<SystemStreamPartition, TaskSideInputStorageManager> sideInputStorageManagers; // Map of sideInput storageManagers indexed by ssp, for simpler lookup for process()
  private SystemConsumers sideInputSystemConsumers;
  private final Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> initialSideInputSSPMetadata
      = new ConcurrentHashMap<>(); // Recorded sspMetadata of the taskSideInputSSPs recorded at start, used to determine when sideInputs are caughtup and container init can proceed
  private volatile CountDownLatch sideInputsCaughtUp; // Used by the sideInput-read thread to signal to the main thread
  private volatile boolean shouldShutdown = false;
  private final Map<SystemStreamPartition, Optional<String>> checkpointedSideInputSSPOffsets;
  private RunLoop sideInputRunLoop;

  private final ExecutorService sideInputsReadExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat(SIDEINPUTS_READ_THREAD_NAME).build());

  private final ScheduledExecutorService sideInputsFlushExecutor = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat(SIDEINPUTS_FLUSH_THREAD_NAME).build());
  private ScheduledFuture sideInputsFlushFuture;
  private volatile Throwable sideInputException = null;

  private final Config config;
  private final StorageManagerUtil storageManagerUtil = new StorageManagerUtil();

  public ContainerStorageManager(
      CheckpointManager checkpointManager,
      ContainerModel containerModel,
      StreamMetadataCache streamMetadataCache,
      SSPMetadataCache sspMetadataCache,
      SystemAdmins systemAdmins,
      Map<String, SystemStream> changelogSystemStreams,
      Map<String, Set<SystemStream>> sideInputSystemStreams,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, SystemFactory> systemFactories,
      Map<String, Serde<Object>> serdes,
      Config config,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      SamzaContainerMetrics samzaContainerMetrics,
      JobContext jobContext,
      ContainerContext containerContext,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors,
      File loggedStoreBaseDirectory,
      File nonLoggedStoreBaseDirectory,
      int maxChangeLogStreamPartitions,
      SerdeManager serdeManager,
      Clock clock) {
    this.checkpointManager = checkpointManager;
    this.containerModel = containerModel;
    this.taskSideInputStoreSSPs = getTaskSideInputSSPs(containerModel, sideInputSystemStreams, changelogSystemStreams);
    this.sspMetadataCache = sspMetadataCache;
    this.changelogSystemStreams = getChangelogSystemStreams(containerModel, changelogSystemStreams);
    this.checkpointedSideInputSSPOffsets = new ConcurrentHashMap<>();

    Set<SystemStream> allSideInputSystemStreams = this.taskSideInputStoreSSPs.values().stream()
        .flatMap(map -> map.values().stream())
        .flatMap(Set::stream)
        .map(SystemStreamPartition::getSystemStream)
        .collect(Collectors.toSet());

    LOG.info("Starting with changelogSystemStreams = {} sideInputSystemStreams = {}", this.changelogSystemStreams, allSideInputSystemStreams);

    this.storageEngineFactories = storageEngineFactories;
    this.serdes = serdes;
    this.loggedStoreBaseDirectory = loggedStoreBaseDirectory;
    this.nonLoggedStoreBaseDirectory = nonLoggedStoreBaseDirectory;

    if (loggedStoreBaseDirectory != null && loggedStoreBaseDirectory.equals(nonLoggedStoreBaseDirectory)) {
      LOG.warn("Logged and non-logged store base directory are configured to same path: {}. It is recommended to configure"
          + "them separately to ensure clean up of non-logged store data doesn't accidentally impact logged store data.",
          loggedStoreBaseDirectory);
    }

    // set the config
    this.config = config;

    this.taskInstanceMetrics = taskInstanceMetrics;

    // Setting the metrics registry
    this.samzaContainerMetrics = samzaContainerMetrics;

    this.jobContext = jobContext;
    this.containerContext = containerContext;

    this.taskInstanceCollectors = taskInstanceCollectors;

    // initializing the set of store directory paths
    this.storeDirectoryPaths = new HashSet<>();

    // Setting the restore thread pool size equal to the number of taskInstances
    this.parallelRestoreThreadPoolSize = containerModel.getTasks().size();

    this.maxChangeLogStreamPartitions = maxChangeLogStreamPartitions;
    this.streamMetadataCache = streamMetadataCache;
    this.systemAdmins = systemAdmins;

    // create taskStores for all tasks in the containerModel and each store in storageEngineFactories
    this.taskStores = createTaskStores(containerModel, jobContext, containerContext, storageEngineFactories, serdes, taskInstanceMetrics, taskInstanceCollectors);

    // create system consumers (1 per store system in changelogSystemStreams), and index it by storeName
    Map<String, SystemConsumer> storeSystemConsumers = createConsumers(this.changelogSystemStreams.values().stream().map(SystemStream::getSystem).collect(
        Collectors.toSet()),
        systemFactories, config, this.samzaContainerMetrics.registry());

    this.storeConsumers = createStoreIndexedMap(this.changelogSystemStreams, storeSystemConsumers);

    // creating task restore managers
    this.taskRestoreManagers = createTaskRestoreManagers(systemAdmins, clock, this.samzaContainerMetrics);

    // create sideInput storage managers
    sideInputStorageManagers = createSideInputStorageManagers(clock);

    Set<String> sideInputSystems = this.taskSideInputStoreSSPs.values().stream()
        .flatMap(map -> map.values().stream())
        .flatMap(Set::stream)
        .map(SystemStreamPartition::getSystem)
        .collect(Collectors.toSet());

    this.sideInputSSPLocks = this.taskSideInputStoreSSPs.values().stream()
        .flatMap(map -> map.values().stream())
        .flatMap(Collection::stream)
        .distinct()
        .collect(Collectors.toMap(
            Function.identity(),
            ssp -> new Object()
        ));

    // create SystemConsumers for consuming from taskSideInputSSPs, if sideInputs are being used
    if (sideInputsPresent()) {

      // create sideInput consumers indexed by systemName
      // Mapping from storeSystemNames to SystemConsumers
      Map<String, SystemConsumer> sideInputConsumers =
          createConsumers(sideInputSystems, systemFactories, config, this.samzaContainerMetrics.registry());

      scala.collection.immutable.Map<SystemStream, SystemStreamMetadata> inputStreamMetadata = streamMetadataCache.getStreamMetadata(
          JavaConversions.asScalaSet(sideInputSystems).toSet(),
          false);

      SystemConsumersMetrics sideInputSystemConsumersMetrics = new SystemConsumersMetrics(samzaContainerMetrics.registry(), SIDEINPUTS_METRICS_PREFIX);

      // we use the same registry as samza-container-metrics
      MessageChooser chooser = DefaultChooser.apply(inputStreamMetadata, new RoundRobinChooserFactory(), config,
          sideInputSystemConsumersMetrics.registry(), systemAdmins);

      sideInputSystemConsumers =
          new SystemConsumers(chooser, ScalaJavaUtil.toScalaMap(sideInputConsumers), systemAdmins, serdeManager,
              sideInputSystemConsumersMetrics, SystemConsumers.DEFAULT_NO_NEW_MESSAGES_TIMEOUT(), SystemConsumers.DEFAULT_DROP_SERIALIZATION_ERROR(),
              TaskConfig.DEFAULT_POLL_INTERVAL_MS, ScalaJavaUtil.toScalaFunction(() -> System.nanoTime()));
    }

  }

  /**
   * Add all sideInputs to a map of maps, indexed first by taskName, then by sideInput store name. For each Standby
   * task, we add its changelog partition
   *
   * @param containerModel the containerModel to use
   * @param sideInputSystemStreams the map of store to sideInput system stream
   * @return taskSideInputSSPs map
   */
  private Map<TaskName, Map<String, Set<SystemStreamPartition>>> getTaskSideInputSSPs(ContainerModel containerModel,
      Map<String, Set<SystemStream>> sideInputSystemStreams, Map<String, SystemStream> changelogSystemStreams) {
    Map<TaskName, Map<String, Set<SystemStreamPartition>>> taskSideInputSSPs = new HashMap<>();

    containerModel.getTasks().forEach((taskName, taskModel) -> {
        taskSideInputSSPs.putIfAbsent(taskName, new HashMap<>());
        sideInputSystemStreams.keySet().forEach(storeName -> {
            Set<SystemStreamPartition> taskSideInputs = taskModel.getSystemStreamPartitions().stream()
                .filter(ssp -> sideInputSystemStreams.get(storeName).contains(ssp.getSystemStream()))
                .collect(Collectors.toSet());
            taskSideInputSSPs.get(taskName).put(storeName, taskSideInputs);
          });
      });

    // for standby tasks, assign changelog ssps as side inputs
    getTasks(containerModel, TaskMode.Standby).forEach((taskName, taskModel) -> {
        changelogSystemStreams.forEach((storeName, systemStream) -> {
            SystemStreamPartition ssp = new SystemStreamPartition(systemStream, taskModel.getChangelogPartition());
            taskSideInputSSPs.putIfAbsent(taskName, new HashMap<>());
            taskSideInputSSPs.get(taskName).put(storeName, Collections.singleton(ssp));
            this.checkpointedSideInputSSPOffsets.put(ssp, Optional.empty());
          });
      });

    return taskSideInputSSPs;
  }

  /**
   * Computes the changelog system streams across all active tasks.
   *
   * @param containerModel the container's model
   * @param changelogSystemStreams the passed in set of changelogSystemStreams
   * @return A map of changeLogSSP to storeName across active tasks, assuming no two stores have the same changelogSSP
   */
  private Map<String, SystemStream> getChangelogSystemStreams(ContainerModel containerModel, Map<String, SystemStream> changelogSystemStreams) {

    if (MapUtils.invertMap(changelogSystemStreams).size() != changelogSystemStreams.size()) {
      throw new SamzaException("Two stores cannot have the same changelog system-stream");
    }

    Map<SystemStreamPartition, String> changelogSSPToStore = new HashMap<>();

    // changelogSystemStreams correspond only to active tasks (since those of standby-tasks moved to sideInputs)
    changelogSystemStreams.forEach((storeName, systemStream) ->
        getTasks(containerModel, TaskMode.Active).forEach((taskName, taskModel) -> {
            changelogSSPToStore.put(new SystemStreamPartition(systemStream, taskModel.getChangelogPartition()), storeName);
          })
    );

    return MapUtils.invertMap(changelogSSPToStore).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getSystemStream()));
  }


  /**
   *  Creates SystemConsumer objects for store restoration, creating one consumer per system.
   */
  private Map<String, SystemConsumer> createConsumers(Set<String> storeSystems,
      Map<String, SystemFactory> systemFactories, Config config, MetricsRegistry registry) {
    // Create one consumer for each system in use, map with one entry for each such system
    Map<String, SystemConsumer> storeSystemConsumers = new HashMap<>();

    // Iterate over the list of storeSystems and create one sysConsumer per system
    for (String storeSystemName : storeSystems) {
      SystemFactory systemFactory = systemFactories.get(storeSystemName);
      if (systemFactory == null) {
        throw new SamzaException("Changelog system " + storeSystemName + " does not exist in config");
      }
      storeSystemConsumers.put(storeSystemName,
          systemFactory.getConsumer(storeSystemName, config, registry));
    }

    return storeSystemConsumers;

  }

  private static Map<String, SystemConsumer> createStoreIndexedMap(Map<String, SystemStream> changelogSystemStreams,
      Map<String, SystemConsumer> storeSystemConsumers) {
    // Map of each storeName to its respective systemConsumer
    Map<String, SystemConsumer> storeConsumers = new HashMap<>();

    // Populate the map of storeName to its relevant systemConsumer
    for (String storeName : changelogSystemStreams.keySet()) {
      storeConsumers.put(storeName, storeSystemConsumers.get(changelogSystemStreams.get(storeName).getSystem()));
    }
    return storeConsumers;
  }

  private Map<TaskName, TaskRestoreManager> createTaskRestoreManagers(SystemAdmins systemAdmins, Clock clock, SamzaContainerMetrics samzaContainerMetrics) {
    Map<TaskName, TaskRestoreManager> taskRestoreManagers = new HashMap<>();
    containerModel.getTasks().forEach((taskName, taskModel) -> {
        taskRestoreManagers.put(taskName,
            TaskRestoreManagerFactory.create(
                taskModel, changelogSystemStreams, getNonSideInputStores(taskName), systemAdmins,
                streamMetadataCache, sspMetadataCache, storeConsumers, maxChangeLogStreamPartitions,
                loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory, config, clock));
        samzaContainerMetrics.addStoresRestorationGauge(taskName);
      });
    return taskRestoreManagers;
  }

  // Helper method to filter active Tasks from the container model
  private static Map<TaskName, TaskModel> getTasks(ContainerModel containerModel, TaskMode taskMode) {
    return containerModel.getTasks().entrySet().stream()
        .filter(x -> x.getValue().getTaskMode().equals(taskMode))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Create taskStores for all stores in storageEngineFactories.
   * The store mode is chosen as bulk-load if its a non-sideinput store, and readWrite if its a sideInput store
   */
  private Map<TaskName, Map<String, StorageEngine>> createTaskStores(ContainerModel containerModel, JobContext jobContext, ContainerContext containerContext,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories, Map<String, Serde<Object>> serdes,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors) {

    Map<TaskName, Map<String, StorageEngine>> taskStores = new HashMap<>();

    // iterate over each task in the containerModel, and each store in storageEngineFactories
    for (Map.Entry<TaskName, TaskModel> task : containerModel.getTasks().entrySet()) {
      TaskName taskName = task.getKey();
      TaskModel taskModel = task.getValue();

      if (!taskStores.containsKey(taskName)) {
        taskStores.put(taskName, new HashMap<>());
      }

      for (String storeName : storageEngineFactories.keySet()) {

        StorageEngineFactory.StoreMode storeMode = taskSideInputStoreSSPs.get(taskName).containsKey(storeName) ?
            StorageEngineFactory.StoreMode.ReadWrite : StorageEngineFactory.StoreMode.BulkLoad;

        StorageEngine storageEngine =
            createStore(storeName, taskName, taskModel, jobContext, containerContext, storageEngineFactories, serdes, taskInstanceMetrics, taskInstanceCollectors, storeMode);

        // add created store to map
        taskStores.get(taskName).put(storeName, storageEngine);

        LOG.info("Created store {} for task {} in mode {}", storeName, taskName, storeMode);
      }
    }

    return taskStores;
  }

  /**
   * Recreate all non-sideInput persistent stores in ReadWrite mode.
   *
   */
  private void recreatePersistentTaskStoresInReadWriteMode(ContainerModel containerModel, JobContext jobContext,
      ContainerContext containerContext, Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, Serde<Object>> serdes, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors) {

    // iterate over each task and each storeName
    for (Map.Entry<TaskName, TaskModel> task : containerModel.getTasks().entrySet()) {
      TaskName taskName = task.getKey();
      TaskModel taskModel = task.getValue();
      Map<String, StorageEngine> nonSideInputStores = getNonSideInputStores(taskName);

      for (String storeName : nonSideInputStores.keySet()) {

        // if this store has been already created then re-create and overwrite it only if it is a
        // persistentStore and a non-sideInputStore, because sideInputStores are always created in RW mode
        if (nonSideInputStores.get(storeName).getStoreProperties().isPersistedToDisk()) {

          StorageEngine storageEngine =
              createStore(storeName, taskName, taskModel, jobContext, containerContext, storageEngineFactories, serdes, taskInstanceMetrics, taskInstanceCollectors,
                  StorageEngineFactory.StoreMode.ReadWrite);

          // add created store to map
          this.taskStores.get(taskName).put(storeName, storageEngine);

          LOG.info("Re-created store {} in read-write mode for task {} because it a persistent store", storeName, taskName);
        } else {
          LOG.info("Skipping re-creation of store {} for task {}", storeName, taskName);
        }
      }
    }
  }

  /**
   * Method to instantiate a StorageEngine with the given parameters, and populate the storeDirectory paths (used to monitor
   * disk space).
   */
  private StorageEngine createStore(String storeName, TaskName taskName, TaskModel taskModel, JobContext jobContext,
      ContainerContext containerContext, Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, Serde<Object>> serdes, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors, StorageEngineFactory.StoreMode storeMode) {

    StorageConfig storageConfig = new StorageConfig(config);

    SystemStreamPartition changeLogSystemStreamPartition =
        (changelogSystemStreams.containsKey(storeName)) ? new SystemStreamPartition(
            changelogSystemStreams.get(storeName), taskModel.getChangelogPartition()) : null;

    // Use the logged-store-base-directory for change logged stores and sideInput stores, and non-logged-store-base-dir
    // for non logged stores
    File storeDirectory;
    if (changeLogSystemStreamPartition != null || taskSideInputStoreSSPs.get(taskName).containsKey(storeName)) {
      storeDirectory = storageManagerUtil.getTaskStoreDir(this.loggedStoreBaseDirectory, storeName, taskName,
          taskModel.getTaskMode());
    } else {
      storeDirectory = storageManagerUtil.getTaskStoreDir(this.nonLoggedStoreBaseDirectory, storeName, taskName,
          taskModel.getTaskMode());
    }

    this.storeDirectoryPaths.add(storeDirectory.toPath());

    Optional<String> storageKeySerde = storageConfig.getStorageKeySerde(storeName);
    if (!storageKeySerde.isPresent()) {
      throw new SamzaException("No key serde defined for store: " + storeName);
    }

    Serde keySerde = serdes.get(storageKeySerde.get());
    if (keySerde == null) {
      throw new SamzaException(
          "StorageKeySerde: No class defined for serde: " + storageKeySerde.get());
    }

    Optional<String> storageMsgSerde = storageConfig.getStorageMsgSerde(storeName);
    if (!storageMsgSerde.isPresent()) {
      throw new SamzaException("No msg serde defined for store: " + storeName);
    }

    Serde messageSerde = serdes.get(storageMsgSerde.get());
    if (messageSerde == null) {
      throw new SamzaException(
          "StorageMsgSerde: No class defined for serde: " + storageMsgSerde.get());
    }

    // if taskInstanceMetrics are specified use those for store metrics,
    // otherwise (in case of StorageRecovery) use a blank MetricsRegistryMap
    MetricsRegistry storeMetricsRegistry =
        taskInstanceMetrics.get(taskName) != null ? taskInstanceMetrics.get(taskName).registry()
            : new MetricsRegistryMap();

    return storageEngineFactories.get(storeName)
        .getStorageEngine(storeName, storeDirectory, keySerde, messageSerde, taskInstanceCollectors.get(taskName),
            storeMetricsRegistry, changeLogSystemStreamPartition, jobContext, containerContext, storeMode);
  }


  // Create sideInput store processors, one per store per task
  private Map<TaskName, Map<String, SideInputsProcessor>> createSideInputProcessors(StorageConfig config,
      ContainerModel containerModel, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics) {

    Map<TaskName, Map<String, SideInputsProcessor>> sideInputStoresToProcessors = new HashMap<>();
    containerModel.getTasks().forEach((taskName, taskModel) -> {
        sideInputStoresToProcessors.put(taskName, new HashMap<>());
        TaskMode taskMode = taskModel.getTaskMode();

        for (String storeName : this.taskSideInputStoreSSPs.get(taskName).keySet()) {

          SideInputsProcessor sideInputsProcessor;
          Optional<String> sideInputsProcessorSerializedInstance =
              config.getSideInputsProcessorSerializedInstance(storeName);

          if (sideInputsProcessorSerializedInstance.isPresent()) {

            sideInputsProcessor = SerdeUtils.deserialize("Side Inputs Processor", sideInputsProcessorSerializedInstance.get());
            LOG.info("Using serialized side-inputs-processor for store: {}, task: {}", storeName, taskName);

          } else if (config.getSideInputsProcessorFactory(storeName).isPresent()) {
            String sideInputsProcessorFactoryClassName = config.getSideInputsProcessorFactory(storeName).get();
            SideInputsProcessorFactory sideInputsProcessorFactory =
                ReflectionUtil.getObj(sideInputsProcessorFactoryClassName, SideInputsProcessorFactory.class);
            sideInputsProcessor = sideInputsProcessorFactory.getSideInputsProcessor(config, taskInstanceMetrics.get(taskName).registry());
            LOG.info("Using side-inputs-processor from factory: {} for store: {}, task: {}", config.getSideInputsProcessorFactory(storeName).get(), storeName, taskName);

          } else {
            // if this is a active-task with a side-input store but no sideinput-processor-factory defined in config, we rely on upstream validations to fail the deploy

            // if this is a standby-task and the store is a non-side-input changelog store
            // we creating identity sideInputProcessor for stores of standbyTasks
            // have to use the right serde because the sideInput stores are created

            Serde keySerde = serdes.get(config.getStorageKeySerde(storeName)
                .orElseThrow(() -> new SamzaException("Could not find storage key serde for store: " + storeName)));
            Serde msgSerde = serdes.get(config.getStorageMsgSerde(storeName)
                .orElseThrow(() -> new SamzaException("Could not find storage msg serde for store: " + storeName)));
            sideInputsProcessor = new SideInputsProcessor() {
              @Override
              public Collection<Entry<?, ?>> process(IncomingMessageEnvelope message, KeyValueStore store) {
                // Ignore message if the key is null
                if (message.getKey() == null) {
                  return ImmutableList.of();
                } else {
                  // Skip serde if the message is null
                  return ImmutableList.of(new Entry<>(keySerde.fromBytes((byte[]) message.getKey()),
                      message.getMessage() == null ? null : msgSerde.fromBytes((byte[]) message.getMessage())));
                }
              }
            };
            LOG.info("Using identity side-inputs-processor for store: {}, task: {}", storeName, taskName);
          }

          sideInputStoresToProcessors.get(taskName).put(storeName, sideInputsProcessor);
        }
      });

    return sideInputStoresToProcessors;
  }

  // Create task sideInput storage managers, one per task, index by the SSP they are responsible for consuming
  private Map<SystemStreamPartition, TaskSideInputStorageManager> createSideInputStorageManagers(Clock clock) {
    // creating sideInput store processors, one per store per task
    Map<TaskName, Map<String, SideInputsProcessor>> taskSideInputProcessors =
        createSideInputProcessors(new StorageConfig(config), this.containerModel, this.taskInstanceMetrics);

    Map<SystemStreamPartition, TaskSideInputStorageManager> sideInputStorageManagers = new HashMap<>();

    if (sideInputsPresent()) {
      containerModel.getTasks().forEach((taskName, taskModel) -> {

          Map<String, StorageEngine> sideInputStores = getSideInputStores(taskName);
          Map<String, Set<SystemStreamPartition>> sideInputStoresToSSPs = new HashMap<>();

          for (String storeName : sideInputStores.keySet()) {
            Set<SystemStreamPartition> storeSSPs = taskSideInputStoreSSPs.get(taskName).get(storeName);
            sideInputStoresToSSPs.put(storeName, storeSSPs);
          }

          TaskSideInputStorageManager taskSideInputStorageManager;
          TaskConfig taskConfig = new TaskConfig(config);
          if (taskConfig.getTransactionalStateCheckpointEnabled()) {
            taskSideInputStorageManager = new TransactionalTaskSideInputStorageManager(taskName, taskModel.getTaskMode(),
                streamMetadataCache, loggedStoreBaseDirectory, sideInputStores, taskSideInputProcessors.get(taskName),
                sideInputStoresToSSPs, systemAdmins, config, clock);
          } else {
            taskSideInputStorageManager = new NonTransactionalTaskSideInputStorageManager(taskName, taskModel.getTaskMode(),
                streamMetadataCache, loggedStoreBaseDirectory, sideInputStores, taskSideInputProcessors.get(taskName),
                sideInputStoresToSSPs, systemAdmins, config, clock);
          }

          sideInputStoresToSSPs.values().stream().flatMap(Set::stream).forEach(ssp -> {
              sideInputStorageManagers.put(ssp, taskSideInputStorageManager);
            });

          LOG.info("Created taskSideInputStorageManager for task {}, sideInputStores {} and loggedStoreBaseDirectory {}",
              taskName, sideInputStores, loggedStoreBaseDirectory);
        });
    }
    return sideInputStorageManagers;
  }

  private Map<String, StorageEngine> getSideInputStores(TaskName taskName) {
    return taskStores.get(taskName).entrySet().stream()
        .filter(e -> taskSideInputStoreSSPs.get(taskName).containsKey(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<String, StorageEngine> getNonSideInputStores(TaskName taskName) {
    return taskStores.get(taskName).entrySet().stream()
        .filter(e -> !taskSideInputStoreSSPs.get(taskName).containsKey(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public void start() throws SamzaException, InterruptedException {
    Map<SystemStreamPartition, String> checkpointedChangelogSSPOffsets = new HashMap<>();
    if (new TaskConfig(config).getTransactionalStateRestoreEnabled()) {
      getTasks(containerModel, TaskMode.Active).forEach((taskName, taskModel) -> {
          if (checkpointManager != null) {
            Set<SystemStream> changelogSystemStreams = new HashSet<>(this.changelogSystemStreams.values());
            Checkpoint checkpoint = checkpointManager.readLastCheckpoint(taskName);
            if (checkpoint != null) {
              checkpoint.getOffsets().forEach((ssp, offset) -> {
                  if (changelogSystemStreams.contains(new SystemStream(ssp.getSystem(), ssp.getStream()))) {
                    checkpointedChangelogSSPOffsets.put(ssp, offset);
                  }
                });
            }
          }
        });
    }
    LOG.info("Checkpointed changelog ssp offsets: {}", checkpointedChangelogSSPOffsets);
    restoreStores(checkpointedChangelogSSPOffsets);
    if (sideInputsPresent()) {
      startSideInputs();
    }
  }

  // Restoration of all stores, in parallel across tasks
  private void restoreStores(Map<SystemStreamPartition, String> checkpointedChangelogSSPOffsets)
      throws InterruptedException {
    LOG.info("Store Restore started");

    // initialize each TaskStorageManager
    this.taskRestoreManagers.values().forEach(taskStorageManager ->
       taskStorageManager.init(checkpointedChangelogSSPOffsets));

    // Start each store consumer once
    this.storeConsumers.values().stream().distinct().forEach(SystemConsumer::start);

    // Create a thread pool for parallel restores (and stopping of persistent stores)
    ExecutorService executorService = Executors.newFixedThreadPool(this.parallelRestoreThreadPoolSize,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(RESTORE_THREAD_NAME).build());

    List<Future> taskRestoreFutures = new ArrayList<>(this.taskRestoreManagers.entrySet().size());

    // Submit restore callable for each taskInstance
    this.taskRestoreManagers.forEach((taskInstance, taskRestoreManager) -> {
        taskRestoreFutures.add(executorService.submit(
            new TaskRestoreCallable(this.samzaContainerMetrics, taskInstance, taskRestoreManager)));
      });

    // loop-over the future list to wait for each thread to finish, catch any exceptions during restore and throw
    // as samza exceptions
    for (Future future : taskRestoreFutures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        LOG.warn("Received an interrupt during store restoration. Issuing interrupts to the store restoration workers to exit "
            + "prematurely without restoring full state.");
        executorService.shutdownNow();
        throw e;
      } catch (Exception e) {
        LOG.error("Exception when restoring ", e);
        throw new SamzaException("Exception when restoring ", e);
      }
    }

    executorService.shutdown();

    // Stop each store consumer once
    this.storeConsumers.values().stream().distinct().forEach(SystemConsumer::stop);

    // Now re-create persistent stores in read-write mode, leave non-persistent stores as-is
    recreatePersistentTaskStoresInReadWriteMode(this.containerModel, jobContext, containerContext,
        storageEngineFactories, serdes, taskInstanceMetrics, taskInstanceCollectors);

    LOG.info("Store Restore complete");
  }

  // Read sideInputs until all sideInputStreams are caughtup, so start() can return
  private void startSideInputs() {

    LOG.info("SideInput Restore started");

    // initialize the sideInputStorageManagers
    this.sideInputStorageManagers.values().forEach(TaskSideInputStorageManager::init);

    // read the active task checkpoints and save any offsets related to this task's side inputs
    getTasks(containerModel, TaskMode.Standby).forEach((taskName, taskModel) -> {
        TaskName activeTaskName = StandbyTaskUtil.getActiveTaskName(taskName);
        Checkpoint checkpoint = checkpointManager.readLastCheckpoint(activeTaskName);
        if (checkpoint != null) {
          checkpoint.getOffsets().forEach((ssp, offset) -> {
              if (this.sideInputStorageManagers.containsKey(ssp)) {
                this.checkpointedSideInputSSPOffsets.put(ssp, Optional.ofNullable(offset));
              }
            });
        }
      });

    Map<TaskName, RunLoopTask> taskMap = new HashMap<>();

    // since changelog side input consumption is checkpoint based, we separate it's executor from other side inputs so we do not
    // unnecessary block side input. TODO expand to configurable thread pool sizes
    KeyBasedExecutorService checkpointedSideInputExecutor = new KeyBasedExecutorService("checkpointedSideInputs", 1);
    KeyBasedExecutorService nonCheckpointedSideInputExecutor = new KeyBasedExecutorService("nonCheckpointedSideInputs", 1);

    this.taskSideInputStoreSSPs.forEach((taskName, storeAndSSPs) ->
        storeAndSSPs.forEach((store, storeSSPs) -> {
            TaskSideInputStorageManager taskSideInputStorageManager = sideInputStorageManagers.values().stream()
                .filter(storageManager -> storageManager.getTaskName() == taskName)
                .findFirst()
                .get();

            RunLoopTask runLoopTask = new RunLoopTask() {

              @Override
              public void process(IncomingMessageEnvelope envelope, ReadableCoordinator coordinator, TaskCallbackFactory callbackFactory) {
                TaskCallback callback = callbackFactory.createCallback();
                SystemStreamPartition envelopeSSP = envelope.getSystemStreamPartition();
                String offset = envelope.getOffset();
                boolean isSspCheckpointed = checkpointedSideInputSSPOffsets.containsKey(envelopeSSP);
                KeyBasedExecutorService executorToUse = isSspCheckpointed ? checkpointedSideInputExecutor : nonCheckpointedSideInputExecutor;

                executorToUse.submitOrdered(store, () -> {
                    SystemAdmin admin = systemAdmins.getSystemAdmin(envelopeSSP.getSystem());

                    // if the incoming envelope has an offset greater than the checkpoint, have this thread wait until
                    // the checkpoint updating thread wakes it back up
                    synchronized (sideInputSSPLocks.get(envelopeSSP)) {
                      while (isSspCheckpointed && checkpointedSideInputSSPOffsets.get(envelopeSSP).isPresent()
                          && admin.offsetComparator(offset, checkpointedSideInputSSPOffsets.get(envelopeSSP).get()) > 0) {
                        try {
                          sideInputSSPLocks.get(envelopeSSP).wait();
                        } catch (InterruptedException e) {
                          LOG.error("Side input restore interrupted when waiting for new checkpoint: " + store, e);
                          callback.failure(e);
                          throw new SamzaException(e);
                        }
                      }
                    }

                    sideInputStorageManagers.get(envelopeSSP).process(envelope);

                    // TODO is this call thread-safe?
                    checkSideInputCaughtUp(envelopeSSP, offset, SystemStreamMetadata.OffsetType.NEWEST);
                    callback.complete();
                  });
              }

              @Override
              public void commit() {
                Map<SystemStreamPartition, String> lastProcessedOffsets = storeSSPs.stream()
                    .collect(Collectors.toMap(
                        ssp -> ssp,
                        taskSideInputStorageManager::getLastProcessedOffset));

                String checkpointId = CheckpointId.create().toString();

                taskSideInputStorageManager.flush();
                taskSideInputStorageManager.checkpoint(checkpointId, lastProcessedOffsets);
                taskSideInputStorageManager.removeOldCheckpoints(checkpointId);
              }

              @Override
              public TaskInstanceMetrics metrics() {
                return taskInstanceMetrics.get(taskName);
              }

              @Override
              public scala.collection.immutable.Set<SystemStreamPartition> systemStreamPartitions() {
                return JavaConversions.asScalaSet(storeSSPs).toSet();
              }

              @Override
              public TaskName taskName() {
                return taskName;
              }
            };
            taskMap.put(taskName, runLoopTask);
          }));

    sideInputRunLoop = new RunLoop(taskMap,
        null,
        sideInputSystemConsumers,
        1, // TODO separate max concurrency for standby restore into its own config
        -1L,
        new TaskConfig(this.config).getCommitMs(),
        new TaskConfig(this.config).getCallbackTimeoutMs(),
        1,
        new TaskConfig(this.config).getMaxIdleMs(),
        samzaContainerMetrics,
        System::nanoTime,
        false);

    TaskConfig taskConfig = new TaskConfig(config);
    sideInputsFlushFuture = sideInputsFlushExecutor.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        getTasks(containerModel, TaskMode.Standby).forEach((taskName, taskModel) -> {
            TaskName activeTaskName = StandbyTaskUtil.getActiveTaskName(taskName);
            Checkpoint checkpoint = checkpointManager.readLastCheckpoint(activeTaskName);
            if (checkpoint != null) {
              checkpoint.getOffsets().forEach((ssp, latestOffset) -> {
                  if (sideInputStorageManagers.containsKey(ssp)) {
                    Optional<String> currentOffsetOpt = checkpointedSideInputSSPOffsets.get(ssp);
                    Optional<String> latestOffsetOpt = Optional.ofNullable(latestOffset);
                    SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(ssp.getSystem());

                    // if current isn't present and latest is, or
                    // current is present and latest > current
                    if ((!currentOffsetOpt.isPresent() && latestOffsetOpt.isPresent())
                        || (currentOffsetOpt.isPresent() && systemAdmin.offsetComparator(latestOffset, currentOffsetOpt.get()) > 0)) {
                      synchronized (sideInputSSPLocks.get(ssp)) {
                        checkpointedSideInputSSPOffsets.put(ssp, latestOffsetOpt);
                        sideInputSSPLocks.get(ssp).notifyAll();
                      }
                    }
                  }
                });
            }
          });
      }
    }, 0, taskConfig.getCommitMs(), TimeUnit.MILLISECONDS);

    // set the latch to the number of sideInput SSPs
    this.sideInputsCaughtUp = new CountDownLatch(this.sideInputStorageManagers.keySet().size());

    // register all sideInput SSPs with the consumers
    for (SystemStreamPartition ssp : sideInputStorageManagers.keySet()) {
      String startingOffset = sideInputStorageManagers.get(ssp).getStartingOffset(ssp);

      if (startingOffset == null) {
        throw new SamzaException("No offset defined for SideInput SystemStreamPartition : " + ssp);
      }

      // register startingOffset with the sysConsumer and register a metric for it
      sideInputSystemConsumers.register(ssp, startingOffset);
      taskInstanceMetrics.get(sideInputStorageManagers.get(ssp).getTaskName()).addOffsetGauge(
          ssp, ScalaJavaUtil.toScalaFunction(() -> sideInputStorageManagers.get(ssp).getLastProcessedOffset(ssp)));

      SystemStreamMetadata systemStreamMetadata = streamMetadataCache.getSystemStreamMetadata(ssp.getSystemStream(), false);
      SystemStreamMetadata.SystemStreamPartitionMetadata sspMetadata =
          (systemStreamMetadata == null) ? null : systemStreamMetadata.getSystemStreamPartitionMetadata().get(ssp.getPartition());

      // record a copy of the sspMetadata, to later check if its caught up
      initialSideInputSSPMetadata.put(ssp, sspMetadata);

      // check if the ssp is caught to upcoming, even at start
      checkSideInputCaughtUp(ssp, startingOffset, SystemStreamMetadata.OffsetType.UPCOMING);
    }

    // start the systemConsumers for consuming input
    this.sideInputSystemConsumers.start();

    try {
      sideInputsReadExecutor.submit(() -> {
          try {
            sideInputRunLoop.run();
          } catch (Exception e) {
            LOG.error("Exception in reading sideInputs", e);
            sideInputException = e;
          }
        });

      // Make the main thread wait until all sideInputs have been caughtup or an exception was thrown
      while (!shouldShutdown && sideInputException == null &&
          !this.sideInputsCaughtUp.await(SIDE_INPUT_READ_THREAD_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        LOG.debug("Waiting for SideInput bootstrap to complete");
      }

      if (sideInputException != null) { // Throw exception if there was an exception in catching-up sideInputs
        throw new SamzaException("Exception in restoring sideInputs", sideInputException);
      }

    } catch (InterruptedException e) {
      LOG.warn("Received an interrupt during side inputs store restoration."
          + " Exiting prematurely without completing store restore.");
      /*
       * We want to stop side input restoration and rethrow the exception upstream. Container should handle the
       * interrupt exception and shutdown the components and cleaning up the resource. We don't want to clean up the
       * resources prematurely here.
       */
      shouldShutdown = true;
      throw new SamzaException("Side inputs read was interrupted", e);
    }

    LOG.info("SideInput Restore complete");
  }

  private boolean sideInputsPresent() {
    return !this.taskSideInputStoreSSPs.values().stream()
        .allMatch(Map::isEmpty);
  }

  // Method to check if the given offset means the stream is caught up for reads
  // TODO how to check that checkpointed side inputs are caught up
  void checkSideInputCaughtUp(SystemStreamPartition ssp, String offset, SystemStreamMetadata.OffsetType offsetType) {

    SystemStreamMetadata.SystemStreamPartitionMetadata sspMetadata = this.initialSideInputSSPMetadata.get(ssp);
    String offsetToCheck = sspMetadata == null ? null : sspMetadata.getOffset(offsetType);
    LOG.trace("Checking {} offset {} against {} for {}.", offsetType, offset, offsetToCheck, ssp);

    // Let's compare offset of the chosen message with offsetToCheck.
    Integer comparatorResult;
    if (offset == null || offsetToCheck == null) {
      comparatorResult = -1;
    } else {
      SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(ssp.getSystem());
      comparatorResult = systemAdmin.offsetComparator(offset, offsetToCheck);
    }

    // The SSP is no longer lagging if the envelope's offset is greater than or equal to the
    // latest offset.
    if (comparatorResult != null && comparatorResult.intValue() >= 0) {

      LOG.info("Side input ssp {} has caught up to offset {} ({}).", ssp, offset, offsetType);
      // if its caught up, we remove the ssp from the map, and countDown the latch
      this.initialSideInputSSPMetadata.remove(ssp);
      this.sideInputsCaughtUp.countDown();
      return;
    }
  }

  /**
   * Get the {@link StorageEngine} instance with a given name for a given task.
   * @param taskName the task name for which the storage engine is desired.
   * @param storeName the desired store's name.
   * @return the task store.
   */
  public Optional<StorageEngine> getStore(TaskName taskName, String storeName) {
    return Optional.ofNullable(this.taskStores.get(taskName).get(storeName));
  }

  /**
   *  Get all {@link StorageEngine} instance used by a given task.
   * @param taskName  the task name, all stores for which are desired.
   * @return map of stores used by the given task, indexed by storename
   */
  public Map<String, StorageEngine> getAllStores(TaskName taskName) {
    return this.taskStores.get(taskName);
  }

  /**
   * Set of directory paths for all stores restored by this {@link ContainerStorageManager}.
   * @return the set of all store directory paths
   */
  public Set<Path> getStoreDirectoryPaths() {
    return this.storeDirectoryPaths;
  }

  @VisibleForTesting
  public void stopStores() {
    this.taskStores.forEach((taskName, storeMap) -> storeMap.forEach((storeName, store) -> store.stop()));
  }

  public void shutdown() {
    // stop all nonsideinputstores including persistent and non-persistent stores
    this.containerModel.getTasks().forEach((taskName, taskModel) ->
        getNonSideInputStores(taskName).forEach((storeName, store) -> store.stop())
    );

    this.shouldShutdown = true;

    // stop all sideinput consumers and stores
    if (sideInputsPresent()) {
      sideInputRunLoop.shutdown();
      sideInputsReadExecutor.shutdownNow();

      this.sideInputSystemConsumers.stop();

      // cancel all future sideInput flushes, shutdown the executor, and await for finish
      sideInputsFlushFuture.cancel(false);
      sideInputsFlushExecutor.shutdown();
      try {
        sideInputsFlushExecutor.awaitTermination(SIDE_INPUT_FLUSH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new SamzaException("Exception while shutting down sideInputs", e);
      }

      // stop all sideInputStores -- this will perform one last flush on the KV stores, and write the offset file
      this.sideInputStorageManagers.values().forEach(TaskSideInputStorageManager::stop);
    }
    LOG.info("Shutdown complete");
  }

  /**
   * Callable for performing the restore on a task restore manager and emitting the task-restoration metric.
   * After restoration, all persistent stores are stopped (which will invoke compaction in case of certain persistent
   * stores that were opened in bulk-load mode).
   * Performing stop here parallelizes this compaction, which is a time-intensive operation.
   *
   */
  private class TaskRestoreCallable implements Callable<Void> {

    private TaskName taskName;
    private TaskRestoreManager taskRestoreManager;
    private SamzaContainerMetrics samzaContainerMetrics;

    public TaskRestoreCallable(SamzaContainerMetrics samzaContainerMetrics, TaskName taskName,
        TaskRestoreManager taskRestoreManager) {
      this.samzaContainerMetrics = samzaContainerMetrics;
      this.taskName = taskName;
      this.taskRestoreManager = taskRestoreManager;
    }

    @Override
    public Void call() {
      long startTime = System.currentTimeMillis();
      try {
        LOG.info("Starting stores in task instance {}", this.taskName.getTaskName());
        taskRestoreManager.restore();
      } catch (InterruptedException e) {
        /*
         * The container thread is the only external source to trigger an interrupt to the restoration thread and thus
         * it is okay to swallow this exception and not propagate it upstream. If the container is interrupted during
         * the store restoration, ContainerStorageManager signals the restore workers to abandon restoration and then
         * finally propagates the exception upstream to trigger container shutdown.
         */
        LOG.warn("Received an interrupt during store restoration for task: {}.", this.taskName.getTaskName());
      } finally {
        // Stop all persistent stores after restoring. Certain persistent stores opened in BulkLoad mode are compacted
        // on stop, so paralleling stop() also parallelizes their compaction (a time-intensive operation).
        taskRestoreManager.stopPersistentStores();
        long timeToRestore = System.currentTimeMillis() - startTime;

        if (this.samzaContainerMetrics != null) {
          Gauge taskGauge = this.samzaContainerMetrics.taskStoreRestorationMetrics().getOrDefault(this.taskName, null);

          if (taskGauge != null) {
            taskGauge.set(timeToRestore);
          }
        }
      }

      return null;
    }
  }
}
