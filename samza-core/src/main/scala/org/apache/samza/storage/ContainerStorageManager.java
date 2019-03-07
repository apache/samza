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
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeManager;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.system.SystemConsumersMetrics;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionIterator;
import org.apache.samza.system.chooser.DefaultChooser;
import org.apache.samza.system.chooser.MessageChooser;
import org.apache.samza.system.chooser.RoundRobinChooserFactory;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.task.TaskInstanceCollector;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.apache.samza.util.ScalaJavaUtil;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;


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
 *  In case, where a user explicitly requests a task-commit, it will not include committing side inputs.
 */
public class ContainerStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerStorageManager.class);
  private static final String RESTORE_THREAD_NAME = "Samza Restore Thread-%d";
  private static final String SIDEINPUTS_FLUSH_THREAD_NAME = "SideInputs Flush Thread";
  private static final String SIDEINPUTS_METRICS_SOURCE = "samza-container-%s-" + ContainerStorageManager.class.getName();
  // We populate this class as the source to differentiate the SystemConsumersMetrics in CSM from the one in SamzaContainer

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
  private final SamzaContainerMetrics samzaContainerMetrics;

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
  private final Map<String, Set<SystemStream>> sideInputSystemStreams; // Map of side input system-streams indexed by store name
  private final Map<TaskName, Map<String, Set<SystemStreamPartition>>> taskSideInputSSPs;
  private final Map<SystemStreamPartition, TaskSideInputStorageManager> sideInputStorageManagers; // Map of sideInput storageManagers indexed by ssp, for simpler lookup for process()
  private final Map<String, SystemConsumer> sideInputConsumers; // Mapping from storeSystemNames to SystemConsumers
  private SystemConsumers sideInputSystemConsumers;
  private SystemConsumersMetrics sideInputSystemConsumersMetrics;
  private final Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> initialSideInputSSPMetadata
      = new ConcurrentHashMap<>(); // Recorded sspMetadata of the taskSideInputSSPs recorded at start, used to determine when sideInputs are caughtup and container init can proceed
  private volatile CountDownLatch sideInputsCaughtUp; // Used by the sideInput-read thread to signal to the main thread
  private volatile boolean shutDownSideInputRead = false;
  private final ScheduledExecutorService sideInputsFlushExecutor = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat(SIDEINPUTS_FLUSH_THREAD_NAME).build());
  private ScheduledFuture sideInputsFlushFuture;
  private static final Duration SIDE_INPUT_FLUSH_TIMEOUT = Duration.ofMinutes(1);
  private volatile Optional<Throwable> sideInputException = Optional.empty();

  private final Config config;

  public ContainerStorageManager(ContainerModel containerModel, StreamMetadataCache streamMetadataCache,
      SystemAdmins systemAdmins, Map<String, SystemStream> changelogSystemStreams,
      Map<String, Set<SystemStream>> sideInputSystemStreams,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, SystemFactory> systemFactories, Map<String, Serde<Object>> serdes, Config config,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics, SamzaContainerMetrics samzaContainerMetrics,
      JobContext jobContext, ContainerContext containerContext,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors, File loggedStoreBaseDirectory,
      File nonLoggedStoreBaseDirectory, int maxChangeLogStreamPartitions, SerdeManager serdeManager, Clock clock) {

    this.containerModel = containerModel;
    this.sideInputSystemStreams = new HashMap<>(sideInputSystemStreams);
    this.taskSideInputSSPs = getTaskSideInputSSPs(containerModel, sideInputSystemStreams);

    this.changelogSystemStreams = getChangelogSystemStreams(containerModel, changelogSystemStreams); // handling standby tasks

    LOG.info("Starting with changelogSystemStreams = {} sideInputSystemStreams = {}", this.changelogSystemStreams, this.sideInputSystemStreams);

    this.storageEngineFactories = storageEngineFactories;
    this.serdes = serdes;
    this.loggedStoreBaseDirectory = loggedStoreBaseDirectory;
    this.nonLoggedStoreBaseDirectory = nonLoggedStoreBaseDirectory;

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
    Map<String, SystemConsumer> storeSystemConsumers = createConsumers(this.changelogSystemStreams.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
        e -> Collections.singleton(e.getValue()))), systemFactories, config, this.samzaContainerMetrics.registry());
    this.storeConsumers = createStoreIndexedMap(this.changelogSystemStreams, storeSystemConsumers);

    // creating task restore managers
    this.taskRestoreManagers = createTaskRestoreManagers(systemAdmins, clock, this.samzaContainerMetrics);

    // create side input storage managers
    sideInputStorageManagers = createSideInputStorageManagers(clock);

    // create side Input consumers indexed by systemName
    this.sideInputConsumers = createConsumers(this.sideInputSystemStreams, systemFactories, config, this.samzaContainerMetrics.registry());

    // create SystemConsumers for consuming from taskSideInputSSPs, if sideInputs are being used
    if (sideInputsPresent()) {

      scala.collection.immutable.Map<SystemStream, SystemStreamMetadata> inputStreamMetadata = streamMetadataCache.getStreamMetadata(JavaConversions.asScalaSet(
          this.sideInputSystemStreams.values().stream().flatMap(Set::stream).collect(Collectors.toSet())).toSet(), false);

      sideInputSystemConsumersMetrics = new SystemConsumersMetrics(new MetricsRegistryMap(), String.format(SIDEINPUTS_METRICS_SOURCE, containerModel.getId()));

      MessageChooser chooser = DefaultChooser.apply(inputStreamMetadata, new RoundRobinChooserFactory(), config,
          sideInputSystemConsumersMetrics.registry(), systemAdmins);

      sideInputSystemConsumers =
          new SystemConsumers(chooser, ScalaJavaUtil.toScalaMap(this.sideInputConsumers), serdeManager,
              sideInputSystemConsumersMetrics, SystemConsumers.DEFAULT_NO_NEW_MESSAGES_TIMEOUT(), SystemConsumers.DEFAULT_DROP_SERIALIZATION_ERROR(),
              SystemConsumers.DEFAULT_POLL_INTERVAL_MS(), ScalaJavaUtil.toScalaFunction(() -> System.nanoTime()));
    }

  }

  /**
   * Add all side inputs to a map of maps, indexed first by taskName, then by sideInput store name.
   *
   * @param containerModel the containerModel to use
   * @param sideInputSystemStreams the map of store to side input system stream
   * @return taskSideInputSSPs map
   */
  private Map<TaskName, Map<String, Set<SystemStreamPartition>>> getTaskSideInputSSPs(ContainerModel containerModel, Map<String, Set<SystemStream>> sideInputSystemStreams) {
    Map<TaskName, Map<String, Set<SystemStreamPartition>>> taskSideInputSSPs = new HashMap<>();

    containerModel.getTasks().forEach((taskName, taskModel) -> {
        sideInputSystemStreams.keySet().forEach(storeName -> {
            Set<SystemStreamPartition> taskSideInputs = taskModel.getSystemStreamPartitions().stream().filter(ssp -> sideInputSystemStreams.get(storeName).contains(ssp.getSystemStream())).collect(Collectors.toSet());
            taskSideInputSSPs.putIfAbsent(taskName, new HashMap<>());
            taskSideInputSSPs.get(taskName).put(storeName, taskSideInputs);
          });
      });
    return taskSideInputSSPs;
  }

  /**
   * For each standby task, we remove its changeLogSSPs from changelogSSP map and add it to the task's taskSideInputSSPs.
   * The task's sideInputManager will consume and restore these as well.
   *
   * @param containerModel the container's model
   * @param changelogSystemStreams the passed in set of changelogSystemStreams
   * @return A map of changeLogSSP to storeName across all tasks, assuming no two stores have the same changelogSSP
   */
  private Map<String, SystemStream> getChangelogSystemStreams(ContainerModel containerModel, Map<String, SystemStream> changelogSystemStreams) {

    if (MapUtils.invertMap(changelogSystemStreams).size() != changelogSystemStreams.size()) {
      throw new SamzaException("Two stores cannot have the same changelog system-stream");
    }

    Map<SystemStreamPartition, String> changelogSSPToStore = new HashMap<>();
    changelogSystemStreams.forEach((storeName, systemStream) ->
        containerModel.getTasks().forEach((taskName, taskModel) -> { changelogSSPToStore.put(new SystemStreamPartition(systemStream, taskModel.getChangelogPartition()), storeName); })
    );

    getTasks(containerModel, TaskMode.Standby).forEach((taskName, taskModel) -> {
        changelogSystemStreams.forEach((storeName, systemStream) -> {
            SystemStreamPartition ssp = new SystemStreamPartition(systemStream, taskModel.getChangelogPartition());
            changelogSSPToStore.remove(ssp);
            this.taskSideInputSSPs.putIfAbsent(taskName, new HashMap<>());
            this.sideInputSystemStreams.put(storeName, Collections.singleton(ssp.getSystemStream()));
            this.taskSideInputSSPs.get(taskName).put(storeName, Collections.singleton(ssp));
          });
      });

    // changelogSystemStreams correspond only to active tasks (since those of standby-tasks moved to side inputs above)
    return MapUtils.invertMap(changelogSSPToStore).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getSystemStream()));
  }


  /**
   *  Creates SystemConsumer objects for store restoration, creating one consumer per system.
   */
  private static Map<String, SystemConsumer> createConsumers(Map<String, Set<SystemStream>> systemStreams,
      Map<String, SystemFactory> systemFactories, Config config, MetricsRegistry registry) {
    // Determine the set of systems being used across all stores
    Set<String> storeSystems =
        systemStreams.values().stream().flatMap(Set::stream).map(SystemStream::getSystem).collect(Collectors.toSet());

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
            new TaskRestoreManager(taskModel, changelogSystemStreams, getNonSideInputStores(taskName), systemAdmins, clock));
        samzaContainerMetrics.addStoresRestorationGauge(taskName);
      });
    return taskRestoreManagers;
  }

  // Helper method to filter active Tasks from the container model
  private static Map<TaskName, TaskModel> getTasks(ContainerModel containerModel, TaskMode taskMode) {
    return containerModel.getTasks().entrySet().stream()
            .filter(x -> x.getValue().getTaskMode().equals(taskMode)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Create taskStores for all stores in storageEngineFactories.
   * The store mode is chosen as bulk-load if its a non-sideinput store, and readWrite if its a side input store
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

        StorageEngineFactory.StoreMode storeMode = this.sideInputSystemStreams.containsKey(storeName) ?
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

    // Use the logged-store-base-directory for change logged stores and side input stores, and non-logged-store-base-dir
    // for non logged stores
    File storeDirectory;
    if (changeLogSystemStreamPartition != null || sideInputSystemStreams.containsKey(storeName)) {
      storeDirectory = StorageManagerUtil.getStorePartitionDir(this.loggedStoreBaseDirectory, storeName, taskName, taskModel.getTaskMode());
    } else {
      storeDirectory = StorageManagerUtil.getStorePartitionDir(this.nonLoggedStoreBaseDirectory, storeName, taskName, taskModel.getTaskMode());
    }

    this.storeDirectoryPaths.add(storeDirectory.toPath());

    if (storageConfig.getStorageKeySerde(storeName).isEmpty()) {
      throw new SamzaException("No key serde defined for store: " + storeName);
    }

    Serde keySerde = serdes.get(storageConfig.getStorageKeySerde(storeName).get());
    if (keySerde == null) {
      throw new SamzaException(
          "StorageKeySerde: No class defined for serde: " + storageConfig.getStorageKeySerde(storeName));
    }

    if (storageConfig.getStorageMsgSerde(storeName).isEmpty()) {
      throw new SamzaException("No msg serde defined for store: " + storeName);
    }

    Serde messageSerde = serdes.get(storageConfig.getStorageMsgSerde(storeName).get());
    if (messageSerde == null) {
      throw new SamzaException(
          "StorageMsgSerde: No class defined for serde: " + storageConfig.getStorageMsgSerde(storeName));
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


  // Create side input store processors, one per store per task
  private Map<TaskName, Map<String, SideInputsProcessor>> createSideInputProcessors(StorageConfig config, ContainerModel containerModel,
      Map<String, Set<SystemStream>> sideInputSystemStreams, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics) {

    Map<TaskName, Map<String, SideInputsProcessor>> sideInputStoresToProcessors = new HashMap<>();
    getTasks(containerModel, TaskMode.Active).forEach((taskName, taskModel) -> {
        sideInputStoresToProcessors.put(taskName, new HashMap<>());
        for (String storeName : sideInputSystemStreams.keySet()) {
          if (config.getSideInputsProcessorSerializedInstance(storeName).isDefined()) {
            sideInputStoresToProcessors.get(taskName)
                .put(storeName, SerdeUtils.deserialize("Side Inputs Processor",
                    config.getSideInputsProcessorSerializedInstance(storeName).get()));
          } else {
            sideInputStoresToProcessors.get(taskName)
                .put(storeName, Util.getObj(config.getSideInputsProcessorFactory(storeName).get(),
                    SideInputsProcessorFactory.class).getSideInputsProcessor(config,
                    taskInstanceMetrics.get(taskName).registry()));
          }
        }
      });

    // creating identity sideInputProcessor for stores of standbyTasks
    getTasks(containerModel, TaskMode.Standby).forEach((taskName, taskModel) -> {
        sideInputStoresToProcessors.put(taskName, new HashMap<>());
        for (String storeName : sideInputSystemStreams.keySet()) {

          // have to use the right serde because the sideInput stores are created
          Serde keySerde = serdes.get(new StorageConfig(config).getStorageKeySerde(storeName).get());
          Serde msgSerde = serdes.get(new StorageConfig(config).getStorageMsgSerde(storeName).get());
          sideInputStoresToProcessors.get(taskName).put(storeName, new SideInputsProcessor() {
            @Override
            public Collection<Entry<?, ?>> process(IncomingMessageEnvelope message, KeyValueStore store) {
              return ImmutableList.of(new Entry<>(keySerde.fromBytes((byte[]) message.getKey()), msgSerde.fromBytes((byte[]) message.getMessage())));
            }
          });
        }
      });

    return sideInputStoresToProcessors;
  }

  // Create task side input storage managers, one per task, index by the SSP they are responsible for consuming
  private Map<SystemStreamPartition, TaskSideInputStorageManager> createSideInputStorageManagers(Clock clock) {

    // creating side input store processors, one per store per task
    Map<TaskName, Map<String, SideInputsProcessor>> taskSideInputProcessors = createSideInputProcessors(new StorageConfig(config), this.containerModel, this.sideInputSystemStreams, this.taskInstanceMetrics);

    Map<SystemStreamPartition, TaskSideInputStorageManager> sideInputStorageManagers = new HashMap<>();

    if (sideInputsPresent()) {
      containerModel.getTasks().forEach((taskName, taskModel) -> {

          Map<String, StorageEngine> sideInputStores = getSideInputStores(taskName);
          Map<String, Set<SystemStreamPartition>> sideInputStoresToSSPs = new HashMap<>();

          for (String storeName : sideInputStores.keySet()) {
            Set<SystemStreamPartition> storeSSPs = taskSideInputSSPs.get(taskName).get(storeName);
            sideInputStoresToSSPs.put(storeName, storeSSPs);
          }

          TaskSideInputStorageManager taskSideInputStorageManager =
              new TaskSideInputStorageManager(taskName, taskModel.getTaskMode(), streamMetadataCache,
                  loggedStoreBaseDirectory, sideInputStores, taskSideInputProcessors.get(taskName), sideInputStoresToSSPs,
                  systemAdmins, config, clock);

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
    return taskStores.get(taskName).entrySet().stream().
        filter(e -> sideInputSystemStreams.containsKey(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<String, StorageEngine> getNonSideInputStores(TaskName taskName) {
    return taskStores.get(taskName).entrySet().stream().
        filter(e -> !sideInputSystemStreams.containsKey(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Set<TaskSideInputStorageManager> getSideInputStorageManagers() {
    return this.sideInputStorageManagers.values().stream().collect(Collectors.toSet());
  }

  /**
   * Registers any CSM created metrics such as side-inputs related metrics, and standby-task related metrics.
   * @param metricsReporters metrics reporters to use
   */
  public void registerMetrics(Map<String, MetricsReporter> metricsReporters) {
    if (sideInputsPresent()) {
      metricsReporters.values()
          .forEach(reporter -> reporter.register(sideInputSystemConsumersMetrics.source(),
              this.sideInputSystemConsumersMetrics.registry()));
    }

    this.containerModel.getTasks().forEach((taskName, taskModel) -> {
        if (taskModel.getTaskMode().equals(TaskMode.Standby)) {
          metricsReporters.values()
              .forEach(reporter -> reporter.register(this.taskInstanceMetrics.get(taskName).source(),
                  this.taskInstanceMetrics.get(taskName).registry()));
        }
      });
  }

  public void start() throws SamzaException {
    restoreStores();
    if (sideInputsPresent()) {
      startSideInputs();
    }
  }

  // Restoration of all stores, in parallel across tasks
  private void restoreStores() {
    LOG.info("Store Restore started");

    // initialize each TaskStorageManager
    this.taskRestoreManagers.values().forEach(taskStorageManager -> taskStorageManager.initialize());

    // Start store consumers
    this.storeConsumers.values().forEach(systemConsumer -> systemConsumer.start());

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
      } catch (Exception e) {
        LOG.error("Exception when restoring ", e);
        throw new SamzaException("Exception when restoring ", e);
      }
    }

    executorService.shutdown();

    // Stop store consumers
    this.storeConsumers.values().forEach(systemConsumer -> systemConsumer.stop());

    // Now re-create persistent stores in read-write mode, leave non-persistent stores as-is
    recreatePersistentTaskStoresInReadWriteMode(this.containerModel, jobContext, containerContext,
        storageEngineFactories, serdes, taskInstanceMetrics, taskInstanceCollectors);

    LOG.info("Store Restore complete");
  }

  // Read sideInputs until all sideInputStreams are caughtup, so start() can return
  private void startSideInputs() {

    LOG.info("SideInput Restore started");

    // initialize the sideInputStorageManagers
    getSideInputStorageManagers().forEach(sideInputStorageManager -> sideInputStorageManager.init());

    // start the checkpointing thread at the commit-ms frequency
    sideInputsFlushFuture = sideInputsFlushExecutor.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          getSideInputStorageManagers().forEach(sideInputStorageManager -> sideInputStorageManager.flush());
        } catch (Exception e) {
          LOG.error("Exception during flushing side inputs", e);
          sideInputException = Optional.of(e);
        }
      }
    }, 0, new TaskConfig(config).getCommitMs(), TimeUnit.MILLISECONDS);

    // set the latch to the number of sideInput SSPs
    this.sideInputsCaughtUp = new CountDownLatch(this.sideInputStorageManagers.keySet().size());

    // register all side input SSPs with the consumers
    for (SystemStreamPartition ssp : sideInputStorageManagers.keySet()) {
      String startingOffset = sideInputStorageManagers.get(ssp).getStartingOffset(ssp);

      if (startingOffset == null) {
        throw new SamzaException("No offset defined for SideInput SystemStreamPartition : " + ssp);
      }

      // register startingOffset with the sysConsumer and register a metric for it
      sideInputSystemConsumers.register(ssp, startingOffset, null);
      taskInstanceMetrics.get(sideInputStorageManagers.get(ssp).getTaskName()).addOffsetGauge(
          ssp, ScalaJavaUtil.toScalaFunction(() -> sideInputStorageManagers.get(ssp).getLastProcessedOffset(ssp)));

      SystemStreamMetadata systemStreamMetadata = streamMetadataCache.getSystemStreamMetadata(ssp.getSystemStream(), false);
      SystemStreamMetadata.SystemStreamPartitionMetadata sspMetadata =
          (systemStreamMetadata == null) ? null : systemStreamMetadata.getSystemStreamPartitionMetadata().get(ssp.getPartition());

      // record a copy of the sspMetadata, to later check if its caught up
      initialSideInputSSPMetadata.put(ssp, sspMetadata);

      // check if the ssp is caught to upcoming, even at start
      checkSideInputCaughtUp(ssp, startingOffset, SystemStreamMetadata.OffsetType.UPCOMING, false);
    }

    // start the systemConsumers for consuming input
    this.sideInputSystemConsumers.start();

    // create a thread for sideInput reads
    Thread readSideInputs = new Thread(() -> {
        while (!shutDownSideInputRead) {
          IncomingMessageEnvelope envelope = sideInputSystemConsumers.choose(true);
          if (envelope != null) {

            if (!envelope.isEndOfStream())
              sideInputStorageManagers.get(envelope.getSystemStreamPartition()).process(envelope);

            checkSideInputCaughtUp(envelope.getSystemStreamPartition(), envelope.getOffset(),
                SystemStreamMetadata.OffsetType.NEWEST, envelope.isEndOfStream());

          } else {
            LOG.trace("No incoming message was available");
          }
        }
      });

    readSideInputs.setDaemon(true);
    readSideInputs.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        sideInputException = Optional.of(e);
        sideInputsCaughtUp.countDown();
      }
    });

    try {
      readSideInputs.start();
      // Make the main thread wait until all sideInputs have been caughtup or thrown an exception
      this.sideInputsCaughtUp.await();

      if (sideInputException.isPresent()) { // Throw exception if there was an exception in catching-up sideInputs
        // TODO: SAMZA-2113 relay exception to main thread
        throw new SamzaException("Exception in restoring side inputs", sideInputException.get());
      }
    } catch (InterruptedException e) {
      sideInputException = Optional.of(e);
      throw new SamzaException("Side inputs read was interrupted", e);
    }

    LOG.info("SideInput Restore complete");
  }

  private boolean sideInputsPresent() {
    return !this.sideInputSystemStreams.isEmpty();
  }

  // Method to check if the given offset means the stream is caught up for reads
  private void checkSideInputCaughtUp(SystemStreamPartition ssp, String offset, SystemStreamMetadata.OffsetType offsetType, boolean isEndOfStream) {

    if (isEndOfStream) {
      this.initialSideInputSSPMetadata.remove(ssp);
      this.sideInputsCaughtUp.countDown();
      LOG.info("Side input ssp {} has caught up to offset {} ({}).", ssp, offset, offsetType);
      return;
    }

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

    // stop all sideinput consumers and stores
    if (sideInputsPresent()) {
      // stop reading sideInputs
      this.shutDownSideInputRead = true;

      this.sideInputSystemConsumers.stop();

      // cancel all future sideInput flushes, shutdown the executor, and await for finish
      sideInputsFlushFuture.cancel(false);
      sideInputsFlushExecutor.shutdown();
      try {
        sideInputsFlushExecutor.awaitTermination(SIDE_INPUT_FLUSH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new SamzaException("Exception while shutting down side inputs", e);
      }

      // stop all sideInputStores -- this will perform one last flush on the KV stores, and write the offset file
      this.getSideInputStorageManagers().forEach(sideInputStorageManager -> sideInputStorageManager.stop());
    }
    LOG.info("Shutdown complete");
  }

  /**
   * Callable for performing the restoreStores on a task restore manager and emitting the task-restoration metric.
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
      LOG.info("Starting stores in task instance {}", this.taskName.getTaskName());
      taskRestoreManager.restoreStores();

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
      return null;
    }
  }

  /**
   * Restore logic for all stores of a task including directory cleanup, setup, changelogSSP validation, registering
   * with the respective consumer, restoring stores, and stopping stores.
   */
  private class TaskRestoreManager {
    private final Map<String, StorageEngine> taskStores; // Map of all StorageEngines for this task indexed by store name
    private final Set<String> taskStoresToRestore;
    // Set of store names which need to be restored by consuming using system-consumers (see registerStartingOffsets)

    private final TaskModel taskModel;
    private final Clock clock; // Clock value used to validate base-directories for staleness. See isLoggedStoreValid.
    private Map<SystemStream, String> changeLogOldestOffsets; // Map of changelog oldest known offsets
    private final Map<SystemStreamPartition, String> fileOffsets; // Map of offsets read from offset file indexed by changelog SSP
    private final Map<String, SystemStream> changelogSystemStreams; // Map of change log system-streams indexed by store name
    private final SystemAdmins systemAdmins;

    public TaskRestoreManager(TaskModel taskModel, Map<String, SystemStream> changelogSystemStreams,
        Map<String, StorageEngine> taskStores, SystemAdmins systemAdmins, Clock clock) {
      this.taskStores = taskStores;
      this.taskModel = taskModel;
      this.clock = clock;
      this.changelogSystemStreams = changelogSystemStreams;
      this.systemAdmins = systemAdmins;
      this.fileOffsets = new HashMap<>();
      this.taskStoresToRestore = this.taskStores.entrySet().stream()
          .filter(x -> x.getValue().getStoreProperties().isLoggedStore())
          .map(x -> x.getKey()).collect(Collectors.toSet());
    }

    /**
     * Cleans up and sets up store directories, validates changeLog SSPs for all stores of this task,
     * and registers SSPs with the respective consumers.
     */
    public void initialize() {
      cleanBaseDirsAndReadOffsetFiles();
      setupBaseDirs();
      validateChangelogStreams();
      getOldestChangeLogOffsets();
      registerStartingOffsets();
    }

    /**
     * For each store for this task,
     * a. Deletes the corresponding non-logged-store base dir.
     * b. Deletes the logged-store-base-dir if it not valid. See {@link #isLoggedStoreValid} for validation semantics.
     * c. If the logged-store-base-dir is valid, this method reads the offset file and stores each offset.
     */
    private void cleanBaseDirsAndReadOffsetFiles() {
      LOG.debug("Cleaning base directories for stores.");

      taskStores.keySet().forEach(storeName -> {
          File nonLoggedStorePartitionDir =
              StorageManagerUtil.getStorePartitionDir(nonLoggedStoreBaseDirectory, storeName, taskModel.getTaskName(), taskModel.getTaskMode());
          LOG.info("Got non logged storage partition directory as " + nonLoggedStorePartitionDir.toPath().toString());

          if (nonLoggedStorePartitionDir.exists()) {
            LOG.info("Deleting non logged storage partition directory " + nonLoggedStorePartitionDir.toPath().toString());
            FileUtil.rm(nonLoggedStorePartitionDir);
          }

          File loggedStorePartitionDir =
              StorageManagerUtil.getStorePartitionDir(loggedStoreBaseDirectory, storeName, taskModel.getTaskName(), taskModel.getTaskMode());
          LOG.info("Got logged storage partition directory as " + loggedStorePartitionDir.toPath().toString());

          // Delete the logged store if it is not valid.
          if (!isLoggedStoreValid(storeName, loggedStorePartitionDir)) {
            LOG.info("Deleting logged storage partition directory " + loggedStorePartitionDir.toPath().toString());
            FileUtil.rm(loggedStorePartitionDir);
          } else {

            SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStreams.get(storeName), taskModel.getChangelogPartition());
            Map<SystemStreamPartition, String> offset =
                StorageManagerUtil.readOffsetFile(loggedStorePartitionDir, Collections.singleton(changelogSSP));
            LOG.info("Read offset {} for the store {} from logged storage partition directory {}", offset, storeName, loggedStorePartitionDir);

            if (offset.containsKey(changelogSSP)) {
              fileOffsets.put(changelogSSP, offset.get(changelogSSP));
            }
          }
        });
    }

    /**
     * Directory loggedStoreDir associated with the logged store storeName is determined to be valid
     * if all of the following conditions are true.
     * a) If the store has to be persisted to disk.
     * b) If there is a valid offset file associated with the logged store.
     * c) If the logged store has not gone stale.
     *
     * @return true if the logged store is valid, false otherwise.
     */
    private boolean isLoggedStoreValid(String storeName, File loggedStoreDir) {
      long changeLogDeleteRetentionInMs = StorageConfig.DEFAULT_CHANGELOG_DELETE_RETENTION_MS();

      if (new StorageConfig(config).getChangeLogDeleteRetentionsInMs().get(storeName).isDefined()) {
        changeLogDeleteRetentionInMs =
            (long) new StorageConfig(config).getChangeLogDeleteRetentionsInMs().get(storeName).get();
      }

      if (changelogSystemStreams.containsKey(storeName)) {
        SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStreams.get(storeName), taskModel.getChangelogPartition());
        return this.taskStores.get(storeName).getStoreProperties().isPersistedToDisk() && StorageManagerUtil.isOffsetFileValid(loggedStoreDir, Collections.singleton(changelogSSP))
            && !StorageManagerUtil.isStaleStore(loggedStoreDir, changeLogDeleteRetentionInMs, clock.currentTimeMillis());
      }

      return false;
    }

    /**
     * Create stores' base directories for logged-stores if they dont exist.
     */
    private void setupBaseDirs() {
      LOG.debug("Setting up base directories for stores.");
      taskStores.forEach((storeName, storageEngine) -> {
          if (storageEngine.getStoreProperties().isLoggedStore()) {

            File loggedStorePartitionDir =
                StorageManagerUtil.getStorePartitionDir(loggedStoreBaseDirectory, storeName, taskModel.getTaskName(), taskModel.getTaskMode());

            LOG.info("Using logged storage partition directory: " + loggedStorePartitionDir.toPath().toString()
                + " for store: " + storeName);

            if (!loggedStorePartitionDir.exists()) {
              loggedStorePartitionDir.mkdirs();
            }
          } else {
            File nonLoggedStorePartitionDir =
                StorageManagerUtil.getStorePartitionDir(nonLoggedStoreBaseDirectory, storeName, taskModel.getTaskName(), taskModel.getTaskMode());
            LOG.info("Using non logged storage partition directory: " + nonLoggedStorePartitionDir.toPath().toString()
                + " for store: " + storeName);
            nonLoggedStorePartitionDir.mkdirs();
          }
        });
    }

    /**
     *  Validates each changelog system-stream with its respective SystemAdmin.
     */
    private void validateChangelogStreams() {
      LOG.info("Validating change log streams: " + changelogSystemStreams);

      for (SystemStream changelogSystemStream : changelogSystemStreams.values()) {
        SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(changelogSystemStream.getSystem());
        StreamSpec changelogSpec =
            StreamSpec.createChangeLogStreamSpec(changelogSystemStream.getStream(), changelogSystemStream.getSystem(),
                maxChangeLogStreamPartitions);

        systemAdmin.validateStream(changelogSpec);
      }
    }

    /**
     * Get the oldest offset for each changelog SSP based on the stream's metadata (obtained from streamMetadataCache).
     */
    private void getOldestChangeLogOffsets() {

      Map<SystemStream, SystemStreamMetadata> changeLogMetadata = JavaConverters.mapAsJavaMapConverter(
          streamMetadataCache.getStreamMetadata(
              JavaConverters.asScalaSetConverter(new HashSet<>(changelogSystemStreams.values())).asScala().toSet(),
              false)).asJava();

      LOG.info("Got change log stream metadata: {}", changeLogMetadata);

      changeLogOldestOffsets =
          getChangeLogOldestOffsetsForPartition(taskModel.getChangelogPartition(), changeLogMetadata);
      LOG.info("Assigning oldest change log offsets for taskName {} : {}", taskModel.getTaskName(),
          changeLogOldestOffsets);
    }

    /**
     * Builds a map from SystemStreamPartition to oldest offset for changelogs.
     */
    private Map<SystemStream, String> getChangeLogOldestOffsetsForPartition(Partition partition,
        Map<SystemStream, SystemStreamMetadata> inputStreamMetadata) {

      Map<SystemStream, String> retVal = new HashMap<>();

      // NOTE: do not use Collectors.Map because of https://bugs.openjdk.java.net/browse/JDK-8148463
      inputStreamMetadata.entrySet()
          .stream()
          .filter(x -> x.getValue().getSystemStreamPartitionMetadata().get(partition) != null)
          .forEach(e -> retVal.put(e.getKey(),
              e.getValue().getSystemStreamPartitionMetadata().get(partition).getOldestOffset()));

      return retVal;
    }

    /**
     * Determines the starting offset for each store SSP (based on {@link #getStartingOffset(SystemStreamPartition, SystemAdmin)}) and
     * registers it with the respective SystemConsumer for starting consumption.
     */
    private void registerStartingOffsets() {

      for (Map.Entry<String, SystemStream> changelogSystemStreamEntry : changelogSystemStreams.entrySet()) {
        SystemStreamPartition systemStreamPartition =
            new SystemStreamPartition(changelogSystemStreamEntry.getValue(), taskModel.getChangelogPartition());
        SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(changelogSystemStreamEntry.getValue().getSystem());
        SystemConsumer systemConsumer = storeConsumers.get(changelogSystemStreamEntry.getKey());

        String offset = getStartingOffset(systemStreamPartition, systemAdmin);

        if (offset != null) {
          LOG.info("Registering change log consumer with offset " + offset + " for %" + systemStreamPartition);
          systemConsumer.register(systemStreamPartition, offset);
        } else {
          LOG.info("Skipping change log restoration for {} because stream appears to be empty (offset was null).",
              systemStreamPartition);
          taskStoresToRestore.remove(changelogSystemStreamEntry.getKey());
        }
      }
    }

    /**
     * Returns the offset with which the changelog consumer should be initialized for the given SystemStreamPartition.
     *
     * If a file offset exists, it represents the last changelog offset which is also reflected in the on-disk state.
     * In that case, we use the next offset after the file offset, as long as it is newer than the oldest offset
     * currently available in the stream.
     *
     * If there isn't a file offset or it's older than the oldest available offset, we simply start with the oldest.
     *
     * @param systemStreamPartition  the changelog partition for which the offset is needed.
     * @param systemAdmin                  the [[SystemAdmin]] for the changelog.
     * @return the offset to from which the changelog consumer should be initialized.
     */
    private String getStartingOffset(SystemStreamPartition systemStreamPartition, SystemAdmin systemAdmin) {
      String fileOffset = fileOffsets.get(systemStreamPartition);

      // NOTE: changeLogOldestOffsets may contain a null-offset for the given SSP (signifying an empty stream)
      // therefore, we need to differentiate that from the case where the offset is simply missing
      if (!changeLogOldestOffsets.containsKey(systemStreamPartition.getSystemStream())) {
        throw new SamzaException("Missing a change log offset for " + systemStreamPartition);
      }

      String oldestOffset = changeLogOldestOffsets.get(systemStreamPartition.getSystemStream());
      return StorageManagerUtil.getStartingOffset(systemStreamPartition, systemAdmin, fileOffset, oldestOffset);
    }


    /**
     * Restore each store in taskStoresToRestore sequentially
     */
    public void restoreStores() {
      LOG.debug("Restoring stores for task: {}", taskModel.getTaskName());

      for (String storeName : taskStoresToRestore) {
        SystemConsumer systemConsumer = storeConsumers.get(storeName);
        SystemStream systemStream = changelogSystemStreams.get(storeName);

        SystemStreamPartitionIterator systemStreamPartitionIterator = new SystemStreamPartitionIterator(systemConsumer,
            new SystemStreamPartition(systemStream, taskModel.getChangelogPartition()));

        taskStores.get(storeName).restore(systemStreamPartitionIterator);
      }
    }

    /**
     * Stop all stores.
     */
    public void stop() {
      this.taskStores.values().forEach(storageEngine -> {
          storageEngine.stop();
        });
    }

    /**
     * Stop only persistent stores. In case of certain stores and store mode (such as RocksDB), this
     * can invoke compaction.
     */
    public void stopPersistentStores() {

      Map<String, StorageEngine> persistentStores = this.taskStores.entrySet().stream().filter(e -> {
          return e.getValue().getStoreProperties().isPersistedToDisk();
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      persistentStores.forEach((storeName, storageEngine) -> {
          storageEngine.stop();
          this.taskStores.remove(storeName);
        });
      LOG.info("Stopped persistent stores {}", persistentStores);
    }
  }
}
