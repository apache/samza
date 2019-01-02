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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionIterator;
import org.apache.samza.task.TaskInstanceCollector;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 */
public class ContainerStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerStorageManager.class);

  // Naming convention to be used for restore threads
  private static final String RESTORE_THREAD_NAME = "Samza Restore Thread-%d";

  // Map of task stores for each task
  private final Map<TaskName, Map<String, StorageEngine>> taskStores;

  // Map of task restore managers for each task
  private final Map<TaskName, TaskRestoreManager> taskRestoreManagers;

  // Mapping of from storeSystemNames to SystemConsumers
  private final Map<String, SystemConsumer> systemConsumers;

  // Size of thread-pool to be used for parallel restores
  private final int parallelRestoreThreadPoolSize;

  private final Config config;

  // The partition count of each changelog-stream topic. This is used for validating changelog streams before restoring.
  private final int maxChangeLogStreamPartitions;

  private final StreamMetadataCache streamMetadataCache;
  private final SamzaContainerMetrics samzaContainerMetrics;

  /* Parameters required to re-create taskStores post-restoration */
  private final ContainerModel containerModel;
  private final JobContext jobContext;
  private final ContainerContext containerContext;
  private final Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories; // Map of storageEngineFactories indexed by store name
  private final Map<String, SystemStream> changelogSystemStreams; // Map of changelog system-streams indexed by store name
  private final Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics; // Map of taskInstance metrics indexed by task name
  private final Map<TaskName, TaskInstanceCollector> taskInstanceCollectors; // Map of task instance collectors indexed by task name
  private final Map<String, Serde<Object>> serdes; // Map of Serde objects indexed by serde name (specified in config)

  private final Optional<File> loggedStoreBaseDirectory; // The base directory to use for logged stores' files, if not specified defaults to a default dir.
  private final Optional<File> nonLoggedStoreBaseDirectory; // The base directory to use for nonLogged stores' files, if not specified defaults to a default direct

  // the set of store directory paths, used by SamzaContainer to initialize its disk-space-monitor
  private final Set<Path> storeDirectoryPaths;

  public ContainerStorageManager(ContainerModel containerModel, StreamMetadataCache streamMetadataCache,
      SystemAdmins systemAdmins, Map<String, SystemStream> changelogSystemStreams,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, SystemFactory> systemFactories, Map<String, Serde<Object>> serdes, Config config,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics, SamzaContainerMetrics samzaContainerMetrics,
      JobContext jobContext, ContainerContext containerContext,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors, Optional<File> loggedStoreBaseDirectory,
      Optional<File> nonLoggedStoreBaseDirectory, int maxChangeLogStreamPartitions, Clock clock) {

    this.containerModel = containerModel;
    this.changelogSystemStreams = changelogSystemStreams;
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

    // initialize the taskStores
    this.taskStores = new HashMap<>();
    createTaskStores(containerModel, jobContext, containerContext, storageEngineFactories, changelogSystemStreams,
        serdes, taskInstanceMetrics, taskInstanceCollectors, StorageEngineFactory.StoreMode.BulkLoad);

    // create system consumers (1 per store system)
    this.systemConsumers = createStoreConsumers(changelogSystemStreams, systemFactories, config, this.samzaContainerMetrics.registry());

    // creating task restore managers
    this.taskRestoreManagers = containerModel.getTasks().entrySet().stream().
        collect(Collectors.toMap(entry -> entry.getKey(),
            entry -> new TaskRestoreManager(entry.getValue(), changelogSystemStreams, taskStores.get(entry.getKey()),
                systemAdmins, clock)));
  }

  /**
   *  Creates SystemConsumer objects for store restoration, creating one consumer per system.
   */
  private static Map<String, SystemConsumer> createStoreConsumers(Map<String, SystemStream> changelogSystemStreams,
      Map<String, SystemFactory> systemFactories, Config config, MetricsRegistry registry) {
    // Determine the set of systems being used across all stores
    Set<String> storeSystems =
        changelogSystemStreams.values().stream().map(SystemStream::getSystem).collect(Collectors.toSet());

    // Create one consumer for each system in use, map with one entry for each such system
    Map<String, SystemConsumer> storeSystemConsumers = new HashMap<>();

    // Map of each storeName to its respective systemConsumer
    Map<String, SystemConsumer> storeConsumers = new HashMap<>();

    // Iterate over the list of storeSystems and create one sysConsumer per system
    for (String storeSystemName : storeSystems) {
      SystemFactory systemFactory = systemFactories.get(storeSystemName);
      if (systemFactory == null) {
        throw new SamzaException("Changelog system " + storeSystemName + " does not exist in config");
      }
      storeSystemConsumers.put(storeSystemName,
          systemFactory.getConsumer(storeSystemName, config, registry));
    }

    // Populate the map of storeName to its relevant systemConsumer
    for (String storeName : changelogSystemStreams.keySet()) {
      storeConsumers.put(storeName, storeSystemConsumers.get(changelogSystemStreams.get(storeName).getSystem()));
    }

    return storeConsumers;
  }

  /**
   * Create taskStores with the given store mode.
   */
  private void createTaskStores(ContainerModel containerModel, JobContext jobContext, ContainerContext containerContext,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, SystemStream> changelogSystemStreams, Map<String, Serde<Object>> serdes,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors, StorageEngineFactory.StoreMode storeMode) {

    // iterate over each task and each storeName
    for (Map.Entry<TaskName, TaskModel> task : containerModel.getTasks().entrySet()) {
      TaskName taskName = task.getKey();
      TaskModel taskModel = task.getValue();

      for (String storeName : storageEngineFactories.keySet()) {

        if (!this.taskStores.containsKey(taskName)) {
          this.taskStores.put(taskName, new HashMap<>());
        }

        StorageEngine storageEngine =
            createStore(storeName, taskName, taskModel, jobContext, containerContext, storageEngineFactories,
                changelogSystemStreams, serdes, taskInstanceMetrics, taskInstanceCollectors, storeMode);

        // add created store to map
        this.taskStores.get(taskName).put(storeName, storageEngine);

        LOG.info("Created store {} for task {}", storeName, taskName);
      }
    }
  }

  /**
   * Recreate all persistent stores in ReadWrite mode.
   *
   */
  private void recreatePersistentTaskStoresInReadWriteMode(ContainerModel containerModel, JobContext jobContext,
      ContainerContext containerContext, Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, SystemStream> changelogSystemStreams, Map<String, Serde<Object>> serdes,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors) {

    // iterate over each task and each storeName
    for (Map.Entry<TaskName, TaskModel> task : containerModel.getTasks().entrySet()) {
      TaskName taskName = task.getKey();
      TaskModel taskModel = task.getValue();

      for (String storeName : storageEngineFactories.keySet()) {

        // if this store has been already created in the taskStores, then re-create and overwrite it only if it is a persistentStore
        if (this.taskStores.get(taskName).containsKey(storeName) && this.taskStores.get(taskName)
            .get(storeName)
            .getStoreProperties()
            .isPersistedToDisk()) {

          StorageEngine storageEngine =
              createStore(storeName, taskName, taskModel, jobContext, containerContext, storageEngineFactories,
                  changelogSystemStreams, serdes, taskInstanceMetrics, taskInstanceCollectors,
                  StorageEngineFactory.StoreMode.ReadWrite);

          // add created store to map
          this.taskStores.get(taskName).put(storeName, storageEngine);

          LOG.info("Re-created store {} in read-write mode for task {} because it a persistent store", storeName, taskName);
        } else {

          LOG.info("Skipping re-creation of store {} for task {} because it a non-persistent store", storeName, taskName);
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
      Map<String, SystemStream> changelogSystemStreams, Map<String, Serde<Object>> serdes,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors, StorageEngineFactory.StoreMode storeMode) {

    StorageConfig storageConfig = new StorageConfig(config);

    SystemStreamPartition changeLogSystemStreamPartition =
        (changelogSystemStreams.containsKey(storeName)) ? new SystemStreamPartition(
            changelogSystemStreams.get(storeName), taskModel.getChangelogPartition()) : null;

    // Use the logged-store-base-directory for change logged stores, and non-logged-store-base-dir for non logged stores
    File storeDirectory =
        (changeLogSystemStreamPartition != null) ? StorageManagerUtil.getStorePartitionDir(getLoggedStorageBaseDir(),
            storeName, taskName)
            : StorageManagerUtil.getStorePartitionDir(getNonLoggedStorageBaseDir(), storeName, taskName);
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

  public void start() throws SamzaException {
    LOG.info("Restore started");

    // initialize each TaskStorageManager
    this.taskRestoreManagers.values().forEach(taskStorageManager -> taskStorageManager.initialize());

    // Start consumers
    this.systemConsumers.values().forEach(systemConsumer -> systemConsumer.start());

    // Create a thread pool for parallel restores (and stopping of persistent stores)
    ExecutorService executorService = Executors.newFixedThreadPool(this.parallelRestoreThreadPoolSize,
        new ThreadFactoryBuilder().setNameFormat(RESTORE_THREAD_NAME).build());

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

    // Stop consumers
    this.systemConsumers.values().forEach(systemConsumer -> systemConsumer.stop());

    // Now re-create persistent stores in read-write mode, leave non-persistent stores as-is
    recreatePersistentTaskStoresInReadWriteMode(this.containerModel, jobContext, containerContext,
        storageEngineFactories, changelogSystemStreams, serdes, taskInstanceMetrics, taskInstanceCollectors);

    LOG.info("Restore complete");
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

  /**
   * Obtain the base directory used by this {@link ContainerStorageManager} for all non-logged stores.
   * If a base-directory was explicitly provided during instantiation (e.g., when instantiated by the StorageRecovery tool),
   * it is used, otherwise the base directory is obtained from SamzaContainer.
   *
   * @return base directory to be used for non-logged stores
   */
  public final File getNonLoggedStorageBaseDir() {

    // if a loggedStoreBaseDirectory is explicitly specified then use that
    if (nonLoggedStoreBaseDirectory.isPresent()) {
      return nonLoggedStoreBaseDirectory.get();
    }
    return SamzaContainer.getNonLoggedStorageBaseDir(config, getDefaultStoreBaseDir());
  }

  /**
   * Obtain the base directory used by this {@link ContainerStorageManager} for all change-logged stores.
   * If a base-directory was explicitly provided during instantiation (e.g., when instantiated by the StorageRecovery tool),
   * it is used, otherwise the base directory is obtained from SamzaContainer
   *
   * @return base directory to be used for logged stores
   */
  public final File getLoggedStorageBaseDir() {

    // if a loggedStoreBaseDirectory is explicitly specified then use that
    if (loggedStoreBaseDirectory.isPresent()) {
      return loggedStoreBaseDirectory.get();
    }

    return SamzaContainer.getLoggedStorageBaseDir(config, getDefaultStoreBaseDir());
  }

  /**
   * Construct a default store base directory using the <user.dir> and <state> System properties.
   */
  private final File getDefaultStoreBaseDir() {
    File defaultStoreBaseDir = new File(System.getProperty("user.dir"), "state");
    LOG.info("Got default storage engine base directory " + defaultStoreBaseDir);
    return defaultStoreBaseDir;
  }

  @VisibleForTesting
  public void stopStores() {
    this.taskStores.forEach((taskName, storeMap) -> storeMap.forEach((storeName, store) -> store.stop()));
  }

  public void shutdown() {
    this.taskRestoreManagers.forEach((taskInstance, taskRestoreManager) -> {
        if (taskRestoreManager != null) {
          LOG.debug("Shutting down task storage manager for taskName: {} ", taskInstance);
          taskRestoreManager.stop();
        } else {
          LOG.debug("Skipping task storage manager shutdown for taskName: {}", taskInstance);
        }
      });

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

    private final static String OFFSET_FILE_NAME = "OFFSET";
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
              StorageManagerUtil.getStorePartitionDir(getNonLoggedStorageBaseDir(), storeName, taskModel.getTaskName());
          LOG.info("Got non logged storage partition directory as " + nonLoggedStorePartitionDir.toPath().toString());

          if (nonLoggedStorePartitionDir.exists()) {
            LOG.info("Deleting non logged storage partition directory " + nonLoggedStorePartitionDir.toPath().toString());
            FileUtil.rm(nonLoggedStorePartitionDir);
          }

          File loggedStorePartitionDir =
              StorageManagerUtil.getStorePartitionDir(getLoggedStorageBaseDir(), storeName, taskModel.getTaskName());
          LOG.info("Got logged storage partition directory as " + loggedStorePartitionDir.toPath().toString());

          // Delete the logged store if it is not valid.
          if (!isLoggedStoreValid(storeName, loggedStorePartitionDir)) {
            LOG.info("Deleting logged storage partition directory " + loggedStorePartitionDir.toPath().toString());
            FileUtil.rm(loggedStorePartitionDir);
          } else {
            String offset = StorageManagerUtil.readOffsetFile(loggedStorePartitionDir, OFFSET_FILE_NAME);
            LOG.info("Read offset " + offset + " for the store " + storeName + " from logged storage partition directory "
                + loggedStorePartitionDir);

            if (offset != null) {
              fileOffsets.put(
                  new SystemStreamPartition(changelogSystemStreams.get(storeName), taskModel.getChangelogPartition()),
                  offset);
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

      return this.taskStores.get(storeName).getStoreProperties().isPersistedToDisk()
          && StorageManagerUtil.isOffsetFileValid(loggedStoreDir, OFFSET_FILE_NAME) && !StorageManagerUtil.isStaleStore(
          loggedStoreDir, OFFSET_FILE_NAME, changeLogDeleteRetentionInMs, clock.currentTimeMillis());
    }

    /**
     * Create stores' base directories for logged-stores if they dont exist.
     */
    private void setupBaseDirs() {
      LOG.debug("Setting up base directories for stores.");
      taskStores.forEach((storeName, storageEngine) -> {
          if (storageEngine.getStoreProperties().isLoggedStore()) {

            File loggedStorePartitionDir =
                StorageManagerUtil.getStorePartitionDir(getLoggedStorageBaseDir(), storeName, taskModel.getTaskName());

            LOG.info("Using logged storage partition directory: " + loggedStorePartitionDir.toPath().toString()
                + " for store: " + storeName);

            if (!loggedStorePartitionDir.exists()) {
              loggedStorePartitionDir.mkdirs();
            }
          } else {
            File nonLoggedStorePartitionDir =
                StorageManagerUtil.getStorePartitionDir(getNonLoggedStorageBaseDir(), storeName, taskModel.getTaskName());
            LOG.info("Using non logged storage partition directory: " + nonLoggedStorePartitionDir.toPath().toString()
                + " for store: " + storeName);
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
      LOG.info("Assigning oldest change log offsets for taskName {}:{}", taskModel.getTaskName(),
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
        SystemConsumer systemConsumer = systemConsumers.get(changelogSystemStreamEntry.getKey());

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
        SystemConsumer systemConsumer = systemConsumers.get(storeName);
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
      this.taskStores.values().stream().filter(storageEngine -> {
          return storageEngine.getStoreProperties().isPersistedToDisk();
        }).forEach(storageEngine -> {
            storageEngine.stop();
          });
    }
  }
}
