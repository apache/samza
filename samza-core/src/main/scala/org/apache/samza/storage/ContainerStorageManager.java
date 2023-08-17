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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeManager;
import org.apache.samza.storage.blobstore.BlobStoreStateBackendFactory;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskInstanceCollector;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private static final int RESTORE_THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS = 60;

  /** Maps containing relevant per-task objects */
  private final Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics;
  private final Map<TaskName, TaskInstanceCollector> taskInstanceCollectors;
  private final Map<TaskName, Map<String, StorageEngine>> inMemoryStores; // subset of taskStores after #start()
  private Map<TaskName, Map<String, StorageEngine>> taskStores; // Will be available in #start() after #restoreStores()

  private final Map<String, SystemConsumer> storeConsumers; // Mapping from store name to SystemConsumers
  private final Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories; // Map of storageEngineFactories indexed by store name
  private final Map<String, SystemStream> changelogSystemStreams;
  private final Map<String, SystemStream> activeTaskChangelogSystemStreams; // Map of changelog system-streams indexed by store name
  private final Map<String, Serde<Object>> serdes; // Map of Serde objects indexed by serde name (specified in config)
  private final SerdeManager serdeManager;
  private final SystemAdmins systemAdmins;
  private final Map<String, SystemFactory> systemFactories;
  private final Clock clock;
  private final Map<String, StateBackendFactory> restoreStateBackendFactories;

  private final Map<String, Set<SystemStream>> sideInputSystemStreams;
  private final StreamMetadataCache streamMetadataCache;
  private final SamzaContainerMetrics samzaContainerMetrics;

  private final CheckpointManager checkpointManager;
  /* Parameters required to re-create taskStores post-restoration */
  private final ContainerModel containerModel;
  private final JobContext jobContext;
  private final ContainerContext containerContext;

  private final File loggedStoreBaseDirectory;
  private final File nonLoggedStoreBaseDirectory;
  private final Set<Path> storeDirectoryPaths; // the set of store directory paths, used by SamzaContainer to initialize its disk-space-monitor

  private final ExecutorService restoreExecutor;

  private final Config config;
  private final StorageManagerUtil storageManagerUtil = new StorageManagerUtil();

  private final Set<String> sideInputStoreNames;
  private SideInputsManager sideInputsManager; // created in start() after restoreStores() for regular stores is complete.

  private boolean isStarted = false;

  public ContainerStorageManager(
      CheckpointManager checkpointManager,
      ContainerModel containerModel,
      StreamMetadataCache streamMetadataCache,
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
      Map<String, StateBackendFactory> restoreStateBackendFactories,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors,
      File loggedStoreBaseDirectory,
      File nonLoggedStoreBaseDirectory,
      SerdeManager serdeManager,
      Clock clock) {
    this.checkpointManager = checkpointManager;
    this.containerModel = containerModel;
    this.streamMetadataCache = streamMetadataCache;
    this.systemAdmins = systemAdmins;
    this.changelogSystemStreams = changelogSystemStreams;
    this.sideInputSystemStreams = sideInputSystemStreams;
    this.storageEngineFactories = storageEngineFactories;
    this.systemFactories = systemFactories;
    this.serdes = serdes;
    this.config = config;
    this.taskInstanceMetrics = taskInstanceMetrics;
    this.samzaContainerMetrics = samzaContainerMetrics;
    this.jobContext = jobContext;
    this.containerContext = containerContext;
    this.restoreStateBackendFactories = restoreStateBackendFactories;
    this.taskInstanceCollectors = taskInstanceCollectors;
    this.loggedStoreBaseDirectory = loggedStoreBaseDirectory;
    this.nonLoggedStoreBaseDirectory = nonLoggedStoreBaseDirectory;
    this.serdeManager = serdeManager;
    this.clock = clock;

    this.sideInputStoreNames =
        ContainerStorageManagerUtil.getSideInputStoreNames(sideInputSystemStreams, changelogSystemStreams, containerModel);
    this.activeTaskChangelogSystemStreams =
        ContainerStorageManagerUtil.getActiveTaskChangelogSystemStreams(changelogSystemStreams, containerModel);

    LOG.info("Starting with changelogSystemStreams = {} activeTaskChangelogSystemStreams = {} sideInputSystemStreams = {}",
        changelogSystemStreams, activeTaskChangelogSystemStreams, sideInputSystemStreams);

    if (loggedStoreBaseDirectory != null && loggedStoreBaseDirectory.equals(nonLoggedStoreBaseDirectory)) {
      LOG.warn("Logged and non-logged store base directory are configured to same path: {}. It is recommended to configure"
              + "them separately to ensure clean up of non-logged store data doesn't accidentally impact logged store data.",
          loggedStoreBaseDirectory);
    }

    this.storeDirectoryPaths = new HashSet<>();

    this.inMemoryStores = ContainerStorageManagerUtil.createInMemoryStores(
        activeTaskChangelogSystemStreams, storageEngineFactories, sideInputStoreNames,
        storeDirectoryPaths, containerModel, jobContext, containerContext,
        taskInstanceMetrics, taskInstanceCollectors, serdes, storageManagerUtil,
        loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory, config);

    // Refactor Note (prateekm): in previous version, there's a subtle difference between 'this.changelogSystemStreams'
    // (which is actually activeTaskChangelogSystemStreams) vs the passed in changelogSystemStreams.
    // create a map from storeNames to changelog system consumers (1 per system in activeTaskChangelogSystemStreams)
    this.storeConsumers = ContainerStorageManagerUtil.createStoreChangelogConsumers(
        activeTaskChangelogSystemStreams, systemFactories, samzaContainerMetrics.registry(), config);

    JobConfig jobConfig = new JobConfig(config);
    int restoreThreadPoolSize =
        Math.min(
            Math.max(containerModel.getTasks().size() * restoreStateBackendFactories.size() * 2,
                jobConfig.getRestoreThreadPoolSize()),
            jobConfig.getRestoreThreadPoolMaxSize()
        );
    this.restoreExecutor = Executors.newFixedThreadPool(restoreThreadPoolSize,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(RESTORE_THREAD_NAME).build());
  }

  /**
   * Starts all the task stores.
   * Returns the latest checkpoint for each task. This checkpoint may be different from the lastCheckpoint returned by
   * checkpoint manager in case of a BlobStoreRestoreManager.
   */
  public Map<TaskName, Checkpoint> start() throws SamzaException, InterruptedException {
    // Restores and recreates stores.
    Map<TaskName, Checkpoint> taskCheckpoints = restoreStores();

    // Shutdown restore executor since it will no longer be used
    try {
      restoreExecutor.shutdown();
      if (restoreExecutor.awaitTermination(RESTORE_THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.MILLISECONDS)) {
        restoreExecutor.shutdownNow();
      }
    } catch (Exception e) {
      LOG.error("Error shutting down restore executor", e);
    }

    // create and restore side input stores
    this.sideInputsManager = new SideInputsManager(
        sideInputSystemStreams, systemFactories,
        changelogSystemStreams, activeTaskChangelogSystemStreams,
        storageEngineFactories, storeDirectoryPaths,
        containerModel, jobContext, containerContext,
        samzaContainerMetrics, taskInstanceMetrics, taskInstanceCollectors,
        streamMetadataCache, systemAdmins, serdeManager, serdes, storageManagerUtil,
        loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory, config, clock);

    // blocks until initial side inputs restore is complete
    sideInputsManager.start();

    // Refactor Note (prateekm): In the previous version, side input stores were created in constructor,
    // and were added to taskStores (which are created in restoreStores()) at the end of restoreStores().
    // In this version, side input stores are created much later during start() (this method),
    // in the SideInputsManager constructor call above, and are added to taskStores here after side input restore
    // (in sideInputsManager.start()) is complete. Completing the side input restore first isn't strictly necessary.
    sideInputsManager.getSideInputStores().forEach((taskName, stores) -> {
      if (!this.taskStores.containsKey(taskName)) {
        taskStores.put(taskName, new HashMap<>());
      }
      taskStores.get(taskName).putAll(stores);
    });

    isStarted = true;
    return taskCheckpoints;
  }

  // Restoration of all stores, in parallel across tasks
  private Map<TaskName, Checkpoint> restoreStores() throws InterruptedException {
    LOG.info("Store Restore started");
    Set<TaskName> activeTasks = ContainerStorageManagerUtil.getTasks(containerModel, TaskMode.Active).keySet();
    // Find all non-side input stores
    Set<String> nonSideInputStoreNames = storageEngineFactories.keySet()
        .stream()
        .filter(storeName -> !sideInputStoreNames.contains(storeName))
        .collect(Collectors.toSet());

    // Obtain the checkpoints for each task
    Map<TaskName, Map<String, TaskRestoreManager>> taskRestoreManagers = new HashMap<>();
    Map<TaskName, Checkpoint> taskCheckpoints = new HashMap<>();
    Map<TaskName, Map<String, Set<String>>> taskBackendFactoryToStoreNames = new HashMap<>();
    containerModel.getTasks().forEach((taskName, taskModel) -> {
      Checkpoint taskCheckpoint = null;
      if (checkpointManager != null && activeTasks.contains(taskName)) {
        // only pass in checkpoints for active tasks
        taskCheckpoint = checkpointManager.readLastCheckpoint(taskName);
        LOG.info("Obtained checkpoint: {} for state restore for taskName: {}", taskCheckpoint, taskName);
      }
      taskCheckpoints.put(taskName, taskCheckpoint);

      Map<String, Set<String>> backendFactoryToStoreNames =
          ContainerStorageManagerUtil.getBackendFactoryStoreNames(
              nonSideInputStoreNames, taskCheckpoint, new StorageConfig(config));

      Map<String, Set<String>> backendFactoryToSideInputStoreNames =
          ContainerStorageManagerUtil.getBackendFactoryStoreNames(
              sideInputStoreNames, taskCheckpoint, new StorageConfig(config));

      // include side input stores for (initial bulk) restore if backed up using blob store state backend
      String blobStoreStateBackendFactory = BlobStoreStateBackendFactory.class.getName();
      if (backendFactoryToSideInputStoreNames.containsKey(blobStoreStateBackendFactory)) {
        Set<String> sideInputStoreNames = backendFactoryToSideInputStoreNames.get(blobStoreStateBackendFactory);

        if (backendFactoryToStoreNames.containsKey(blobStoreStateBackendFactory)) {
          backendFactoryToStoreNames.get(blobStoreStateBackendFactory).addAll(sideInputStoreNames);
        } else {
          backendFactoryToStoreNames.put(blobStoreStateBackendFactory, sideInputStoreNames);
        }
      }

      Map<String, TaskRestoreManager> taskStoreRestoreManagers =
          ContainerStorageManagerUtil.createTaskRestoreManagers(
              taskName, backendFactoryToStoreNames, restoreStateBackendFactories,
              storageEngineFactories, storeConsumers,
              inMemoryStores, systemAdmins, restoreExecutor,
              taskModel, jobContext, containerContext,
              samzaContainerMetrics, taskInstanceMetrics, taskInstanceCollectors, serdes,
              loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory, config, clock);
      taskRestoreManagers.put(taskName, taskStoreRestoreManagers);
      taskBackendFactoryToStoreNames.put(taskName, backendFactoryToStoreNames);
    });

    // Init all taskRestores and if successful, restores all the task stores concurrently
    LOG.debug("Pre init and restore checkpoints is: {}", taskCheckpoints);
    CompletableFuture<Map<TaskName, Checkpoint>> initRestoreAndNewCheckpointFuture =
        ContainerStorageManagerRestoreUtil.initAndRestoreTaskInstances(taskRestoreManagers, samzaContainerMetrics,
            checkpointManager, jobContext, containerModel, taskCheckpoints, taskBackendFactoryToStoreNames, config,
            restoreExecutor, taskInstanceMetrics, loggedStoreBaseDirectory, storeConsumers);

    // Update the task checkpoints map, if it was updated during the restore. Throw an exception if the restore or
    // creating a new checkpoint (in case of BlobStoreBackendFactory) failed.
    try {
      Map<TaskName, Checkpoint> newTaskCheckpoints = initRestoreAndNewCheckpointFuture.get();
      taskCheckpoints.putAll(newTaskCheckpoints);
      LOG.debug("Post init and restore checkpoints is: {}. NewTaskCheckpoints are: {}", taskCheckpoints, newTaskCheckpoints);
    } catch (InterruptedException e) {
      LOG.warn("Received an interrupt during store restoration. Interrupting the restore executor to exit "
          + "prematurely without restoring full state.");
      restoreExecutor.shutdownNow();
      throw e;
    } catch (Exception e) {
      LOG.error("Exception when restoring state.", e);
      throw new SamzaException("Exception when restoring state.", e);
    }

    // Stop each store consumer once
    this.storeConsumers.values().stream().distinct().forEach(SystemConsumer::stop);

    // Now create persistent non-side-input stores in read-write mode, leave non-persistent and side-input stores as-is
    Set<String> inMemoryStoreNames =
        ContainerStorageManagerUtil.getInMemoryStoreNames(this.storageEngineFactories, this.config);
    Set<String> storesToCreate = nonSideInputStoreNames.stream()
        .filter(s -> !inMemoryStoreNames.contains(s)).collect(Collectors.toSet());
    this.taskStores = ContainerStorageManagerUtil.createTaskStores(
        storesToCreate, this.storageEngineFactories, this.sideInputStoreNames,
        this.activeTaskChangelogSystemStreams, this.storeDirectoryPaths,
        this.containerModel, this.jobContext, this.containerContext,
        this.serdes, this.taskInstanceMetrics, this.taskInstanceCollectors, this.storageManagerUtil,
        this.loggedStoreBaseDirectory, this.nonLoggedStoreBaseDirectory, this.config);

    // Add in memory stores
    this.inMemoryStores.forEach((taskName, stores) -> {
      if (!this.taskStores.containsKey(taskName)) {
        taskStores.put(taskName, new HashMap<>());
      }
      taskStores.get(taskName).putAll(stores);
    });

    LOG.info("Store Restore complete");
    return taskCheckpoints;
  }

  /**
   * Get the {@link StorageEngine} instance with a given name for a given task.
   * @param taskName the task name for which the storage engine is desired.
   * @param storeName the desired store's name.
   * @return the task store.
   */
  public Optional<StorageEngine> getStore(TaskName taskName, String storeName) {
    if (!isStarted) {
      throw new SamzaException(String.format(
          "Attempting to access store %s for task %s before ContainerStorageManager is started.",
          storeName, taskName));
    }
    return Optional.ofNullable(this.taskStores.get(taskName).get(storeName));
  }

  /**
   * Get all {@link StorageEngine} instance used by a given task.
   * @param taskName the task name, all stores for which are desired.
   * @return map of stores used by the given task, indexed by storename
   */
  public Map<String, StorageEngine> getAllStores(TaskName taskName) {
    if (!isStarted) {
      throw new SamzaException(String.format(
          "Attempting to access stores for task %s before ContainerStorageManager is started.", taskName));
    }
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
    // stop all non side input stores including persistent and non-persistent stores
    if (taskStores != null) {
      this.containerModel.getTasks()
          .forEach((taskName, taskModel) -> taskStores.get(taskName)
              .entrySet().stream()
              .filter(e -> !sideInputStoreNames.contains(e.getKey()))
              .forEach(e -> e.getValue().stop()));
    }

    this.sideInputsManager.shutdown();
    LOG.info("Shutdown complete");
  }
}
