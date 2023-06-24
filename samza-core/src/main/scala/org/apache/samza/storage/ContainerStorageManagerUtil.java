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

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.config.BlobStoreConfig;
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
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.blobstore.BlobStoreBackupManager;
import org.apache.samza.storage.blobstore.BlobStoreManager;
import org.apache.samza.storage.blobstore.BlobStoreManagerFactory;
import org.apache.samza.storage.blobstore.BlobStoreRestoreManager;
import org.apache.samza.storage.blobstore.BlobStoreStateBackendFactory;
import org.apache.samza.storage.blobstore.exceptions.DeletedException;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.metrics.BlobStoreBackupManagerMetrics;
import org.apache.samza.storage.blobstore.metrics.BlobStoreRestoreManagerMetrics;
import org.apache.samza.storage.blobstore.util.BlobStoreUtil;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskInstanceCollector;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FutureUtil;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerStorageManagerUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerStorageManagerUtil.class);

  /**
   * Create taskStores for all stores in storesToCreate.
   * The store mode is chosen as read-write mode.
   * Adds newly created stores to storeDirectoryPaths.
   */
  public static Map<TaskName, Map<String, StorageEngine>> createTaskStores(Set<String> storesToCreate,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Set<String> sideInputStoreNames,
      Map<String, SystemStream> activeTaskChangelogSystemStreams,
      Set<Path> storeDirectoryPaths,
      ContainerModel containerModel, JobContext jobContext, ContainerContext containerContext,
      Map<String, Serde<Object>> serdes,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors,
      StorageManagerUtil storageManagerUtil,
      File loggedStoreBaseDirectory, File nonLoggedStoreBaseDirectory,
      Config config) {
    Map<TaskName, Map<String, StorageEngine>> taskStores = new HashMap<>();
    StorageConfig storageConfig = new StorageConfig(config);

    // iterate over each task and each storeName
    for (Map.Entry<TaskName, TaskModel> task : containerModel.getTasks().entrySet()) {
      TaskName taskName = task.getKey();
      TaskModel taskModel = task.getValue();
      if (!taskStores.containsKey(taskName)) {
        taskStores.put(taskName, new HashMap<>());
      }

      for (String storeName : storesToCreate) {
        List<String> storeBackupManagers = storageConfig.getStoreBackupFactories(storeName);
        // A store is considered durable if it is backed by a changelog or another backupManager factory
        boolean isDurable = activeTaskChangelogSystemStreams.containsKey(storeName) || !storeBackupManagers.isEmpty();
        boolean isSideInput = sideInputStoreNames.contains(storeName);
        // Use the logged-store-base-directory for change logged stores and sideInput stores, and non-logged-store-base-dir
        // for non logged stores
        File storeBaseDir = isDurable || isSideInput ? loggedStoreBaseDirectory : nonLoggedStoreBaseDirectory;
        File storeDirectory = storageManagerUtil.getTaskStoreDir(storeBaseDir, storeName, taskName,
            taskModel.getTaskMode());
        storeDirectoryPaths.add(storeDirectory.toPath());

        // if taskInstanceMetrics are specified use those for store metrics,
        // otherwise (in case of StorageRecovery) use a blank MetricsRegistryMap
        MetricsRegistry storeMetricsRegistry =
            taskInstanceMetrics.get(taskName) != null ?
                taskInstanceMetrics.get(taskName).registry() : new MetricsRegistryMap();

        StorageEngine storageEngine =
            createStore(storeName, storeDirectory, StorageEngineFactory.StoreMode.ReadWrite,
                storageEngineFactories, activeTaskChangelogSystemStreams,
                taskModel, jobContext, containerContext,
                serdes, storeMetricsRegistry, taskInstanceCollectors.get(taskName),
                config);

        // add created store to map
        taskStores.get(taskName).put(storeName, storageEngine);

        LOG.info("Created task store {} in read-write mode for task {} in path {}",
            storeName, taskName, storeDirectory.getAbsolutePath());
      }
    }
    return taskStores;
  }

  /**
   * Method to instantiate a StorageEngine with the given parameters, and populate the storeDirectory paths (used to monitor
   * disk space).
   */
  public static StorageEngine createStore(
      String storeName,
      File storeDirectory,
      StorageEngineFactory.StoreMode storeMode,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, SystemStream> changelogSystemStreams,
      TaskModel taskModel, JobContext jobContext, ContainerContext containerContext,
      Map<String, Serde<Object>> serdes,
      MetricsRegistry storeMetricsRegistry,
      MessageCollector messageCollector,
      Config config) {

    StorageConfig storageConfig = new StorageConfig(config);
    SystemStreamPartition changeLogSystemStreamPartition = changelogSystemStreams.containsKey(storeName) ?
        new SystemStreamPartition(changelogSystemStreams.get(storeName), taskModel.getChangelogPartition()) : null;

    Optional<String> storageKeySerde = storageConfig.getStorageKeySerde(storeName);
    Serde keySerde = null;
    if (storageKeySerde.isPresent()) {
      keySerde = serdes.get(storageKeySerde.get());
    }
    Optional<String> storageMsgSerde = storageConfig.getStorageMsgSerde(storeName);
    Serde messageSerde = null;
    if (storageMsgSerde.isPresent()) {
      messageSerde = serdes.get(storageMsgSerde.get());
    }

    return storageEngineFactories.get(storeName)
        .getStorageEngine(storeName, storeDirectory, keySerde, messageSerde, messageCollector,
            storeMetricsRegistry, changeLogSystemStreamPartition, jobContext, containerContext, storeMode);
  }

  public static Map<TaskName, Map<String, StorageEngine>> createInMemoryStores(
      Map<String, SystemStream> activeTaskChangelogSystemStreams,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Set<String> sideInputStoreNames,
      Set<Path> storeDirectoryPaths,
      ContainerModel containerModel, JobContext jobContext, ContainerContext containerContext,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors,
      Map<String, Serde<Object>> serdes,
      StorageManagerUtil storageManagerUtil,
      File loggedStoreBaseDirectory, File nonLoggedStoreBaseDirectory,
      Config config) {
    Set<String> inMemoryStoreNames = getInMemoryStoreNames(storageEngineFactories, config);
    return ContainerStorageManagerUtil.createTaskStores(
        inMemoryStoreNames, storageEngineFactories, sideInputStoreNames,
        activeTaskChangelogSystemStreams, storeDirectoryPaths,
        containerModel, jobContext, containerContext, serdes,
        taskInstanceMetrics, taskInstanceCollectors, storageManagerUtil,
        loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory, config);
  }

  public static Set<String> getInMemoryStoreNames(
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Config config) {
    StorageConfig storageConfig = new StorageConfig(config);
    return storageEngineFactories.keySet().stream()
        .filter(storeName -> {
          Optional<String> storeFactory = storageConfig.getStorageFactoryClassName(storeName);
          return storeFactory.isPresent() && storeFactory.get()
              .equals(StorageConfig.INMEMORY_KV_STORAGE_ENGINE_FACTORY);
        })
        .collect(Collectors.toSet());
  }

  /**
   * Returns a map from store names to their corresponding changelog system consumers.
   * @return map of store name to its changelog system consumer.
   */
  public static Map<String, SystemConsumer> createStoreChangelogConsumers(
      Map<String, SystemStream> activeTaskChangelogSystemStreams,
      Map<String, SystemFactory> systemFactories, MetricsRegistry registry, Config config) {
    Set<String> activeTaskChangelogSystems = activeTaskChangelogSystemStreams.values().stream()
        .map(SystemStream::getSystem)
        .collect(Collectors.toSet());

    Map<String, SystemConsumer> systemNameToSystemConsumers = ContainerStorageManagerUtil.createSystemConsumers(
        activeTaskChangelogSystems, systemFactories, registry, config);

    // Map of each storeName to its respective systemConsumer
    Map<String, SystemConsumer> storeConsumers = new HashMap<>();

    // Populate the map of storeName to its relevant systemConsumer
    for (String storeName : activeTaskChangelogSystemStreams.keySet()) {
      storeConsumers.put(storeName,
          systemNameToSystemConsumers.get(activeTaskChangelogSystemStreams.get(storeName).getSystem()));
    }
    return storeConsumers;
  }

  /**
   * Creates SystemConsumers for store restore, one consumer per system.
   * @return map of system name to its system consumer.
   */
  public static Map<String, SystemConsumer> createSystemConsumers(Set<String> storeSystems,
      Map<String, SystemFactory> systemFactories, MetricsRegistry registry, Config config) {
    // Create one consumer for each system in use, map with one entry for each such system
    Map<String, SystemConsumer> consumers = new HashMap<>();

    // Iterate over the list of storeSystems and create one sysConsumer per system
    for (String storeSystemName : storeSystems) {
      SystemFactory systemFactory = systemFactories.get(storeSystemName);
      if (systemFactory == null) {
        throw new SamzaException("System " + storeSystemName + " does not exist in config");
      }
      consumers.put(storeSystemName, systemFactory.getConsumer(storeSystemName, config, registry));
    }

    return consumers;
  }

  public static Map<String, TaskRestoreManager> createTaskRestoreManagers(TaskName taskName,
      Map<String, Set<String>> backendFactoryStoreNames,
      Map<String, StateBackendFactory> stateBackendFactories,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, SystemConsumer> storeConsumers,
      Map<TaskName, Map<String, StorageEngine>> inMemoryStores,
      SystemAdmins systemAdmins,
      ExecutorService restoreExecutor,
      TaskModel taskModel, JobContext jobContext, ContainerContext containerContext,
      SamzaContainerMetrics samzaContainerMetrics,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors,
      Map<String, Serde<Object>> serdes,
      File loggedStoreBaseDirectory, File nonLoggedStoreBaseDirectory,
      Config config, Clock clock) {
    // Get the factories for the task based on the stores of the tasks to be restored from the factory
    Map<String, TaskRestoreManager> backendFactoryRestoreManagers = new HashMap<>(); // backendFactoryName -> restoreManager
    MetricsRegistry taskMetricsRegistry =
        taskInstanceMetrics.get(taskName) != null ?
            taskInstanceMetrics.get(taskName).registry() : new MetricsRegistryMap();

    backendFactoryStoreNames.forEach((factoryName, storeNames) -> {
      StateBackendFactory factory = stateBackendFactories.get(factoryName);
      if (factory == null) {
        throw new SamzaException(
            String.format("Required restore state backend factory: %s not found in configured factories %s",
                factoryName, String.join(", ", stateBackendFactories.keySet())));
      }
      KafkaChangelogRestoreParams kafkaChangelogRestoreParams = new KafkaChangelogRestoreParams(storeConsumers,
          inMemoryStores.get(taskName), systemAdmins.getSystemAdmins(), storageEngineFactories, serdes,
          taskInstanceCollectors.get(taskName));
      TaskRestoreManager restoreManager = factory.getRestoreManager(jobContext, containerContext, taskModel,
          restoreExecutor, taskMetricsRegistry, storeNames, config, clock,
          loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory,
          kafkaChangelogRestoreParams);

      backendFactoryRestoreManagers.put(factoryName, restoreManager);
    });
    samzaContainerMetrics.addStoresRestorationGauge(taskName);
    return backendFactoryRestoreManagers;
  }

  /**
   * Returns a map of storeNames to changelogSSPs associated with the active tasks.
   * The standby changelogs will be consumed and restored as side inputs.
   *
   * @param changelogSystemStreams the passed in map of storeName to changelogSystemStreams
   * @param containerModel the container's model
   * @return A map of storeName to changelogSSPs across all active tasks, assuming no two stores have the same changelogSSP
   */
  public static Map<String, SystemStream> getActiveTaskChangelogSystemStreams(
      Map<String, SystemStream> changelogSystemStreams, ContainerModel containerModel) {
    if (MapUtils.invertMap(changelogSystemStreams).size() != changelogSystemStreams.size()) {
      throw new SamzaException("Two stores cannot have the same changelog system-stream");
    }

    Map<SystemStreamPartition, String> changelogSSPToStore = new HashMap<>();
    changelogSystemStreams.forEach((storeName, systemStream) ->
        containerModel.getTasks().forEach((taskName, taskModel) ->
            changelogSSPToStore.put(new SystemStreamPartition(systemStream, taskModel.getChangelogPartition()), storeName))
    );

    getTasks(containerModel, TaskMode.Standby).forEach((taskName, taskModel) -> {
      changelogSystemStreams.forEach((storeName, systemStream) -> {
        SystemStreamPartition ssp = new SystemStreamPartition(systemStream, taskModel.getChangelogPartition());
        changelogSSPToStore.remove(ssp);
      });
    });

    // changelogSystemStreams correspond only to active tasks (since those of standby-tasks moved to sideInputs above)
    return MapUtils.invertMap(changelogSSPToStore).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getSystemStream()));
  }

  /**
   * Returns a map of backend factory names to subset of provided storeNames that should be restored using it.
   * For CheckpointV1, only includes stores that should be restored using a configured changelog.
   * For CheckpointV2, associates stores with the highest precedence configured restore factory that has a SCM in
   * the checkpoint, or the highest precedence restore factory configured if there are no SCMs in the checkpoint.
   */
  public static Map<String, Set<String>> getBackendFactoryStoreNames(
      Set<String> storeNames, Checkpoint checkpoint, StorageConfig storageConfig) {
    Map<String, Set<String>> backendFactoryStoreNames = new HashMap<>(); // backendFactoryName -> set(storeNames)

    if (checkpoint != null && checkpoint.getVersion() == 1) {
      // Only restore stores with changelog streams configured
      Set<String> changelogStores = storeNames.stream()
          .filter(storeName -> storageConfig.getChangelogStream(storeName).isPresent())
          .collect(Collectors.toSet());
      // Default to changelog backend factory when using checkpoint v1 for backwards compatibility
      if (!changelogStores.isEmpty()) {
        backendFactoryStoreNames.put(StorageConfig.KAFKA_STATE_BACKEND_FACTORY, changelogStores);
      }
      if (storeNames.size() > changelogStores.size()) {
        Set<String> nonChangelogStores = storeNames.stream()
            .filter(storeName -> !changelogStores.contains(storeName))
            .collect(Collectors.toSet());
        LOG.info("Stores: {}, do not have a configured store changelogs for checkpoint V1,"
                + "changelog restore for the store will be skipped.",
            nonChangelogStores);
      }
    } else if (checkpoint == null ||  checkpoint.getVersion() == 2) {
      // Extract the state checkpoint markers if checkpoint exists
      Map<String, Map<String, String>> stateCheckpointMarkers = checkpoint == null ? Collections.emptyMap() :
          ((CheckpointV2) checkpoint).getStateCheckpointMarkers();

      // Find stores associated to each state backend factory
      storeNames.forEach(storeName -> {
        List<String> storeFactories = storageConfig.getStoreRestoreFactories(storeName);

        if (storeFactories.isEmpty()) {
          // If the restore factory is not configured for the store and the store does not have a changelog topic
          LOG.info("Store: {} does not have a configured restore factory or a changelog topic, "
                  + "restore for the store will be skipped.",
              storeName);
        } else {
          // Search the ordered list for the first matched state backend factory in the checkpoint
          // If the checkpoint does not exist or state checkpoint markers does not exist, we match the first configured
          // restore manager
          Optional<String> factoryNameOpt = storeFactories.stream()
              .filter(factoryName -> stateCheckpointMarkers.containsKey(factoryName) &&
                  stateCheckpointMarkers.get(factoryName).containsKey(storeName))
              .findFirst();
          String factoryName;
          if (factoryNameOpt.isPresent()) {
            factoryName = factoryNameOpt.get();
          } else { // Restore factories configured but no checkpoints found
            // Use first configured restore factory
            factoryName = storeFactories.get(0);
            LOG.warn("No matching SCMs found for configured restore factories: {} for storeName: {}, " +
                "defaulting to using the first configured factory with no SCM.", storeFactories, storeName);
          }
          if (!backendFactoryStoreNames.containsKey(factoryName)) {
            backendFactoryStoreNames.put(factoryName, new HashSet<>());
          }
          backendFactoryStoreNames.get(factoryName).add(storeName);
        }
      });
    } else {
      throw new SamzaException(String.format("Unsupported checkpoint version %s", checkpoint.getVersion()));
    }
    return backendFactoryStoreNames;
  }

  // Helper method to filter active Tasks from the container model
  public static Map<TaskName, TaskModel> getTasks(ContainerModel containerModel, TaskMode taskMode) {
    return containerModel.getTasks().entrySet().stream()
        .filter(x -> x.getValue().getTaskMode().equals(taskMode))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Fetch the side input stores. For active containers, the stores correspond to the side inputs and for standbys, they
   * include the durable stores.
   * @param sideInputSystemStreams the map of store to side input system streams
   * @param changelogSystemStreams the map of store to changelog system streams
   * @param containerModel the container's model
   * @return A set of side input stores
   */
  public static Set<String> getSideInputStoreNames(
      Map<String, Set<SystemStream>> sideInputSystemStreams,
      Map<String, SystemStream> changelogSystemStreams,
      ContainerModel containerModel) {
    // add all the side input stores by default regardless of active vs standby
    Set<String> sideInputStores = new HashSet<>(sideInputSystemStreams.keySet());

    // In case of standby tasks, we treat the stores that have changelogs as side input stores for bootstrapping state
    if (getTasks(containerModel, TaskMode.Standby).size() > 0) {
      sideInputStores.addAll(changelogSystemStreams.keySet());
    }
    return sideInputStores;
  }

  public static List<Future<Void>> initAndRestoreTaskInstances(Map<TaskName, Map<String, TaskRestoreManager>> taskRestoreManagers,
      SamzaContainerMetrics samzaContainerMetrics, CheckpointManager checkpointManager, JobContext jobContext,
      ContainerModel containerModel, Map<TaskName, Checkpoint> taskCheckpoints,
      Map<TaskName, Map<String, Set<String>>> taskBackendFactoryToStoreNames, Config config, ExecutorService executor,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics, File loggerStoreDir) {

    // Initialize each TaskStorageManager.
    taskRestoreManagers.forEach((taskName, restoreManagers) ->
        restoreManagers.forEach((factoryName, taskRestoreManager) -> {
      try {
        taskRestoreManager.init(taskCheckpoints.get(taskName));
      } catch (DeletedException ex) {
        LOG.warn("Init for BlobStoreRestoreManager failed with DeletedException for task: {}, checkpoint: {}. "
            + "Retying with getDeletedEnabled", taskName, taskCheckpoints.get(taskName));
        if (taskRestoreManager instanceof BlobStoreRestoreManager) {

          // try to get the deleted SnapshotIndexes, recreate new SnapshotIndex blobs and return a new StateCheckpointMarker
          Map<String, String> newSCMs =
              getAndRecreateDeletedSnapshotIndexBlob(checkpointManager, taskName, config, executor, taskInstanceMetrics);

          // Write a new checkpoint with new snapshot index blobs
          CheckpointId checkpointId = CheckpointId.create();
          Checkpoint newCheckpoint = writeNewCheckpoint(taskName, checkpointId, newSCMs, checkpointManager);
          //update TaskCheckpoints map with new checkpoint
          taskCheckpoints.put(taskName, newCheckpoint);

          // init again - if it fails again, do not reattempt
          BlobStoreRestoreManager blobStoreRestoreManager = (BlobStoreRestoreManager) taskRestoreManager;
          blobStoreRestoreManager.init(taskCheckpoints.get(taskName), true);
        }
      }
    }));

    return restoreAllTaskInstances(taskRestoreManagers, taskBackendFactoryToStoreNames, jobContext, containerModel,
        samzaContainerMetrics, checkpointManager, config, taskInstanceMetrics, executor, loggerStoreDir);
  }

  private static Map<String, String> getAndRecreateDeletedSnapshotIndexBlob(CheckpointManager checkpointManager,
      TaskName taskName, Config config, ExecutorService executor, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics) {
    Checkpoint oldCheckpoint = checkpointManager.readLastCheckpoint(taskName);

    BlobStoreUtil blobStoreUtil =
        new BlobStoreUtil(getBlobStoreManager(config, executor), executor, new BlobStoreConfig(config), null,
            new BlobStoreRestoreManagerMetrics(taskInstanceMetrics.get(taskName).registry()));

    // Get snapshot indexes with getDelete flag
    Map<String, Pair<String, SnapshotIndex>> storeSnapshotIndexes =
        blobStoreUtil.getStoreSnapshotIndexes(new JobConfig(config).getName().get(), new JobConfig(config).getJobId(),
            taskName.getTaskName(), oldCheckpoint, ImmutableSet.copyOf(
                new StorageConfig(config).getPersistentStoresWithBackupFactory(BlobStoreStateBackendFactory.class.getName())),
            true);

    Map<String, CompletableFuture<String>> storeNameSnapshotIndexFuture = new HashMap<>();
    storeSnapshotIndexes.forEach((store, blobIdSnapshotIndexPair) -> {
      SnapshotIndex snapshotIndex = blobIdSnapshotIndexPair.getRight();
      storeNameSnapshotIndexFuture.put(store, blobStoreUtil.putSnapshotIndex(snapshotIndex));
    });
    return FutureUtil.toFutureOfMap(storeNameSnapshotIndexFuture).join();
  }

  /**
   * Restores all TaskInstances and returns a future for each TaskInstance restore. If this restore fails with DeletedException
   * it will retry the restore with getDeleted flag, get all the blobs, recreate a new checkpoint in blob store and
   * write that checkpoint to checkpoint topic.
   * @param taskRestoreManagers map of TaskName -> factory name -> TaskRestoreManager map.
   */
  public static List<Future<Void>> restoreAllTaskInstances(Map<TaskName, Map<String, TaskRestoreManager>> taskRestoreManagers,
      Map<TaskName, Map<String, Set<String>>> taskBackendFactoryToStoreNames, JobContext jobContext, ContainerModel containerModel,
      SamzaContainerMetrics samzaContainerMetrics, CheckpointManager checkpointManager, Config config,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics, ExecutorService executor,
      File loggedStoreDir) {

    List<Future<Void>> taskRestoreFutures = new ArrayList<>();

    // Submit restore callable for each taskInstance
    taskRestoreManagers.forEach((taskInstanceName, restoreManagersMap) -> {
      // Submit for each restore factory
      restoreManagersMap.forEach((factoryName, taskRestoreManager) -> {
        long startTime = System.currentTimeMillis();
        String taskName = taskInstanceName.getTaskName();
        LOG.info("Starting restore for state for task: {}", taskName);

        CompletableFuture<Void> taskRestoreFuture = taskRestoreManager.restore().handle((res, ex) -> {
          updateRestoreTime(startTime, samzaContainerMetrics, taskInstanceName);

          if (ex != null) {
            if (ex instanceof DeletedException) {
              LOG.warn("Restore state for task: {} received DeletedException. Reattempting with getDeletedBlobs enabled",
                  taskInstanceName.getTaskName());

              // Try to restore with getDeleted flag
              CompletableFuture<Void> future =
                  attemptRestoreWithGetDeletedBlobs(taskInstanceName, checkpointManager, taskRestoreManager, config,
                      taskInstanceMetrics, executor, taskBackendFactoryToStoreNames.get(taskInstanceName).get(factoryName),
                      loggedStoreDir, jobContext, containerModel);
              try {
                if (future != null) {
                  future.join();
                }
              } catch (Exception e) {
                String msg = String.format("Unable to recover from DeletedException for task %s.", taskName);
                throw new SamzaException(msg, e);
              } finally {
                updateRestoreTime(startTime, samzaContainerMetrics, taskInstanceName);
              }
            } else {
              // log and rethrow exception to communicate restore failure
              String msg = String.format("Error restoring state for task: %s", taskName);
              LOG.error(msg, ex);
              throw new SamzaException(msg, ex); // wrap in unchecked exception to throw from lambda
            }
          }

          // Stop all persistent stores after restoring. Certain persistent stores opened in BulkLoad mode are compacted
          // on stop, so paralleling stop() also parallelizes their compaction (a time-intensive operation).
          try {
            taskRestoreManager.close();
          } catch (Exception e) {
            LOG.error("Error closing restore manager for task: {} after {} restore",
                taskName, ex != null ? "unsuccessful" : "successful", e);
            // ignore exception from close. container may still be able to continue processing/backups
            // if restore manager close fails.
          }
          return null;
        });
        taskRestoreFutures.add(taskRestoreFuture);
      });
    });

    return taskRestoreFutures;
  }

  public static CompletableFuture<Void> attemptRestoreWithGetDeletedBlobs(TaskName taskName,
      CheckpointManager checkpointManager, TaskRestoreManager taskRestoreManager, Config config,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics, ExecutorService executor, Set<String> storesToRestore,
      File loggedStoreBaseDirectory, JobContext jobContext, ContainerModel containerModel) {

    TaskModel taskModel = containerModel.getTasks().get(taskName);

    // if taskInstanceMetrics are specified use those for store metrics,
    // otherwise (in case of StorageRecovery) use a blank MetricsRegistryMap
    MetricsRegistry metricsRegistry =
        taskInstanceMetrics.get(taskName) != null ?
            taskInstanceMetrics.get(taskName).registry() : new MetricsRegistryMap();

    BlobStoreManager blobStoreManager = getBlobStoreManager(config, executor);

    if (!(taskRestoreManager instanceof BlobStoreRestoreManager)) {
      //this should not happen since DeletedException is thrown only from BlobStoreRestoreManager
      LOG.warn("Won't reattempt restore for none blob store restore manager");
      return null;
    }

    BlobStoreRestoreManager blobStoreRestoreManager = (BlobStoreRestoreManager) taskRestoreManager;

    return blobStoreRestoreManager.restore(true).handle((r, e) -> {
          if (e != null) {
            // restore failing with DeletedException (or any other exception) for the second time. Fail.
            throw new SamzaException(e);
          } else {
            // backup newly restored stores and recreate the checkpoint.
            CheckpointId checkpointId = CheckpointId.create();

            createCheckpointDirFromStoreDirCopy(taskName, taskModel, loggedStoreBaseDirectory, storesToRestore, checkpointId);

            CompletableFuture<Map<String, String>> uploadFutures = backupStores(jobContext, containerModel, taskName,
                config, checkpointId, loggedStoreBaseDirectory, blobStoreManager, metricsRegistry, executor);

            uploadFutures.whenComplete((uploadSCMs, ex) -> {
              if (ex != null) {
                String msg = String.format("Upload after restore with deletedBlob failed for task %s", taskName);
                throw new SamzaException(msg, ex);
              }
              writeNewCheckpoint(taskName, checkpointId, uploadSCMs, checkpointManager);
            });
            return null;
          }
        });
  }

  private static Checkpoint writeNewCheckpoint(TaskName taskName, CheckpointId checkpointId, Map<String, String> newSCMs,
      CheckpointManager checkpointManager) {
    CheckpointV2 oldCheckpoint = (CheckpointV2) checkpointManager.readLastCheckpoint(taskName); //TODO add check?
    Map<SystemStreamPartition, String> checkpointOffsets = oldCheckpoint.getOffsets();
    Map<String, Map<String, String>> stateCheckpointMarkers = oldCheckpoint.getStateCheckpointMarkers();
    stateCheckpointMarkers.put(BlobStoreStateBackendFactory.class.getName(), newSCMs);
    CheckpointV2 checkpointV2 = new CheckpointV2(checkpointId, checkpointOffsets, stateCheckpointMarkers);
    checkpointManager.writeCheckpoint(taskName, checkpointV2); //TODO call flush on producer?
    return checkpointV2;
  }

  private static CompletableFuture<Map<String, String>> backupStores(JobContext jobContext, ContainerModel containerModel,
      TaskName taskName, Config config, CheckpointId checkpointId,File loggedStoreBaseDirectory,
      BlobStoreManager blobStoreManager, MetricsRegistry metricsRegistry, ExecutorService executor) {
    BlobStoreBackupManagerMetrics BlobStoreBackupManagerMetrics = new BlobStoreBackupManagerMetrics(metricsRegistry);
    BlobStoreBackupManager blobStoreBackupManager =
        new BlobStoreBackupManager(jobContext.getJobModel(), containerModel,
            containerModel.getTasks().get(taskName), executor, BlobStoreBackupManagerMetrics, config,
            SystemClock.instance(), loggedStoreBaseDirectory, new StorageManagerUtil(), blobStoreManager);
    return blobStoreBackupManager.upload(checkpointId, new HashMap<>());
  }

  private static void createCheckpointDirFromStoreDirCopy(TaskName taskName, TaskModel taskModel,
      File loggedStoreBaseDir, Set<String> storeName, CheckpointId checkpointId) {
    StorageManagerUtil storageManagerUtil = new StorageManagerUtil();
    for (String store: storeName) {
      try {
        File storeDirectory = storageManagerUtil.getTaskStoreDir(loggedStoreBaseDir, store, taskName,
            taskModel.getTaskMode());
        File checkpointDir = new File(storageManagerUtil.getStoreCheckpointDir(storeDirectory, checkpointId));
        FileUtils.copyDirectory(storeDirectory, checkpointDir);
      } catch (IOException exception) {
        String msg = String.format("Unable to create a copy of store directory %s into checkpoint dir %s while "
            + "attempting to recover from DeletedException", store, checkpointId);
        throw new SamzaException(msg, exception);
      }
    }
  }
  private static BlobStoreManager getBlobStoreManager(Config config, ExecutorService executor) {
    BlobStoreConfig blobStoreConfig = new BlobStoreConfig(config);
    String blobStoreManagerFactory = blobStoreConfig.getBlobStoreManagerFactory();
    BlobStoreManagerFactory factory = ReflectionUtil.getObj(blobStoreManagerFactory, BlobStoreManagerFactory.class);
    return factory.getRestoreBlobStoreManager(config, executor);
  }

  private static void updateRestoreTime(long startTime, SamzaContainerMetrics samzaContainerMetrics,
      TaskName taskInstance) {
    long timeToRestore = System.currentTimeMillis() - startTime;
    if (samzaContainerMetrics != null) {
      Gauge taskGauge = samzaContainerMetrics.taskStoreRestorationMetrics().getOrDefault(taskInstance, null);

      if (taskGauge != null) {
        taskGauge.set(timeToRestore);
      }
    }
  }
}
