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

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskInstanceCollector;
import org.apache.samza.util.Clock;
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
    StorageConfig storageConfig = new StorageConfig(config);
    Set<String> inMemoryStoreNames = storageEngineFactories.keySet().stream()
        .filter(storeName -> {
          Optional<String> storeFactory = storageConfig.getStorageFactoryClassName(storeName);
          return storeFactory.isPresent() && storeFactory.get()
              .equals(StorageConfig.INMEMORY_KV_STORAGE_ENGINE_FACTORY);
        })
        .collect(Collectors.toSet());
    return ContainerStorageManagerUtil.createTaskStores(
        inMemoryStoreNames, storageEngineFactories, sideInputStoreNames,
        activeTaskChangelogSystemStreams, storeDirectoryPaths,
        containerModel, jobContext, containerContext, serdes,
        taskInstanceMetrics, taskInstanceCollectors, storageManagerUtil,
        loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory, config);
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
   * Return a map of backend factory names to set of stores that should be restored using it
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
        LOG.info("non-Side input stores: {}, do not have a configured store changelogs for checkpoint V1,"
                + "restore for the store will be skipped",
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
          LOG.info("non-Side input store: {}, does not have a configured restore factories nor store changelogs,"
                  + "restore for the store will be skipped",
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
            LOG.warn("No matching checkpoints found for configured factories: {}, " +
                "defaulting to using the first configured factory with no checkpoints", storeFactories);
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
}
