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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.ContainerContextImpl;
import org.apache.samza.context.JobContextImpl;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Clock;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.StreamUtil;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Recovers the state storage from the changelog streams and stores the state
 * in the directory provided by the users. The changelog streams are derived
 * from the job's config file.
 */
public class StorageRecovery {
  private static final Logger LOG = LoggerFactory.getLogger(StorageRecovery.class);

  private final Config jobConfig;
  private final File storeBaseDir;
  private final SystemAdmins systemAdmins;
  private final Map<String, SystemStream> changeLogSystemStreams = new HashMap<>();
  private final Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories = new HashMap<>();
  private final Map<String, ContainerStorageManager> containerStorageManagers = new HashMap<>();

  private int maxPartitionNumber = 0;
  private JobModel jobModel;
  private Map<String, ContainerModel> containers = new HashMap<>();

  /**
   * Construct the StorageRecovery
   *
   * @param config
   *          the job config
   * @param path
   *          the directory path where we put the stores
   */
  StorageRecovery(Config config, String path) {
    jobConfig = config;
    storeBaseDir = new File(path, "state");
    systemAdmins = new SystemAdmins(config);
  }

  /**
   * setup phase which assigns required values to the variables used for all
   * tasks.
   */
  private void setup() {
    LOG.info("setting up the recovery...");

    getContainerModels();
    getChangeLogSystemStreamsAndStorageFactories();
    getChangeLogMaxPartitionNumber();
    getContainerStorageManagers();
  }

  /**
   * run the setup phase and restore all the task storage
   */
  public void run() {
    setup();

    LOG.info("start recovering...");

    systemAdmins.start();
    this.containerStorageManagers.forEach((containerName, containerStorageManager) -> {
      try {
        containerStorageManager.start();
      } catch (InterruptedException e) {
        // we can ignore the exception since its only used in the context of a command line tool and bubbling the
        // exception upstream isn't needed.
        LOG.warn("Received an interrupt during store restoration for container {}."
            + " Proceeding with the next container", containerName);
      }
    });
    this.containerStorageManagers.forEach((containerName, containerStorageManager) -> containerStorageManager.shutdown());
    systemAdmins.stop();

    LOG.info("successfully recovered in " + storeBaseDir.toString());
  }

  /**
   * Build ContainerModels from job config file and put the results in the containerModels map.
   */
  private void getContainerModels() {
    MetricsRegistryMap metricsRegistryMap = new MetricsRegistryMap();
    CoordinatorStreamStore coordinatorStreamStore = new CoordinatorStreamStore(jobConfig, metricsRegistryMap);
    coordinatorStreamStore.init();
    try {
      Config configFromCoordinatorStream = CoordinatorStreamUtil.readConfigFromCoordinatorStream(coordinatorStreamStore);
      ChangelogStreamManager changelogStreamManager = new ChangelogStreamManager(coordinatorStreamStore);
      JobModelManager jobModelManager =
          JobModelManager.apply(configFromCoordinatorStream, changelogStreamManager.readPartitionMapping(),
              coordinatorStreamStore, metricsRegistryMap);
      JobModel jobModel = jobModelManager.jobModel();
      this.jobModel = jobModel;
      containers = jobModel.getContainers();
    } finally {
      coordinatorStreamStore.close();
    }
  }

  /**
   * Get the changelog streams and the storage factories from the config file
   * and put them into the maps
   */
  private void getChangeLogSystemStreamsAndStorageFactories() {
    StorageConfig config = new StorageConfig(jobConfig);
    List<String> storeNames = config.getStoreNames();

    LOG.info("Got store names: " + storeNames.toString());

    for (String storeName : storeNames) {
      Optional<String> streamName = config.getChangelogStream(storeName);

      LOG.info("stream name for " + storeName + " is " + streamName.orElse(null));

      streamName.ifPresent(name -> changeLogSystemStreams.put(storeName, StreamUtil.getSystemStreamFromNames(name)));

      Optional<String> factoryClass = config.getStorageFactoryClassName(storeName);
      if (factoryClass.isPresent()) {
        @SuppressWarnings("unchecked")
        StorageEngineFactory<Object, Object> factory =
            (StorageEngineFactory<Object, Object>) ReflectionUtil.getObj(factoryClass.get(), StorageEngineFactory.class);

        storageEngineFactories.put(storeName, factory);
      } else {
        throw new SamzaException("Missing storage factory for " + storeName + ".");
      }
    }
  }

  /**
   * get the max partition number of the changelog stream
   */
  private void getChangeLogMaxPartitionNumber() {
    int maxPartitionId = 0;
    for (ContainerModel containerModel : containers.values()) {
      for (TaskModel taskModel : containerModel.getTasks().values()) {
        maxPartitionId = Math.max(maxPartitionId, taskModel.getChangelogPartition().getPartitionId());
      }
    }
    maxPartitionNumber = maxPartitionId + 1;
  }

  private Map<String, Serde<Object>> getSerdes() {
    Map<String, Serde<Object>> serdeMap = new HashMap<>();
    SerializerConfig serializerConfig = new SerializerConfig(jobConfig);

    // Adding all serdes from factories
    serializerConfig.getSerdeNames()
        .forEach(serdeName -> {
          String serdeClassName = serializerConfig.getSerdeFactoryClass(serdeName)
            .orElseGet(() -> SerializerConfig.getPredefinedSerdeFactoryName(serdeName));
          @SuppressWarnings("unchecked")
          Serde<Object> serde =
              ReflectionUtil.getObj(serdeClassName, SerdeFactory.class).getSerde(serdeName, serializerConfig);
          serdeMap.put(serdeName, serde);
        });

    return serdeMap;
  }

  /**
   * create one TaskStorageManager for each task. Add all of them to the
   * List<TaskStorageManager>
   */
  @SuppressWarnings("rawtypes")
  private void getContainerStorageManagers() {
    String factoryClass = new StorageConfig(jobConfig).getStateBackendRestoreFactory();
    Clock clock = SystemClock.instance();
    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(systemAdmins, 5000, clock);
    // don't worry about prefetching for this; looks like the tool doesn't flush to offset files anyways
    Map<String, SystemFactory> systemFactories = new SystemConfig(jobConfig).getSystemFactories();
    CheckpointManager checkpointManager = new TaskConfig(jobConfig)
        .getCheckpointManager(new MetricsRegistryMap()).orElse(null);

    for (ContainerModel containerModel : containers.values()) {
      ContainerContext containerContext = new ContainerContextImpl(containerModel, new MetricsRegistryMap());

      ContainerStorageManager containerStorageManager =
          new ContainerStorageManager(
              checkpointManager,
              containerModel,
              streamMetadataCache,
              systemAdmins,
              changeLogSystemStreams,
              new HashMap<>(),
              storageEngineFactories,
              systemFactories,
              this.getSerdes(),
              jobConfig,
              new HashMap<>(),
              new SamzaContainerMetrics(containerModel.getId(), new MetricsRegistryMap(), ""),
              JobContextImpl.fromConfigWithDefaults(jobConfig, jobModel),
              containerContext,
              ReflectionUtil.getObj(factoryClass, StateBackendFactory.class),
              new HashMap<>(),
              storeBaseDir,
              storeBaseDir,
              null,
              new SystemClock());
      this.containerStorageManagers.put(containerModel.getId(), containerStorageManager);
    }
  }
}
