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
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.ContainerContextImpl;
import org.apache.samza.context.JobContextImpl;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.stream.CoordinatorStreamManager;
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
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.ScalaJavaUtil;
import org.apache.samza.util.StreamUtil;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;


/**
 * Recovers the state storages from the changelog streams and store the storages
 * in the directory provided by the users. The changelog streams are derived
 * from the job's config file.
 */
public class StorageRecovery extends CommandLine {

  private Config jobConfig;
  private int maxPartitionNumber = 0;
  private File storeBaseDir = null;
  private HashMap<String, SystemStream> changeLogSystemStreams = new HashMap<>();
  private HashMap<String, StorageEngineFactory<Object, Object>> storageEngineFactories = new HashMap<>();
  private Map<String, ContainerModel> containers = new HashMap<>();
  private Map<String, ContainerStorageManager> containerStorageManagers = new HashMap<>();

  private Logger log = LoggerFactory.getLogger(StorageRecovery.class);
  private SystemAdmins systemAdmins = null;

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
    log.info("setting up the recovery...");

    getContainerModels();
    getChangeLogSystemStreamsAndStorageFactories();
    getChangeLogMaxPartitionNumber();
    getContainerStorageManagers();
  }

  /**
   * run the setup phase and restore all the task storages
   */
  public void run() {
    setup();

    log.info("start recovering...");

    systemAdmins.start();
    this.containerStorageManagers.forEach((containerName, containerStorageManager) -> {
        containerStorageManager.start();
      });
    this.containerStorageManagers.forEach((containerName, containerStorageManager) -> {
        containerStorageManager.shutdown();
      });
    systemAdmins.stop();

    log.info("successfully recovered in " + storeBaseDir.toString());
  }

  /**
   * Build ContainerModels from job config file and put the results in the containerModels map.
   */
  private void getContainerModels() {
    MetricsRegistryMap metricsRegistryMap = new MetricsRegistryMap();
    CoordinatorStreamManager coordinatorStreamManager = new CoordinatorStreamManager(jobConfig, metricsRegistryMap);
    coordinatorStreamManager.register(getClass().getSimpleName());
    coordinatorStreamManager.start();
    coordinatorStreamManager.bootstrap();
    ChangelogStreamManager changelogStreamManager = new ChangelogStreamManager(coordinatorStreamManager);
    JobModel jobModel = JobModelManager.apply(coordinatorStreamManager.getConfig(), changelogStreamManager.readPartitionMapping(), metricsRegistryMap).jobModel();
    containers = jobModel.getContainers();
    coordinatorStreamManager.stop();
  }

  /**
   * Get the changelog streams and the storage factories from the config file
   * and put them into the maps
   */
  private void getChangeLogSystemStreamsAndStorageFactories() {
    JavaStorageConfig config = new JavaStorageConfig(jobConfig);
    List<String> storeNames = config.getStoreNames();

    log.info("Got store names: " + storeNames.toString());

    for (String storeName : storeNames) {
      String streamName = config.getChangelogStream(storeName);

      log.info("stream name for " + storeName + " is " + streamName);

      if (streamName != null) {
        changeLogSystemStreams.put(storeName, StreamUtil.getSystemStreamFromNames(streamName));
      }

      String factoryClass = config.getStorageFactoryClassName(storeName);
      if (factoryClass != null) {
        storageEngineFactories.put(storeName, Util.getObj(factoryClass, StorageEngineFactory.class));
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
    ScalaJavaUtil.toJavaCollection(serializerConfig.getSerdeNames())
        .stream()
        .forEach(serdeName -> {
            Option<String> serdeClassName = serializerConfig.getSerdeClass(serdeName);

            if (serdeClassName.isEmpty()) {
              serdeClassName = Option.apply(SerializerConfig.getSerdeFactoryName(serdeName));
            }

            Serde serde = Util.getObj(serdeClassName.get(), SerdeFactory.class).getSerde(serdeName, serializerConfig);
            serdeMap.put(serdeName, serde);
          });

    return serdeMap;
  }

  /**
   * create one TaskStorageManager for each task. Add all of them to the
   * List<TaskStorageManager>
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void getContainerStorageManagers() {
    Clock clock = SystemClock.instance();
    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(systemAdmins, 5000, clock);
    // don't worry about prefetching for this; looks like the tool doesn't flush to offset files anyways

    Map<String, SystemFactory> systemFactories = new SystemConfig(jobConfig).getSystemFactories();

    for (ContainerModel containerModel : containers.values()) {
      ContainerContext containerContext = new ContainerContextImpl(containerModel, new MetricsRegistryMap());

      ContainerStorageManager containerStorageManager =
          new ContainerStorageManager(containerModel, streamMetadataCache, systemAdmins, changeLogSystemStreams,
              new HashMap<>(), storageEngineFactories, systemFactories, this.getSerdes(), jobConfig, new HashMap<>(),
              new SamzaContainerMetrics(containerModel.getId(), new MetricsRegistryMap()),
              JobContextImpl.fromConfigWithDefaults(jobConfig), containerContext, new HashMap<>(),
              storeBaseDir, storeBaseDir, maxPartitionNumber, null, new SystemClock());
      this.containerStorageManagers.put(containerModel.getId(), containerStorageManager);
    }
  }
}
