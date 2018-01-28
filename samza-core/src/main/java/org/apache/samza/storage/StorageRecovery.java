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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.stream.CoordinatorStream;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.ByteSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private HashMap<String, StorageEngineFactory<?, ?>> storageEngineFactories = new HashMap<>();
  private Map<String, ContainerModel> containers = new HashMap<>();
  private List<TaskStorageManager> taskStorageManagers = new ArrayList<>();
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
    getTaskStorageManagers();
  }

  /**
   * run the setup phase and restore all the task storages
   */
  public void run() {
    setup();

    log.info("start recovering...");

    systemAdmins.start();
    for (TaskStorageManager taskStorageManager : taskStorageManagers) {
      taskStorageManager.init();
      taskStorageManager.stopStores();
      log.debug("restored " + taskStorageManager.toString());
    }
    systemAdmins.stop();

    log.info("successfully recovered in " + storeBaseDir.toString());
  }

  /**
   * build the ContainerModels from job config file and put the results in the
   * map
   */
  private void getContainerModels() {
    CoordinatorStream coordinatorStream = new CoordinatorStream(jobConfig, new MetricsRegistryMap(), getClass().getSimpleName());
    coordinatorStream.startConsumer();
    coordinatorStream.startProducer();
    ChangelogPartitionManager changelogPartitionManager = new ChangelogPartitionManager(coordinatorStream);
    JobModel jobModel = JobModelManager.apply(coordinatorStream, changelogPartitionManager.readPartitionMapping()).jobModel();
    containers = jobModel.getContainers();
  }

  /**
   * get the changelog streams and the storage factories from the config file
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
        changeLogSystemStreams.put(storeName, Util.getSystemStreamFromNames(streamName));
      }

      String factoryClass = config.getStorageFactoryClassName(storeName);
      if (factoryClass != null) {
        storageEngineFactories.put(storeName, Util.<StorageEngineFactory<Object, Object>>getObj(factoryClass));
      } else {
        throw new SamzaException("Missing storage factory for " + storeName + ".");
      }
    }
  }

  /**
   * get the SystemConsumers for the stores
   */
  private HashMap<String, SystemConsumer> getStoreConsumers() {
    HashMap<String, SystemConsumer> storeConsumers = new HashMap<>();
    Map<String, SystemFactory> systemFactories = new JavaSystemConfig(jobConfig).getSystemFactories();

    for (Entry<String, SystemStream> entry : changeLogSystemStreams.entrySet()) {
      String storeSystem = entry.getValue().getSystem();
      if (!systemFactories.containsKey(storeSystem)) {
        throw new SamzaException("Changelog system " + storeSystem + " for store " + entry.getKey() + " does not exist in the config.");
      }
      storeConsumers.put(entry.getKey(), systemFactories.get(storeSystem).getConsumer(storeSystem, jobConfig, new MetricsRegistryMap()));
    }

    return storeConsumers;
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

  /**
   * create one TaskStorageManager for each task. Add all of them to the
   * List<TaskStorageManager>
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void getTaskStorageManagers() {
    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(systemAdmins, 5000, SystemClock.instance());

    for (ContainerModel containerModel : containers.values()) {
      HashMap<String, StorageEngine> taskStores = new HashMap<String, StorageEngine>();
      SamzaContainerContext containerContext = new SamzaContainerContext(containerModel.getProcessorId(), jobConfig, containerModel.getTasks()
          .keySet(), new MetricsRegistryMap());

      for (TaskModel taskModel : containerModel.getTasks().values()) {
        HashMap<String, SystemConsumer> storeConsumers = getStoreConsumers();

        for (Entry<String, StorageEngineFactory<?, ?>> entry : storageEngineFactories.entrySet()) {
          String storeName = entry.getKey();

          if (changeLogSystemStreams.containsKey(storeName)) {
            SystemStreamPartition changeLogSystemStreamPartition = new SystemStreamPartition(changeLogSystemStreams.get(storeName),
                taskModel.getChangelogPartition());
            File storePartitionDir = TaskStorageManager.getStorePartitionDir(storeBaseDir, storeName, taskModel.getTaskName());

            log.info("Got storage engine directory: " + storePartitionDir);

            StorageEngine storageEngine = (entry.getValue()).getStorageEngine(
                storeName,
                storePartitionDir,
                (Serde) new ByteSerde(),
                (Serde) new ByteSerde(),
                null,
                new MetricsRegistryMap(),
                changeLogSystemStreamPartition,
                containerContext);
            taskStores.put(storeName, storageEngine);
          }
        }
        TaskStorageManager taskStorageManager = new TaskStorageManager(
            taskModel.getTaskName(),
            Util.javaMapAsScalaMap(taskStores),
            Util.javaMapAsScalaMap(storeConsumers),
            Util.javaMapAsScalaMap(changeLogSystemStreams),
            maxPartitionNumber,
            streamMetadataCache,
            storeBaseDir,
            storeBaseDir,
            taskModel.getChangelogPartition(),
            systemAdmins,
            new StorageConfig(jobConfig).getChangeLogDeleteRetentionsInMs(),
            new SystemClock());

        taskStorageManagers.add(taskStorageManager);
      }
    }
  }
}
