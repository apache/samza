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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.ChangelogSSPIterator;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


/**
 * This is the legacy state restoration, based on changelog and offset files.
 *
 * Restore logic for all stores of a task including directory cleanup, setup, changelogSSP validation, registering
 * with the respective consumer, restoring stores, and stopping stores.
 */
class NonTransactionalStateTaskRestoreManager implements TaskRestoreManager {
  private static final Logger LOG = LoggerFactory.getLogger(NonTransactionalStateTaskRestoreManager.class);

  private final Map<String, StorageEngine> taskStores; // Map of all StorageEngines for this task indexed by store name
  private final Set<String> taskStoresToRestore;
  // Set of store names which need to be restored by consuming using system-consumers (see registerStartingOffsets)

  private final TaskModel taskModel;
  private final Clock clock; // Clock value used to validate base-directories for staleness. See isLoggedStoreValid.
  private Map<SystemStream, String> changeLogOldestOffsets; // Map of changelog oldest known offsets
  private final Map<SystemStreamPartition, String> fileOffsets; // Map of offsets read from offset file indexed by changelog SSP
  private final Map<String, SystemStream> storeChangelogs; // Map of change log system-streams indexed by store name
  private final SystemAdmins systemAdmins;
  private final File loggedStoreBaseDirectory;
  private final File nonLoggedStoreBaseDirectory;
  private final StreamMetadataCache streamMetadataCache;
  private final Map<String, SystemConsumer> storeConsumers;
  private final int maxChangeLogStreamPartitions;
  private final Config config;
  private final StorageManagerUtil storageManagerUtil;

  NonTransactionalStateTaskRestoreManager(
      Set<String> storeNames,
      JobContext jobContext,
      ContainerContext containerContext,
      TaskModel taskModel,
      Map<String, SystemStream> storeChangelogs,
      Map<String, StorageEngine> inMemoryStores,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, Serde<Object>> serdes,
      SystemAdmins systemAdmins,
      StreamMetadataCache streamMetadataCache,
      Map<String, SystemConsumer> storeConsumers,
      MetricsRegistry metricsRegistry,
      MessageCollector messageCollector,
      int maxChangeLogStreamPartitions,
      File loggedStoreBaseDirectory,
      File nonLoggedStoreBaseDirectory,
      Config config,
      Clock clock) {
    this.taskModel = taskModel;
    this.clock = clock;
    this.storeChangelogs = storeChangelogs;
    this.systemAdmins = systemAdmins;
    this.fileOffsets = new HashMap<>();
    this.loggedStoreBaseDirectory = loggedStoreBaseDirectory;
    this.nonLoggedStoreBaseDirectory = nonLoggedStoreBaseDirectory;
    this.streamMetadataCache = streamMetadataCache;
    this.storeConsumers = storeConsumers;
    this.maxChangeLogStreamPartitions = maxChangeLogStreamPartitions;
    this.config = config;
    this.storageManagerUtil = new StorageManagerUtil();
    this.taskStores = createStoreEngines(storeNames, jobContext, containerContext,
        storageEngineFactories, serdes, metricsRegistry, messageCollector, inMemoryStores);
    this.taskStoresToRestore = this.taskStores.entrySet().stream()
        .filter(x -> x.getValue().getStoreProperties().isLoggedStore())
        .map(x -> x.getKey()).collect(Collectors.toSet());
  }

  /**
   * Cleans up and sets up store directories, validates changeLog SSPs for all stores of this task,
   * and registers SSPs with the respective consumers.
   */
  @Override
  public void init(Checkpoint checkpoint) {
    cleanBaseDirsAndReadOffsetFiles();
    setupBaseDirs();
    validateChangelogStreams();
    getOldestChangeLogOffsets();
    registerStartingOffsets();
  }

  /** For each store for this task,
   * a. Deletes the corresponding non-logged-store base dir.
   * b. Deletes the logged-store-base-dir (depending on {@param cleanLoggedStoreDirs} value). See {@link #isLoggedStoreValid} for validation semantics.
   * c. If the logged-store-base-dir is valid, this method reads the offset file and stores each offset.
   */
  private void cleanBaseDirsAndReadOffsetFiles() {
    LOG.debug("Cleaning base directories for stores.");
    StorageConfig storageConfig = new StorageConfig(config);
    FileUtil fileUtil = new FileUtil();
    taskStores.forEach((storeName, storageEngine) -> {
      if (!storageEngine.getStoreProperties().isLoggedStore()) {
        File nonLoggedStorePartitionDir =
            storageManagerUtil.getTaskStoreDir(nonLoggedStoreBaseDirectory, storeName, taskModel.getTaskName(), taskModel.getTaskMode());
        LOG.info("Got non logged storage partition directory as " + nonLoggedStorePartitionDir.toPath().toString());

        if (nonLoggedStorePartitionDir.exists()) {
          LOG.info("Deleting non logged storage partition directory " + nonLoggedStorePartitionDir.toPath().toString());
          fileUtil.rm(nonLoggedStorePartitionDir);
        }
      } else {
        File loggedStorePartitionDir =
            storageManagerUtil.getTaskStoreDir(loggedStoreBaseDirectory, storeName, taskModel.getTaskName(), taskModel.getTaskMode());
        LOG.info("Got logged storage partition directory as " + loggedStorePartitionDir.toPath().toString());

        // Delete the logged store if it is not valid.
        if (!isLoggedStoreValid(storeName, loggedStorePartitionDir) || storageConfig.getCleanLoggedStoreDirsOnStart(storeName)) {
          LOG.info("Deleting logged storage partition directory " + loggedStorePartitionDir.toPath().toString());
          fileUtil.rm(loggedStorePartitionDir);
        } else {

          SystemStreamPartition changelogSSP = new SystemStreamPartition(storeChangelogs.get(storeName), taskModel.getChangelogPartition());
          Map<SystemStreamPartition, String> offset =
              storageManagerUtil.readOffsetFile(loggedStorePartitionDir, Collections.singleton(changelogSSP), false);
          LOG.info("Read offset {} for the store {} from logged storage partition directory {}", offset, storeName, loggedStorePartitionDir);

          if (offset.containsKey(changelogSSP)) {
            fileOffsets.put(changelogSSP, offset.get(changelogSSP));
          }
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
    long changeLogDeleteRetentionInMs = new StorageConfig(config).getChangeLogDeleteRetentionInMs(storeName);

    if (storeChangelogs.containsKey(storeName)) {
      SystemStreamPartition changelogSSP = new SystemStreamPartition(storeChangelogs.get(storeName), taskModel.getChangelogPartition());
      return this.taskStores.get(storeName).getStoreProperties().isPersistedToDisk()
          && storageManagerUtil.isOffsetFileValid(loggedStoreDir, Collections.singleton(changelogSSP), false)
          && !storageManagerUtil.isStaleStore(loggedStoreDir, changeLogDeleteRetentionInMs, clock.currentTimeMillis(), false);
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
            storageManagerUtil.getTaskStoreDir(loggedStoreBaseDirectory, storeName, taskModel.getTaskName(), taskModel.getTaskMode());

        LOG.info("Using logged storage partition directory: " + loggedStorePartitionDir.toPath().toString()
            + " for store: " + storeName);

        if (!loggedStorePartitionDir.exists()) {
          loggedStorePartitionDir.mkdirs();
        }
      } else {
        File nonLoggedStorePartitionDir =
            storageManagerUtil.getTaskStoreDir(nonLoggedStoreBaseDirectory, storeName, taskModel.getTaskName(), taskModel.getTaskMode());
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
    LOG.info("Validating change log streams: " + storeChangelogs);

    for (SystemStream changelogSystemStream : storeChangelogs.values()) {
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
            JavaConverters.asScalaSetConverter(new HashSet<>(storeChangelogs.values())).asScala().toSet(),
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

    for (Map.Entry<String, SystemStream> changelogSystemStreamEntry : storeChangelogs.entrySet()) {
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
    return storageManagerUtil.getStartingOffset(systemStreamPartition, systemAdmin, fileOffset, oldestOffset);
  }

  // TODO dchen put this in common code path for transactional and non-transactional
  private Map<String, StorageEngine> createStoreEngines(Set<String> storeNames, JobContext jobContext,
      ContainerContext containerContext, Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, Serde<Object>> serdes, MetricsRegistry metricsRegistry,
      MessageCollector messageCollector, Map<String, StorageEngine> nonPersistedStores) {
    Map<String, StorageEngine> storageEngines = new HashMap<>();
    // Put non persisted stores
    nonPersistedStores.forEach(storageEngines::put);
    // Create persisted stores
    storeNames.forEach(storeName -> {
      boolean isLogged = this.storeChangelogs.containsKey(storeName);
      File storeBaseDir = isLogged ? this.loggedStoreBaseDirectory : this.nonLoggedStoreBaseDirectory;
      File storeDirectory = storageManagerUtil.getTaskStoreDir(storeBaseDir, storeName, taskModel.getTaskName(),
          taskModel.getTaskMode());
      StorageEngine engine = ContainerStorageManager.createStore(storeName, storeDirectory, taskModel, jobContext, containerContext,
          storageEngineFactories, serdes, metricsRegistry, messageCollector,
          StorageEngineFactory.StoreMode.BulkLoad, this.storeChangelogs, this.config);
      storageEngines.put(storeName, engine);
    });
    return storageEngines;
  }

  /**
   * Restore each store in taskStoresToRestore sequentially
   */
  @Override
  public void restore() throws InterruptedException {
    for (String storeName : taskStoresToRestore) {
      LOG.info("Restoring store: {} for task: {}", storeName, taskModel.getTaskName());
      SystemConsumer systemConsumer = storeConsumers.get(storeName);
      SystemStream systemStream = storeChangelogs.get(storeName);
      SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(systemStream.getSystem());
      ChangelogSSPIterator changelogSSPIterator = new ChangelogSSPIterator(systemConsumer,
          new SystemStreamPartition(systemStream, taskModel.getChangelogPartition()), null, systemAdmin, false);

      taskStores.get(storeName).restore(changelogSSPIterator);
    }
  }

  /**
   * Stop only persistent stores. In case of certain stores and store mode (such as RocksDB), this
   * can invoke compaction.
   */
  public void close() {

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
