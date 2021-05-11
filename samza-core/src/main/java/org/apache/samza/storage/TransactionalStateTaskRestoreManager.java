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
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV1;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.checkpoint.kafka.KafkaChangelogSSPOffset;
import org.apache.samza.checkpoint.kafka.KafkaStateCheckpointMarker;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.ChangelogSSPIterator;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the state restore based on state snapshots of checkpoints and changelog.
 */
public class TransactionalStateTaskRestoreManager implements TaskRestoreManager {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionalStateTaskRestoreManager.class);

  private final TaskModel taskModel;
  private final Map<String, StorageEngine> storeEngines; // store name to storage engines
  private final Map<String, SystemStream> storeChangelogs; // store name to changelog system stream
  private final SystemAdmins systemAdmins;
  private final Map<String, SystemConsumer> storeConsumers; // store name to system consumer
  private final SSPMetadataCache sspMetadataCache;
  private final File loggedStoreBaseDirectory;
  private final File nonLoggedStoreBaseDirectory;
  private final Config config;
  private final Clock clock;
  private final StorageManagerUtil storageManagerUtil;
  private final FileUtil fileUtil;

  private StoreActions storeActions; // available after init
  private Map<SystemStreamPartition, SystemStreamPartitionMetadata> currentChangelogOffsets;

  public TransactionalStateTaskRestoreManager(
      Set<String> storeNames,  // non-side input stores
      JobContext jobContext,
      ContainerContext containerContext,
      TaskModel taskModel,
      Map<String, SystemStream> storeChangelogs,
      Map<String, StorageEngine> inMemoryStores, // in memory stores to be mutated during restore
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, Serde<Object>> serdes,
      SystemAdmins systemAdmins,
      Map<String, SystemConsumer> storeConsumers,
      MetricsRegistry metricsRegistry,
      MessageCollector messageCollector,
      SSPMetadataCache sspMetadataCache,
      File loggedStoreBaseDirectory,
      File nonLoggedStoreBaseDirectory,
      Config config,
      Clock clock) {
    this.taskModel = taskModel;
    this.storeChangelogs = storeChangelogs;
    this.systemAdmins = systemAdmins;
    this.storeConsumers = storeConsumers;
    // OK to use SSPMetadataCache here since unlike commit newest changelog ssp offsets will not change
    // between cache init and restore completion
    this.sspMetadataCache = sspMetadataCache;
    this.loggedStoreBaseDirectory = loggedStoreBaseDirectory;
    this.nonLoggedStoreBaseDirectory = nonLoggedStoreBaseDirectory;
    this.config = config;
    this.clock = clock;
    this.storageManagerUtil = new StorageManagerUtil();
    this.fileUtil = new FileUtil();
    this.storeEngines = createStoreEngines(storeNames, jobContext, containerContext,
        storageEngineFactories, serdes, metricsRegistry, messageCollector, inMemoryStores);
  }

  @Override
  public void init(Checkpoint checkpoint) {
    Map<String, KafkaStateCheckpointMarker> storeStateCheckpointMarkers = getCheckpointedChangelogOffsets(checkpoint);
    currentChangelogOffsets = getCurrentChangelogOffsets(taskModel, storeChangelogs, sspMetadataCache);

    this.storeActions = getStoreActions(taskModel, storeEngines, storeChangelogs,
        storeStateCheckpointMarkers, getCheckpointId(checkpoint), currentChangelogOffsets, systemAdmins, storageManagerUtil,
        loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory, config, clock);

    setupStoreDirs(taskModel, storeEngines, storeActions, storageManagerUtil, fileUtil,
        loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory);
    registerStartingOffsets(taskModel, storeActions, storeChangelogs, systemAdmins, storeConsumers,
        currentChangelogOffsets);
  }

  @Override
  public void restore() throws InterruptedException {
    Map<String, RestoreOffsets> storesToRestore = storeActions.storesToRestore;

    for (Map.Entry<String, RestoreOffsets> entry : storesToRestore.entrySet()) {
      String storeName = entry.getKey();
      String endOffset = entry.getValue().endingOffset;
      SystemStream systemStream = storeChangelogs.get(storeName);
      SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(systemStream.getSystem());
      SystemConsumer systemConsumer = storeConsumers.get(storeName);
      SystemStreamPartition changelogSSP = new SystemStreamPartition(systemStream, taskModel.getChangelogPartition());

      ChangelogSSPIterator changelogSSPIterator =
          new ChangelogSSPIterator(systemConsumer, changelogSSP, endOffset, systemAdmin, true,
              currentChangelogOffsets.get(changelogSSP).getNewestOffset());
      StorageEngine taskStore = storeEngines.get(storeName);

      LOG.info("Restoring store: {} for task: {}", storeName, taskModel.getTaskName());
      taskStore.restore(changelogSSPIterator);
    }
  }

  /**
   * Stop only persistent stores. In case of certain stores and store mode (such as RocksDB), this
   * can invoke compaction. Persisted stores are recreated in read-write mode in {@link ContainerStorageManager}.
   */
  public void close() {
    TaskName taskName = taskModel.getTaskName();
    storeEngines.forEach((storeName, storeEngine) -> {
      if (storeEngine.getStoreProperties().isPersistedToDisk())
        storeEngine.stop();
      LOG.info("Stopped persistent store: {} in task: {}", storeName, taskName);
    });
  }

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
   * Get offset metadata for each changelog SSP for this task. A task may have multiple changelog streams
   * (e.g., for different stores), but will have the same partition for all of them.
   */
  @VisibleForTesting
  static Map<SystemStreamPartition, SystemStreamPartitionMetadata> getCurrentChangelogOffsets(
      TaskModel taskModel, Map<String, SystemStream> storeChangelogs, SSPMetadataCache sspMetadataCache) {
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> changelogOffsets = new HashMap<>();

    Partition changelogPartition = taskModel.getChangelogPartition();
    for (Map.Entry<String, SystemStream> storeChangelog : storeChangelogs.entrySet()) {
      SystemStream changelog = storeChangelog.getValue();
      SystemStreamPartition changelogSSP = new SystemStreamPartition(
          changelog.getSystem(), changelog.getStream(), changelogPartition);
      SystemStreamPartitionMetadata metadata = sspMetadataCache.getMetadata(changelogSSP);
      changelogOffsets.put(changelogSSP, metadata);
    }

    LOG.info("Got current changelog offsets for taskName: {} as: {}", taskModel.getTaskName(), changelogOffsets);
    return changelogOffsets;
  }

  /**
   * Marks each persistent but non-logged store for deletion.
   *
   * For each logged store, based on the current, checkpointed and local changelog offsets,
   * 1. decides which directories (current and checkpoints) to delete for persistent stores.
   * 2. decides which directories (checkpoints) to retain for persistent stores.
   * 3. decides which stores (persistent or not) need to be restored, and the beginning and end offsets for the restore.
   *
   * When this method returns, in StoreActions,
   * 1. all persistent store current directories will be present in storeDirsToDelete
   * 2. each persistent store checkpoint directory will be present in either storeDirToRetain or storeDirsToDelete.
   * 3. there will be at most one storeDirToRetain per persistent store, which will be a checkpoint directory.
   * 4. any stores (persistent or not) that need to be restored from changelogs will be present in
   *    storesToRestore with appropriate offsets.
   */
  @VisibleForTesting
  static StoreActions getStoreActions(
      TaskModel taskModel,
      Map<String, StorageEngine> storeEngines,
      Map<String, SystemStream> storeChangelogs,
      Map<String, KafkaStateCheckpointMarker> kafkaStateCheckpointMarkers,
      CheckpointId checkpointId,
      Map<SystemStreamPartition, SystemStreamPartitionMetadata> currentChangelogOffsets,
      SystemAdmins systemAdmins,
      StorageManagerUtil storageManagerUtil,
      File loggedStoreBaseDirectory,
      File nonLoggedStoreBaseDirectory,
      Config config,
      Clock clock) {
    TaskName taskName = taskModel.getTaskName();
    TaskMode taskMode = taskModel.getTaskMode();

    Map<String, File> storeDirToRetain = new HashMap<>();
    ListMultimap<String, File> storeDirsToDelete = ArrayListMultimap.create();
    Map<String, RestoreOffsets> storesToRestore = new HashMap<>();

    storeEngines.forEach((storeName, storageEngine) -> {
      // do nothing if store is non persistent and not logged (e.g. in memory cache only)
      if (!storageEngine.getStoreProperties().isPersistedToDisk() &&
        !storageEngine.getStoreProperties().isLoggedStore()) {
        return;
      }

      // persistent but non-logged stores are always deleted
      if (storageEngine.getStoreProperties().isPersistedToDisk() &&
          !storageEngine.getStoreProperties().isLoggedStore()) {
        File currentDir = storageManagerUtil.getTaskStoreDir(
            nonLoggedStoreBaseDirectory, storeName, taskName, taskMode);
        LOG.info("Marking current directory: {} for store: {} in task: {} for deletion since it is not a logged store.",
            currentDir, storeName, taskName);
        storeDirsToDelete.put(storeName, currentDir);
        // persistent but non-logged stores should not have checkpoint dirs
        return;
      }

      // get the oldest and newest current changelog SSP offsets as well as the checkpointed changelog SSP offset
      SystemStream changelog = storeChangelogs.get(storeName);
      SystemStreamPartition changelogSSP = new SystemStreamPartition(changelog, taskModel.getChangelogPartition());
      SystemAdmin admin = systemAdmins.getSystemAdmin(changelogSSP.getSystem());
      SystemStreamPartitionMetadata changelogSSPMetadata = currentChangelogOffsets.get(changelogSSP);
      String oldestOffset = changelogSSPMetadata.getOldestOffset();
      String newestOffset = changelogSSPMetadata.getNewestOffset();

      String checkpointedOffset = null; // can be null if no message, or message has null offset
      if (kafkaStateCheckpointMarkers.containsKey(storeName) &&
          StringUtils.isNotBlank(kafkaStateCheckpointMarkers.get(storeName).getChangelogOffset())) {
        checkpointedOffset = kafkaStateCheckpointMarkers.get(storeName).getChangelogOffset();
      }
      long timeSinceLastCheckpointInMs = checkpointId == null ? Long.MAX_VALUE : System.currentTimeMillis() - checkpointId.getMillis();

      // if the clean.store.start config is set, delete current and checkpoint dirs, restore from oldest offset to checkpointed
      if (storageEngine.getStoreProperties().isPersistedToDisk() && new StorageConfig(
        config).getCleanLoggedStoreDirsOnStart(storeName)) {
        File currentDir = storageManagerUtil.getTaskStoreDir(loggedStoreBaseDirectory, storeName, taskName, taskMode);
        LOG.info("Marking current directory: {} for store: {} in task: {} for deletion due to clean.on.container.start config.",
            currentDir, storeName, taskName);
        storeDirsToDelete.put(storeName, currentDir);

        storageManagerUtil.getTaskStoreCheckpointDirs(loggedStoreBaseDirectory, storeName, taskName, taskMode)
            .forEach(checkpointDir -> {
              LOG.info("Marking checkpoint directory: {} for store: {} in task: {} for deletion due to clean.on.container.start config.",
                  checkpointDir, storeName, taskName);
              storeDirsToDelete.put(storeName, checkpointDir);
            });

        LOG.info("Marking restore offsets for store: {} in task: {} to {}, {} ", storeName, taskName, oldestOffset, checkpointedOffset);
        storesToRestore.put(storeName, new RestoreOffsets(oldestOffset, checkpointedOffset));
        return;
      }

      Optional<File> currentDirOptional;
      Optional<List<File>> checkpointDirsOptional;

      if (!storageEngine.getStoreProperties().isPersistedToDisk()) {
        currentDirOptional = Optional.empty();
        checkpointDirsOptional = Optional.empty();
      } else {
        currentDirOptional = Optional.of(storageManagerUtil.getTaskStoreDir(
            loggedStoreBaseDirectory, storeName, taskName, taskMode));
        checkpointDirsOptional = Optional.of(storageManagerUtil.getTaskStoreCheckpointDirs(
            loggedStoreBaseDirectory, storeName, taskName, taskMode));
      }

      LOG.info("For store: {} in task: {} got current dir: {}, checkpoint dirs: {}, checkpointed changelog offset: {}",
          storeName, taskName, currentDirOptional, checkpointDirsOptional, checkpointedOffset);

      currentDirOptional.ifPresent(currentDir -> {
        LOG.info("Marking current directory: {} for store: {} in task: {} for deletion.",
            currentDir, storeName, taskName);
        storeDirsToDelete.put(storeName, currentDir);
      });

      if (checkpointedOffset == null && oldestOffset != null) {
        // this can mean that either this is the initial migration for this feature and there are no previously
        // checkpointed changelog offsets, or that this is a new store or changelog topic after the initial migration.

        // if this is the first time migration, it might be desirable to retain existing data.
        // if this is new store or topic, it is possible that the container previously died after writing some data to
        // the changelog but before a commit, so it is desirable to delete the store, not restore anything and
        // trim the changelog

        // since we can't tell the difference b/w the two scenarios by just looking at the store and changelogs,
        // we'll request users to indicate whether to retain existing data using a config flag. this flag should only
        // be set during migrations, and turned off after the first successful commit of the new container (i.e. next
        // deploy). for simplicity, we'll always delete the local store, and restore from changelog if necessary.

        // the former scenario should not be common. the recommended way to opt-in to the transactional state feature
        // is to first upgrade to the latest samza version but keep the transactional state restore config off.
        // this will create the store checkpoint directories and write the changelog offset to the checkpoint, but
        // will not use them during restore. once this is done (i.e. at least one commit after upgrade), the
        // transactional state restore feature can be turned on on subsequent deploys. this code path exists as a
        // fail-safe against clearing changelogs in case users do not follow upgrade instructions and enable the
        // feature directly.
        checkpointDirsOptional.ifPresent(checkpointDirs ->
            checkpointDirs.forEach(checkpointDir -> {
              LOG.info("Marking checkpoint directory: {} for store: {} in task: {} for deletion since checkpointed " +
                      "offset is null and oldest offset: {} is not.",
                  checkpointDir, storeName, taskName, oldestOffset);
              storeDirsToDelete.put(storeName, checkpointDir);
            }));

        if (new TaskConfig(config).getTransactionalStateRetainExistingState()) {
          // mark for restore from (oldest, newest) to recreate local state.
          LOG.warn("Checkpointed offset for store: {} in task: {} is null. Since retain existing state is true, " +
              "local state will be fully restored from current changelog contents. " +
              "There is no transactional local state guarantee.", storeName, taskName);
          storesToRestore.put(storeName, new RestoreOffsets(oldestOffset, newestOffset));
        } else {
          LOG.warn("Checkpointed offset for store: {} in task: {} is null. Since retain existing state is false, " +
              "any local state and changelog topic contents will be deleted.", storeName, taskName);
          // mark for restore from (oldest, null) to trim entire changelog.
          storesToRestore.put(storeName, new RestoreOffsets(oldestOffset, null));
        }
      } else if (// check if the checkpointed offset is out of range of current oldest and newest offsets
          admin.offsetComparator(oldestOffset, checkpointedOffset) > 0 ||
          admin.offsetComparator(checkpointedOffset, newestOffset) > 0) {
        // checkpointed offset is out of range. this could mean that this is a TTL topic and the checkpointed
        // offset was TTLd, or that the changelog topic was manually deleted and then recreated.
        // we cannot guarantee transactional state for TTL stores, so delete everything and do a full restore
        // for local store. if the topic was deleted and recreated, this will have the side effect of
        // clearing the store as well.
        LOG.warn("Checkpointed offset: {} for store: {} in task: {} is out of range of oldest: {} or newest: {} offset." +
                "Deleting existing store and fully restoring from changelog topic from oldest to newest offset. If the topic " +
                "has time-based retention, there is no transactional local state guarantees. If the topic was changed," +
                "local state will be cleaned up and fully restored to match the new topic contents.",
            checkpointedOffset, storeName, taskName, oldestOffset, newestOffset);
        checkpointDirsOptional.ifPresent(checkpointDirs ->
            checkpointDirs.forEach(checkpointDir -> storeDirsToDelete.put(storeName, checkpointDir)));
        storesToRestore.put(storeName, new RestoreOffsets(oldestOffset, newestOffset));
      } else { // happy path. checkpointed offset is in range of current oldest and newest offsets
        if (!checkpointDirsOptional.isPresent()) { // non-persistent logged store
          LOG.info("Did not find any checkpoint directories for logged (maybe non-persistent) store: {}. Local state " +
              "will be fully restored from current changelog contents.", storeName);
          storesToRestore.put(storeName, new RestoreOffsets(oldestOffset, checkpointedOffset));
        } else { // persistent logged store
          String targetOffset;

          // check checkpoint time against min.compaction.lag.ms. if older, restore from checkpointed offset to newest
          // with no trim. be conservative. allow 10% safety margin to avoid deletions when the downtime is close
          // to min.compaction.lag.ms
          long minCompactionLagMs = new StorageConfig(config).getChangelogMinCompactionLagMs(storeName);
          if (timeSinceLastCheckpointInMs > .9 * minCompactionLagMs) {
            LOG.warn("Checkpointed offset for store: {} in task: {} is: {}. It is in range of oldest: {} and " +
                "newest: {} changelog offset. However, time since last checkpoint is: {}, which is greater than " +
                "0.9 * min.compaction.lag.ms: {} for the changelog topic. Since there is a chance that" +
                "the changelog topic has been compacted, restoring store to the end of the current changelog contents." +
                "There is no transactional local state guarantee.", storeName, taskName, checkpointedOffset,
                oldestOffset, newestOffset, timeSinceLastCheckpointInMs, minCompactionLagMs);
            targetOffset = newestOffset;
          } else {
            targetOffset = checkpointedOffset;
          }

          // if there exists a valid store checkpoint directory with oldest offset <= local offset <= target offset,
          // retain it and restore the delta. delete all other checkpoint directories for the store. if more than one such
          // checkpoint directory exists, retain the one with the highest local offset and delete the rest.
          boolean hasValidCheckpointDir = false;
          for (File checkpointDir: checkpointDirsOptional.get()) {
            if (storageManagerUtil.isLoggedStoreValid(
                storeName, checkpointDir, config, storeChangelogs, taskModel, clock, storeEngines)) {
              String localOffset = storageManagerUtil.readOffsetFile(
                  checkpointDir, Collections.singleton(changelogSSP), false).get(changelogSSP);
              LOG.info("Read local offset: {} for store: {} checkpoint dir: {} in task: {}", localOffset, storeName,
                  checkpointDir, taskName);

              if (admin.offsetComparator(localOffset, oldestOffset) >= 0 &&
                  admin.offsetComparator(localOffset, targetOffset) <= 0 &&
                  (storesToRestore.get(storeName) == null ||
                      admin.offsetComparator(localOffset, storesToRestore.get(storeName).startingOffset) > 0)) {
                hasValidCheckpointDir = true;
                LOG.info("Temporarily marking checkpoint dir: {} for store: {} in task: {} for retention. " +
                    "May be overridden later.", checkpointDir, storeName, taskName);
                storeDirToRetain.put(storeName, checkpointDir);
                // mark for restore even if local == checkpointed, so that the changelog gets trimmed.
                LOG.info("Temporarily marking store: {} in task: {} for restore from beginning offset: {} to " +
                    "ending offset: {}. May be overridden later", storeName, taskName, localOffset, targetOffset);
                storesToRestore.put(storeName, new RestoreOffsets(localOffset, targetOffset));
              }
            }
          }

          // delete all non-retained checkpoint directories
          for (File checkpointDir: checkpointDirsOptional.get()) {
            if (storeDirToRetain.get(storeName) == null ||
                !storeDirToRetain.get(storeName).equals(checkpointDir)) {
              LOG.info("Marking checkpoint directory: {} for store: {} in task: {} for deletion since it is not " +
                  "marked for retention.", checkpointDir, storeName, taskName);
              storeDirsToDelete.put(storeName, checkpointDir);
            }
          }

          // if the store had not valid checkpoint dirs to retain, restore from changelog
          if (!hasValidCheckpointDir) {
            storesToRestore.put(storeName, new RestoreOffsets(oldestOffset, targetOffset));
          }
        }
      }
    });

    LOG.info("Store directories to be retained in Task: {} are: {}", taskName, storeDirToRetain);
    LOG.info("Store directories to be deleted in Task: {} are: {}", taskName, storeDirsToDelete);
    LOG.info("Stores to be restored in Task: {} are: {}", taskName, storesToRestore);
    return new StoreActions(storeDirToRetain, storeDirsToDelete, storesToRestore);
  }

  /**
   * For each store for this task,
   * a. Deletes current directory if persistent but non-logged store.
   * b. Deletes current and checkpoint directories if persistent logged store and directory is marked for deletion
   * c. Moves the valid persistent logged store checkpoint directory to current directory if marked for retention.
   * d. Creates all missing (i.e. not retained in step c) persistent logged store dirs.
   *
   * When this method returns,
   * a. There will be a empty current dir for each persistent but non-logged store.
   * b. There will be a current dir for each persistent logged store. This dir may or may not be empty.
   * c. There will be no remaining checkpoint dirs for persistent logged stores.
   */
  @VisibleForTesting
  static void setupStoreDirs(
      TaskModel taskModel,
      Map<String, StorageEngine> storeEngines,
      StoreActions storeActions,
      StorageManagerUtil storageManagerUtil,
      FileUtil fileUtil,
      File loggedStoreBaseDirectory,
      File nonLoggedStoreBaseDirectory) {
    TaskName taskName = taskModel.getTaskName();
    TaskMode taskMode = taskModel.getTaskMode();
    ListMultimap<String, File> storeDirsToDelete = storeActions.storeDirsToDelete;
    Map<String, File> storeDirsToRetain = storeActions.storeDirsToRetain;

    // delete all persistent store directories marked for deletion
    storeDirsToDelete.entries().forEach(entry -> {
      String storeName = entry.getKey();
      File storeDirToDelete = entry.getValue();
      LOG.info("Deleting persistent store directory: {} for store: {} in task: {}",
          storeDirToDelete, storeName, taskName);
      fileUtil.rm(storeDirToDelete);
    });

    // rename all retained persistent logged store checkpoint directories to current directory
    storeDirsToRetain.forEach((storeName, storeDirToRetain) -> {
      File currentDir = storageManagerUtil.getTaskStoreDir(
          loggedStoreBaseDirectory, storeName, taskName, taskMode);
      LOG.info("Moving logged store checkpoint directory: {} for store: {} in task: {} to current directory: {}",
          storeDirsToRetain.toString(), storeName, taskName, currentDir);
      storageManagerUtil.restoreCheckpointFiles(storeDirToRetain, currentDir);
      // do not remove the checkpoint directory yet. in case commit fails and container restarts,
      // we can retry the move. if we delete the checkpoint, the current dir will be deleted as well on
      // restart, and we will have to do a full restore.
    });

    // create any missing (not retained) current directories for persistent stores
    storeEngines.forEach((storeName, storageEngine) -> {
      if (storageEngine.getStoreProperties().isPersistedToDisk()) {
        File currentDir;
        if (storageEngine.getStoreProperties().isLoggedStore()) {
          currentDir = storageManagerUtil.getTaskStoreDir(
              loggedStoreBaseDirectory, storeName, taskName, taskMode);
        } else {
          currentDir = storageManagerUtil.getTaskStoreDir(
              nonLoggedStoreBaseDirectory, storeName, taskName, taskMode);
        }

        try {
          if (!fileUtil.exists(currentDir.toPath())) {
            LOG.info("Creating missing persistent store current directory: {} for store: {} in task: {}",
                currentDir, storeName, taskName);
            fileUtil.createDirectories(currentDir.toPath());
          }
        } catch (Exception e) {
          throw new SamzaException(String.format("Error setting up current directory for store: %s", storeName), e);
        }
      }
    });
  }

  /**
   * Determines the starting offset for each store changelog SSP that needs to be restored from,
   * and registers it with the respective SystemConsumer.
   */
  @VisibleForTesting
  static void registerStartingOffsets(
      TaskModel taskModel,
      StoreActions storeActions,
      Map<String, SystemStream> storeChangelogs,
      SystemAdmins systemAdmins,
      Map<String, SystemConsumer> storeConsumers,
      Map<SystemStreamPartition, SystemStreamPartitionMetadata> currentChangelogOffsets) {
    Map<String, RestoreOffsets> storesToRestore = storeActions.storesToRestore;

    // must register at least one SSP with each changelog system consumer otherwise start will throw.
    // hence we register upcoming offset as the dummy offset by default and override it later if necessary.
    // using upcoming offset ensures that no messages are replayed by default.
    storeChangelogs.forEach((storeName, changelog) -> {
      SystemStreamPartition changelogSSP = new SystemStreamPartition(changelog, taskModel.getChangelogPartition());
      SystemConsumer systemConsumer = storeConsumers.get(storeName);
      SystemStreamPartitionMetadata currentOffsets = currentChangelogOffsets.get(changelogSSP);
      String upcomingOffset = currentOffsets.getUpcomingOffset();
      LOG.info("Temporarily registering upcoming offset: {} as the starting offest for changelog ssp: {}. " +
          "This might be overridden later for stores that need restoring.", upcomingOffset, changelogSSP);
      systemConsumer.register(changelogSSP, upcomingOffset);
    });

    // now register the actual starting offset if necessary. system consumer will ensure that the lower of the
    // two registered offsets is used as the starting offset.
    storesToRestore.forEach((storeName, restoreOffsets) -> {
      SystemStream changelog = storeChangelogs.get(storeName);
      SystemStreamPartition changelogSSP = new SystemStreamPartition(changelog, taskModel.getChangelogPartition());
      SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(changelog.getSystem());
      validateRestoreOffsets(restoreOffsets, systemAdmin);

      SystemConsumer systemConsumer = storeConsumers.get(storeName);
      SystemStreamPartitionMetadata currentOffsets = currentChangelogOffsets.get(changelogSSP);
      String oldestOffset = currentOffsets.getOldestOffset();

      // if the starting offset equals oldest offset (e.g. for full restore), start from the oldest offset (inclusive).
      // else, start from the next (upcoming) offset.
      String startingOffset;
      if (systemAdmin.offsetComparator(restoreOffsets.startingOffset, oldestOffset) == 0) {
        startingOffset = oldestOffset;
      } else {
        Map<SystemStreamPartition, String> offsetMap = ImmutableMap.of(changelogSSP, restoreOffsets.startingOffset);
        startingOffset = systemAdmin.getOffsetsAfter(offsetMap).get(changelogSSP);
      }
      LOG.info("Registering starting offset: {} for changelog ssp: {}", startingOffset, changelogSSP);
      systemConsumer.register(changelogSSP, startingOffset);
    });
  }

  private static void validateRestoreOffsets(RestoreOffsets restoreOffsets, SystemAdmin systemAdmin) {
    String startingOffset = restoreOffsets.startingOffset;
    String endingOffset = restoreOffsets.endingOffset;
    // cannot enforce that starting offset be non null since SSPMetadata allows oldest offset == null for empty topics.
    if (endingOffset != null) {
      Preconditions.checkState(systemAdmin.offsetComparator(endingOffset, startingOffset) >= 0,
          String.format("Ending offset: %s must be equal to or greater than starting offset: %s",
              endingOffset, startingOffset));
    }
  }

  private Map<String, KafkaStateCheckpointMarker> getCheckpointedChangelogOffsets(Checkpoint checkpoint) {
    Map<String, KafkaStateCheckpointMarker> checkpointedChangelogOffsets = new HashMap<>();
    if (checkpoint == null) return checkpointedChangelogOffsets;

    if (checkpoint instanceof CheckpointV2) {
      Map<String, Map<String, String>> factoryStoreSCMs = ((CheckpointV2) checkpoint).getStateCheckpointMarkers();
      if (factoryStoreSCMs.containsKey(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME)) {
        factoryStoreSCMs.get(KafkaStateCheckpointMarker.KAFKA_STATE_BACKEND_FACTORY_NAME)
            .forEach((storeName, scmString) -> {
              KafkaStateCheckpointMarker kafkaSCM = KafkaStateCheckpointMarker.deserialize(scmString);
              checkpointedChangelogOffsets.put(storeName, kafkaSCM);
            });
      } // skip the non-KafkaStateCheckpointMarkers
    } else if (checkpoint instanceof CheckpointV1) {
      // If the checkpoint v1 is used, we need to fetch the changelog SSPs in the inputOffsets in order to get the
      // store offset.
      Map<SystemStreamPartition, String> checkpointedOffsets = ((CheckpointV1) checkpoint).getOffsets();
      storeChangelogs.forEach((storeName, systemStream) -> {
        Partition changelogPartition = taskModel.getChangelogPartition();
        SystemStreamPartition storeChangelogSSP = new SystemStreamPartition(systemStream, changelogPartition);
        String checkpointedOffset = checkpointedOffsets.get(storeChangelogSSP);
        if (StringUtils.isNotBlank(checkpointedOffset)) {
          KafkaChangelogSSPOffset kafkaChangelogSSPOffset = KafkaChangelogSSPOffset.fromString(checkpointedOffset);
          KafkaStateCheckpointMarker marker = new KafkaStateCheckpointMarker(
              storeChangelogSSP, kafkaChangelogSSPOffset.getChangelogOffset());
          checkpointedChangelogOffsets.put(storeName, marker);
        }
      });
    } else {
      throw new SamzaException("Unsupported checkpoint version: " + checkpoint.getVersion());
    }

    return checkpointedChangelogOffsets;
  }

  private CheckpointId getCheckpointId(Checkpoint checkpoint) {
    if (checkpoint == null) return null;
    if (checkpoint instanceof CheckpointV1) {
      for (Map.Entry<String, SystemStream> storeNameSystemStream : storeChangelogs.entrySet()) {
        SystemStreamPartition storeChangelogSSP = new SystemStreamPartition(storeNameSystemStream.getValue(), taskModel.getChangelogPartition());
        String checkpointMessage = checkpoint.getOffsets().get(storeChangelogSSP);
        if (StringUtils.isNotBlank(checkpointMessage)) {
          KafkaChangelogSSPOffset kafkaStateChanglogOffset = KafkaChangelogSSPOffset.fromString(checkpointMessage);
          return kafkaStateChanglogOffset.getCheckpointId();
        }
      }
    } else if (checkpoint instanceof CheckpointV2) {
      return ((CheckpointV2) checkpoint).getCheckpointId();
    } else {
      throw new SamzaException("Unsupported checkpoint version: " + checkpoint.getVersion());
    }
    return null;
  }

  @VisibleForTesting
  static class StoreActions {
    final Map<String, File> storeDirsToRetain;
    final ListMultimap<String, File> storeDirsToDelete;
    final Map<String, RestoreOffsets> storesToRestore;

    StoreActions(Map<String, File> storeDirsToRetain,
        ListMultimap<String, File> storeDirsToDelete,
        Map<String, RestoreOffsets> storesToRestore) {
      this.storeDirsToRetain = storeDirsToRetain;
      this.storeDirsToDelete = storeDirsToDelete;
      this.storesToRestore = storesToRestore;
    }
  }

  /**
   * Starting offset must be non-null, and a non-null ending offset must be equal to or greater than starting offset.
   *
   * If ending offset == null (e.g. empty topic) or starting offset == ending offset, we should trim everything
   * from starting to head. E.g. to trim entire changelog, restore offsets == (oldest, null).
   */
  @VisibleForTesting
  static class RestoreOffsets {
    final String startingOffset;
    final String endingOffset;

    RestoreOffsets(String startingOffset, String endingOffset) {
      this.startingOffset = startingOffset;
      this.endingOffset = endingOffset;
    }

    @Override
    public String toString() {
      return String.format("startingOffset: %s, endingOffset: %s", startingOffset, endingOffset);
    }
  }
}
