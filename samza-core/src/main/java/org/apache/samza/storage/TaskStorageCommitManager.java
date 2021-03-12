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
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointV1;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.checkpoint.kafka.KafkaChangelogSSPOffset;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the commit of the state stores of the task.
 */
public class TaskStorageCommitManager {

  private static final Logger LOG = LoggerFactory.getLogger(TaskStorageCommitManager.class);

  private final TaskName taskName;
  private final CheckpointManager checkpointManager;
  private final Map<String, TaskBackupManager> stateBackendToBackupManager;
  private final Map<String, StorageEngine> storageEngines;
  private final Partition taskChangelogPartition;
  private final StorageManagerUtil storageManagerUtil;
  private final File durableStoreBaseDir;
  private final Map<String, SystemStream> storeChangelogs;


  public TaskStorageCommitManager(TaskName taskName, Map<String, TaskBackupManager> stateBackendToBackupManager,
      Map<String, StorageEngine> storageEngines, Map<String, SystemStream> storeChangelogs, Partition changelogPartition,
      CheckpointManager checkpointManager, Config config, StorageManagerUtil storageManagerUtil, File durableStoreBaseDir) {
    this.taskName = taskName;
    this.stateBackendToBackupManager = stateBackendToBackupManager;
    this.storageEngines = storageEngines;
    this.taskChangelogPartition = changelogPartition;
    this.checkpointManager = checkpointManager;
    this.durableStoreBaseDir = durableStoreBaseDir;
    this.storeChangelogs = storeChangelogs;
    this.storageManagerUtil = storageManagerUtil;
  }

  public void init() {
    if (checkpointManager != null) {
      Checkpoint checkpoint = checkpointManager.readLastCheckpoint(taskName);
      LOG.debug("Last checkpoint on start for task: {} is: {}", taskName, checkpoint);
      stateBackendToBackupManager.values().forEach(storageBackupManager -> storageBackupManager.init(checkpoint));
    } else {
      stateBackendToBackupManager.values().forEach(storageBackupManager -> storageBackupManager.init(null));
    }
  }

  /**
   * // TODO MED dchen fix documentation
   * Backs up the local state to the remote storage and returns
   *
   * @return Committed Map of FactoryName to (Map of StoreName to StateCheckpointMarker) mappings of the committed SSPs
   */
  public Map<String, Map<String, String>> commit(CheckpointId checkpointId) {
    // Flush all stores
    storageEngines.values().forEach(StorageEngine::flush);

    // Checkpoint all persisted stores and logged stores
    // We are assuming that logged stores are the definition of durable stores
    // TODO MED dchen clarify "are the definition of durable stores". Might be worth adding a helper method
    // isDurableStore() with documentation in StorageManagerUtil that currently returns isLoggedStore, and updating all
    // call sites for isLoggedStore. Will clarify the semantics / expectations at the call sites and will be a
    // simple change to fix semantics later.
    storageEngines.forEach((storeName, storageEngine) -> {
      if (storageEngine.getStoreProperties().isPersistedToDisk() &&
          storageEngine.getStoreProperties().isLoggedStore()) {
        storageEngine.checkpoint(checkpointId);
      }
    });

    // { state backend factory -> { store Name -> state checkpoint marker }}
    Map<String, Map<String, String>> stateBackendToStoreSCMs = new HashMap<>();

    // for each configured state backend factory, backup the state for all stores in this task.
    stateBackendToBackupManager.forEach((stateBackendFactoryName, backupManager) -> {
      Map<String, String> snapshotSCMs = backupManager.snapshot(checkpointId);
      LOG.debug("Found snapshot SCMs for taskName: {}, checkpoint id: {}, state backend: {} to be: {}",
          taskName, checkpointId, stateBackendFactoryName, snapshotSCMs);

      CompletableFuture<Map<String, String>> uploadFuture = backupManager.upload(checkpointId, snapshotSCMs);
      try {
        // TODO: HIGH dchen Make async with andThen and add thread management for concurrency and add timeouts,
        // need to make upload threads independent
        Map<String, String> uploadSCMs = uploadFuture.get();
        LOG.debug("Found upload SCMs for taskName: {}, checkpoint id: {}, state backend: {} to be: {}",
            taskName, checkpointId, stateBackendFactoryName, uploadSCMs);

        stateBackendToStoreSCMs.put(stateBackendFactoryName, uploadSCMs);
      } catch (Exception e) {
        throw new SamzaException(
            String.format("Error backing up local state for taskName: %s, checkpoint id: %s, state backend: %s",
                taskName, checkpointId, stateBackendFactoryName), e);
      }
    });

    return stateBackendToStoreSCMs;
  }

  /**
   * // TODO HIGH dchen fix method name and documentation. local file system isn't very relevant here (too general).
   * // Maybe rename to "writeCheckpointToStoreDirs" or something and update docs.
   * Writes the {@link Checkpoint} returned by {@link #commit(CheckpointId)}
   * locally to the file system on disk if the checkpoint passed in is an instance of {@link CheckpointV2},
   * otherwise if it is an instance of {@link CheckpointV1} persists the Kafka changelog ssp-offsets only.
   *
   * Note: The assumption is that this method will be invoked once for each {@link Checkpoint} version that the
   * task needs to write.
   *
   * @param checkpoint the latest checkpoint to be persisted to local file system
   */
  public void persistToLocalFileSystem(Checkpoint checkpoint) {
    if (checkpoint instanceof CheckpointV1) {
      LOG.debug("Persisting CheckpointV1 to store and checkpoint directories for taskName: {} with checkpoint: {}",
          taskName, checkpoint);
      // Write CheckpointV1 changelog offsets to store and checkpoint directories
      writeChangelogOffsetFiles(checkpoint.getOffsets());
    } else if (checkpoint instanceof CheckpointV2) {
      LOG.debug("Persisting CheckpointV2 to store and checkpoint directories for taskName: {} with checkpoint: {}",
          taskName, checkpoint);
      storageEngines.forEach((storeName, storageEngine) -> {
        // Only write the checkpoint file if the store is durable and persisted to disk
        if (storageEngine.getStoreProperties().isLoggedStore() &&
            storageEngine.getStoreProperties().isPersistedToDisk()) {
          CheckpointV2 checkpointV2 = (CheckpointV2) checkpoint;

          // TODO HIGH dchen add try-catch with info about task, store, checkpointId etc. Write can throw IOException.
          File storeDir = storageManagerUtil.getTaskStoreDir(durableStoreBaseDir, storeName, taskName, TaskMode.Active);
          storageManagerUtil.writeCheckpointFile(storeDir, checkpointV2);

          // TODO HIGH dchen add try-catch with info about task, store, checkpointId etc. Write can throw IOException.
          CheckpointId checkpointId = checkpointV2.getCheckpointId();
          File checkpointDir = Paths.get(StorageManagerUtil.getCheckpointDirPath(storeDir, checkpointId)).toFile();
          storageManagerUtil.writeCheckpointFile(checkpointDir, checkpointV2);
        }
      });
    } else {
      throw new SamzaException("Unsupported checkpoint version: " + checkpoint.getVersion());
    }
  }

  /**
   * // TODO MED dchen: fix documentation
   * Cleanup the commit state for each of the task backup managers
   * @param latestCheckpointId CheckpointId of the most recent successful commit
   * @param stateCheckpointMarkers map of map(stateBackendFactoryName to map(storeName to state checkpoint markers) from
   *                              the latest commit
   */
  public void cleanUp(CheckpointId latestCheckpointId, Map<String, Map<String, String>> stateCheckpointMarkers) {
    // Call cleanup on each backup manager
    stateCheckpointMarkers.forEach((factoryName, storeSCMs) -> {
      if (stateBackendToBackupManager.containsKey(factoryName)) {
        TaskBackupManager backupManager = stateBackendToBackupManager.get(factoryName);
        if (backupManager != null) {
          backupManager.cleanUp(latestCheckpointId, storeSCMs);
        }
        // TODO HIGH dchen when is it ok for backupmanager to be null? should we throw an exception and fail loudly?
      } else {
        // throw an error and fail instead?
        LOG.warn("Ignored cleanup for scm: {} due to unknown factory: {} ", storeSCMs, factoryName);
      }
    });

    // Delete directories for checkpoints older than latestCheckpointId
    if (latestCheckpointId != null) {
      LOG.debug("Deleting checkpoints older than Checkpoint Id: {}", latestCheckpointId);
      File[] files = durableStoreBaseDir.listFiles();
      if (files != null) {
        for (File storeDir : files) {
          String storeName = storeDir.getName();
          String taskStoreName = storageManagerUtil
              .getTaskStoreDir(durableStoreBaseDir, storeName, taskName, TaskMode.Active).getName();
          FileFilter fileFilter = new WildcardFileFilter(taskStoreName + "-*");
          File[] checkpointDirs = storeDir.listFiles(fileFilter);
          if (checkpointDirs != null) {
            for (File checkpointDir : checkpointDirs) {
              if (!checkpointDir.getName().contains(latestCheckpointId.toString())) {
                try {
                  FileUtils.deleteDirectory(checkpointDir);
                } catch (IOException e) {
                  throw new SamzaException(
                      String.format("Unable to delete checkpoint directory: %s", checkpointDir.getName()), e);
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Close all the state backup managers
   */
  public void close() {
    LOG.debug("Stopping backup managers for task {}.", taskName);
    stateBackendToBackupManager.values().forEach(storageBackupManager -> {
      if (storageBackupManager != null) {
        storageBackupManager.close();
      }
    });
    LOG.debug("Stopping stores for task {}.", taskName);
    storageEngines.values().forEach(StorageEngine::stop);
  }

  /**
   * Writes the newest changelog ssp offset for each logged and persistent store to the OFFSET file in the current
   * store directory (for allowing rollbacks). If the Kafka transactional backup manager is enabled, also writes to
   * the store checkpoint directory.
   *
   * These files are used during container startup to ensure transactional state, and to determine whether the
   * there is any new information in the changelog that is not reflected in the on-disk copy of the store.
   * If there is any delta, it is replayed from the changelog. E.g. this can happen if the job was run on this host,
   * then another host, and then back to this host.
   */
  // TODO HIGH dchen move tests from KafkaBackupManager to TaskCommitManager
  @VisibleForTesting
  void writeChangelogOffsetFiles(Map<SystemStreamPartition, String> checkpointOffsets) {
    storeChangelogs.forEach((storeName, systemStream) -> {
      SystemStreamPartition changelogSSP = new SystemStreamPartition(
          systemStream.getSystem(), systemStream.getStream(), taskChangelogPartition);

      // Only write if the store is durably backed by Kafka changelog && persisted to disk
      if (checkpointOffsets.containsKey(changelogSSP) &&
          storageEngines.containsKey(storeName) &&
          storageEngines.get(storeName).getStoreProperties().isLoggedStore() &&
          storageEngines.get(storeName).getStoreProperties().isPersistedToDisk()) {
        LOG.debug("Writing changelog offset for taskName {} store {} changelog {}.", taskName, storeName, systemStream);
        File currentStoreDir = storageManagerUtil.getTaskStoreDir(durableStoreBaseDir, storeName, taskName, TaskMode.Active);
        try {
          KafkaChangelogSSPOffset kafkaChangelogSSPOffset = KafkaChangelogSSPOffset
              .fromString(checkpointOffsets.get(changelogSSP));
          // Write offsets to file system if it is non-null
          String newestOffset = kafkaChangelogSSPOffset.getChangelogOffset();
          if (newestOffset != null) {
            // Write changelog SSP offset to the OFFSET files in the task store directory
            writeChangelogOffsetFile(storeName, changelogSSP, newestOffset, currentStoreDir);

            // Write changelog SSP offset to the OFFSET files in the store checkpoint directory
            File checkpointDir = Paths.get(StorageManagerUtil.getCheckpointDirPath(
                currentStoreDir, kafkaChangelogSSPOffset.getCheckpointId())).toFile();
            writeChangelogOffsetFile(storeName, changelogSSP, newestOffset, checkpointDir);
          } else {
            // If newestOffset is null, then it means the changelog ssp is (or has become) empty. This could be
            // either because the changelog topic was newly added, repartitioned, or manually deleted and recreated.
            // No need to persist the offset file.
            storageManagerUtil.deleteOffsetFile(currentStoreDir);
            LOG.debug("Deleting OFFSET file for taskName {} store {} changelog ssp {} since the newestOffset is null.",
                taskName, storeName, changelogSSP);
          }
        } catch (IOException e) {
          throw new SamzaException(
              String.format("Error storing offset for taskName %s store %s changelog %s.", taskName, storeName,
                  systemStream), e);
        }
      }
    });
    LOG.debug("Done writing OFFSET files for logged persistent key value stores for task {}", taskName);
  }

  @VisibleForTesting
  void writeChangelogOffsetFile(String storeName, SystemStreamPartition ssp, String newestOffset,
      File writeDirectory) throws IOException {
    LOG.debug("Storing newest offset {} for taskName {} store {} changelog ssp {} in OFFSET file at path: {}.",
        newestOffset, taskName, storeName, ssp, writeDirectory);
    storageManagerUtil.writeOffsetFile(writeDirectory, Collections.singletonMap(ssp, newestOffset), false);
    LOG.debug("Successfully stored offset {} for taskName {} store {} changelog {} in OFFSET file at path: {}.",
        newestOffset, taskName, storeName, ssp, writeDirectory);
  }
}
