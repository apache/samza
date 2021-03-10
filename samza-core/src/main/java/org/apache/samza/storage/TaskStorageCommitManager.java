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
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.SamzaContainer;
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
  private final StorageManagerUtil storageManagerUtil = new StorageManagerUtil();
  private final File loggedStoreBaseDir;
  private final Map<String, SystemStream> storeChangelogs;
  private final boolean isTransactionalStateCheckpointEnabled;


  public TaskStorageCommitManager(TaskName taskName, Map<String, TaskBackupManager> stateBackendToBackupManager,
      Map<String, StorageEngine> storageEngines, Partition changelogPartition, CheckpointManager checkpointManager,
      Config config) {
    this.taskName = taskName;
    this.stateBackendToBackupManager = stateBackendToBackupManager;
    this.storageEngines = storageEngines;
    this.taskChangelogPartition = changelogPartition;
    this.checkpointManager = checkpointManager;
    this.loggedStoreBaseDir = SamzaContainer.getLoggedStorageBaseDir(new JobConfig(config),
        new File(System.getProperty("user.dir"), "state"));
    this.storeChangelogs = new StorageConfig(config).getStoreChangelogs();
    this.isTransactionalStateCheckpointEnabled = new TaskConfig(config).getTransactionalStateCheckpointEnabled();
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
   * Commits the local state on the remote backup implementation
   * @return Committed Map of FactoryName to (Map of StoreName to StateCheckpointMarker) mappings of the committed SSPs
   */
  public Map<String, Map<String, String>> commit(CheckpointId checkpointId) {
    // { state backend factory -> { store Name -> state checkpoint marker }}
    Map<String, Map<String, String>> backendFactoryStoreStateMarkers = new HashMap<>();

    // Flush all stores
    storageEngines.values().forEach(StorageEngine::flush);
    // Checkpoint all persisted stores
    storageEngines.forEach((storeName, storageEngine) -> {
      if (storageEngine.getStoreProperties().isPersistedToDisk()) {
        storageEngine.checkpoint(checkpointId);
      }
    });

    // for each configured state backend factory, backup the state for all store in this task.
    stateBackendToBackupManager.forEach((stateBackendFactoryName, storageBackupManager) -> {
      Map<String, String> snapshotSCMs = storageBackupManager.snapshot(checkpointId);
      LOG.debug("Found snapshot SCMs for taskName: {}, checkpoint id: {}, storage backup manager: {} to be: {}",
          taskName, checkpointId, storageBackupManager, snapshotSCMs);

      CompletableFuture<Map<String, String>> uploadFuture = storageBackupManager.upload(checkpointId, snapshotSCMs);
      try {
        // TODO: HIGH dchen Make async with andThen and add thread management for concurrency and add timeouts,
        // need to make upload threads independent
        Map<String, String> uploadedStoreStateMarkers = uploadFuture.get();
        LOG.debug("Found upload SCMs for taskName: {}, checkpoint id: {}, storage backup manager: {} to be: {}",
            taskName, checkpointId, storageBackupManager, uploadedStoreStateMarkers);

        backendFactoryStoreStateMarkers.put(stateBackendFactoryName, uploadedStoreStateMarkers);
      } catch (Exception e) {
        throw new SamzaException(
            "Error uploading StateCheckpointMarkers, state commit upload phase could not be completed for taskName", e);
      }
    });

    return backendFactoryStoreStateMarkers;
  }

  /**
   * Writes a copy of the persisted {@link Checkpoint} from {@link #commit(CheckpointId)}
   * locally to the file system on disk if the checkpoint passed in is an instance of {@link CheckpointV2},
   * otherwise if it is an instance of {@link CheckpointV1} persists the Kafka changelog ssp-offsets only.
   *
   * This method is assuming that it will be evoked once for each checkpoint version that the committing job needs to
   * be backwards compatible with.
   *
   * @param checkpoint the latest checkpoint to be persisted to local file system
   */
  public void persistToLocalFileSystem(Checkpoint checkpoint) {
    LOG.debug("Persisting Checkpoint to store checkpoint directory for taskName: {} with checkpoint: {}",
        taskName, checkpoint);
    if (checkpoint instanceof CheckpointV1) {
      LOG.debug("Persisting CheckpointV1 to store checkpoint directory for taskName: {} with checkpoint: {}",
          taskName, checkpoint);
      // Write Checkpoint v1 checkpoint changelog offsets for backwards compatibility
      writeChangelogOffsetsFiles(checkpoint.getOffsets());
    } else if (checkpoint instanceof CheckpointV2) {
      LOG.debug("Persisting CheckpointV2 to store checkpoint directory for taskName: {} with checkpoint: {}, checkpoint id {}",
          taskName, checkpoint, ((CheckpointV2) checkpoint).getCheckpointId());
      // Write v2 checkpoint
      storageEngines.forEach((storeName, engine) -> {
        // Only stores if the write checkpoint if the store is durable and
        if (engine.getStoreProperties().isLoggedStore() &&
            engine.getStoreProperties().isPersistedToDisk()) {
          try {
            storageManagerUtil.writeCheckpointFile(storageManagerUtil
                    .getTaskStoreDir(loggedStoreBaseDir, storeName, taskName, TaskMode.Active), (CheckpointV2) checkpoint);
          } catch (IOException e) {
            throw new SamzaException(
                String.format("Error storing checkpoint for taskName: %s, checkpoint: %s to path %s.", taskName,
                    checkpoint, loggedStoreBaseDir));
          }
        }
      });
    } else {
      throw new SamzaException("Unsupported checkpoint version: " + checkpoint.getVersion());
    }

  }

  /**
   * Cleanup the commit state for each of the task backup managers
   * @param latestCheckpointId CheckpointId of the most recent successful commit
   * @param stateCheckpointMarkers map of map(stateBackendFactoryName to map(storeName to StateCheckpointMarkers) from
   *                              the latest commit
   */
  public void cleanUp(CheckpointId latestCheckpointId, Map<String, Map<String, String>> stateCheckpointMarkers) {
    stateCheckpointMarkers.forEach((factoryName, scms) -> {
      if (stateBackendToBackupManager.containsKey(factoryName)) {
        TaskBackupManager backupManager = stateBackendToBackupManager.get(factoryName);
        if (backupManager != null) {
          backupManager.cleanUp(latestCheckpointId, scms);
        }
      } else {
        LOG.warn("Ignored cleanup for scm: {} due to unknown factory: {} ", scms, factoryName);
      }
    });
    // Clear checkpoint directories
    if (latestCheckpointId != null) {
      LOG.debug("Deleting checkpoints older than Checkpoint Id: {}", latestCheckpointId);
      File[] files = loggedStoreBaseDir.listFiles();
      if (files != null) {
        for (File storeDir : files) {
          String storeName = storeDir.getName();
          String taskStoreName = storageManagerUtil
              .getTaskStoreDir(loggedStoreBaseDir, storeName, taskName, TaskMode.Active).getName();
          FileFilter fileFilter = new WildcardFileFilter(taskStoreName + "-*");
          File[] checkpointDirs = storeDir.listFiles(fileFilter);
          if (checkpointDirs != null) {
            for (File checkpointDir : checkpointDirs) {
              if (!checkpointDir.getName().contains(latestCheckpointId.toString())) {
                try {
                  FileUtils.deleteDirectory(checkpointDir);
                } catch (IOException e) {
                  throw new SamzaException(String.format("Unable to delete checkpoint directory {}", checkpointDir.getName()), e);
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
   * Writes the newest changelog ssp offset for each persistent store to the OFFSET file in the current store
   * directory (for allowing rollbacks). If the Kafka transactional backup manager is enabled, also write to
   * the StorageEngine checkpoint directory
   *
   * These files are used during container startup to ensure transactional state, and to determine whether the
   * there is any new information in the changelog that is not reflected in the on-disk copy of the store.
   * If there is any delta, it is replayed from the changelog e.g. This can happen if the job was run on this host,
   * then another host, and then back to this host.
   */
  // TODO HIGH dchen move tests from KafkaBackupManager to TaskCommitManager
  private void writeChangelogOffsetsFiles(Map<SystemStreamPartition, String> checkpointOffsets) {
    storeChangelogs.forEach((storeName, systemStream) -> {
      // Only write if the store is durably backed by Kafka changelog && persisted to disk
      if (storageEngines.containsKey(storeName) &&
          storageEngines.get(storeName).getStoreProperties().isLoggedStore() &&
          storageEngines.get(storeName).getStoreProperties().isPersistedToDisk()) {
        LOG.debug("Writing changelog offset for taskName {} store {} changelog {}.", taskName, storeName, systemStream);
        File currentStoreDir = storageManagerUtil.getTaskStoreDir(loggedStoreBaseDir, storeName, taskName, TaskMode.Active);
        try {
          SystemStreamPartition changelogSSP = new SystemStreamPartition(systemStream.getSystem(),
              systemStream.getStream(), taskChangelogPartition);
          KafkaChangelogSSPOffset kafkaChangelogSSPOffset = KafkaChangelogSSPOffset
              .fromString(checkpointOffsets.get(changelogSSP));
          // Write offsets to file system if it is non-null
          if (kafkaChangelogSSPOffset.getChangelogOffset() != null) {
            String newestOffset = kafkaChangelogSSPOffset.getChangelogOffset();

            // Write to ssp changelog to OFFSET file location
            writeChangelogOffsetFile(storeName, changelogSSP, newestOffset, currentStoreDir);
            // Write both ssp to changelog offsets to OFFSET file path and OFFSET-V2 path if using
            // transactional checkpoints
            if (isTransactionalStateCheckpointEnabled) {
              writeChangelogOffsetFile(storeName, changelogSSP, newestOffset,
                  Paths.get(StorageManagerUtil.getStoreCheckpointPath(currentStoreDir, kafkaChangelogSSPOffset.getCheckpointId())).toFile());
            }
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

  private void writeChangelogOffsetFile(String storeName, SystemStreamPartition ssp, String newestOffset,
      File writeDirectory) throws IOException {
    LOG.debug("Storing newest offset {} for taskName {} store {} changelog ssp {} in OFFSET file at path: {}.",
        newestOffset, taskName, storeName, ssp, writeDirectory);
    // TaskStorageManagers are only created for active tasks
    storageManagerUtil
        .writeOffsetFile(writeDirectory, Collections.singletonMap(ssp, newestOffset), false);
    LOG.debug("Successfully stored offset {} for taskName {} store {} changelog {} in OFFSET file at path: {}.",
        newestOffset, taskName, storeName, ssp, writeDirectory);
  }
}
