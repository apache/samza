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

package org.apache.samza.storage.blobstore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.config.BlobStoreConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.storage.StorageManagerUtil;
import org.apache.samza.storage.TaskRestoreManager;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.metrics.BlobStoreRestoreManagerMetrics;
import org.apache.samza.storage.blobstore.util.BlobStoreUtil;
import org.apache.samza.storage.blobstore.util.DirDiffUtil;
import org.apache.samza.util.FileUtil;
import org.apache.samza.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BlobStoreRestoreManager implements TaskRestoreManager {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreRestoreManager.class);
  // when checking if checkpoint dir is the same as remote snapshot, exclude the "OFFSET" family of files files
  // that are written to the checkpoint dir after the remote upload is complete as part of
  // TaskStorageCommitManager#writeCheckpointToStoreDirectories.
  private static final Set<String> FILES_TO_IGNORE = ImmutableSet.of(
      StorageManagerUtil.OFFSET_FILE_NAME_LEGACY,
      StorageManagerUtil.OFFSET_FILE_NAME_NEW,
      StorageManagerUtil.SIDE_INPUT_OFFSET_FILE_NAME_LEGACY,
      StorageManagerUtil.CHECKPOINT_FILE_NAME);

  private final TaskModel taskModel;
  private final String jobName;
  private final String jobId;
  private final ExecutorService executor;
  private final Config config;
  private final StorageConfig storageConfig;
  private final BlobStoreConfig blobStoreConfig;
  private final StorageManagerUtil storageManagerUtil;
  private final BlobStoreUtil blobStoreUtil;
  private final DirDiffUtil dirDiffUtil;
  private final File loggedBaseDir;
  private final File nonLoggedBaseDir;
  private final String taskName;
  private final Set<String> storesToRestore;
  private final BlobStoreRestoreManagerMetrics metrics;

  private BlobStoreManager blobStoreManager;

  /**
   * Map of store name and Pair of blob id of SnapshotIndex and the corresponding SnapshotIndex from last successful
   * task checkpoint
   */
  private Map<String, Pair<String, SnapshotIndex>> prevStoreSnapshotIndexes;

  public BlobStoreRestoreManager(TaskModel taskModel, ExecutorService restoreExecutor, Set<String> storesToRestore,
      BlobStoreRestoreManagerMetrics metrics, Config config, File loggedBaseDir, File nonLoggedBaseDir,
      StorageManagerUtil storageManagerUtil, BlobStoreManager blobStoreManager) {
    this.taskModel = taskModel;
    this.jobName = new JobConfig(config).getName().get();
    this.jobId = new JobConfig(config).getJobId();
    this.executor = restoreExecutor;
    this.config = config;
    this.storageConfig = new StorageConfig(config);
    this.blobStoreConfig = new BlobStoreConfig(config);
    this.storageManagerUtil = storageManagerUtil;
    this.blobStoreManager = blobStoreManager;
    this.blobStoreUtil = createBlobStoreUtil(blobStoreManager, executor, metrics);
    this.dirDiffUtil = new DirDiffUtil();
    this.prevStoreSnapshotIndexes = new HashMap<>();
    this.loggedBaseDir = loggedBaseDir;
    this.nonLoggedBaseDir = nonLoggedBaseDir;
    this.taskName = taskModel.getTaskName().getTaskName();
    this.storesToRestore = storesToRestore;
    this.metrics = metrics;
  }

  @Override
  public void init(Checkpoint checkpoint) {
    long startTime = System.nanoTime();
    LOG.debug("Initializing blob store restore manager for task: {}", taskName);

    blobStoreManager.init();

    // get previous SCMs from checkpoint
    prevStoreSnapshotIndexes = blobStoreUtil.getStoreSnapshotIndexes(jobName, jobId, taskName, checkpoint);
    metrics.getSnapshotIndexNs.set(System.nanoTime() - startTime);
    LOG.trace("Found previous snapshot index during blob store restore manager init for task: {} to be: {}",
        taskName, prevStoreSnapshotIndexes);

    metrics.initStoreMetrics(storesToRestore);

    // Note: blocks the caller thread.
    deleteUnusedStoresFromBlobStore(jobName, jobId, taskName, storageConfig, blobStoreConfig, prevStoreSnapshotIndexes,
        blobStoreUtil, executor);
    metrics.initNs.set(System.nanoTime() - startTime);
  }

  /**
   * Restore state from checkpoints and state snapshots.
   * State restore is performed by first retrieving the SnapshotIndex of the previous commit for every store from the
   * prevStoreSnapshotIndexes map. Local store is deleted to perform a restore from local checkpoint directory or remote
   * directory. If no local state checkpoint directory is found, or if the local checkpoint directory is different from
   * the remote snapshot, local checkpoint directory is deleted and a restore from the remote store is done by
   * downloading the state asynchronously and in parallel.
   *
   */
  @Override
  public CompletableFuture<Void> restore() {
    return restoreStores(jobName, jobId, taskModel.getTaskName(), storesToRestore, prevStoreSnapshotIndexes, loggedBaseDir,
        storageConfig, metrics, storageManagerUtil, blobStoreUtil, dirDiffUtil, executor);
  }

  @Override
  public void close() {
    blobStoreManager.close();
  }

  @VisibleForTesting
  protected BlobStoreUtil createBlobStoreUtil(BlobStoreManager blobStoreManager, ExecutorService executor,
      BlobStoreRestoreManagerMetrics metrics) {
    return new BlobStoreUtil(blobStoreManager, executor, null, metrics);
  }

  /**
   * Deletes blob store contents for stores that were present in the last checkpoint but are either no longer
   * present in job configs (removed by user since last deployment) or are no longer configured to be backed
   * up using blob stores.
   *
   * This method blocks until all the necessary store contents and snapshot index blobs have been marked for deletion.
   */
  @VisibleForTesting
  static void deleteUnusedStoresFromBlobStore(String jobName, String jobId, String taskName, StorageConfig storageConfig,
      BlobStoreConfig blobStoreConfig, Map<String, Pair<String, SnapshotIndex>> initialStoreSnapshotIndexes,
      BlobStoreUtil blobStoreUtil, ExecutorService executor) {

    List<String> storesToBackup =
        storageConfig.getStoresWithBackupFactory(BlobStoreStateBackendFactory.class.getName());
    List<String> storesToRestore =
        storageConfig.getStoresWithRestoreFactory(BlobStoreStateBackendFactory.class.getName());

    List<CompletionStage<Void>> storeDeletionFutures = new ArrayList<>();
    initialStoreSnapshotIndexes.forEach((storeName, scmAndSnapshotIndex) -> {
      if (!storesToBackup.contains(storeName) && !storesToRestore.contains(storeName)) {
        LOG.debug("Removing task: {} store: {} from blob store. It is either no longer used, " +
            "or is no longer configured to be backed up or restored with blob store.", taskName, storeName);
        DirIndex dirIndex = scmAndSnapshotIndex.getRight().getDirIndex();
        Metadata requestMetadata =
            new Metadata(Metadata.SNAPSHOT_INDEX_PAYLOAD_PATH, Optional.empty(), jobName, jobId, taskName, storeName);
        CompletionStage<Void> storeDeletionFuture =
            blobStoreUtil.cleanUpDir(dirIndex, requestMetadata) // delete files and sub-dirs previously marked for removal
                .thenComposeAsync(v ->
                    blobStoreUtil.deleteDir(dirIndex, requestMetadata), executor) // deleted files and dirs still present
                .thenComposeAsync(v -> blobStoreUtil.deleteSnapshotIndexBlob(
                    scmAndSnapshotIndex.getLeft(), requestMetadata),
                    executor); // delete the snapshot index blob
        storeDeletionFutures.add(storeDeletionFuture);
      }
    });

    FutureUtil.allOf(storeDeletionFutures).join();
  }

  /**
   * Restores all eligible stores in the task.
   */
  @VisibleForTesting
  static CompletableFuture<Void> restoreStores(String jobName, String jobId, TaskName taskName, Set<String> storesToRestore,
      Map<String, Pair<String, SnapshotIndex>> prevStoreSnapshotIndexes,
      File loggedBaseDir, StorageConfig storageConfig, BlobStoreRestoreManagerMetrics metrics,
      StorageManagerUtil storageManagerUtil, BlobStoreUtil blobStoreUtil, DirDiffUtil dirDiffUtil,
      ExecutorService executor) {
    long restoreStartTime = System.nanoTime();
    List<CompletionStage<Void>> restoreFutures = new ArrayList<>();

    LOG.debug("Starting restore for task: {} stores: {}", taskName, storesToRestore);
    storesToRestore.forEach(storeName -> {
      if (!prevStoreSnapshotIndexes.containsKey(storeName)) {
        LOG.info("No checkpointed snapshot index found for task: {} store: {}. Skipping restore.", taskName, storeName);
        // TODO HIGH shesharm what should we do with the local state already present on disk, if any?
        // E.g. this will be the case if user changes a store from changelog based backup and restore to
        // blob store based backup and restore, both at the same time.
        return;
      }

      Pair<String, SnapshotIndex> scmAndSnapshotIndex = prevStoreSnapshotIndexes.get(storeName);

      long storeRestoreStartTime = System.nanoTime();
      SnapshotIndex snapshotIndex = scmAndSnapshotIndex.getRight();
      DirIndex dirIndex = snapshotIndex.getDirIndex();

      DirIndex.Stats stats = DirIndex.getStats(dirIndex);
      metrics.filesToRestore.getValue().addAndGet(stats.filesPresent);
      metrics.bytesToRestore.getValue().addAndGet(stats.bytesPresent);
      metrics.filesRemaining.getValue().addAndGet(stats.filesPresent);
      metrics.bytesRemaining.getValue().addAndGet(stats.bytesPresent);

      CheckpointId checkpointId = snapshotIndex.getSnapshotMetadata().getCheckpointId();
      File storeDir = storageManagerUtil.getTaskStoreDir(loggedBaseDir, storeName, taskName, TaskMode.Active);
      Path storeCheckpointDir = Paths.get(storageManagerUtil.getStoreCheckpointDir(storeDir, checkpointId));
      LOG.trace("Got task: {} store: {} local store directory: {} and local store checkpoint directory: {}",
          taskName, storeName, storeDir, storeCheckpointDir);

      // we always delete the store dir to preserve transactional state guarantees.
      try {
        LOG.debug("Deleting local store directory: {}. Will be restored from local store checkpoint directory " +
            "or remote snapshot.", storeDir);
        FileUtils.deleteDirectory(storeDir);
      } catch (IOException e) {
        throw new SamzaException(String.format("Error deleting store directory: %s", storeDir), e);
      }

      boolean shouldRestore = shouldRestore(taskName.getTaskName(), storeName, dirIndex,
          storeCheckpointDir, storageConfig, dirDiffUtil);

      if (shouldRestore) { // restore the store from the remote blob store
        // delete all store checkpoint directories. if we only delete the store directory and don't
        // delete the checkpoint directories, the store size on disk will grow to 2x after restore
        // until the first commit is completed and older checkpoint dirs are deleted. This is
        // because the hard-linked checkpoint dir files will no longer be de-duped with the
        // now-deleted main store directory contents and will take up additional space of their
        // own during the restore.
        deleteCheckpointDirs(taskName, storeName, loggedBaseDir, storageManagerUtil);

        metrics.storePreRestoreNs.get(storeName).set(System.nanoTime() - storeRestoreStartTime);
        enqueueRestore(jobName, jobId, taskName.toString(), storeName, storeDir, dirIndex, storeRestoreStartTime,
            restoreFutures, blobStoreUtil, dirDiffUtil, metrics, executor);
      } else {
        LOG.debug("Renaming store checkpoint directory: {} to store directory: {} since its contents are identical " +
            "to the remote snapshot.", storeCheckpointDir, storeDir);
        // atomically rename the checkpoint dir to the store dir
        new FileUtil().move(storeCheckpointDir.toFile(), storeDir);

        // delete any other checkpoint dirs.
        deleteCheckpointDirs(taskName, storeName, loggedBaseDir, storageManagerUtil);
      }
    });

    // wait for all restores to finish
    return FutureUtil.allOf(restoreFutures).whenComplete((res, ex) -> {
      LOG.info("Restore completed for task: {} stores", taskName);
      metrics.restoreNs.set(System.nanoTime() - restoreStartTime);
    });
  }

  /**
   * Determines if the store needs to be restored from remote snapshot based on local and remote state.
   */
  @VisibleForTesting
  static boolean shouldRestore(String taskName, String storeName, DirIndex dirIndex,
      Path storeCheckpointDir, StorageConfig storageConfig, DirDiffUtil dirDiffUtil) {
    // if a store checkpoint directory exists for the last successful task checkpoint, try to use it.
    boolean restoreStore;
    if (Files.exists(storeCheckpointDir)) {
      if (storageConfig.cleanLoggedStoreDirsOnStart(storeName)) {
        LOG.debug("Restoring task: {} store: {} from remote snapshot since the store is configured to be " +
            "restored on each restart.", taskName, storeName);
        restoreStore = true;
      } else if (dirDiffUtil.areSameDir(FILES_TO_IGNORE, false).test(storeCheckpointDir.toFile(), dirIndex)) {
        restoreStore = false; // no restore required for this store.
      } else {
        // we don't optimize for the case when the local host doesn't contain the most recent store checkpoint
        // directory but contains an older checkpoint directory which could have partial overlap with the remote
        // snapshot. we also don't try to optimize for any edge cases where the most recent checkpoint directory
        // contents could be partially different than the remote store (afaik, there is no known valid scenario
        // where this could happen right now, except for the offset file handling above).
        // it's simpler and fast enough for now to restore the entire store instead.

        LOG.error("Local store checkpoint directory: {} contents are not the same as the remote snapshot. " +
            "Queuing for restore from remote snapshot.", storeCheckpointDir);
        restoreStore = true;
      }
    } else { // did not find last checkpoint dir, restore the store from the remote blob store
      LOG.debug("No local store checkpoint directory found at: {}. " +
          "Queuing for restore from remote snapshot.", storeCheckpointDir);
      restoreStore = true;
    }

    return restoreStore;
  }

  /**
   * Starts the restore for the store, enqueuing all restore-completion futures into {@param restoreFutures}.
   */
  @VisibleForTesting
  static void enqueueRestore(String jobName, String jobId, String taskName, String storeName, File storeDir, DirIndex dirIndex,
      long storeRestoreStartTime, List<CompletionStage<Void>> restoreFutures, BlobStoreUtil blobStoreUtil,
      DirDiffUtil dirDiffUtil, BlobStoreRestoreManagerMetrics metrics, ExecutorService executor) {

    Metadata requestMetadata = new Metadata(storeDir.getAbsolutePath(), Optional.empty(), jobName, jobId, taskName, storeName);
    CompletableFuture<Void> restoreFuture =
        blobStoreUtil.restoreDir(storeDir, dirIndex, requestMetadata).thenRunAsync(() -> {
          metrics.storeRestoreNs.get(storeName).set(System.nanoTime() - storeRestoreStartTime);

          long postRestoreStartTime = System.nanoTime();
          LOG.trace("Comparing restored store directory: {} and remote directory to verify restore.", storeDir);
          if (!dirDiffUtil.areSameDir(FILES_TO_IGNORE, true).test(storeDir, dirIndex)) {
            metrics.storePostRestoreNs.get(storeName).set(System.nanoTime() - postRestoreStartTime);
            throw new SamzaException(
                String.format("Restored store directory: %s contents " + "are not the same as the remote snapshot.",
                    storeDir.getAbsolutePath()));
          } else {
            metrics.storePostRestoreNs.get(storeName).set(System.nanoTime() - postRestoreStartTime);
            LOG.info("Restore from remote snapshot completed for store: {}", storeDir);
          }
        }, executor);

    restoreFutures.add(restoreFuture);
  }

  private static void deleteCheckpointDirs(TaskName taskName, String storeName, File loggedBaseDir, StorageManagerUtil storageManagerUtil) {
    try {
      List<File> checkpointDirs = storageManagerUtil.getTaskStoreCheckpointDirs(
          loggedBaseDir, storeName, taskName, TaskMode.Active);
      for (File checkpointDir: checkpointDirs) {
        LOG.debug("Deleting local store checkpoint directory: {} before restore.", checkpointDir);
        FileUtils.deleteDirectory(checkpointDir);
      }
    } catch (Exception e) {
      throw new SamzaException(
          String.format("Error deleting checkpoint directory for task: %s store: %s.",
              taskName, storeName), e);
    }
  }
}
