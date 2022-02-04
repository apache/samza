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
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.storage.StorageManagerUtil;
import org.apache.samza.storage.TaskBackupManager;
import org.apache.samza.storage.blobstore.diff.DirDiff;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;
import org.apache.samza.storage.blobstore.metrics.BlobStoreBackupManagerMetrics;
import org.apache.samza.storage.blobstore.util.BlobStoreUtil;
import org.apache.samza.storage.blobstore.util.DirDiffUtil;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BlobStoreBackupManager implements TaskBackupManager {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreBackupManager.class);

  private final JobModel jobModel;
  private final ExecutorService executor;
  private final String jobName;
  private final String jobId;
  private final ContainerModel containerModel;
  private final TaskModel taskModel;
  private final String taskName;
  private final Config config;
  private final Clock clock;
  private final StorageManagerUtil storageManagerUtil;
  private final List<String> storesToBackup;
  private final File loggedStoreBaseDir;
  private final BlobStoreManager blobStoreManager;
  private final BlobStoreUtil blobStoreUtil;
  private final BlobStoreBackupManagerMetrics metrics;

  /**
   * Map of store name to a Pair of blob id of {@link SnapshotIndex} and the corresponding {@link SnapshotIndex} from
   * last successful task checkpoint or {@link #upload}.
   *
   * After {@link #init}, the map reflects the contents of the last completed checkpoint for the task from the previous
   * deployment, if any.
   *
   * During regular processing, this map is updated after each successful {@link #upload} with the blob id of
   * {@link SnapshotIndex} and the corresponding {@link SnapshotIndex} of the upload.
   *
   * The contents of this map are used to calculate the diff for local state between the last and the current checkpoint
   * during {@link #upload}.
   *
   * Since the task commit process guarantees that the async stage of the previous commit is complete before another
   * commit can start, this future is guaranteed to be complete in the call to {@link #upload} during the next commit.
   *
   * This field is non-final, since the future itself is replaced in its entirety after init/upload.
   * The internal map contents are never directly modified (e.g. using puts). It's volatile to ensure visibility
   * across threads since the map assignment may happen on a different thread than the one reading the contents.
   */
  private volatile CompletableFuture<Map<String, Pair<String, SnapshotIndex>>>
      prevStoreSnapshotIndexesFuture;

  public BlobStoreBackupManager(JobModel jobModel, ContainerModel containerModel, TaskModel taskModel,
      ExecutorService backupExecutor, BlobStoreBackupManagerMetrics blobStoreTaskBackupMetrics, Config config,
      Clock clock, File loggedStoreBaseDir, StorageManagerUtil storageManagerUtil, BlobStoreManager blobStoreManager) {
    this.jobModel = jobModel;
    this.jobName = new JobConfig(config).getName().get();
    this.jobId = new JobConfig(config).getJobId();
    this.containerModel = containerModel;
    this.taskModel = taskModel;
    this.taskName = taskModel.getTaskName().getTaskName();
    this.executor = backupExecutor;
    this.config = config;
    this.clock = clock;
    this.storageManagerUtil = storageManagerUtil;
    StorageConfig storageConfig = new StorageConfig(config);
    this.storesToBackup =
        storageConfig.getPersistentStoresWithBackupFactory(BlobStoreStateBackendFactory.class.getName());
    this.loggedStoreBaseDir = loggedStoreBaseDir;
    this.blobStoreManager = blobStoreManager;
    this.blobStoreUtil = createBlobStoreUtil(blobStoreManager, executor, blobStoreTaskBackupMetrics);
    this.prevStoreSnapshotIndexesFuture = CompletableFuture.completedFuture(ImmutableMap.of());
    this.metrics = blobStoreTaskBackupMetrics;
    metrics.initStoreMetrics(storesToBackup);
  }

  @Override
  public void init(Checkpoint checkpoint) {
    long startTime = System.nanoTime();
    LOG.debug("Initializing blob store backup manager for task: {}", taskName);

    blobStoreManager.init();

    // Note: blocks the caller thread.
    // TODO LOW shesharma exclude stores that are no longer configured during init
    Map<String, Pair<String, SnapshotIndex>> prevStoreSnapshotIndexes =
        blobStoreUtil.getStoreSnapshotIndexes(jobName, jobId, taskName, checkpoint, new HashSet<>(storesToBackup));
    this.prevStoreSnapshotIndexesFuture =
        CompletableFuture.completedFuture(ImmutableMap.copyOf(prevStoreSnapshotIndexes));
    metrics.initNs.set(System.nanoTime() - startTime);
  }

  @Override
  public Map<String, String> snapshot(CheckpointId checkpointId) {
    // No-op. Stores are flushed and checkpoints are created by commit manager
    return Collections.emptyMap();
  }

  @Override
  public CompletableFuture<Map<String, String>> upload(CheckpointId checkpointId, Map<String, String> storeSCMs) {
    long uploadStartTime = System.nanoTime();

    // reset gauges for each upload
    metrics.filesToUpload.getValue().set(0L);
    metrics.bytesToUpload.getValue().set(0L);
    metrics.filesUploaded.getValue().set(0L);
    metrics.bytesUploaded.getValue().set(0L);
    metrics.filesRemaining.getValue().set(0L);
    metrics.bytesRemaining.getValue().set(0L);
    metrics.filesToRetain.getValue().set(0L);
    metrics.bytesToRetain.getValue().set(0L);

    // This map is used to atomically replace the prevStoreSnapshotIndexesFuture map at the end of the task commit
    Map<String, CompletableFuture<Pair<String, SnapshotIndex>>>
        storeToSCMAndSnapshotIndexPairFutures = new HashMap<>();
    // This map is used to return serialized State Checkpoint Markers to the caller
    Map<String, CompletableFuture<String>> storeToSerializedSCMFuture = new HashMap<>();

    storesToBackup.forEach((storeName) -> {
      long storeUploadStartTime = System.nanoTime();
      try {
        // metadata for the current store snapshot to upload
        SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, storeName);

        // get the local store dir corresponding to the current checkpointId
        File storeDir = storageManagerUtil.getTaskStoreDir(loggedStoreBaseDir, storeName,
            taskModel.getTaskName(), taskModel.getTaskMode());
        String checkpointDirPath = storageManagerUtil.getStoreCheckpointDir(storeDir, checkpointId);
        File checkpointDir = new File(checkpointDirPath);

        LOG.debug("Got task: {} store: {} storeDir: {} and checkpointDir: {}",
            taskName, storeName, storeDir, checkpointDir);

        // guaranteed to be available since a new task commit may not start until the previous one is complete
        Map<String, Pair<String, SnapshotIndex>> prevStoreSnapshotIndexes =
            prevStoreSnapshotIndexesFuture.get(0, TimeUnit.MILLISECONDS);

        // get the previous store directory contents
        DirIndex prevDirIndex;

        if (prevStoreSnapshotIndexes.containsKey(storeName)) {
          prevDirIndex = prevStoreSnapshotIndexes.get(storeName).getRight().getDirIndex();
        } else {
          // no previous SnapshotIndex means that this is the first commit for this store. Create an empty DirIndex.
          prevDirIndex = new DirIndex(checkpointDir.getName(), Collections.emptyList(), Collections.emptyList(),
              Collections.emptyList(), Collections.emptyList());
        }

        long dirDiffStartTime = System.nanoTime();
        // get the diff between previous and current store directories
        DirDiff dirDiff = DirDiffUtil.getDirDiff(checkpointDir, prevDirIndex, DirDiffUtil.areSameFile(false));
        metrics.storeDirDiffNs.get(storeName).update(System.nanoTime() - dirDiffStartTime);

        DirDiff.Stats stats = DirDiff.getStats(dirDiff);
        updateStoreDiffMetrics(storeName, stats);
        metrics.filesToUpload.getValue().addAndGet(stats.filesAdded);
        metrics.bytesToUpload.getValue().addAndGet(stats.bytesAdded);
        // Note: FilesRemaining metric is set to FilesAdded in the beginning of the current upload and then counted down
        // for each upload.
        metrics.filesRemaining.getValue().addAndGet(stats.filesAdded);
        metrics.bytesRemaining.getValue().addAndGet(stats.bytesAdded);
        metrics.filesToRetain.getValue().addAndGet(stats.filesRetained);
        metrics.bytesToRetain.getValue().addAndGet(stats.bytesRetained);

        // upload the diff to the blob store and get the new directory index
        CompletionStage<DirIndex> dirIndexFuture = blobStoreUtil.putDir(dirDiff, snapshotMetadata);

        CompletionStage<SnapshotIndex> snapshotIndexFuture =
            dirIndexFuture.thenApplyAsync(dirIndex -> {
              LOG.trace("Dir upload complete. Returning new SnapshotIndex for task: {} store: {}.", taskName, storeName);
              Optional<String> prevSnapshotIndexBlobId =
                  Optional.ofNullable(prevStoreSnapshotIndexes.get(storeName)).map(Pair::getLeft);
              return new SnapshotIndex(clock.currentTimeMillis(), snapshotMetadata, dirIndex, prevSnapshotIndexBlobId);
            }, executor);

        // upload the new snapshot index to the blob store and get its blob id
        CompletionStage<String> snapshotIndexBlobIdFuture =
            snapshotIndexFuture
                .thenComposeAsync(si -> {
                  LOG.trace("Uploading Snapshot index for task: {} store: {}", taskName, storeName);
                  return blobStoreUtil.putSnapshotIndex(si);
                }, executor);

        // save store name and it's SnapshotIndex blob id and SnapshotIndex pair. At the end of the upload, atomically
        // update previous snapshot index map with this.
        CompletableFuture<Pair<String, SnapshotIndex>> scmAndSnapshotIndexPairFuture =
            FutureUtil.toFutureOfPair(
                Pair.of(snapshotIndexBlobIdFuture.toCompletableFuture(), snapshotIndexFuture.toCompletableFuture()));

        scmAndSnapshotIndexPairFuture.whenComplete((res, ex) -> {
          long uploadTimeNs = System.nanoTime() - storeUploadStartTime;
          metrics.storeUploadNs.get(storeName).update(uploadTimeNs);
        });

        storeToSCMAndSnapshotIndexPairFutures.put(storeName, scmAndSnapshotIndexPairFuture);
        storeToSerializedSCMFuture.put(storeName, snapshotIndexBlobIdFuture.toCompletableFuture());
      } catch (Exception e) {
        throw new SamzaException(
            String.format("Error uploading store snapshot to blob store for task: %s, store: %s, checkpointId: %s",
                taskName, storeName, checkpointId), e);
      }
    });

    // replace the previous storeName to snapshot index mapping with the new mapping.
    this.prevStoreSnapshotIndexesFuture =
        FutureUtil.toFutureOfMap(storeToSCMAndSnapshotIndexPairFutures);

    return FutureUtil.toFutureOfMap(storeToSerializedSCMFuture)
        .whenComplete((res, ex) -> metrics.uploadNs.update(System.nanoTime() - uploadStartTime));
  }

  /**
   * Clean up would be called at the end of every commit as well as on a container start/restart.
   * Clean up involves the following steps:
   * 1. Remove TTL of the snapshot index blob and for any associated files and sub-dirs marked for retention.
   * 2. Delete the files/subdirs marked for deletion in the snapshot index.
   * 3. Delete the remote {@link SnapshotIndex} blob for the previous checkpoint.
   * @param checkpointId the {@link CheckpointId} of the last successfully committed checkpoint.
   * @param storeSCMs store name to state checkpoint markers for the last successfully committed checkpoint
   */
  @Override
  public CompletableFuture<Void> cleanUp(CheckpointId checkpointId, Map<String, String> storeSCMs) {
    long startTime = System.nanoTime();
    List<CompletionStage<Void>> removeTTLFutures = new ArrayList<>();
    List<CompletionStage<Void>> cleanupRemoteSnapshotFutures = new ArrayList<>();
    List<CompletionStage<Void>> removePrevRemoteSnapshotFutures = new ArrayList<>();

    // SCM, in case of blob store backup and restore, is just the blob id of SnapshotIndex representing the remote snapshot
    storeSCMs.forEach((storeName, snapshotIndexBlobId) -> {
      // Only perform cleanup for stores configured with BlobStore State Backend Factory
      if (storesToBackup.contains(storeName)) {
        Metadata requestMetadata =
            new Metadata(Metadata.SNAPSHOT_INDEX_PAYLOAD_PATH, Optional.empty(), jobName, jobId, taskName, storeName);
        CompletionStage<SnapshotIndex> snapshotIndexFuture =
            blobStoreUtil.getSnapshotIndex(snapshotIndexBlobId, requestMetadata);

        // 1. remove TTL of index blob and all of its files and sub-dirs marked for retention
        CompletionStage<Void> removeTTLFuture =
            snapshotIndexFuture.thenComposeAsync(snapshotIndex -> {
              LOG.debug("Removing TTL for index blob: {} and all of its files and sub-dirs for task: {} store :{}",
                  snapshotIndexBlobId, taskName, storeName);
              return blobStoreUtil.removeTTL(snapshotIndexBlobId, snapshotIndex, requestMetadata);
            }, executor);
        removeTTLFutures.add(removeTTLFuture);

        // 2. delete the files/subdirs marked for deletion in the snapshot index.
        CompletionStage<Void> cleanupRemoteSnapshotFuture =
            snapshotIndexFuture.thenComposeAsync(snapshotIndex -> {
              LOG.debug("Deleting files and dirs to remove for current index blob: {} for task: {} store: {}",
                  snapshotIndexBlobId, taskName, storeName);
              return blobStoreUtil.cleanUpDir(snapshotIndex.getDirIndex(), requestMetadata);
            }, executor);

        cleanupRemoteSnapshotFutures.add(cleanupRemoteSnapshotFuture);

        // 3. delete the remote {@link SnapshotIndex} blob for the previous checkpoint.
        CompletionStage<Void> removePrevRemoteSnapshotFuture =
            snapshotIndexFuture.thenComposeAsync(snapshotIndex -> {
              if (snapshotIndex.getPrevSnapshotIndexBlobId().isPresent()) {
                String blobId = snapshotIndex.getPrevSnapshotIndexBlobId().get();
                LOG.debug("Removing previous snapshot index blob: {} from blob store for task: {} store: {}.",
                    blobId, taskName, storeName);
                return blobStoreUtil.deleteSnapshotIndexBlob(blobId, requestMetadata);
              } else {
                // complete future immediately. There are no previous snapshots index blobs to delete.
                return CompletableFuture.completedFuture(null);
              }
            }, executor);
        removePrevRemoteSnapshotFutures.add(removePrevRemoteSnapshotFuture);
      }
    });

    return FutureUtil.allOf(removeTTLFutures, cleanupRemoteSnapshotFutures, removePrevRemoteSnapshotFutures)
        .whenComplete((res, ex) -> metrics.cleanupNs.update(System.nanoTime() - startTime));
  }

  @Override
  public void close() {
    blobStoreManager.close();
  }

  @VisibleForTesting
  protected BlobStoreUtil createBlobStoreUtil(BlobStoreManager blobStoreManager, ExecutorService executor,
      BlobStoreBackupManagerMetrics metrics) {
    return new BlobStoreUtil(blobStoreManager, executor, metrics, null);
  }

  private void updateStoreDiffMetrics(String storeName, DirDiff.Stats stats) {
    metrics.storeFilesToUpload.get(storeName).set((long) stats.filesAdded);
    metrics.storeFilesToRetain.get(storeName).set((long) stats.filesRetained);
    metrics.storeFilesToRemove.get(storeName).set((long) stats.filesRemoved);

    metrics.storeSubDirsToUpload.get(storeName).set((long) stats.subDirsAdded);
    metrics.storeSubDirsToRetain.get(storeName).set((long) stats.subDirsRetained);
    metrics.storeSubDirsToRemove.get(storeName).set((long) stats.subDirsRemoved);

    metrics.storeBytesToUpload.get(storeName).set(stats.bytesAdded);
    metrics.storeBytesToRetain.get(storeName).set(stats.bytesRetained);
    metrics.storeBytesToRemove.get(storeName).set(stats.bytesRemoved);
  }
}
