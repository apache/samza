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

package org.apache.samza.storage.blobstore.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.storage.blobstore.BlobStoreManager;
import org.apache.samza.storage.blobstore.BlobStoreStateBackendFactory;
import org.apache.samza.storage.blobstore.Metadata;
import org.apache.samza.storage.blobstore.diff.DirDiff;
import org.apache.samza.storage.blobstore.exceptions.DeletedException;
import org.apache.samza.storage.blobstore.exceptions.RetriableException;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.FileBlob;
import org.apache.samza.storage.blobstore.index.FileIndex;
import org.apache.samza.storage.blobstore.index.FileMetadata;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;
import org.apache.samza.storage.blobstore.index.serde.SnapshotIndexSerde;
import org.apache.samza.storage.blobstore.metrics.BlobStoreBackupManagerMetrics;
import org.apache.samza.storage.blobstore.metrics.BlobStoreRestoreManagerMetrics;
import org.apache.samza.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper methods to interact with remote blob store service and GET/PUT/DELETE a
 * {@link SnapshotIndex} or {@link DirDiff}.
 */
public class BlobStoreUtil {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreUtil.class);

  private final BlobStoreManager blobStoreManager;
  private final ExecutorService executor;
  private final BlobStoreBackupManagerMetrics backupMetrics;
  private final BlobStoreRestoreManagerMetrics restoreMetrics;

  public BlobStoreUtil(BlobStoreManager blobStoreManager, ExecutorService executor,
      BlobStoreBackupManagerMetrics backupMetrics, BlobStoreRestoreManagerMetrics restoreMetrics) {
    this.blobStoreManager = blobStoreManager;
    this.executor = executor;
    this.backupMetrics = backupMetrics;
    this.restoreMetrics = restoreMetrics;
  }

  /**
   * Get the blob id of {@link SnapshotIndex} and {@link SnapshotIndex}es for the provided {@code task}
   * in the provided {@code checkpoint}.
   * @param jobName job name is used to build request metadata
   * @param jobId job id is used to build request metadata
   * @param taskName task name to get the store state checkpoint markers and snapshot indexes for
   * @param checkpoint {@link Checkpoint} instance to get the store state checkpoint markers from. Only
   *                   {@link CheckpointV2} and newer are supported for blob stores.
   * @return Map of store name to its blob id of snapshot indices and their corresponding snapshot indices for the task.
   */
  public Map<String, Pair<String, SnapshotIndex>> getStoreSnapshotIndexes(
      String jobName, String jobId, String taskName, Checkpoint checkpoint) {
    //TODO MED shesharma document error handling (checkpoint ver, blob not found, getBlob)
    if (checkpoint == null) {
      LOG.debug("No previous checkpoint found for taskName: {}", taskName);
      return ImmutableMap.of();
    }

    if (checkpoint.getVersion() == 1) {
      throw new SamzaException("Checkpoint version 1 is not supported for blob store backup and restore.");
    }

    Map<String, CompletableFuture<Pair<String, SnapshotIndex>>>
        storeSnapshotIndexFutures = new HashMap<>();

    CheckpointV2 checkpointV2 = (CheckpointV2) checkpoint;
    Map<String, Map<String, String>> factoryToStoreSCMs = checkpointV2.getStateCheckpointMarkers();
    Map<String, String> storeSnapshotIndexBlobIds = factoryToStoreSCMs.get(BlobStoreStateBackendFactory.class.getName());

    if (storeSnapshotIndexBlobIds != null) {
      storeSnapshotIndexBlobIds.forEach((storeName, snapshotIndexBlobId) -> {
        try {
          LOG.debug("Getting snapshot index for taskName: {} store: {} blobId: {}", taskName, storeName, snapshotIndexBlobId);
          Metadata requestMetadata =
              new Metadata(Metadata.SNAPSHOT_INDEX_PAYLOAD_PATH, Optional.empty(), jobName, jobId, taskName, storeName);
          CompletableFuture<SnapshotIndex> snapshotIndexFuture =
              getSnapshotIndex(snapshotIndexBlobId, requestMetadata).toCompletableFuture();
          Pair<CompletableFuture<String>, CompletableFuture<SnapshotIndex>> pairOfFutures =
              Pair.of(CompletableFuture.completedFuture(snapshotIndexBlobId), snapshotIndexFuture);

          // save the future and block once in the end instead of blocking for each request.
          storeSnapshotIndexFutures.put(storeName, FutureUtil.toFutureOfPair(pairOfFutures));
        } catch (Exception e) {
          throw new SamzaException(
              String.format("Error getting SnapshotIndex for blobId: %s for taskName: %s store: %s",
                  snapshotIndexBlobId, taskName, storeName), e);
        }
      });
    } else {
      LOG.debug("No store SCMs found for blob store state backend in for taskName: {} in checkpoint {}",
          taskName, checkpointV2.getCheckpointId());
    }

    try {
      return FutureUtil.toFutureOfMap(t -> {
        Throwable unwrappedException = FutureUtil.unwrapExceptions(CompletionException.class, t);
        if (unwrappedException instanceof DeletedException) {
          LOG.warn("Ignoring already deleted snapshot index for taskName: {}", taskName, t);
          return true;
        } else {
          return false;
        }
      }, storeSnapshotIndexFutures).join();
    } catch (Exception e) {
      throw new SamzaException(
          String.format("Error while waiting to get store snapshot indexes for task %s", taskName), e);
    }
  }

  /**
   * GETs the {@link SnapshotIndex} from the blob store.
   * @param blobId blob ID of the {@link SnapshotIndex} to get
   * @return a Future containing the {@link SnapshotIndex}
   */
  public CompletableFuture<SnapshotIndex> getSnapshotIndex(String blobId, Metadata metadata) {
    Preconditions.checkState(StringUtils.isNotBlank(blobId));
    String opName = "getSnapshotIndex: " + blobId;
    return FutureUtil.executeAsyncWithRetries(opName, () -> {
      ByteArrayOutputStream indexBlobStream = new ByteArrayOutputStream(); // no need to close ByteArrayOutputStream
      return blobStoreManager.get(blobId, indexBlobStream, metadata).toCompletableFuture()
          .thenApplyAsync(f -> new SnapshotIndexSerde().fromBytes(indexBlobStream.toByteArray()), executor);
    }, isCauseNonRetriable(), executor);
  }

  /**
   * PUTs the {@link SnapshotIndex} to the blob store.
   * @param snapshotIndex SnapshotIndex to put.
   * @return a Future containing the blob ID of the {@link SnapshotIndex}.
   */
  public CompletableFuture<String> putSnapshotIndex(SnapshotIndex snapshotIndex) {
    byte[] bytes = new SnapshotIndexSerde().toBytes(snapshotIndex);
    String opName = "putSnapshotIndex for checkpointId: " + snapshotIndex.getSnapshotMetadata().getCheckpointId();
    return FutureUtil.executeAsyncWithRetries(opName, () -> {
      InputStream inputStream = new ByteArrayInputStream(bytes); // no need to close ByteArrayInputStream
      SnapshotMetadata snapshotMetadata = snapshotIndex.getSnapshotMetadata();
      Metadata metadata = new Metadata(Metadata.SNAPSHOT_INDEX_PAYLOAD_PATH, Optional.of((long) bytes.length),
          snapshotMetadata.getJobName(), snapshotMetadata.getJobId(), snapshotMetadata.getTaskName(),
          snapshotMetadata.getStoreName());
      return blobStoreManager.put(inputStream, metadata).toCompletableFuture();
    }, isCauseNonRetriable(), executor);
  }

  /**
   * WARNING: This method deletes the **SnapshotIndex blob** from the snapshot. This should only be called to clean
   * up an older snapshot **AFTER** all the files and sub-dirs to be deleted from this snapshot are already deleted
   * using {@link #cleanUpDir(DirIndex, Metadata)}
   *
   * @param snapshotIndexBlobId blob ID of SnapshotIndex blob to delete
   * @return a future that completes when the index blob is deleted from remote store.
   */
  public CompletionStage<Void> deleteSnapshotIndexBlob(String snapshotIndexBlobId, Metadata metadata) {
    Preconditions.checkState(StringUtils.isNotBlank(snapshotIndexBlobId));
    LOG.debug("Deleting SnapshotIndex blob: {} from blob store", snapshotIndexBlobId);
    String opName = "deleteSnapshotIndexBlob: " + snapshotIndexBlobId;
    return FutureUtil.executeAsyncWithRetries(opName, () ->
        blobStoreManager.delete(snapshotIndexBlobId, metadata).toCompletableFuture(), isCauseNonRetriable(), executor);
  }

  /**
   * Non-blocking restore of a {@link SnapshotIndex} to local store by downloading all the files and sub-dirs associated
   * with this remote snapshot.
   * @return A future that completes when all the async downloads completes
   */
  public CompletableFuture<Void> restoreDir(File baseDir, DirIndex dirIndex, Metadata metadata) {
    LOG.debug("Restoring contents of directory: {} from remote snapshot.", baseDir);

    List<CompletableFuture<Void>> downloadFutures = new ArrayList<>();

    try {
      // create parent directories if they don't exist
      Files.createDirectories(baseDir.toPath());
    } catch (IOException exception) {
      LOG.error("Error creating directory: {} for restore", baseDir.getAbsolutePath(), exception);
      throw new SamzaException(String.format("Error creating directory: %s for restore",
          baseDir.getAbsolutePath()), exception);
    }

    // restore all files in the directory
    for (FileIndex fileIndex : dirIndex.getFilesPresent()) {
      File fileToRestore = Paths.get(baseDir.getAbsolutePath(), fileIndex.getFileName()).toFile();
      Metadata requestMetadata =
          new Metadata(fileToRestore.getAbsolutePath(), Optional.of(fileIndex.getFileMetadata().getSize()),
              metadata.getJobName(), metadata.getJobId(), metadata.getTaskName(), metadata.getStoreName());
      List<FileBlob> fileBlobs = fileIndex.getBlobs();

      String opName = "restoreFile: " + fileToRestore.getAbsolutePath();
      CompletableFuture<Void> fileRestoreFuture =
          FutureUtil.executeAsyncWithRetries(opName, () -> getFile(fileBlobs, fileToRestore, requestMetadata),
              isCauseNonRetriable(), executor);
      downloadFutures.add(fileRestoreFuture);
    }

    // restore any sub-directories
    List<DirIndex> subDirs = dirIndex.getSubDirsPresent();
    for (DirIndex subDir : subDirs) {
      File subDirFile = Paths.get(baseDir.getAbsolutePath(), subDir.getDirName()).toFile();
      downloadFutures.add(restoreDir(subDirFile, subDir, metadata));
    }

    return FutureUtil.allOf(downloadFutures);
  }

  /**
   * Recursively upload all new files and upload or update contents of all subdirs in the {@link DirDiff} and return a
   * Future containing the {@link DirIndex} associated with the directory.
   * @param dirDiff diff for the contents of this directory
   * @return A future with the {@link DirIndex} if the upload completed successfully.
   */
  public CompletionStage<DirIndex> putDir(DirDiff dirDiff, SnapshotMetadata snapshotMetadata) {
    // Upload all new files in the dir
    List<File> filesToUpload = dirDiff.getFilesAdded();
    List<CompletionStage<FileIndex>> fileFutures = filesToUpload.stream()
        .map(file -> putFile(file, snapshotMetadata))
        .collect(Collectors.toList());

    CompletableFuture<Void> allFilesFuture =
        CompletableFuture.allOf(fileFutures.toArray(new CompletableFuture[0]));

    List<CompletionStage<DirIndex>> subDirFutures = new ArrayList<>();
    // recursively upload all new subdirs of this dir
    for (DirDiff subDirAdded: dirDiff.getSubDirsAdded()) {
      subDirFutures.add(putDir(subDirAdded, snapshotMetadata));
    }
    // recursively update contents of all subdirs that are retained but might have been modified
    for (DirDiff subDirRetained: dirDiff.getSubDirsRetained()) {
      subDirFutures.add(putDir(subDirRetained, snapshotMetadata));
    }
    CompletableFuture<Void> allDirBlobsFuture =
        CompletableFuture.allOf(subDirFutures.toArray(new CompletableFuture[0]));

    return CompletableFuture.allOf(allDirBlobsFuture, allFilesFuture)
        .thenApplyAsync(f -> {
          LOG.trace("All file and dir uploads complete for task: {} store: {}",
              snapshotMetadata.getTaskName(), snapshotMetadata.getStoreName());
          List<FileIndex> filesPresent = fileFutures.stream()
              .map(blob -> blob.toCompletableFuture().join())
              .collect(Collectors.toList());

          filesPresent.addAll(dirDiff.getFilesRetained());

          List<DirIndex> subDirsPresent = subDirFutures.stream()
              .map(subDir -> subDir.toCompletableFuture().join())
              .collect(Collectors.toList());

          LOG.debug("Uploaded diff for task: {} store: {} with statistics: {}",
              snapshotMetadata.getTaskName(), snapshotMetadata.getStoreName(), DirDiff.getStats(dirDiff));

          LOG.trace("Returning new DirIndex for task: {} store: {}",
              snapshotMetadata.getTaskName(), snapshotMetadata.getStoreName());
          return new DirIndex(dirDiff.getDirName(),
              filesPresent,
              dirDiff.getFilesRemoved(),
              subDirsPresent,
              dirDiff.getSubDirsRemoved());
        }, executor);
  }

  /**
   * WARNING: Recursively delete **ALL** the associated files and subdirs within the provided {@link DirIndex}.
   * @param dirIndex {@link DirIndex} whose entire contents are to be deleted.
   * @param metadata {@link Metadata} related to the request
   * @return a future that completes when ALL the files and subdirs associated with the dirIndex have been
   * marked for deleted in the remote blob store.
   */
  public CompletionStage<Void> deleteDir(DirIndex dirIndex, Metadata metadata) {
    LOG.debug("Completely deleting dir: {} in blob store", dirIndex.getDirName());
    List<CompletionStage<Void>> deleteFutures = new ArrayList<>();
    // Delete all files present in subDir
    for (FileIndex file: dirIndex.getFilesPresent()) {
      Metadata requestMetadata =
          new Metadata(file.getFileName(), Optional.of(file.getFileMetadata().getSize()),
              metadata.getJobName(), metadata.getJobId(), metadata.getTaskName(), metadata.getStoreName());
      deleteFutures.add(deleteFile(file, requestMetadata));
    }

    // Delete all subDirs present recursively
    for (DirIndex subDir: dirIndex.getSubDirsPresent()) {
      deleteFutures.add(deleteDir(subDir, metadata));
    }

    return CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]));
  }


  /**
   * Recursively issue delete requests for files and dirs marked to be removed in a previously created remote snapshot.
   * Note: We do not immediately delete files/dirs to be removed when uploading a snapshot to the remote
   * store. We just track them for deletion during the upload, and delete them AFTER the snapshot is uploaded, and the
   * blob IDs have been persisted as part of the checkpoint. This is to prevent data loss if a failure happens
   * part way through the commit. We issue delete these file/subdirs in cleanUp() phase of commit lifecycle.
   * @param dirIndex the dir in the remote snapshot to clean up.
   * @param metadata Metadata related to the request
   * @return a future that completes when all the files and subdirs marked for deletion are cleaned up.
   */
  public CompletionStage<Void> cleanUpDir(DirIndex dirIndex, Metadata metadata) {
    String dirName = dirIndex.getDirName();
    if (DirIndex.ROOT_DIR_NAME.equals(dirName)) {
      LOG.debug("Cleaning up root dir in blob store.");
    } else {
      LOG.debug("Cleaning up dir: {} in blob store.", dirIndex.getDirName());
    }

    List<CompletionStage<Void>> cleanUpFuture = new ArrayList<>();
    List<FileIndex> files = dirIndex.getFilesRemoved();
    for (FileIndex file: files) {
      Metadata requestMetadata =
          new Metadata(file.getFileName(), Optional.of(file.getFileMetadata().getSize()), metadata.getJobName(),
              metadata.getJobId(), metadata.getTaskName(), metadata.getStoreName());
      cleanUpFuture.add(deleteFile(file, requestMetadata));
    }

    for (DirIndex subDirToDelete : dirIndex.getSubDirsRemoved()) {
      // recursively delete ALL contents of the subDirToDelete.
      cleanUpFuture.add(deleteDir(subDirToDelete, metadata));
    }

    for (DirIndex subDirToRetain : dirIndex.getSubDirsPresent()) {
      // recursively clean up the subDir, only deleting files and subdirs marked for deletion.
      cleanUpFuture.add(cleanUpDir(subDirToRetain, metadata));
    }

    return CompletableFuture.allOf(cleanUpFuture.toArray(new CompletableFuture[0]));
  }

  /**
   * Gets a file from the blob store.
   * @param fileBlobs List of {@link FileBlob}s that constitute this file.
   * @param fileToRestore File pointing to the local path where the file will be restored.
   * @param requestMetadata {@link Metadata} associated with this request
   * @return a future that completes when the file is downloaded and written or if an exception occurs.
   */
  @VisibleForTesting
  CompletableFuture<Void> getFile(List<FileBlob> fileBlobs, File fileToRestore, Metadata requestMetadata) {
    FileOutputStream outputStream = null;
    try {
      long restoreFileStartTime = System.nanoTime();
      if (fileToRestore.exists()) {
        // delete the file if it already exists, e.g. from a previous retry.
        Files.delete(fileToRestore.toPath());
      }

      outputStream = new FileOutputStream(fileToRestore);
      final FileOutputStream finalOutputStream = outputStream;
      // TODO HIGH shesharm add integration tests to ensure empty files and directories are handled correctly E2E.
      fileToRestore.createNewFile(); // create file for 0 byte files (fileIndex entry but no fileBlobs).
      // create a copy to ensure list being sorted is mutable.
      List<FileBlob> fileBlobsCopy = new ArrayList<>(fileBlobs);
      fileBlobsCopy.sort(Comparator.comparingInt(FileBlob::getOffset)); // sort by offset.

      // chain the futures such that write to file for blobs is sequential.
      // can be optimized to write concurrently to the file later.
      CompletableFuture<Void> resultFuture = CompletableFuture.completedFuture(null);
      for (FileBlob fileBlob : fileBlobsCopy) {
        resultFuture = resultFuture.thenComposeAsync(v -> {
          LOG.debug("Starting restore for file: {} with blob id: {} at offset: {}", fileToRestore, fileBlob.getBlobId(),
              fileBlob.getOffset());
          return blobStoreManager.get(fileBlob.getBlobId(), finalOutputStream, requestMetadata);
        }, executor);
      }

      resultFuture = resultFuture.thenRunAsync(() -> {
        LOG.debug("Finished restore for file: {}. Closing output stream.", fileToRestore);
        try {
          // flush the file contents to disk
          finalOutputStream.getFD().sync();
          finalOutputStream.close();
        } catch (Exception e) {
          throw new SamzaException(String.format("Error closing output stream for file: %s", fileToRestore.getAbsolutePath()), e);
        }
      }, executor);

      resultFuture.whenComplete((res, ex) -> {
        if (restoreMetrics != null) {
          restoreMetrics.avgFileRestoreNs.update(System.nanoTime() - restoreFileStartTime);

          long fileSize = requestMetadata.getPayloadSize();
          restoreMetrics.restoreRate.inc(fileSize);
          restoreMetrics.filesRestored.getValue().addAndGet(1);
          restoreMetrics.bytesRestored.getValue().addAndGet(fileSize);
          restoreMetrics.filesRemaining.getValue().addAndGet(-1);
          restoreMetrics.bytesRemaining.getValue().addAndGet(-1 * fileSize);
        }
      });
      return resultFuture;
    } catch (Exception exception) {
      try {
        if (outputStream != null) {
          outputStream.close();
        }
      } catch (Exception err) {
        LOG.error("Error closing output stream for file: {}", fileToRestore.getAbsolutePath(), err);
      }

      throw new SamzaException(String.format("Error restoring file: %s in path: %s",
          fileToRestore.getName(), requestMetadata.getPayloadPath()), exception);
    }
  }

  /**
   * Upload a File to blob store.
   * @param file File to upload to blob store.
   * @return A future containing the {@link FileIndex} for the uploaded file.
   */
  @VisibleForTesting
  public CompletableFuture<FileIndex> putFile(File file, SnapshotMetadata snapshotMetadata) {
    if (file == null || !file.isFile()) {
      String message = file != null ? "Dir or Symbolic link" : "null";
      throw new SamzaException(String.format("Required a non-null parameter of type file, provided: %s", message));
    }
    long putFileStartTime = System.nanoTime();

    String opName = "putFile: " + file.getAbsolutePath();
    Supplier<CompletionStage<FileIndex>> fileUploadAction = () -> {
      LOG.debug("Putting file: {} to blob store.", file.getPath());
      CompletableFuture<FileIndex> fileBlobFuture;
      CheckedInputStream inputStream = null;
      try {
        // TODO HIGH shesharm maybe use the more efficient CRC32C / PureJavaCRC32 impl
        inputStream = new CheckedInputStream(new FileInputStream(file), new CRC32());
        CheckedInputStream finalInputStream = inputStream;
        FileMetadata fileMetadata = FileMetadata.fromFile(file);
        if (backupMetrics != null) {
          backupMetrics.avgFileSizeBytes.update(fileMetadata.getSize());
        }

        Metadata metadata =
            new Metadata(file.getAbsolutePath(), Optional.of(fileMetadata.getSize()), snapshotMetadata.getJobName(),
                snapshotMetadata.getJobId(), snapshotMetadata.getTaskName(), snapshotMetadata.getStoreName());

        fileBlobFuture = blobStoreManager.put(inputStream, metadata)
            .thenApplyAsync(id -> {
              LOG.trace("Put complete. Received Blob ID {}. Closing input stream for file: {}.", id, file.getPath());
              try {
                finalInputStream.close();
              } catch (Exception e) {
                throw new SamzaException(String.format("Error closing input stream for file: %s",
                    file.getAbsolutePath()), e);
              }

              LOG.trace("Returning new FileIndex for file: {}.", file.getPath());
              return new FileIndex(
                  file.getName(),
                  Collections.singletonList(new FileBlob(id, 0)),
                  fileMetadata,
                  finalInputStream.getChecksum().getValue());
            }, executor).toCompletableFuture();
      } catch (Exception e) {
        try {
          if (inputStream != null) {
            inputStream.close();
          }
        } catch (Exception err) {
          LOG.error("Error closing input stream for file: {}", file.getName(), err);
        }
        LOG.error("Error putting file: {}", file.getName(), e);
        throw new SamzaException(String.format("Error putting file %s", file.getAbsolutePath()), e);
      }
      return fileBlobFuture;
    };

    return FutureUtil.executeAsyncWithRetries(opName, fileUploadAction, isCauseNonRetriable(), executor)
        .whenComplete((res, ex) -> {
          if (backupMetrics != null) {
            backupMetrics.avgFileUploadNs.update(System.nanoTime() - putFileStartTime);

            long fileSize = file.length();
            backupMetrics.uploadRate.inc(fileSize);
            backupMetrics.filesUploaded.getValue().addAndGet(1);
            backupMetrics.bytesUploaded.getValue().addAndGet(fileSize);
            backupMetrics.filesRemaining.getValue().addAndGet(-1);
            backupMetrics.bytesRemaining.getValue().addAndGet(-1 * fileSize);
          }
        });
  }

  /**
   * Delete a {@link FileIndex} from the remote store by deleting all {@link FileBlob}s associated with it.
   * @param fileIndex FileIndex of the file to delete from the remote store.
   * @param metadata
   * @return a future that completes when the FileIndex has been marked for deletion in the remote blob store.
   */
  private CompletionStage<Void> deleteFile(FileIndex fileIndex, Metadata metadata) {
    List<CompletionStage<Void>> deleteFutures = new ArrayList<>();
    List<FileBlob> fileBlobs = fileIndex.getBlobs();
    for (FileBlob fileBlob : fileBlobs) {
      LOG.debug("Deleting file: {} blobId: {} from blob store.", fileIndex.getFileName(), fileBlob.getBlobId());
      String opName = "deleteFile: " + fileIndex.getFileName() + " blobId: " + fileBlob.getBlobId();
      Supplier<CompletionStage<Void>> fileDeletionAction = () ->
          blobStoreManager.delete(fileBlob.getBlobId(), metadata).toCompletableFuture();
      CompletableFuture<Void> fileDeletionFuture =
          FutureUtil.executeAsyncWithRetries(opName, fileDeletionAction, isCauseNonRetriable(), executor);
      deleteFutures.add(fileDeletionFuture);
    }

    return CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]));
  }

  /**
   * Recursively mark all the blobs associated with the {@link DirIndex} to never expire (remove TTL).
   * @param dirIndex the {@link DirIndex} whose contents' TTL needs to be removed
   * @param metadata {@link Metadata} related to the request
   * @return A future that completes when all the blobs associated with this dirIndex are marked to
   * never expire.
   */
  private CompletableFuture<Void> removeTTL(DirIndex dirIndex, Metadata metadata) {
    String dirName = dirIndex.getDirName();
    if (DirIndex.ROOT_DIR_NAME.equals(dirName)) {
      LOG.debug("Removing TTL for files and dirs present in DirIndex for root dir.");
    } else {
      LOG.debug("Removing TTL for files and dirs present in DirIndex for dir: {}", dirName);
    }

    List<CompletableFuture<Void>> updateTTLsFuture = new ArrayList<>();
    for (DirIndex subDir: dirIndex.getSubDirsPresent()) {
      updateTTLsFuture.add(removeTTL(subDir, metadata));
    }

    for (FileIndex file: dirIndex.getFilesPresent()) {
      Metadata requestMetadata =
          new Metadata(file.getFileName(), Optional.of(file.getFileMetadata().getSize()),
              metadata.getJobName(), metadata.getJobId(), metadata.getTaskName(), metadata.getStoreName());
      List<FileBlob> fileBlobs = file.getBlobs();
      for (FileBlob fileBlob : fileBlobs) {
        String opname = "removeTTL for fileBlob: " + file.getFileName() + " with blobId: {}" + fileBlob.getBlobId();
        Supplier<CompletionStage<Void>> ttlRemovalAction = () ->
            blobStoreManager.removeTTL(fileBlob.getBlobId(), requestMetadata).toCompletableFuture();
        CompletableFuture<Void> ttlRemovalFuture =
            FutureUtil.executeAsyncWithRetries(opname, ttlRemovalAction, isCauseNonRetriable(), executor);
        updateTTLsFuture.add(ttlRemovalFuture);
      }
    }

    return CompletableFuture.allOf(updateTTLsFuture.toArray(new CompletableFuture[0]));
  }


  /**
   * Marks all the blobs associated with an {@link SnapshotIndex} to never expire.
   * @param snapshotIndex {@link SnapshotIndex} of the remote snapshot
   * @param metadata {@link Metadata} related to the request
   * @return A future that completes when all the files and subdirs associated with this remote snapshot are marked to
   * never expire.
   */
  public CompletionStage<Void> removeTTL(String indexBlobId, SnapshotIndex snapshotIndex, Metadata metadata) {
    SnapshotMetadata snapshotMetadata = snapshotIndex.getSnapshotMetadata();
    LOG.debug("Marking contents of SnapshotIndex: {} to never expire", snapshotMetadata.toString());

    String opName = "removeTTL for SnapshotIndex for checkpointId: " + snapshotMetadata.getCheckpointId();
    Supplier<CompletionStage<Void>> removeDirIndexTTLAction =
      () -> removeTTL(snapshotIndex.getDirIndex(), metadata).toCompletableFuture();
    CompletableFuture<Void> dirIndexTTLRemovalFuture =
        FutureUtil.executeAsyncWithRetries(opName, removeDirIndexTTLAction, isCauseNonRetriable(), executor);

    return dirIndexTTLRemovalFuture.thenComposeAsync(aVoid -> {
      String op2Name = "removeTTL for indexBlobId: " + indexBlobId;
      Supplier<CompletionStage<Void>> removeIndexBlobTTLAction =
        () -> blobStoreManager.removeTTL(indexBlobId, metadata).toCompletableFuture();
      return FutureUtil.executeAsyncWithRetries(op2Name, removeIndexBlobTTLAction, isCauseNonRetriable(), executor);
    }, executor);
  }

  private static Predicate<Throwable> isCauseNonRetriable() {
    return throwable -> {
      Throwable unwrapped = FutureUtil.unwrapExceptions(CompletionException.class, throwable);
      return unwrapped != null && !RetriableException.class.isAssignableFrom(unwrapped.getClass());
    };
  }
}