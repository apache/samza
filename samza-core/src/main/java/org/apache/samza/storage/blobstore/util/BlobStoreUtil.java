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
import java.util.Optional;
import org.apache.samza.storage.blobstore.BlobStoreManager;
import org.apache.samza.storage.blobstore.metrics.BlobStoreBackupManagerMetrics;
import org.apache.samza.storage.blobstore.Metadata;
import org.apache.samza.storage.blobstore.diff.DirDiff;
import org.apache.samza.storage.blobstore.exceptions.RetriableException;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.FileBlob;
import org.apache.samza.storage.blobstore.index.FileIndex;
import org.apache.samza.storage.blobstore.index.FileMetadata;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;
import org.apache.samza.storage.blobstore.index.serde.SnapshotIndexSerde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
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

  private final SnapshotIndexSerde snapshotIndexSerde = new SnapshotIndexSerde();
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

    // TODO LOW shesharm can we cancel other uploads if any one of them fails?
    //  Check with Ambry: if client closes, what happens to inflight uploads
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
   * PUTs the {@link SnapshotIndex} to the blob store.
   * @param snapshotIndex SnapshotIndex to put.
   * @return a Future containing the blob ID of the {@link SnapshotIndex}.
   */
  public CompletableFuture<String> putSnapshotIndex(SnapshotIndex snapshotIndex) {
    byte[] bytes = snapshotIndexSerde.toBytes(snapshotIndex);
    String opName = "putSnapshotIndex for checkpointId " + snapshotIndex.getSnapshotMetadata().getCheckpointId();
    return FutureUtil.executeAsyncWithRetries(opName, () -> {
      InputStream inputStream = new ByteArrayInputStream(bytes); // no need to close ByteArrayInputStream
      SnapshotMetadata snapshotMetadata = snapshotIndex.getSnapshotMetadata();
      Metadata metadata = new Metadata(Metadata.PAYLOAD_PATH_SNAPSHOT_INDEX, Optional.of((long) bytes.length),
          snapshotMetadata.getJobName(), snapshotMetadata.getJobId(), snapshotMetadata.getTaskName(),
          snapshotMetadata.getStoreName());
      return blobStoreManager.put(inputStream, metadata).toCompletableFuture();
    }, isCauseNonRetriable(), executor);
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
          .thenApplyAsync(f -> snapshotIndexSerde.fromBytes(indexBlobStream.toByteArray()), executor);
    }, isCauseNonRetriable(), executor);
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
   * WARNING: This method deletes the **SnapshotIndex blob** from the snapshot. This should only be called to clean
   * up an older snapshot **AFTER** all the files and sub-dirs to be deleted from this snapshot are already deleted
   * using {@link #cleanUpDir(DirIndex, Metadata)}
   *
   * @param snapshotIndexBlobId blob ID of SnapshotIndex blob to delete
   * @return a future that completes when the index blob is deleted from remote store.
   */
  public CompletionStage<Void> deleteSnapshotIndexBlob(String snapshotIndexBlobId, Metadata metadata) {
    Preconditions.checkState(StringUtils.isNotBlank(snapshotIndexBlobId));
    LOG.debug("Deleting SnapshotIndex blob {} from blob store", snapshotIndexBlobId);
    String opName = "deleteSnapshotIndexBlob: " + snapshotIndexBlobId;
    return FutureUtil.executeAsyncWithRetries(opName, () ->
        blobStoreManager.delete(snapshotIndexBlobId, metadata).toCompletableFuture(), isCauseNonRetriable(), executor);
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
    CompletableFuture<Void> dirIndexTTLRemovalFuture = FutureUtil.executeAsyncWithRetries(opName,
        removeDirIndexTTLAction, isCauseNonRetriable(), executor);

    return dirIndexTTLRemovalFuture.thenComposeAsync(aVoid -> {
      String op2Name = "removeTTL for indexBlobId: " + indexBlobId;
      Supplier<CompletionStage<Void>> removeIndexBlobTTLAction = () ->
          blobStoreManager.removeTTL(indexBlobId, metadata).toCompletableFuture();
      return FutureUtil.executeAsyncWithRetries(op2Name, removeIndexBlobTTLAction, isCauseNonRetriable(), executor);
    }, executor);
  }

  /**
   * Upload a File to blob store.
   * @param file File to upload to blob store.
   * @return A future containing the {@link FileIndex} for the uploaded file.
   */
  @VisibleForTesting
  CompletableFuture<FileIndex> putFile(File file, SnapshotMetadata snapshotMetadata) {
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
        // TODO maybe use the more efficient CRC32C / PureJavaCRC32 impl
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
              LOG.trace("Put complete. Closing input stream for file: {}.", file.getPath());
              try {
                finalInputStream.close();
              } catch (Exception e) {
                throw new SamzaException(String.format("Error closing input stream for file %s",
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
          LOG.error("Error closing input stream for file {}", file.getName(), err);
        }
        LOG.error("Error putting file {}", file.getName(), e);
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
   * Non-blocking restore of a {@link SnapshotIndex} to local store by downloading all the files and sub-dirs associated
   * with this remote snapshot.
   * @return A list of future for all the async downloads
   */
  @VisibleForTesting
  public List<CompletableFuture<Void>> restoreDir(File baseDir, DirIndex dirIndex, Metadata metadata) {
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
          new Metadata(fileToRestore.getAbsolutePath(), Optional.of(fileToRestore.length()),
              metadata.getJobName(), metadata.getJobId(), metadata.getTaskName(), metadata.getStoreName());
      List<FileBlob> fileBlobs = fileIndex.getBlobs();

      String opName = "restoreFile: " + fileToRestore.getAbsolutePath();
      CompletableFuture<Void> fileRestoreFuture = FutureUtil.executeAsyncWithRetries(opName, () -> {
        long restoreFileStartTime = System.nanoTime();
        FileOutputStream outputStream = null;
        try {
          // TODO HIGH shesharm ensure that ambry + standby is handled correctly (i.e. no continuous restore for ambry
          //  backed stores, but restore is done correctly on a failover).
          if (fileToRestore.exists()) {
            // delete the file if it already exists, e.g. from a previous retry.
            Files.delete(fileToRestore.toPath());
          }

          // TODO HIGH shesharm add integration tests to ensure empty files and directories are handled correctly E2E.
          fileToRestore.createNewFile(); // create file for 0 byte files (fileIndex entry but no fileBlobs).

          outputStream = new FileOutputStream(fileToRestore);
          final FileOutputStream finalOutputStream = outputStream;
          // create a copy to ensure list being sorted is mutable.
          List<FileBlob> fileBlobsCopy = new ArrayList<>(fileBlobs);
          fileBlobsCopy.sort(Comparator.comparingInt(FileBlob::getOffset)); // sort by offset.

          // chain the futures such that write to file for blobs is sequential.
          // can be optimized to write concurrently to the file later.
          CompletableFuture<Void> resultFuture = CompletableFuture.completedFuture(null);
          for (FileBlob fileBlob : fileBlobsCopy) {
            resultFuture = resultFuture
                .thenComposeAsync(v -> {
                  LOG.debug("Starting restore for file: {} with blob id: {} at offset: {}",
                      fileToRestore, fileBlob.getBlobId(), fileBlob.getOffset());
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
              throw new SamzaException(String.format("Error closing output stream for file: %s",
                  fileToRestore.getAbsolutePath()), e);
            }
          }, executor);

          resultFuture.whenComplete((res, ex) -> {
              if (restoreMetrics != null) {
                restoreMetrics.avgFileRestoreNs.update(System.nanoTime() - restoreFileStartTime);

                long fileSize = fileIndex.getFileMetadata().getSize();
                restoreMetrics.restoreRate.inc(fileSize);
                restoreMetrics.filesRestored.getValue().addAndGet(1);
                restoreMetrics.bytesRestored.getValue().addAndGet(fileSize);
                restoreMetrics.filesRemaining.getValue().addAndGet(-1);
                restoreMetrics.bytesRemaining.getValue().addAndGet(-1 * fileSize);
              }
            });

          return resultFuture;
        } catch (Exception e) {
          try {
            if (outputStream != null) {
              outputStream.close();
            }
          } catch (Exception err) {
            LOG.error("Error closing output stream for file: {}", fileToRestore.getAbsolutePath(), err);
          }

          throw new SamzaException(String.format("Error restoring file: %s in directory: %s",
              fileIndex.getFileName(), dirIndex.getDirName()), e);
        }
      }, isCauseNonRetriable(), executor);

      downloadFutures.add(fileRestoreFuture);
    }

    // restore any sub-directories
    List<DirIndex> subDirs = dirIndex.getSubDirsPresent();
    for (DirIndex subDir : subDirs) {
      File subDirFile = Paths.get(baseDir.getAbsolutePath(), subDir.getDirName()).toFile();
      downloadFutures.addAll(restoreDir(subDirFile, subDir, metadata));
    }

    return downloadFutures;
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
   * Bipredicate to test a local file in the filesystem and a remote file {@link FileIndex} and find out if they represent
   * the same file. Files with same attributes as well as content are same file. A SST file in a special case. They are
   * immutable, so we only compare their attributes but not the content.
   * @param compareLargeFileChecksums whether to compare checksums for large files (> 1 MB).
   * @return BiPredicate to test similarity of local and remote files
   */
  public static BiPredicate<File, FileIndex> areSameFile(boolean compareLargeFileChecksums) {
    return (localFile, remoteFile) -> {
      if (localFile.getName().equals(remoteFile.getFileName())) {
        FileMetadata remoteFileMetadata = remoteFile.getFileMetadata();

        PosixFileAttributes localFileAttrs = null;
        try {
          localFileAttrs = Files.readAttributes(localFile.toPath(), PosixFileAttributes.class);
        } catch (IOException e) {
          LOG.error("Error reading attributes for file {}", localFile.getAbsolutePath());
          throw new RuntimeException(String.format("Error reading attributes for file: %s", localFile.getAbsolutePath()));
        }

        // Don't compare file timestamps. The ctime of a local file just restored will be different than the
        // remote file, and will cause the file to be uploaded again during the first commit after restore.

        boolean areSameFiles =
            localFileAttrs.size() == remoteFileMetadata.getSize() &&
            localFileAttrs.group().getName().equals(remoteFileMetadata.getGroup()) &&
            localFileAttrs.owner().getName().equals(remoteFileMetadata.getOwner()) &&
            PosixFilePermissions.toString(localFileAttrs.permissions()).equals(remoteFileMetadata.getPermissions());

        if (!areSameFiles) {
          LOG.debug("Local file {} and remote file {} are not same. " +
                  "Local file attributes: {}. Remote file attributes: {}.",
              localFile.getAbsolutePath(), remoteFile.getFileName(),
              fileAttributesToString(localFileAttrs), remoteFile.getFileMetadata().toString());
          return false;
        } else {
          LOG.trace("Local file {}. Remote file {}. " +
                  "Local file attributes: {}. Remote file attributes: {}.",
              localFile.getAbsolutePath(), remoteFile.getFileName(),
              fileAttributesToString(localFileAttrs), remoteFile.getFileMetadata().toString());
        }

        boolean isLargeFile = localFileAttrs.size() > 1024 * 1024;
        if (!compareLargeFileChecksums && isLargeFile) {
          // Since RocksDB SST files are immutable after creation, we can skip the expensive checksum computations
          // which requires reading the entire file.
          LOG.debug("Local file {} and remote file {} are same. " +
                  "Skipping checksum calculation for large file of size: {}.",
              localFile.getAbsolutePath(), remoteFile.getFileName(), localFileAttrs.size());
          return true;
        } else {
          try {
            FileInputStream fis = new FileInputStream(localFile);
            CheckedInputStream cis = new CheckedInputStream(fis, new CRC32());
            byte[] buffer = new byte[8 * 1024]; // 8 KB
            while (cis.read(buffer, 0, buffer.length) >= 0) {}
            long localFileChecksum = cis.getChecksum().getValue();
            cis.close();

            boolean areSameChecksum = localFileChecksum == remoteFile.getChecksum();
            if (!areSameChecksum) {
              LOG.debug("Local file {} and remote file {} are not same. " +
                      "Local checksum: {}. Remote checksum: {}",
                  localFile.getAbsolutePath(), remoteFile.getFileName(), localFileChecksum, remoteFile.getChecksum());
            } else {
              LOG.debug("Local file {} and remote file: {} are same. Local checksum: {}. Remote checksum: {}",
                  localFile.getAbsolutePath(), remoteFile.getFileName(), localFileChecksum, remoteFile.getChecksum());
            }
            return areSameChecksum;
          } catch (IOException e) {
            throw new SamzaException("Error calculating checksum for local file: " + localFile.getAbsolutePath(), e);
          }
        }
      }

      return false;
    };
  }

  /**
   * Checks if a local directory and a remote directory are identical. Local and remote directories are identical iff:
   * 1. The local directory has exactly the same set of files as the remote directory, and the files are themselves
   * identical, as determined by {@link #areSameFile(boolean)}, except for those allowed to differ according to
   * {@param filesToIgnore}.
   * 2. The local directory has exactly the same set of sub-directories as the remote directory.
   *
   * @param filesToIgnore a set of file names to ignore during the directory comparisons
   *                      (does not exclude directory names)
   * @param compareLargeFileChecksums whether to compare checksums for large files (> 1 MB).
   * @return boolean indicating whether the local and remote directory are identical.
   */
  // TODO HIGH shesharm add unit tests
  public BiPredicate<File, DirIndex> areSameDir(Set<String> filesToIgnore, boolean compareLargeFileChecksums) {
    return (localDir, remoteDir) -> {
      String remoteDirName = remoteDir.getDirName().equals(DirIndex.ROOT_DIR_NAME) ? "root" : remoteDir.getDirName();
      LOG.debug("Creating diff between local dir: {} and remote dir: {} for comparison.",
          localDir.getAbsolutePath(), remoteDirName);
      DirDiff dirDiff = DirDiffUtil.getDirDiff(localDir, remoteDir, BlobStoreUtil.areSameFile(compareLargeFileChecksums));

      boolean areSameDir = true;
      List<String> filesRemoved = dirDiff.getFilesRemoved().stream()
          .map(FileIndex::getFileName)
          .filter(name -> !filesToIgnore.contains(name))
          .collect(Collectors.toList());

      if (!filesRemoved.isEmpty()) {
        areSameDir = false;
        LOG.error("Local directory: {} is missing files that are present in remote snapshot: {}",
            localDir.getAbsolutePath(), StringUtils.join(filesRemoved, ", "));
      }

      List<DirIndex> subDirsRemoved = dirDiff.getSubDirsRemoved();
      if (!subDirsRemoved.isEmpty()) {
        areSameDir = false;
        List<String> missingSubDirs = subDirsRemoved.stream().map(DirIndex::getDirName).collect(Collectors.toList());
        LOG.error("Local directory: {} is missing sub-dirs that are present in remote snapshot: {}",
            localDir.getAbsolutePath(), StringUtils.join(missingSubDirs, ", "));
      }

      List<String> filesAdded = dirDiff.getFilesAdded().stream()
          .map(File::getName)
          .filter(name -> !filesToIgnore.contains(name))
          .collect(Collectors.toList());
      if (!filesAdded.isEmpty()) {
        areSameDir = false;
        LOG.error("Local directory: {} has additional files that are not present in remote snapshot: {}",
            localDir.getAbsolutePath(), StringUtils.join(filesAdded, ", "));
      }

      List<DirDiff> subDirsAdded = dirDiff.getSubDirsAdded();
      if (!subDirsAdded.isEmpty()) {
        areSameDir = false;
        List<String> addedDirs = subDirsAdded.stream().map(DirDiff::getDirName).collect(Collectors.toList());
        LOG.error("Local directory: {} has additional sub-dirs that are not present in remote snapshot: {}",
            localDir.getAbsolutePath(), StringUtils.join(addedDirs, ", "));
      }

      // dir diff calculation already ensures that all retained files are equal (by definition)
      // recursively test that all retained sub-dirs are equal as well
      Map<String, DirIndex> remoteSubDirs = new HashMap<>();
      for (DirIndex subDir: remoteDir.getSubDirsPresent()) {
        remoteSubDirs.put(subDir.getDirName(), subDir);
      }
      for (DirDiff subDirRetained: dirDiff.getSubDirsRetained()) {
        String localSubDirName = subDirRetained.getDirName();
        File localSubDirFile = Paths.get(localDir.getAbsolutePath(), localSubDirName).toFile();
        DirIndex remoteSubDir = remoteSubDirs.get(localSubDirName);
        boolean areSameSubDir = areSameDir(filesToIgnore, false).test(localSubDirFile, remoteSubDir);
        if (!areSameSubDir) {
          LOG.debug("Local sub-dir: {} and remote sub-dir: {} are not same.",
              localSubDirFile.getAbsolutePath(), remoteSubDir.getDirName());
          areSameDir = false;
        }
      }

      LOG.debug("Local dir: {} and remote dir: {} are {}the same.",
          localDir.getAbsolutePath(), remoteDirName, areSameDir ? "" : "not ");
      return areSameDir;
    };
  }

  private static String fileAttributesToString(PosixFileAttributes fileAttributes) {
    return "PosixFileAttributes{" +
        "creationTimeMillis=" + fileAttributes.creationTime().toMillis() +
        ", lastModifiedTimeMillis=" + fileAttributes.lastModifiedTime().toMillis() +
        ", size=" + fileAttributes.size() +
        ", owner='" + fileAttributes.owner() + '\'' +
        ", group='" + fileAttributes.group() + '\'' +
        ", permissions=" + PosixFilePermissions.toString(fileAttributes.permissions()) +
        '}';
  }

  private static Predicate<Throwable> isCauseNonRetriable() {
    return throwable -> {
      Throwable unwrapped = FutureUtil.unwrapExceptions(CompletionException.class, throwable);
      return unwrapped != null && !RetriableException.class.isAssignableFrom(unwrapped.getClass());
    };
  }
}