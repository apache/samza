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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.CRC32;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.config.BlobStoreConfig;
import org.apache.samza.config.MapConfig;
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
import org.apache.samza.util.FileUtil;
import org.apache.samza.util.FutureUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;


public class TestBlobStoreUtil {
  private static final ExecutorService EXECUTOR = MoreExecutors.newDirectExecutorService();
  private final CheckpointId checkpointId = CheckpointId.deserialize("1234-567");
  private final String jobName = "jobName";
  private final String jobId = "jobId";
  private final String taskName = "taskName";
  private final String storeName = "storeName";
  private final BlobStoreConfig blobStoreConfig = new BlobStoreConfig(new MapConfig());
  private final Metadata metadata =
      new Metadata("payload-path", Optional.empty(), jobName, jobId, taskName, storeName);

  @Test
  // TODO HIGH shesharm test with empty (0 byte) files
  public void testPutDir() throws IOException, InterruptedException, ExecutionException {
    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);

    // File, dir and recursive dir added, retained and removed in local
    String local = "[a, c, z/1, y/2, p/m/3, q/n/4]";
    String remote = "[a, b, z/1, x/2, p/m/3, p/m/5, r/o/6]";
    String expectedAdded = "[c, y/2, q/n/4]";
    String expectedRetained = "[a, z/1, p/m/3]";
    String expectedRemoved = "[b, x/2, r/o/6, p/m/5]";
    SortedSet<String> expectedAddedFiles = BlobStoreTestUtil.getExpected(expectedAdded);
    SortedSet<String> expectedRetainedFiles = BlobStoreTestUtil.getExpected(expectedRetained);
    SortedSet<String> expectedPresentFiles = new TreeSet<>(expectedAddedFiles);
    expectedPresentFiles.addAll(expectedRetainedFiles);
    SortedSet<String> expectedRemovedFiles = BlobStoreTestUtil.getExpected(expectedRemoved);

    // Set up environment
    Path localSnapshotDir = BlobStoreTestUtil.createLocalDir(local);
    String basePath = localSnapshotDir.toAbsolutePath().toString();
    DirIndex remoteSnapshotDir = BlobStoreTestUtil.createDirIndex(remote);
    SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, storeName);
    DirDiff dirDiff = DirDiffUtil.getDirDiff(localSnapshotDir.toFile(), remoteSnapshotDir,
      (localFile, remoteFile) -> localFile.getName().equals(remoteFile.getFileName()));

    SortedSet<String> allUploaded = new TreeSet<>();
    // Set up mocks
    when(blobStoreManager.put(any(InputStream.class), any(Metadata.class)))
        .thenAnswer((Answer<CompletableFuture<String>>) invocation -> {
          Metadata metadata = invocation.getArgumentAt(1, Metadata.class);
          String path = metadata.getPayloadPath();
          allUploaded.add(path.substring(localSnapshotDir.toAbsolutePath().toString().length() + 1));
          return CompletableFuture.completedFuture(path);
        });

    // Execute
    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(blobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    CompletionStage<DirIndex> dirIndexFuture = blobStoreUtil.putDir(dirDiff, snapshotMetadata);
    DirIndex dirIndex = null;
    try {
      // should be already complete. if not, future composition in putDir is broken.
      dirIndex = dirIndexFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fail("Future returned from putDir should be already complete.");
    }

    SortedSet<String> allPresent = new TreeSet<>();
    SortedSet<String> allRemoved = new TreeSet<>();
    BlobStoreTestUtil.getAllPresentInIndex("", dirIndex, allPresent);
    BlobStoreTestUtil.getAllRemovedInIndex("", dirIndex, allRemoved);

    // Assert
    assertEquals(expectedAddedFiles, allUploaded);
    assertEquals(expectedPresentFiles, allPresent);
    assertEquals(expectedRemovedFiles, allRemoved);
  }

  @Test
  public void testPutDirFailsIfAnyFileUploadFails() throws IOException, TimeoutException, InterruptedException {
    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);

    // File, dir and recursive dir added, retained and removed in local
    String local = "[a, b]";
    String remote = "[]";

    // Set up environment
    Path localSnapshotDir = BlobStoreTestUtil.createLocalDir(local);
    String basePath = localSnapshotDir.toAbsolutePath().toString();
    DirIndex remoteSnapshotDir = BlobStoreTestUtil.createDirIndex(remote);
    SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, storeName);
    DirDiff dirDiff = DirDiffUtil.getDirDiff(localSnapshotDir.toFile(), remoteSnapshotDir,
      (localFile, remoteFile) -> localFile.getName().equals(remoteFile.getFileName()));

    // Set up mocks
    SamzaException exception = new SamzaException("Error uploading file");
    CompletableFuture<String> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(exception);
    when(blobStoreManager.put(any(InputStream.class), any(Metadata.class)))
        .thenAnswer((Answer<CompletableFuture<String>>) invocation -> {
          Metadata metadata = invocation.getArgumentAt(1, Metadata.class);
          String path = metadata.getPayloadPath();
          if (path.endsWith("a")) {
            return CompletableFuture.completedFuture("aBlobId");
          } else {
            return failedFuture;
          }
        });

    // Execute
    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(blobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    CompletionStage<DirIndex> dirIndexFuture = blobStoreUtil.putDir(dirDiff, snapshotMetadata);
    try {
      // should be already complete. if not, future composition in putDir is broken.
      dirIndexFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // Assert that the result future fails and that the cause is propagated correctly
      assertEquals(exception, cause);
      return;
    }

    fail("DirIndex future should have been completed with an exception");
  }

  @Test
  public void testPutDirFailsIfAnySubDirFileUploadFails() throws IOException, TimeoutException, InterruptedException {
    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);

    // File, dir and recursive dir added, retained and removed in local
    String local = "[a/1, b/2]";
    String remote = "[]";

    // Set up environment
    Path localSnapshotDir = BlobStoreTestUtil.createLocalDir(local);
    String basePath = localSnapshotDir.toAbsolutePath().toString();
    DirIndex remoteSnapshotDir = BlobStoreTestUtil.createDirIndex(remote);
    SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, storeName);
    DirDiff dirDiff = DirDiffUtil.getDirDiff(localSnapshotDir.toFile(), remoteSnapshotDir,
      (localFile, remoteFile) -> localFile.getName().equals(remoteFile.getFileName()));

    // Set up mocks
    SamzaException exception = new SamzaException("Error uploading file");
    CompletableFuture<String> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(exception);
    when(blobStoreManager.put(any(InputStream.class), any(Metadata.class)))
        .thenAnswer((Answer<CompletableFuture<String>>) invocation -> {
          Metadata metadata = invocation.getArgumentAt(1, Metadata.class);
          String path = metadata.getPayloadPath();
          if (path.endsWith("1")) {
            return CompletableFuture.completedFuture("a1BlobId");
          } else {
            return failedFuture;
          }
        });

    // Execute
    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(blobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    CompletionStage<DirIndex> dirIndexFuture = blobStoreUtil.putDir(dirDiff, snapshotMetadata);
    try {
      // should be already complete. if not, future composition in putDir is broken.
      dirIndexFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // Assert that the result future fails and that the cause is propagated correctly
      assertEquals(exception, cause);
      return;
    }

    fail("DirIndex future should have been completed with an exception");
  }

  @Test
  public void testCleanup() throws IOException, ExecutionException, InterruptedException {
    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);

    // File, dir and recursive dir added, retained and removed in local
    // Using unique file names since test util uses only the file name (leaf node)
    // as the mock blob id, not the full file path.
    String local = "[a, c, z/1, y/2, p/m/3, q/n/4]";
    String remote = "[a, b, z/1, x/5, p/m/3, r/o/6]";
    String expectedRemoved = "[b, 5, 6]";
    // keep only the last character (the file name).
    SortedSet<String> expectedRemovedFiles = BlobStoreTestUtil.getExpected(expectedRemoved);

    // Set up environment
    Path localSnapshotDir = BlobStoreTestUtil.createLocalDir(local);
    String basePath = localSnapshotDir.toAbsolutePath().toString();
    DirIndex remoteSnapshotDir = BlobStoreTestUtil.createDirIndex(remote);
    SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, storeName);
    DirDiff dirDiff = DirDiffUtil.getDirDiff(localSnapshotDir.toFile(), remoteSnapshotDir,
      (localFile, remoteFile) -> localFile.getName().equals(remoteFile.getFileName()));

    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(blobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    when(blobStoreManager.put(any(InputStream.class), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture("blobId"));
    CompletionStage<DirIndex> dirIndexFuture = blobStoreUtil.putDir(dirDiff, snapshotMetadata);
    DirIndex dirIndex = null;
    try {
      // should be already complete. if not, future composition in putDir is broken.
      dirIndex = dirIndexFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fail("Future returned from putDir should be already complete.");
    }

    // Set up mocks
    SortedSet<String> allDeleted = new TreeSet<>();
    when(blobStoreManager.delete(anyString(), any(Metadata.class)))
        .thenAnswer((Answer<CompletableFuture<Void>>) invocation -> {
          String blobId = invocation.getArgumentAt(0, String.class);
          allDeleted.add(blobId);
          return CompletableFuture.completedFuture(null);
        });

    // Execute
    CompletionStage<Void> cleanUpFuture = blobStoreUtil.cleanUpDir(dirIndex, metadata);
    try {
      // should be already complete. if not, future composition in putDir is broken.
      cleanUpFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fail("Future returned from putDir should be already complete.");
    }

    // Assert
    assertEquals(expectedRemovedFiles, allDeleted);
  }

  @Test
  public void testCleanUpFailsIfAnyFileDeleteFails()
      throws IOException, TimeoutException, InterruptedException, ExecutionException {
    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);

    // File, dir and recursive dir added, retained and removed in local
    // Using unique file names since test util uses only the file name (leaf node)
    // as the mock blob id, not the full file path.
    String local = "[a, b]";
    String remote = "[c, d]";

    // Set up environment
    Path localSnapshotDir = BlobStoreTestUtil.createLocalDir(local);
    String basePath = localSnapshotDir.toAbsolutePath().toString();
    DirIndex remoteSnapshotDir = BlobStoreTestUtil.createDirIndex(remote);
    SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, storeName);
    DirDiff dirDiff = DirDiffUtil.getDirDiff(localSnapshotDir.toFile(), remoteSnapshotDir,
      (localFile, remoteFile) -> localFile.getName().equals(remoteFile.getFileName()));

    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(blobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    when(blobStoreManager.put(any(InputStream.class), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture("blobId"));
    CompletionStage<DirIndex> dirIndexFuture = blobStoreUtil.putDir(dirDiff, snapshotMetadata);
    DirIndex dirIndex = null;
    try {
      // should be already complete. if not, future composition in putDir is broken.
      dirIndex = dirIndexFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fail("Future returned from putDir should be already complete.");
    }

    // Set up mocks
    SamzaException exception = new SamzaException("Error deleting file");
    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(exception);
    when(blobStoreManager.delete(anyString(), any(Metadata.class)))
        .thenAnswer((Answer<CompletableFuture<Void>>) invocation -> {
          String blobId = invocation.getArgumentAt(0, String.class);
          if (blobId.equals("c")) {
            return CompletableFuture.completedFuture(null);
          } else {
            return failedFuture;
          }
        });

    // Execute
    CompletionStage<Void> cleanUpFuture = blobStoreUtil.cleanUpDir(dirIndex, metadata);
    try {
      // should be already complete. if not, future composition in putDir is broken.
      cleanUpFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // Assert that the result future fails and that the cause is propagated correctly
      assertEquals(exception, cause);
      return;
    }

    fail("Clean up future should have been completed with an exception");
  }

  @Test
  public void testCleanUpFailsIfAnySubDirFileDeleteFails()
      throws IOException, TimeoutException, InterruptedException, ExecutionException {
    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);

    // File, dir and recursive dir added, retained and removed in local
    // Using unique file names since test util uses only the file name (leaf node)
    // as the mock blob id, not the full file path.
    String local = "[a/1, b/2]";
    String remote = "[c/3, d/4]";

    // Set up environment
    Path localSnapshotDir = BlobStoreTestUtil.createLocalDir(local);
    String basePath = localSnapshotDir.toAbsolutePath().toString();
    DirIndex remoteSnapshotDir = BlobStoreTestUtil.createDirIndex(remote);
    SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, storeName);
    DirDiff dirDiff = DirDiffUtil.getDirDiff(localSnapshotDir.toFile(), remoteSnapshotDir,
      (localFile, remoteFile) -> localFile.getName().equals(remoteFile.getFileName()));

    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(blobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    when(blobStoreManager.put(any(InputStream.class), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture("blobId"));
    CompletionStage<DirIndex> dirIndexFuture = blobStoreUtil.putDir(dirDiff, snapshotMetadata);
    DirIndex dirIndex = null;
    try {
      // should be already complete. if not, future composition in putDir is broken.
      dirIndex = dirIndexFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fail("Future returned from putDir should be already complete.");
    }

    // Set up mocks
    SamzaException exception = new SamzaException("Error deleting file");
    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(exception);
    when(blobStoreManager.delete(anyString(), any(Metadata.class)))
        .thenAnswer((Answer<CompletableFuture<Void>>) invocation -> {
          String blobId = invocation.getArgumentAt(0, String.class);
          if (blobId.equals("3")) { // blob ID == file name (leaf node) in blob store test util
            return CompletableFuture.completedFuture(null);
          } else {
            return failedFuture;
          }
        });

    // Execute
    CompletionStage<Void> cleanUpFuture = blobStoreUtil.cleanUpDir(dirIndex, metadata);
    try {
      // should be already complete. if not, future composition in putDir is broken.
      cleanUpFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // Assert that the result future fails and that the cause is propagated correctly
      assertEquals(exception, cause);
      return;
    }

    fail("Clean up future should have been completed with an exception");
  }

  @Test
  public void testRemoveTTL() throws IOException, ExecutionException, InterruptedException {
    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);

    // File, dir and recursive dir added, retained and removed in local
    // Using unique file names since test setup returns it as the blob id
    String local = "[a, c, z/1, y/2, p/m/3, q/n/4]";
    String remote = "[a, b, z/1, x/5, p/m/3, r/o/6]";
    String expectedAdded = "[c, y/2, q/n/4]";
    String expectedRetained = "[a, z/1, p/m/3]";
    SortedSet<String> expectedAddedFiles = BlobStoreTestUtil.getExpected(expectedAdded);
    SortedSet<String> expectedRetainedFiles = BlobStoreTestUtil.getExpected(expectedRetained);
    SortedSet<String> expectedPresentFiles = new TreeSet<>(expectedAddedFiles);
    expectedPresentFiles.addAll(expectedRetainedFiles);

    // Set up environment
    Path localSnapshotDir = BlobStoreTestUtil.createLocalDir(local);
    String basePath = localSnapshotDir.toAbsolutePath().toString();
    DirIndex remoteSnapshotDir = BlobStoreTestUtil.createDirIndex(remote);
    SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, storeName);
    DirDiff dirDiff = DirDiffUtil.getDirDiff(localSnapshotDir.toFile(), remoteSnapshotDir,
      (localFile, remoteFile) -> localFile.getName().equals(remoteFile.getFileName()));

    when(blobStoreManager.put(any(InputStream.class), any(Metadata.class)))
        .thenAnswer((Answer<CompletableFuture<String>>) invocation -> {
          Metadata metadata = invocation.getArgumentAt(1, Metadata.class);
          String path = metadata.getPayloadPath();
          String fileName = path.substring(path.length() - 1); // use only the last character as file name
          return CompletableFuture.completedFuture(fileName);
        });

    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(blobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    CompletionStage<DirIndex> dirIndexFuture = blobStoreUtil.putDir(dirDiff, snapshotMetadata);
    DirIndex dirIndex = null;
    try {
      // should be already complete. if not, future composition in putDir is broken.
      dirIndex = dirIndexFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fail("Future returned from putDir should be already complete.");
    }

    SnapshotIndex mockSnapshotIndex = mock(SnapshotIndex.class);
    when(mockSnapshotIndex.getSnapshotMetadata()).thenReturn(snapshotMetadata);
    when(mockSnapshotIndex.getDirIndex()).thenReturn(dirIndex);

    SortedSet<String> allTTLRemoved = new TreeSet<>();
    when(blobStoreManager.removeTTL(anyString(), any(Metadata.class)))
        .thenAnswer((Answer<CompletableFuture<String>>) invocation -> {
          String blobId = invocation.getArgumentAt(0, String.class);
          allTTLRemoved.add(blobId);
          return CompletableFuture.completedFuture(null);
        });

    // Execute
    blobStoreUtil.removeTTL("snapshotIndexBlobId", mockSnapshotIndex, metadata);

    // Assert
    SortedSet<String> expectedBlobIds = new TreeSet<>();
    // test uses unique file name (last char) as the blob ID.
    expectedPresentFiles.forEach(f -> expectedBlobIds.add(f.substring(f.length() - 1)));
    expectedBlobIds.add("snapshotIndexBlobId");

    assertEquals(expectedBlobIds, allTTLRemoved);
  }

  @Test
  public void testPutFileChecksumAndMetadata() throws IOException, ExecutionException, InterruptedException {
    // Setup
    SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, storeName);
    Path path = Files.createTempFile("samza-testPutFileChecksum-", ".tmp");
    FileUtil fileUtil = new FileUtil();
    fileUtil.writeToTextFile(path.toFile(), RandomStringUtils.random(1000), false);
    long expectedChecksum = FileUtils.checksumCRC32(path.toFile());

    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);
    ArgumentCaptor<Metadata> argumentCaptor = ArgumentCaptor.forClass(Metadata.class);
    when(blobStoreManager.put(any(InputStream.class), argumentCaptor.capture())).thenAnswer(
      (Answer<CompletionStage<String>>) invocation -> {
        InputStream inputStream = invocation.getArgumentAt(0, InputStream.class);
        // consume input stream to ensure checksum is calculated
        IOUtils.copy(inputStream, NullOutputStream.NULL_OUTPUT_STREAM);
        return CompletableFuture.completedFuture("blobId");
      });

    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(blobStoreManager, EXECUTOR, blobStoreConfig, null, null);

    CompletionStage<FileIndex> fileIndexFuture = blobStoreUtil.putFile(path.toFile(), snapshotMetadata);
    FileIndex fileIndex = null;
    try {
      // should be already complete. if not, future composition in putFile is broken.
      fileIndex = fileIndexFuture.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fail("Future returned from putFile should be already complete.");
    }

    // Assert
    Metadata metadata = (Metadata) argumentCaptor.getValue();
    assertEquals(path.toAbsolutePath().toString(), metadata.getPayloadPath());
    assertEquals(path.toFile().length(), Long.valueOf(metadata.getPayloadSize()).longValue());
    assertEquals(expectedChecksum, fileIndex.getChecksum());
  }

  @Test
  public void testAreSameFile() throws IOException {
    FileUtil fileUtil = new FileUtil();
    // 1. test with sst file with same attributes
    Path sstFile = Files.createTempFile("samza-testAreSameFiles-", ".sst");

    PosixFileAttributes sstFileAttribs = Files.readAttributes(sstFile, PosixFileAttributes.class);
    FileMetadata sstFileMetadata = new FileMetadata(sstFileAttribs.creationTime().toMillis(),
        sstFileAttribs.lastModifiedTime().toMillis(), sstFileAttribs.size(), sstFileAttribs.owner().toString(),
        sstFileAttribs.group().toString(), PosixFilePermissions.toString(sstFileAttribs.permissions()));
    // checksum should be ignored for sst file. Set any dummy value
    FileIndex sstFileIndex = new FileIndex(sstFile.getFileName().toString(), Collections.emptyList(), sstFileMetadata, 0L);

    assertTrue(DirDiffUtil.areSameFile(false, true).test(sstFile.toFile(), sstFileIndex));

    // 2. test with sst file with different timestamps
    // Update last modified time
    Files.setLastModifiedTime(sstFile, FileTime.fromMillis(System.currentTimeMillis() + 1000L));
    assertTrue(DirDiffUtil.areSameFile(false, true).test(sstFile.toFile(), sstFileIndex));

    // 3. test with non-sst files with same metadata and content
    Path tmpFile = Files.createTempFile("samza-testAreSameFiles-", ".tmp");
    fileUtil.writeToTextFile(tmpFile.toFile(), RandomStringUtils.random(1000), false);

    PosixFileAttributes tmpFileAttribs = Files.readAttributes(tmpFile, PosixFileAttributes.class);
    FileMetadata tmpFileMetadata =
        new FileMetadata(tmpFileAttribs.creationTime().toMillis(), tmpFileAttribs.lastModifiedTime().toMillis(),
            tmpFileAttribs.size(), tmpFileAttribs.owner().toString(), tmpFileAttribs.group().toString(),
            PosixFilePermissions.toString(tmpFileAttribs.permissions()));
    FileIndex tmpFileIndex = new FileIndex(tmpFile.getFileName().toString(), Collections.emptyList(), tmpFileMetadata,
        FileUtils.checksumCRC32(tmpFile.toFile()));

    assertTrue(DirDiffUtil.areSameFile(false, true).test(tmpFile.toFile(), tmpFileIndex));

    // 4. test with non-sst files with different attributes
    // change lastModifiedTime of local file
    FileTime prevLastModified = tmpFileAttribs.lastModifiedTime();
    Files.setLastModifiedTime(tmpFile, FileTime.fromMillis(System.currentTimeMillis() + 1000L));
    assertTrue(DirDiffUtil.areSameFile(false, true).test(tmpFile.toFile(), tmpFileIndex));

    // change content/checksum of local file
    Files.setLastModifiedTime(tmpFile, prevLastModified); // reset attributes to match with remote file
    fileUtil.writeToTextFile(tmpFile.toFile(), RandomStringUtils.random(1000), false); //new content
    assertFalse(DirDiffUtil.areSameFile(false, true).test(tmpFile.toFile(), tmpFileIndex));
  }

  @Test
  public void testRestoreDirRestoresMultiPartFilesCorrectly() throws IOException {
    Path restoreDirBasePath = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX);

    // remote file == 26 blobs, blob ids from a to z, blob contents from a to z, offsets 0 to 25.
    DirIndex mockDirIndex = mock(DirIndex.class);
    when(mockDirIndex.getDirName()).thenReturn(DirIndex.ROOT_DIR_NAME);
    FileIndex mockFileIndex = mock(FileIndex.class);
    when(mockFileIndex.getFileName()).thenReturn("1.sst");

    // setup mock file attributes. create a temp file to get current user/group/permissions so that they
    // match with restored files.
    File tmpFile = Paths.get(restoreDirBasePath.toString(), "tempfile-" + new Random().nextInt()).toFile();
    tmpFile.createNewFile();
    PosixFileAttributes attrs = Files.readAttributes(tmpFile.toPath(), PosixFileAttributes.class);
    FileMetadata fileMetadata = new FileMetadata(1234L, 1243L, 26, // ctime mtime does not matter. size == 26
        attrs.owner().getName(), attrs.group().getName(), PosixFilePermissions.toString(attrs.permissions()));
    when(mockFileIndex.getFileMetadata()).thenReturn(fileMetadata);
    Files.delete(tmpFile.toPath()); // delete so that it doesn't show up in restored dir contents.

    List<FileBlob> mockFileBlobs = new ArrayList<>();
    StringBuilder fileContents = new StringBuilder();
    for (int i = 0; i < 26; i++) {
      FileBlob mockFileBlob = mock(FileBlob.class);
      char c = (char) ('a' + i);
      fileContents.append(c); // blob contents == blobId
      when(mockFileBlob.getBlobId()).thenReturn(String.valueOf(c));
      when(mockFileBlob.getOffset()).thenReturn(i);
      mockFileBlobs.add(mockFileBlob);
    }
    when(mockFileIndex.getBlobs()).thenReturn(mockFileBlobs);
    CRC32 checksum = new CRC32();
    checksum.update(fileContents.toString().getBytes());
    when(mockFileIndex.getChecksum()).thenReturn(checksum.getValue());
    when(mockDirIndex.getFilesPresent()).thenReturn(ImmutableList.of(mockFileIndex));

    BlobStoreManager mockBlobStoreManager = mock(BlobStoreManager.class);
    when(mockBlobStoreManager.get(anyString(), any(OutputStream.class), any(Metadata.class), any(Boolean.class))).thenAnswer(
      (Answer<CompletionStage<Void>>) invocationOnMock -> {
        String blobId = invocationOnMock.getArgumentAt(0, String.class);
        OutputStream outputStream = invocationOnMock.getArgumentAt(1, OutputStream.class);
        // blob contents = blob id
        outputStream.write(blobId.getBytes());

        // force flush so that the checksum calculation later uses the full file contents.
        ((FileOutputStream) outputStream).getFD().sync();
        return CompletableFuture.completedFuture(null);
      });

    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(mockBlobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    blobStoreUtil.restoreDir(restoreDirBasePath.toFile(), mockDirIndex, metadata, false).join();

    assertTrue(
        new DirDiffUtil().areSameDir(Collections.emptySet(), false, true).test(restoreDirBasePath.toFile(), mockDirIndex));
  }

  @Test
  public void testRestoreDirRetriesFileRestoreOnRetriableExceptions() throws IOException {
    Path restoreDirBasePath = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX);

    DirIndex mockDirIndex = mock(DirIndex.class);
    when(mockDirIndex.getDirName()).thenReturn(DirIndex.ROOT_DIR_NAME);
    FileIndex mockFileIndex = mock(FileIndex.class);
    when(mockFileIndex.getFileName()).thenReturn("1.sst");

    // setup mock file attributes. create a temp file to get current user/group/permissions so that they
    // match with restored files.
    File tmpFile = Paths.get(restoreDirBasePath.toString(), "tempfile-" + new Random().nextInt()).toFile();
    tmpFile.createNewFile();
    byte[] fileContents = "fileContents".getBytes();
    PosixFileAttributes attrs = Files.readAttributes(tmpFile.toPath(), PosixFileAttributes.class);
    FileMetadata fileMetadata =
        new FileMetadata(1234L, 1243L, fileContents.length, // ctime mtime does not matter. size == 26
            attrs.owner().getName(), attrs.group().getName(), PosixFilePermissions.toString(attrs.permissions()));
    when(mockFileIndex.getFileMetadata()).thenReturn(fileMetadata);
    Files.delete(tmpFile.toPath()); // delete so that it doesn't show up in restored dir contents.

    List<FileBlob> mockFileBlobs = new ArrayList<>();
    FileBlob mockFileBlob = mock(FileBlob.class);
    when(mockFileBlob.getBlobId()).thenReturn("fileBlobId");
    when(mockFileBlob.getOffset()).thenReturn(0);
    mockFileBlobs.add(mockFileBlob);
    when(mockFileIndex.getBlobs()).thenReturn(mockFileBlobs);

    CRC32 checksum = new CRC32();
    checksum.update(fileContents);
    when(mockFileIndex.getChecksum()).thenReturn(checksum.getValue());
    when(mockDirIndex.getFilesPresent()).thenReturn(ImmutableList.of(mockFileIndex));

    BlobStoreManager mockBlobStoreManager = mock(BlobStoreManager.class);
    when(mockBlobStoreManager.get(anyString(), any(OutputStream.class), any(Metadata.class), any(Boolean.class))).thenAnswer(
      (Answer<CompletionStage<Void>>) invocationOnMock -> { // first try, retriable error
        String blobId = invocationOnMock.getArgumentAt(0, String.class);
        OutputStream outputStream = invocationOnMock.getArgumentAt(1, OutputStream.class);
        // write garbage data on first retry to verify that final file contents are correct
        outputStream.write("bad-data".getBytes());
        ((FileOutputStream) outputStream).getFD().sync();
        return FutureUtil.failedFuture(new RetriableException()); // retriable error
      }).thenAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> { // 2nd try
        String blobId = invocationOnMock.getArgumentAt(0, String.class);
        OutputStream outputStream = invocationOnMock.getArgumentAt(1, OutputStream.class);
        // write correct data on first retry to verify that final file contents are correct
        outputStream.write(fileContents);
        ((FileOutputStream) outputStream).getFD().sync();
        return CompletableFuture.completedFuture(null); // success
      });

    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(mockBlobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    blobStoreUtil.restoreDir(restoreDirBasePath.toFile(), mockDirIndex, metadata, false).join();

    assertTrue(
        new DirDiffUtil().areSameDir(Collections.emptySet(), false, true).test(restoreDirBasePath.toFile(), mockDirIndex));
  }

  @Test
  public void testRestoreDirFailsRestoreOnNonRetriableExceptions() throws IOException {
    Path restoreDirBasePath = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX);

    DirIndex mockDirIndex = mock(DirIndex.class);
    when(mockDirIndex.getDirName()).thenReturn(DirIndex.ROOT_DIR_NAME);
    FileIndex mockFileIndex = mock(FileIndex.class);
    when(mockFileIndex.getFileName()).thenReturn("1.sst");

    // setup mock file attributes. create a temp file to get current user/group/permissions so that they
    // match with restored files.
    File tmpFile = Paths.get(restoreDirBasePath.toString(), "tempfile-" + new Random().nextInt()).toFile();
    tmpFile.createNewFile();
    byte[] fileContents = "fileContents".getBytes();
    PosixFileAttributes attrs = Files.readAttributes(tmpFile.toPath(), PosixFileAttributes.class);
    FileMetadata fileMetadata =
        new FileMetadata(1234L, 1243L, fileContents.length, // ctime mtime does not matter. size == 26
            attrs.owner().getName(), attrs.group().getName(), PosixFilePermissions.toString(attrs.permissions()));
    when(mockFileIndex.getFileMetadata()).thenReturn(fileMetadata);
    Files.delete(tmpFile.toPath()); // delete so that it doesn't show up in restored dir contents.

    List<FileBlob> mockFileBlobs = new ArrayList<>();
    FileBlob mockFileBlob = mock(FileBlob.class);
    when(mockFileBlob.getBlobId()).thenReturn("fileBlobId");
    when(mockFileBlob.getOffset()).thenReturn(0);
    mockFileBlobs.add(mockFileBlob);
    when(mockFileIndex.getBlobs()).thenReturn(mockFileBlobs);

    CRC32 checksum = new CRC32();
    checksum.update(fileContents);
    when(mockFileIndex.getChecksum()).thenReturn(checksum.getValue());
    when(mockDirIndex.getFilesPresent()).thenReturn(ImmutableList.of(mockFileIndex));

    BlobStoreManager mockBlobStoreManager = mock(BlobStoreManager.class);
    when(mockBlobStoreManager.get(anyString(), any(OutputStream.class), any(Metadata.class), any(Boolean.class))).thenReturn(
        FutureUtil.failedFuture(new IllegalArgumentException())) // non retriable error
        .thenAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
          String blobId = invocationOnMock.getArgumentAt(0, String.class);
          OutputStream outputStream = invocationOnMock.getArgumentAt(1, OutputStream.class);
          outputStream.write(fileContents);

          // force flush so that the checksum calculation later uses the full file contents.
          ((FileOutputStream) outputStream).getFD().sync();
          return CompletableFuture.completedFuture(null);
        });

    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(mockBlobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    try {
      blobStoreUtil.restoreDir(restoreDirBasePath.toFile(), mockDirIndex, metadata, false).join();
      fail("Should have failed on non-retriable errors during file restore");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }


  @Test
  public void testRestoreIgnoresDifferentFileOwnersOnConfig() throws IOException {
    Path restoreDirBasePath = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX);

    // remote file == 26 blobs, blob ids from a to z, blob contents from a to z, offsets 0 to 25.
    DirIndex mockDirIndex = mock(DirIndex.class);
    when(mockDirIndex.getDirName()).thenReturn(DirIndex.ROOT_DIR_NAME);
    FileIndex mockFileIndex = mock(FileIndex.class);
    when(mockFileIndex.getFileName()).thenReturn("1.sst");

    // setup mock file attributes. create a temp file to get current user/group/permissions so that they
    // match with restored files.
    File tmpFile = Paths.get(restoreDirBasePath.toString(), "tempfile-" + new Random().nextInt()).toFile();
    tmpFile.createNewFile();
    PosixFileAttributes attrs = Files.readAttributes(tmpFile.toPath(), PosixFileAttributes.class);
    // create remote file with different owner than local file
    FileMetadata fileMetadata = new FileMetadata(1234L, 1243L, 26, // ctime mtime does not matter. size == 26
        attrs.owner().getName() + "_different", attrs.group().getName(), PosixFilePermissions.toString(attrs.permissions()));
    when(mockFileIndex.getFileMetadata()).thenReturn(fileMetadata);
    Files.delete(tmpFile.toPath()); // delete so that it doesn't show up in restored dir contents.

    List<FileBlob> mockFileBlobs = new ArrayList<>();
    StringBuilder fileContents = new StringBuilder();
    for (int i = 0; i < 26; i++) {
      FileBlob mockFileBlob = mock(FileBlob.class);
      char c = (char) ('a' + i);
      fileContents.append(c); // blob contents == blobId
      when(mockFileBlob.getBlobId()).thenReturn(String.valueOf(c));
      when(mockFileBlob.getOffset()).thenReturn(i);
      mockFileBlobs.add(mockFileBlob);
    }
    when(mockFileIndex.getBlobs()).thenReturn(mockFileBlobs);
    CRC32 checksum = new CRC32();
    checksum.update(fileContents.toString().getBytes());
    when(mockFileIndex.getChecksum()).thenReturn(checksum.getValue());
    when(mockDirIndex.getFilesPresent()).thenReturn(ImmutableList.of(mockFileIndex));

    BlobStoreManager mockBlobStoreManager = mock(BlobStoreManager.class);
    when(mockBlobStoreManager.get(anyString(), any(OutputStream.class), any(Metadata.class), any(Boolean.class))).thenAnswer(
      (Answer<CompletionStage<Void>>) invocationOnMock -> {
        String blobId = invocationOnMock.getArgumentAt(0, String.class);
        OutputStream outputStream = invocationOnMock.getArgumentAt(1, OutputStream.class);
        // blob contents = blob id
        outputStream.write(blobId.getBytes());

        // force flush so that the checksum calculation later uses the full file contents.
        ((FileOutputStream) outputStream).getFD().sync();
        return CompletableFuture.completedFuture(null);
      });

    BlobStoreConfig config = mock(BlobStoreConfig.class);
    when(config.shouldCompareFileOwnersOnRestore()).thenReturn(false);
    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(mockBlobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    blobStoreUtil.restoreDir(restoreDirBasePath.toFile(), mockDirIndex, metadata, false).join();

    assertTrue(
        new DirDiffUtil().areSameDir(Collections.emptySet(), false, config.shouldCompareFileOwnersOnRestore()).test(restoreDirBasePath.toFile(), mockDirIndex));
  }

  @Test
  @Ignore // TODO remove
  public void testRestoreDirRecreatesEmptyFilesAndDirs() throws IOException {
    String prevSnapshotFiles = "[a, b, z/1, y/1, p/m/1, q/n/1]";
    DirIndex dirIndex = BlobStoreTestUtil.createDirIndex(prevSnapshotFiles);
    String localSnapshotFiles = "[a, b, z/1, y/1, p/m/1, q/n/1]";
    Path localSnapshot = BlobStoreTestUtil.createLocalDir(localSnapshotFiles);
    BlobStoreManager mockBlobStoreManager = mock(BlobStoreManager.class);
    when(mockBlobStoreManager.get(anyString(), any(OutputStream.class), any(Metadata.class), any(Boolean.class))).thenAnswer(
      (Answer<CompletionStage<Void>>) invocationOnMock -> {
        String blobId = invocationOnMock.getArgumentAt(0, String.class);
        OutputStream outputStream = invocationOnMock.getArgumentAt(1, OutputStream.class);
        // blob contents = blob id
        outputStream.write(blobId.getBytes());
        return CompletableFuture.completedFuture(null);
      });
    boolean result = new DirDiffUtil().areSameDir(new TreeSet<>(), false, true).test(localSnapshot.toFile(), dirIndex);
    assertFalse(result);
    //ToDo complete
  }

  @Test
  public void testRestoreDirVerifiesFileChecksums() {
    // ToDo shesharma restore dir only restores SST files. Since other metadata files are in ignore list,
    // no checksum matching would be done? Check later.
  }

  @Test
  public void testRestoreDirCreatesCorrectDirectoryStructure() throws IOException {
    String prevSnapshotFiles = "[a, b, z/1, y/1, p/m/1, q/n/1]";
    DirIndex dirIndex = BlobStoreTestUtil.createDirIndex(prevSnapshotFiles);

    BlobStoreManager mockBlobStoreManager = mock(BlobStoreManager.class);
    when(mockBlobStoreManager.get(anyString(), any(OutputStream.class), any(Metadata.class), any(Boolean.class))).thenAnswer(
      (Answer<CompletionStage<Void>>) invocationOnMock -> {
        String blobId = invocationOnMock.getArgumentAt(0, String.class);
        OutputStream outputStream = invocationOnMock.getArgumentAt(1, OutputStream.class);
        // blob contents = blob id
        outputStream.write(blobId.getBytes());
        return CompletableFuture.completedFuture(null);
      });

    Path restoreDirBasePath = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX);
    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(mockBlobStoreManager, EXECUTOR, blobStoreConfig, null, null);
    blobStoreUtil.restoreDir(restoreDirBasePath.toFile(), dirIndex, metadata, false).join();

    assertTrue(new DirDiffUtil().areSameDir(Collections.emptySet(), false, true).test(restoreDirBasePath.toFile(), dirIndex));
  }

  /**
   * Tests related to {@link BlobStoreUtil#getStoreSnapshotIndexes}
   */

  @Test
  public void testGetSSIReturnsEmptyMapForNullCheckpoint() {
    BlobStoreUtil blobStoreUtil =
        new BlobStoreUtil(mock(BlobStoreManager.class), MoreExecutors.newDirectExecutorService(), blobStoreConfig, null, null);
    Map<String, Pair<String, SnapshotIndex>> snapshotIndexes =
        blobStoreUtil.getStoreSnapshotIndexes("testJobName", "testJobId", "taskName",
            null, new HashSet<>(), false);
    assertTrue(snapshotIndexes.isEmpty());
  }

  public void testGetSSIThrowsExceptionForCheckpointV1() {
    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    when(mockCheckpoint.getVersion()).thenReturn((short) 1);
    BlobStoreUtil blobStoreUtil =
        new BlobStoreUtil(mock(BlobStoreManager.class), MoreExecutors.newDirectExecutorService(), blobStoreConfig, null, null);
    Map<String, Pair<String, SnapshotIndex>> prevSnapshotIndexes =
        blobStoreUtil.getStoreSnapshotIndexes("testJobName", "testJobId", "taskName",
            mockCheckpoint, new HashSet<>(), false);
    assertEquals(prevSnapshotIndexes.size(), 0);
  }

  @Test
  public void testGetSSIReturnsEmptyMapIfNoEntryForBlobStoreBackendFactory() {
    CheckpointV2 mockCheckpoint = mock(CheckpointV2.class);
    when(mockCheckpoint.getVersion()).thenReturn((short) 2);
    when(mockCheckpoint.getStateCheckpointMarkers()).thenReturn(
        ImmutableMap.of("com.OtherStateBackendFactory", ImmutableMap.of("storeName", "otherSCM")));

    BlobStoreUtil blobStoreUtil =
        new BlobStoreUtil(mock(BlobStoreManager.class), MoreExecutors.newDirectExecutorService(), blobStoreConfig, null, null);
    Map<String, Pair<String, SnapshotIndex>> snapshotIndexes =
        blobStoreUtil.getStoreSnapshotIndexes("testJobName", "testJobId", "taskName",
            mockCheckpoint, new HashSet<>(), false);
    assertTrue(snapshotIndexes.isEmpty());
  }

  @Test
  public void testGetSSIReturnsEmptyMapIfNoStoreForBlobStoreBackendFactory() {
    CheckpointV2 mockCheckpoint = mock(CheckpointV2.class);
    when(mockCheckpoint.getVersion()).thenReturn((short) 2);
    when(mockCheckpoint.getStateCheckpointMarkers()).thenReturn(
        ImmutableMap.of(BlobStoreStateBackendFactory.class.getName(), ImmutableMap.of()));

    BlobStoreUtil blobStoreUtil =
        new BlobStoreUtil(mock(BlobStoreManager.class), MoreExecutors.newDirectExecutorService(), blobStoreConfig, null, null);
    Map<String, Pair<String, SnapshotIndex>> snapshotIndexes =
        blobStoreUtil.getStoreSnapshotIndexes("testJobName", "testJobId", "taskName",
            mockCheckpoint, new HashSet<>(), false);
    assertTrue(snapshotIndexes.isEmpty());
  }

  @Test(expected = SamzaException.class)
  public void testGetSSIThrowsExceptionOnSyncBlobStoreErrors() {
    Checkpoint checkpoint = createCheckpointV2(BlobStoreStateBackendFactory.class.getName(),
        ImmutableMap.of("storeName", "snapshotIndexBlobId"));
    Set<String> storesToBackupOrRestore = new HashSet<>();
    storesToBackupOrRestore.add("storeName");
    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);
    when(mockBlobStoreUtil.getSnapshotIndex(anyString(), any(Metadata.class), anyBoolean())).thenThrow(new RuntimeException());
    when(mockBlobStoreUtil.getStoreSnapshotIndexes(anyString(), anyString(), anyString(),
        any(Checkpoint.class), anySetOf(String.class), any(Boolean.class))).thenCallRealMethod();
    mockBlobStoreUtil.getStoreSnapshotIndexes("testJobName", "testJobId", "taskName",
        checkpoint, storesToBackupOrRestore, false);
  }

  @Test
  public void testSerdeException() throws ExecutionException, InterruptedException {
    final String blobId = "foo";

    final BlobStoreManager testBlobStoreManager = new DeserTestBlobStoreManager();
    final BlobStoreUtil util = new BlobStoreUtil(testBlobStoreManager, Executors.newSingleThreadExecutor(), blobStoreConfig, null, null);

    final CompletableFuture<SnapshotIndex> future = util.getSnapshotIndex(blobId, mock(Metadata.class), true)
        .handle((snapshotIndex, throwable) -> {
          if (throwable != null) {
            Assert.assertEquals(throwable.getMessage(), String.format("Unable to get SnapshotIndex blob. The blob ID is : %s", blobId));
            Assert.assertEquals(throwable.getCause().getMessage(), "org.apache.samza.SamzaException: Exception in deserializing SnapshotIndex bytes foobar");
          }
          return snapshotIndex;
        });
    future.get();
  }

  @Test
  public void testGetSSIThrowsExceptionIfAnyNonIgnoredAsyncBlobStoreErrors() {
    String store = "storeName1";
    String otherStore = "storeName2";
    Set<String> storesToBackupOrRestore = new HashSet<>();
    storesToBackupOrRestore.add(store);
    storesToBackupOrRestore.add(otherStore);
    Checkpoint checkpoint = createCheckpointV2(BlobStoreStateBackendFactory.class.getName(),
        ImmutableMap.of(store, "snapshotIndexBlobId1", otherStore, "snapshotIndexBlobId2"));
    SnapshotIndex store1SnapshotIndex = mock(SnapshotIndex.class);
    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);
    when(mockBlobStoreUtil.getStoreSnapshotIndexes(anyString(), anyString(), anyString(),
        any(Checkpoint.class), anySetOf(String.class), anyBoolean())).thenCallRealMethod();
    RuntimeException nonIgnoredException = new RuntimeException();
    CompletableFuture<SnapshotIndex> failedFuture = FutureUtil.failedFuture(nonIgnoredException);
    when(mockBlobStoreUtil.getSnapshotIndex(eq("snapshotIndexBlobId1"), any(Metadata.class), anyBoolean())).thenReturn(
        FutureUtil.failedFuture(new DeletedException())); // should fail even if some errors are ignored
    when(mockBlobStoreUtil.getSnapshotIndex(eq("snapshotIndexBlobId2"), any(Metadata.class), anyBoolean())).thenReturn(failedFuture);

    try {
      mockBlobStoreUtil.getStoreSnapshotIndexes("testJobName", "testJobId", "taskName",
          checkpoint, storesToBackupOrRestore, false);
      fail("Should have thrown an exception");
    } catch (Exception e) {
      Throwable cause =
          FutureUtil.unwrapExceptions(CompletionException.class, FutureUtil.unwrapExceptions(SamzaException.class, e));
      assertEquals(nonIgnoredException, cause);
    }
  }

  @Test
  public void testGetSSIReturnsCorrectSCMSnapshotIndexPair() {
    String storeName = "storeName";
    String otherStoreName = "otherStoreName";
    Set<String> storesToBackupOrRestore = ImmutableSet.of(storeName, otherStoreName);
    String storeSnapshotIndexBlobId = "snapshotIndexBlobId";
    String otherStoreSnapshotIndexBlobId = "otherSnapshotIndexBlobId";
    SnapshotIndex mockStoreSnapshotIndex = mock(SnapshotIndex.class);
    SnapshotIndex mockOtherStooreSnapshotIndex = mock(SnapshotIndex.class);

    CheckpointV2 checkpoint = createCheckpointV2(BlobStoreStateBackendFactory.class.getName(),
        ImmutableMap.of(storeName, storeSnapshotIndexBlobId, otherStoreName, otherStoreSnapshotIndexBlobId));

    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);

    when(mockBlobStoreUtil.getSnapshotIndex(eq(storeSnapshotIndexBlobId), any(Metadata.class), any(Boolean.class))).thenReturn(
        CompletableFuture.completedFuture(mockStoreSnapshotIndex));
    when(mockBlobStoreUtil.getSnapshotIndex(eq(otherStoreSnapshotIndexBlobId), any(Metadata.class), any(Boolean.class))).thenReturn(
        CompletableFuture.completedFuture(mockOtherStooreSnapshotIndex));
    when(mockBlobStoreUtil.getStoreSnapshotIndexes(anyString(), anyString(), anyString(),
        any(Checkpoint.class), anySetOf(String.class), any(Boolean.class))).thenCallRealMethod();

    Map<String, Pair<String, SnapshotIndex>> snapshotIndexes =
        mockBlobStoreUtil.getStoreSnapshotIndexes("testJobName", "testJobId", "taskName",
            checkpoint, storesToBackupOrRestore, false);

    assertEquals(storeSnapshotIndexBlobId, snapshotIndexes.get(storeName).getKey());
    assertEquals(mockStoreSnapshotIndex, snapshotIndexes.get(storeName).getValue());
    assertEquals(otherStoreSnapshotIndexBlobId, snapshotIndexes.get(otherStoreName).getKey());
    assertEquals(mockOtherStooreSnapshotIndex, snapshotIndexes.get(otherStoreName).getValue());
    verify(mockBlobStoreUtil, times(2)).getSnapshotIndex(anyString(), any(Metadata.class), any(Boolean.class));
  }

  @Test
  public void testGetCheckpointIndexIgnoresStoresNotInStoresToBackupRestoreSet() {
    String store = "storeName1";
    String anotherStore = "storeName2";
    String oneMoreStore = "storeName3";
    SnapshotIndex mockStoreSnapshotIndex = mock(SnapshotIndex.class);
    Set<String> storesToBackupOrRestore = ImmutableSet.of(store, anotherStore);
    CheckpointV2 checkpoint = createCheckpointV2(BlobStoreStateBackendFactory.class.getName(),
        ImmutableMap.of(store, "1", anotherStore, "2", oneMoreStore, "3"));
    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);
    when(mockBlobStoreUtil.getSnapshotIndex(any(String.class), any(Metadata.class), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(mockStoreSnapshotIndex));
    when(mockBlobStoreUtil.getStoreSnapshotIndexes(anyString(), anyString(), anyString(),
        any(Checkpoint.class), anySetOf(String.class), anyBoolean())).thenCallRealMethod();

    Map<String, Pair<String, SnapshotIndex>> snapshotIndexes =
        mockBlobStoreUtil.getStoreSnapshotIndexes("testJobName", "testJobId", "taskName",
            checkpoint, storesToBackupOrRestore, false);

    verify(mockBlobStoreUtil, times(storesToBackupOrRestore.size()))
        .getSnapshotIndex(anyString(), any(Metadata.class), anyBoolean());
  }

  /**
   * This test verifies that a retriable exception is retried more than 3 times (default retry is limited to 3 attempts)
   */
  @Test
  public void testPutFileRetriedMoreThanThreeTimes() throws Exception {
    SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, storeName);
    Path path = Files.createTempFile("samza-testPutFileChecksum-", ".tmp");
    FileUtil fileUtil = new FileUtil();
    fileUtil.writeToTextFile(path.toFile(), RandomStringUtils.random(1000), false);

    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);
    ArgumentCaptor<Metadata> argumentCaptor = ArgumentCaptor.forClass(Metadata.class);
    when(blobStoreManager.put(any(InputStream.class), argumentCaptor.capture()))
        .thenAnswer((Answer<CompletionStage<String>>) invocationOnMock -> { // first try, retriable error
          return FutureUtil.failedFuture(new RetriableException()); // retriable error
        }).thenAnswer((Answer<CompletionStage<String>>) invocationOnMock -> { // second try, retriable error
          return FutureUtil.failedFuture(new RetriableException()); // retriable error
        }).thenAnswer((Answer<CompletionStage<String>>) invocationOnMock -> { // third try, retriable error
          return FutureUtil.failedFuture(new RetriableException()); // retriable error
        }).thenAnswer((Answer<CompletionStage<String>>) invocation -> CompletableFuture.completedFuture("blobId"));

    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(blobStoreManager, EXECUTOR, blobStoreConfig, null, null);

    blobStoreUtil.putFile(path.toFile(), snapshotMetadata).join();
    // Verify put operation is retried 4 times
    verify(blobStoreManager, times(4)).put(any(InputStream.class), any(Metadata.class));
  }

  private CheckpointV2 createCheckpointV2(String stateBackendFactory, Map<String, String> storeSnapshotIndexBlobIds) {
    CheckpointId checkpointId = CheckpointId.create();
    Map<String, Map<String, String>> factoryStoreSCMs = new HashMap<>();
    Map<String, String> storeSCMs = new HashMap<>();
    for (Map.Entry<String, String> entry : storeSnapshotIndexBlobIds.entrySet()) {
      storeSCMs.put(entry.getKey(), entry.getValue());
    }

    factoryStoreSCMs.put(stateBackendFactory, storeSCMs);
    return new CheckpointV2(checkpointId, ImmutableMap.of(), factoryStoreSCMs);
  }

  /**
   * Test {@link BlobStoreManager} to be used to assert SnapshotIndex deserialization failure
   * exception message.
   * We write a dummy string's bytes to the OutputStream parameter of get method instead of a SnapshotIndex
   * blob. The OutputStream is used by SnapshotIndexSerde which will fail during deserialization.
   * */
  private static class DeserTestBlobStoreManager extends TestBlobStoreManager {
    @Override
    public CompletionStage<Void> get(String id, OutputStream outputStream, Metadata metadata, boolean getDeletedBlob) {
      final String randBlob = "foobar";
      final byte[] byteArray = randBlob.getBytes(StandardCharsets.UTF_8);
      try {
        outputStream.write(byteArray);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Test BlobStoreManager for unit tests.
   * */
  private static class TestBlobStoreManager implements BlobStoreManager {
    @Override
    public void init() {
    }

    @Override
    public CompletionStage<String> put(InputStream inputStream, Metadata metadata) {
      return null;
    }

    @Override
    public CompletionStage<Void> get(String id, OutputStream outputStream, Metadata metadata, boolean getDeletedBlob) {
      return null;
    }

    @Override
    public CompletionStage<Void> delete(String id, Metadata metadata) {
      return null;
    }

    @Override
    public CompletionStage<Void> removeTTL(String blobId, Metadata metadata) {
      return null;
    }

    @Override
    public void close() {
    }
  }
}
