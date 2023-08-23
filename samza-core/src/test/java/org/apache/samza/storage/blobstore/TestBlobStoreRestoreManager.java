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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.config.BlobStoreConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.storage.StorageManagerUtil;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;
import org.apache.samza.storage.blobstore.metrics.BlobStoreRestoreManagerMetrics;
import org.apache.samza.storage.blobstore.util.BlobStoreTestUtil;
import org.apache.samza.storage.blobstore.util.BlobStoreUtil;
import org.apache.samza.storage.blobstore.util.DirDiffUtil;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


public class TestBlobStoreRestoreManager {
  private static final ExecutorService EXECUTOR = MoreExecutors.newDirectExecutorService();

  @Test
  public void testDeleteUnusedStoresRemovesStoresDeletedFromConfig() {
    String jobName = "testJobName";
    String jobId = "testJobId";
    String taskName = "taskName";
    StorageConfig storageConfig = mock(StorageConfig.class);
    BlobStoreConfig blobStoreConfig = mock(BlobStoreConfig.class);
    SnapshotIndex mockSnapshotIndex = mock(SnapshotIndex.class);
    String blobId = "blobId";
    Map<String, Pair<String, SnapshotIndex>> initialStoreSnapshotIndexes =
        ImmutableMap.of("oldStoreName", Pair.of(blobId, mockSnapshotIndex));

    when(storageConfig.getStoresWithBackupFactory(eq(BlobStoreStateBackendFactory.class.getName())))
        .thenReturn(ImmutableList.of("newStoreName"));
    when(storageConfig.getStoresWithRestoreFactory(eq(BlobStoreStateBackendFactory.class.getName())))
        .thenReturn(ImmutableList.of("newStoreName"));

    DirIndex dirIndex = mock(DirIndex.class);
    when(mockSnapshotIndex.getDirIndex()).thenReturn(dirIndex);

    BlobStoreUtil blobStoreUtil = mock(BlobStoreUtil.class);
    when(blobStoreUtil.cleanSnapshotIndex(anyString(), any(SnapshotIndex.class), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    BlobStoreRestoreManager.deleteUnusedStoresFromBlobStore(
        jobName, jobId, taskName, storageConfig, blobStoreConfig, initialStoreSnapshotIndexes, blobStoreUtil, EXECUTOR);

    verify(blobStoreUtil, times(1)).cleanSnapshotIndex(eq(blobId), any(SnapshotIndex.class), any(Metadata.class));

  }

  @Test
  public void testShouldRestoreIfNoCheckpointDir() throws IOException {
    String taskName = "taskName";
    String storeName = "storeName";
    DirIndex dirIndex = mock(DirIndex.class);
    Path storeCheckpointDir = Paths.get("/tmp/non-existent-checkpoint-dir");
    StorageConfig storageConfig = mock(StorageConfig.class);
    when(storageConfig.cleanLoggedStoreDirsOnStart(anyString())).thenReturn(false);
    DirDiffUtil dirDiffUtil = mock(DirDiffUtil.class);

    boolean shouldRestore = BlobStoreRestoreManager.shouldRestore(
        taskName, storeName, dirIndex, storeCheckpointDir, storageConfig, dirDiffUtil);

    verifyZeroInteractions(dirDiffUtil);
    assertTrue(shouldRestore);
  }

  @Test
  public void testShouldRestoreIfCleanStateOnRestartEnabled() throws  IOException {
    String taskName = "taskName";
    String storeName = "storeName";
    DirIndex dirIndex = mock(DirIndex.class);
    Path storeCheckpointDir = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX); // must exist
    StorageConfig storageConfig = mock(StorageConfig.class);
    when(storageConfig.cleanLoggedStoreDirsOnStart(anyString())).thenReturn(true); // clean on restart
    DirDiffUtil dirDiffUtil = mock(DirDiffUtil.class);

    boolean shouldRestore = BlobStoreRestoreManager.shouldRestore(
        taskName, storeName, dirIndex, storeCheckpointDir, storageConfig, dirDiffUtil);

    verifyZeroInteractions(dirDiffUtil);
    assertTrue(shouldRestore); // should not restore, should retain checkpoint dir instead
  }

  @Test
  public void testShouldRestoreIfCheckpointDirNotIdenticalToRemoteSnapshot() throws IOException {
    String taskName = "taskName";
    String storeName = "storeName";
    DirIndex dirIndex = mock(DirIndex.class);
    Path storeCheckpointDir = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX); // must exist
    StorageConfig storageConfig = mock(StorageConfig.class);
    when(storageConfig.cleanLoggedStoreDirsOnStart(anyString())).thenReturn(false);
    DirDiffUtil dirDiffUtil = mock(DirDiffUtil.class);
    when(dirDiffUtil.areSameDir(anySet(), anyBoolean(), anyBoolean())).thenReturn((arg1, arg2) -> false);

    boolean shouldRestore = BlobStoreRestoreManager.shouldRestore(
        taskName, storeName, dirIndex, storeCheckpointDir, storageConfig, dirDiffUtil);

    assertTrue(shouldRestore);
  }

  @Test
  public void testShouldNotRestoreIfPreviousCheckpointDirIdenticalToRemoteSnapshot() throws  IOException {
    String taskName = "taskName";
    String storeName = "storeName";
    DirIndex dirIndex = mock(DirIndex.class);
    Path storeCheckpointDir = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX); // must exist
    StorageConfig storageConfig = mock(StorageConfig.class);
    when(storageConfig.cleanLoggedStoreDirsOnStart(anyString())).thenReturn(false);
    DirDiffUtil dirDiffUtil = mock(DirDiffUtil.class);
    when(dirDiffUtil.areSameDir(anySet(), anyBoolean(), anyBoolean())).thenReturn((arg1, arg2) -> true); // are same dir

    boolean shouldRestore = BlobStoreRestoreManager.shouldRestore(
        taskName, storeName, dirIndex, storeCheckpointDir, storageConfig, dirDiffUtil);

    verify(dirDiffUtil, times(1)).areSameDir(anySet(), anyBoolean(), anyBoolean());
    assertFalse(shouldRestore); // should not restore, should retain checkpoint dir instead
  }

  @Test
  public void testRestoreDeletesStoreDir() throws IOException {
    String jobName = "testJobName";
    String jobId = "testJobId";
    TaskName taskName = mock(TaskName.class);
    BlobStoreRestoreManagerMetrics metrics = new BlobStoreRestoreManagerMetrics(new MetricsRegistryMap());
    metrics.initStoreMetrics(ImmutableList.of("storeName"));
    Set<String> storesToRestore = ImmutableSet.of("storeName");
    SnapshotIndex snapshotIndex = mock(SnapshotIndex.class);
    Map<String, Pair<String, SnapshotIndex>> prevStoreSnapshotIndexes =
        ImmutableMap.of("storeName", Pair.of("blobId", snapshotIndex));
    DirIndex dirIndex = BlobStoreTestUtil.createDirIndex("[a]");
    when(snapshotIndex.getDirIndex()).thenReturn(dirIndex);
    when(snapshotIndex.getSnapshotMetadata())
        .thenReturn(new SnapshotMetadata(CheckpointId.create(), "jobName", "jobId", "taskName", "storeName"));

    Path loggedBaseDir = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX);

    // create store dir to be deleted during restore
    Path storeDir = Files.createTempDirectory(loggedBaseDir, "storeDir");
    StorageConfig storageConfig = mock(StorageConfig.class);
    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
    when(storageManagerUtil.getStoreCheckpointDir(any(File.class), any(CheckpointId.class)))
      .thenReturn(Paths.get(storeDir.toString(), "checkpointId").toString());
    when(storageManagerUtil.getTaskStoreDir(
        eq(loggedBaseDir.toFile()), eq("storeName"), eq(taskName), eq(TaskMode.Active)))
          .thenReturn(storeDir.toFile());
    BlobStoreUtil blobStoreUtil = mock(BlobStoreUtil.class);
    DirDiffUtil dirDiffUtil = mock(DirDiffUtil.class);

    // return immediately without restoring.
    when(blobStoreUtil.restoreDir(eq(storeDir.toFile()), eq(dirIndex), any(Metadata.class), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(dirDiffUtil.areSameDir(anySet(), anyBoolean(), anyBoolean())).thenReturn((arg1, arg2) -> true);

    BlobStoreRestoreManager.restoreStores(jobName, jobId, taskName, storesToRestore, prevStoreSnapshotIndexes,
        loggedBaseDir.toFile(), storageConfig, metrics,
        storageManagerUtil, blobStoreUtil, dirDiffUtil, EXECUTOR, false, true);

    // verify that the store directory restore was called and skipped (i.e. shouldRestore == true)
    verify(blobStoreUtil, times(1)).restoreDir(eq(storeDir.toFile()), eq(dirIndex), any(Metadata.class), anyBoolean());
    // verify that the store directory was deleted prior to restore
    // (should still not exist at the end since restore is no-op)
    assertFalse(storeDir.toFile().exists());
  }

  @Test
  public void testRestoreDeletesCheckpointDirsIfRestoring() throws IOException {
    String jobName = "testJobName";
    String jobId = "testJobId";
    TaskName taskName = mock(TaskName.class);
    BlobStoreRestoreManagerMetrics metrics = new BlobStoreRestoreManagerMetrics(new MetricsRegistryMap());
    metrics.initStoreMetrics(ImmutableList.of("storeName"));
    Set<String> storesToRestore = ImmutableSet.of("storeName");
    SnapshotIndex snapshotIndex = mock(SnapshotIndex.class);
    Map<String, Pair<String, SnapshotIndex>> prevStoreSnapshotIndexes =
        ImmutableMap.of("storeName", Pair.of("blobId", snapshotIndex));
    DirIndex dirIndex = BlobStoreTestUtil.createDirIndex("[a]");
    when(snapshotIndex.getDirIndex()).thenReturn(dirIndex);
    CheckpointId checkpointId = CheckpointId.create();
    when(snapshotIndex.getSnapshotMetadata())
        .thenReturn(new SnapshotMetadata(checkpointId, "jobName", "jobId", "taskName", "storeName"));

    Path loggedBaseDir = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX);

    // create store dir to be deleted during restore
    Path storeDir = Files.createTempDirectory(loggedBaseDir, "storeDir");
    Path storeCheckpointDir1 = Files.createTempDirectory(loggedBaseDir, "storeDir-" + checkpointId);
    CheckpointId olderCheckpoint = CheckpointId.create();
    Path storeCheckpointDir2 = Files.createTempDirectory(loggedBaseDir, "storeDir-" + olderCheckpoint);
    StorageConfig storageConfig = mock(StorageConfig.class);
    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
    when(storageManagerUtil.getTaskStoreDir(
        eq(loggedBaseDir.toFile()), eq("storeName"), eq(taskName), eq(TaskMode.Active)))
        .thenReturn(storeDir.toFile());
    when(storageManagerUtil.getStoreCheckpointDir(eq(storeDir.toFile()), eq(checkpointId)))
        .thenReturn(Paths.get(storeDir.toString(), checkpointId.toString()).toString());
    when(storageManagerUtil.getTaskStoreCheckpointDirs(any(File.class), anyString(), any(TaskName.class), any(TaskMode.class)))
        .thenReturn(ImmutableList.of(storeCheckpointDir1.toFile(), storeCheckpointDir2.toFile()));
    BlobStoreUtil blobStoreUtil = mock(BlobStoreUtil.class);
    DirDiffUtil dirDiffUtil = mock(DirDiffUtil.class);

    when(dirDiffUtil.areSameDir(anySet(), anyBoolean(), anyBoolean())).thenReturn((arg1, arg2) -> true);
    // return immediately without restoring.
    when(blobStoreUtil.restoreDir(eq(storeDir.toFile()), eq(dirIndex), any(Metadata.class), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));

    BlobStoreRestoreManager.restoreStores(jobName, jobId, taskName, storesToRestore, prevStoreSnapshotIndexes,
        loggedBaseDir.toFile(), storageConfig, metrics,
        storageManagerUtil, blobStoreUtil, dirDiffUtil, EXECUTOR, false, true);

    // verify that the store directory restore was called and skipped (i.e. shouldRestore == true)
    verify(blobStoreUtil, times(1)).restoreDir(eq(storeDir.toFile()), eq(dirIndex), any(Metadata.class), anyBoolean());
    // verify that the checkpoint directories were deleted prior to restore (should not exist at the end)
    assertFalse(storeCheckpointDir1.toFile().exists());
    assertFalse(storeCheckpointDir2.toFile().exists());
  }

  @Test
  public void testRestoreRetainsCheckpointDirsIfValid() throws IOException {
    String jobName = "testJobName";
    String jobId = "testJobId";
    TaskName taskName = mock(TaskName.class);
    BlobStoreRestoreManagerMetrics metrics = new BlobStoreRestoreManagerMetrics(new MetricsRegistryMap());
    metrics.initStoreMetrics(ImmutableList.of("storeName"));
    Set<String> storesToRestore = ImmutableSet.of("storeName");
    SnapshotIndex snapshotIndex = mock(SnapshotIndex.class);
    Map<String, Pair<String, SnapshotIndex>> prevStoreSnapshotIndexes =
        ImmutableMap.of("storeName", Pair.of("blobId", snapshotIndex));
    DirIndex dirIndex = BlobStoreTestUtil.createDirIndex("[a]");
    when(snapshotIndex.getDirIndex()).thenReturn(dirIndex);
    CheckpointId checkpointId = CheckpointId.create();
    when(snapshotIndex.getSnapshotMetadata())
        .thenReturn(new SnapshotMetadata(checkpointId, "jobName", "jobId", "taskName", "storeName"));

    Path loggedBaseDir = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX);

    // create store dir to be deleted during restore
    Path storeDir = Files.createTempDirectory(loggedBaseDir, "storeDir-");

    // create checkpoint dir so that shouldRestore = false (areSameDir == true later)
    Path storeCheckpointDir = Files.createTempDirectory(loggedBaseDir, "storeDir-" + checkpointId + "-");
    // create a dummy file to verify after dir rename.
    Path tempFile = Files.createTempFile(storeCheckpointDir, "tempFile-", null);

    StorageConfig storageConfig = mock(StorageConfig.class);
    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
    when(storageManagerUtil.getTaskStoreDir(
        eq(loggedBaseDir.toFile()), eq("storeName"), eq(taskName), eq(TaskMode.Active)))
        .thenReturn(storeDir.toFile());
    when(storageManagerUtil.getStoreCheckpointDir(any(File.class), eq(checkpointId)))
        .thenReturn(storeCheckpointDir.toString());
    when(storageManagerUtil.getTaskStoreCheckpointDirs(any(File.class), anyString(), any(TaskName.class), any(TaskMode.class)))
        .thenReturn(ImmutableList.of(storeCheckpointDir.toFile()));
    BlobStoreUtil blobStoreUtil = mock(BlobStoreUtil.class);
    DirDiffUtil dirDiffUtil = mock(DirDiffUtil.class);

    // ensures shouldRestore is not called
    when(dirDiffUtil.areSameDir(anySet(), anyBoolean(), anyBoolean())).thenReturn((arg1, arg2) -> true);
    // return immediately without restoring.
    when(blobStoreUtil.restoreDir(eq(storeDir.toFile()), eq(dirIndex), any(Metadata.class), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));

    BlobStoreRestoreManager.restoreStores(jobName, jobId, taskName, storesToRestore, prevStoreSnapshotIndexes,
        loggedBaseDir.toFile(), storageConfig, metrics,
        storageManagerUtil, blobStoreUtil, dirDiffUtil, EXECUTOR, false, true);

    // verify that the store directory restore was not called (should have restored from checkpoint dir)
    verify(blobStoreUtil, times(0)).restoreDir(eq(storeDir.toFile()), eq(dirIndex), any(Metadata.class), anyBoolean());
    // verify that the checkpoint dir was renamed to store dir
    assertFalse(storeCheckpointDir.toFile().exists());
    assertTrue(storeDir.toFile().exists());
    assertTrue(Files.exists(Paths.get(storeDir.toString(), tempFile.getFileName().toString())));
  }

  @Test
  public void testRestoreSkipsStoresWithMissingCheckpointSCM()  {
    // store renamed from oldStoreName to newStoreName. No SCM for newStoreName in previous checkpoint.
    String jobName = "testJobName";
    String jobId = "testJobId";
    TaskName taskName = mock(TaskName.class);
    BlobStoreRestoreManagerMetrics metrics = new BlobStoreRestoreManagerMetrics(new MetricsRegistryMap());
    metrics.initStoreMetrics(ImmutableList.of("newStoreName"));
    Set<String> storesToRestore = ImmutableSet.of("newStoreName"); // new store in config
    SnapshotIndex snapshotIndex = mock(SnapshotIndex.class);
    Map<String, Pair<String, SnapshotIndex>> prevStoreSnapshotIndexes = mock(Map.class);
    when(prevStoreSnapshotIndexes.containsKey("newStoreName")).thenReturn(false);
    DirIndex dirIndex = mock(DirIndex.class);
    when(snapshotIndex.getDirIndex()).thenReturn(dirIndex);
    CheckpointId checkpointId = CheckpointId.create();
    when(snapshotIndex.getSnapshotMetadata())
        .thenReturn(new SnapshotMetadata(checkpointId, "jobName", "jobId", "taskName", "storeName"));
    Path loggedBaseDir = mock(Path.class);

    // create store dir to be deleted during restore
    StorageConfig storageConfig = mock(StorageConfig.class);
    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
    BlobStoreUtil blobStoreUtil = mock(BlobStoreUtil.class);
    DirDiffUtil dirDiffUtil = mock(DirDiffUtil.class);

    BlobStoreRestoreManager.restoreStores(jobName, jobId, taskName, storesToRestore, prevStoreSnapshotIndexes,
        loggedBaseDir.toFile(), storageConfig, metrics,
        storageManagerUtil, blobStoreUtil, dirDiffUtil, EXECUTOR, false, true);

    // verify that we checked the previously checkpointed SCMs.
    verify(prevStoreSnapshotIndexes, times(1)).containsKey(eq("newStoreName"));
    // verify that the store directory restore was never called
    verify(blobStoreUtil, times(0)).restoreDir(any(File.class), any(DirIndex.class), any(Metadata.class), anyBoolean());
  }
}
