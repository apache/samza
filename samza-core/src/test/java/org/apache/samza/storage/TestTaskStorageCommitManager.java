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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointV1;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.checkpoint.kafka.KafkaChangelogSSPOffset;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


public class TestTaskStorageCommitManager {
  @Test
  public void testCommitManagerStart() {
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    Checkpoint checkpoint = mock(Checkpoint.class);

    TaskName taskName = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );
    TaskStorageCommitManager cm = new TaskStorageCommitManager(taskName, backupManagers, containerStorageManager,
        Collections.emptyMap(), new Partition(1), checkpointManager, new MapConfig(),
        ForkJoinPool.commonPool(), new StorageManagerUtil(), null);

    when(checkpointManager.readLastCheckpoint(taskName)).thenReturn(checkpoint);
    cm.init();
    verify(taskBackupManager1).init(eq(checkpoint));
    verify(taskBackupManager2).init(eq(checkpoint));
  }

  @Test
  public void testCommitManagerStartNullCheckpointManager() {
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);

    TaskName task = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );
    TaskStorageCommitManager cm = new TaskStorageCommitManager(task, backupManagers, containerStorageManager,
        Collections.emptyMap(), new Partition(1), null, new MapConfig(),
        ForkJoinPool.commonPool(), new StorageManagerUtil(), null);
    cm.init();
    verify(taskBackupManager1).init(eq(null));
    verify(taskBackupManager2).init(eq(null));
  }

  @Test
  public void testSnapshotAndCommitAllFactories() {
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    Checkpoint checkpoint = mock(Checkpoint.class);

    TaskName taskName = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );
    TaskStorageCommitManager cm = new TaskStorageCommitManager(taskName, backupManagers, containerStorageManager,
        Collections.emptyMap(), new Partition(1), checkpointManager, new MapConfig(),
        ForkJoinPool.commonPool(), new StorageManagerUtil(), null);
    when(checkpointManager.readLastCheckpoint(taskName)).thenReturn(checkpoint);
    cm.init();
    verify(taskBackupManager1).init(eq(checkpoint));
    verify(taskBackupManager2).init(eq(checkpoint));

    CheckpointId newCheckpointId = CheckpointId.create();
    Map<String, String> factory1Checkpoints = ImmutableMap.of(
        "store1", "system;stream;1",
        "store2", "system;stream;2"
    );
    Map<String, String> factory2Checkpoints = ImmutableMap.of(
        "store1", "blobId1",
        "store2", "blobId2"
    );

    when(containerStorageManager.getAllStores(taskName)).thenReturn(Collections.emptyMap());
    when(taskBackupManager1.snapshot(newCheckpointId)).thenReturn(factory1Checkpoints);
    when(taskBackupManager2.snapshot(newCheckpointId)).thenReturn(factory2Checkpoints);

    when(taskBackupManager1.upload(newCheckpointId, factory1Checkpoints))
        .thenReturn(CompletableFuture.completedFuture(factory1Checkpoints));
    when(taskBackupManager2.upload(newCheckpointId, factory2Checkpoints))
        .thenReturn(CompletableFuture.completedFuture(factory2Checkpoints));

    Map<String, Map<String, String>> snapshotSCMs = cm.snapshot(newCheckpointId);
    cm.upload(newCheckpointId, snapshotSCMs);

    // Test flow for snapshot
    verify(taskBackupManager1).snapshot(newCheckpointId);
    verify(taskBackupManager2).snapshot(newCheckpointId);

    // Test flow for upload
    verify(taskBackupManager1).upload(newCheckpointId, factory1Checkpoints);
    verify(taskBackupManager2).upload(newCheckpointId, factory2Checkpoints);
  }

  @Test
  public void testFlushAndCheckpointOnSnapshot() {
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    Checkpoint checkpoint = mock(Checkpoint.class);

    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isDurableStore()).thenReturn(true);
    Path mockPath = mock(Path.class);
    when(mockLPStore.checkpoint(any())).thenReturn(Optional.of(mockPath));

    StorageEngine mockPStore = mock(StorageEngine.class);
    StoreProperties pStoreProps = mock(StoreProperties.class);
    when(mockPStore.getStoreProperties()).thenReturn(pStoreProps);
    when(pStoreProps.isPersistedToDisk()).thenReturn(true);
    when(pStoreProps.isDurableStore()).thenReturn(false);

    StorageEngine mockLIStore = mock(StorageEngine.class);
    StoreProperties liStoreProps = mock(StoreProperties.class);
    when(mockLIStore.getStoreProperties()).thenReturn(liStoreProps);
    when(liStoreProps.isPersistedToDisk()).thenReturn(false);
    when(liStoreProps.isDurableStore()).thenReturn(true);

    StorageEngine mockIStore = mock(StorageEngine.class);
    StoreProperties iStoreProps = mock(StoreProperties.class);
    when(mockIStore.getStoreProperties()).thenReturn(iStoreProps);
    when(iStoreProps.isPersistedToDisk()).thenReturn(false);
    when(iStoreProps.isDurableStore()).thenReturn(false);

    TaskName taskName = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );
    Map<String, StorageEngine> storageEngines = ImmutableMap.of(
        "storeLP", mockLPStore,
        "storeP", mockPStore,
        "storeLI", mockLIStore,
        "storeI", mockIStore
    );

    TaskStorageCommitManager cm = new TaskStorageCommitManager(taskName, backupManagers, containerStorageManager,
        Collections.emptyMap(), new Partition(1), checkpointManager, new MapConfig(),
        ForkJoinPool.commonPool(), new StorageManagerUtil(), null);
    when(checkpointManager.readLastCheckpoint(taskName)).thenReturn(checkpoint);
    cm.init();
    verify(taskBackupManager1).init(eq(checkpoint));
    verify(taskBackupManager2).init(eq(checkpoint));

    CheckpointId newCheckpointId = CheckpointId.create();
    Map<String, String> factory1Checkpoints = ImmutableMap.of(
        "store1", "system;stream;1",
        "store2", "system;stream;2"
    );
    Map<String, String> factory2Checkpoints = ImmutableMap.of(
        "store1", "blobId1",
        "store2", "blobId2"
    );

    when(containerStorageManager.getAllStores(taskName)).thenReturn(storageEngines);
    when(taskBackupManager1.snapshot(newCheckpointId)).thenReturn(factory1Checkpoints);
    when(taskBackupManager1.upload(newCheckpointId, factory1Checkpoints))
        .thenReturn(CompletableFuture.completedFuture(factory1Checkpoints));
    when(taskBackupManager2.snapshot(newCheckpointId)).thenReturn(factory2Checkpoints);
    when(taskBackupManager2.upload(newCheckpointId, factory2Checkpoints))
        .thenReturn(CompletableFuture.completedFuture(factory2Checkpoints));
    when(mockLIStore.checkpoint(newCheckpointId)).thenReturn(Optional.empty());

    cm.init();
    cm.snapshot(newCheckpointId);

    // Assert stores where flushed
    verify(mockIStore).flush();
    verify(mockPStore).flush();
    verify(mockLIStore).flush();
    verify(mockLPStore).flush();
    // only logged and persisted stores are checkpointed
    verify(mockLPStore).checkpoint(newCheckpointId);
    // ensure that checkpoint is never called for non-logged persistent stores since they're
    // always cleared on restart.
    verify(mockPStore, never()).checkpoint(any());
    // ensure that checkpoint is never called for non-persistent stores
    verify(mockIStore, never()).checkpoint(any());
    verify(mockLIStore, never()).checkpoint(any());
  }

  @Test(expected = IllegalStateException.class)
  public void testSnapshotFailsIfErrorCreatingCheckpoint() {
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);

    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isDurableStore()).thenReturn(true);
    when(mockLPStore.checkpoint(any())).thenThrow(new IllegalStateException());

    TaskName taskName = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );
    Map<String, StorageEngine> storageEngines = ImmutableMap.of(
        "storeLP", mockLPStore
    );

    TaskStorageCommitManager cm = new TaskStorageCommitManager(taskName, backupManagers, containerStorageManager,
        Collections.emptyMap(), new Partition(1), checkpointManager, new MapConfig(),
        ForkJoinPool.commonPool(), new StorageManagerUtil(), null);

    when(containerStorageManager.getAllStores(taskName)).thenReturn(storageEngines);
    CheckpointId newCheckpointId = CheckpointId.create();
    cm.init();
    cm.snapshot(newCheckpointId);

    // Assert stores where flushed
    verify(mockLPStore).flush();
    // only logged and persisted stores are checkpointed
    verify(mockLPStore).checkpoint(newCheckpointId);
    verify(taskBackupManager1, never()).snapshot(any());
    verify(taskBackupManager2, never()).snapshot(any());
    verify(taskBackupManager1, never()).upload(any(), any());
    verify(taskBackupManager2, never()).upload(any(), any());
    fail("Should have thrown an exception when the storageEngine#checkpoint did not succeed");
  }

  @Test
  public void testCleanupAllBackupManagers() {
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    Checkpoint checkpoint = mock(Checkpoint.class);
    File durableStoreDir = mock(File.class);
    when(durableStoreDir.listFiles()).thenReturn(new File[0]);

    TaskName taskName = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );
    TaskStorageCommitManager cm = new TaskStorageCommitManager(taskName, backupManagers, containerStorageManager,
        Collections.emptyMap(),  new Partition(1), checkpointManager, new MapConfig(),
        ForkJoinPool.commonPool(), new StorageManagerUtil(), durableStoreDir);
    when(checkpointManager.readLastCheckpoint(taskName)).thenReturn(checkpoint);
    when(containerStorageManager.getAllStores(taskName)).thenReturn(Collections.emptyMap());
    when(taskBackupManager1.cleanUp(any(), any())).thenReturn(CompletableFuture.<Void>completedFuture(null));
    when(taskBackupManager2.cleanUp(any(), any())).thenReturn(CompletableFuture.<Void>completedFuture(null));
    Map<String, String> factory1Checkpoints = ImmutableMap.of(
        "store1", "system;stream;1",
        "store2", "system;stream;2"
    );
    Map<String, String> factory2Checkpoints = ImmutableMap.of(
        "store1", "blobId1",
        "store2", "blobId2"
    );
    Map<String, Map<String, String>> factoryCheckpointsMap = ImmutableMap.of(
        "factory1", factory1Checkpoints,
        "factory2", factory2Checkpoints
    );

    when(taskBackupManager1.cleanUp(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(taskBackupManager2.cleanUp(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    CheckpointId newCheckpointId = CheckpointId.create();
    cm.cleanUp(newCheckpointId, factoryCheckpointsMap).join();

    verify(taskBackupManager1).cleanUp(newCheckpointId, factory1Checkpoints);
    verify(taskBackupManager2).cleanUp(newCheckpointId, factory2Checkpoints);
  }

  @Test
  public void testCleanupFailsIfBackupManagerNotInitiated() {
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    Checkpoint checkpoint = mock(Checkpoint.class);
    File durableStoreDir = mock(File.class);
    when(durableStoreDir.listFiles()).thenReturn(new File[0]);

    TaskName taskName = new TaskName("task1");
    when(containerStorageManager.getAllStores(taskName)).thenReturn(Collections.emptyMap());
    TaskStorageCommitManager cm = new TaskStorageCommitManager(taskName, Collections.emptyMap(), containerStorageManager,
        Collections.emptyMap(),  new Partition(1), checkpointManager, new MapConfig(),
        ForkJoinPool.commonPool(), new StorageManagerUtil(), durableStoreDir);
    when(checkpointManager.readLastCheckpoint(taskName)).thenReturn(checkpoint);

    Map<String, Map<String, String>> factoryCheckpointsMap = ImmutableMap.of(
        "factory3", Collections.emptyMap() // factory 3 should be ignored
    );

    CheckpointId newCheckpointId = CheckpointId.create();
    cm.cleanUp(newCheckpointId, factoryCheckpointsMap);
    // should not fail the commit because the job should ignore any factories checkpoints not initialized
    // in case the user is in a migration phase from on state backend to another
  }

  @Test
  public void testPersistToFileSystemCheckpointV1AndV2Checkpoint() throws IOException {
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isDurableStore()).thenReturn(true);
    Path mockPath = mock(Path.class);
    when(mockLPStore.checkpoint(any())).thenReturn(Optional.of(mockPath));

    StorageEngine mockPStore = mock(StorageEngine.class);
    StoreProperties pStoreProps = mock(StoreProperties.class);
    when(mockPStore.getStoreProperties()).thenReturn(pStoreProps);
    when(pStoreProps.isPersistedToDisk()).thenReturn(true);
    when(pStoreProps.isDurableStore()).thenReturn(false);

    StorageEngine mockLIStore = mock(StorageEngine.class);
    StoreProperties liStoreProps = mock(StoreProperties.class);
    when(mockLIStore.getStoreProperties()).thenReturn(liStoreProps);
    when(liStoreProps.isPersistedToDisk()).thenReturn(false);
    when(liStoreProps.isDurableStore()).thenReturn(true);

    StorageEngine mockIStore = mock(StorageEngine.class);
    StoreProperties iStoreProps = mock(StoreProperties.class);
    when(mockIStore.getStoreProperties()).thenReturn(iStoreProps);
    when(iStoreProps.isPersistedToDisk()).thenReturn(false);
    when(iStoreProps.isDurableStore()).thenReturn(false);

    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of(
        "loggedPersistentStore", mockLPStore,
        "persistentStore", mockPStore,
        "loggedInMemStore", mockLIStore,
        "inMemStore", mockIStore
    );

    Partition changelogPartition = new Partition(0);
    SystemStream changelogSystemStream = new SystemStream("changelogSystem", "changelogStream");
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);
    java.util.Map<String, SystemStream> storeChangelogsStreams = ImmutableMap.of(
        "loggedPersistentStore", changelogSystemStream,
        "loggedInMemStore", new SystemStream("system", "stream")
    );

    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
    File durableStoreDir = new File("durableStorePath");
    when(storageManagerUtil.getTaskStoreDir(eq(durableStoreDir), any(), any(), any())).thenReturn(durableStoreDir);
    TaskName taskName = new TaskName("task");

    when(containerStorageManager.getAllStores(taskName)).thenReturn(taskStores);
    TaskStorageCommitManager commitManager = spy(new TaskStorageCommitManager(taskName,
        Collections.emptyMap(), containerStorageManager, storeChangelogsStreams, changelogPartition,
        null, null, ForkJoinPool.commonPool(), storageManagerUtil, durableStoreDir));
    doNothing().when(commitManager).writeChangelogOffsetFile(any(), any(), any(), any());

    CheckpointId newCheckpointId = CheckpointId.create();

    String newestOffset = "1";
    KafkaChangelogSSPOffset kafkaChangelogSSPOffset = new KafkaChangelogSSPOffset(newCheckpointId, newestOffset);
    java.util.Map<SystemStreamPartition, String> offsetsJava = ImmutableMap.of(
        changelogSSP, kafkaChangelogSSPOffset.toString()
    );

    commitManager.init();
    // invoke persist to file system for v2 checkpoint
    commitManager.writeCheckpointToStoreDirectories(new CheckpointV1(offsetsJava));

    verify(commitManager).writeChangelogOffsetFiles(offsetsJava);
    // evoked twice, for OFFSET-V1 and OFFSET-V2
    verify(commitManager).writeChangelogOffsetFile(
        eq("loggedPersistentStore"), eq(changelogSSP), eq(newestOffset), eq(durableStoreDir));
    File checkpointFile = Paths.get(StorageManagerUtil
        .getCheckpointDirPath(durableStoreDir, kafkaChangelogSSPOffset.getCheckpointId())).toFile();
    verify(commitManager).writeChangelogOffsetFile(
        eq("loggedPersistentStore"), eq(changelogSSP), eq(newestOffset), eq(checkpointFile));

    java.util.Map<String, String> storeSCM = ImmutableMap.of(
        "loggedPersistentStore", "system;loggedPersistentStoreStream;1",
        "persistentStore", "system;persistentStoreStream;1",
        "loggedInMemStore", "system;loggedInMemStoreStream;1",
        "inMemStore", "system;inMemStoreStream;1"
    );
    CheckpointV2 checkpoint = new CheckpointV2(newCheckpointId, Collections.emptyMap(), Collections.singletonMap("factory", storeSCM));

    // invoke persist to file system for v2 checkpoint
    commitManager.writeCheckpointToStoreDirectories(checkpoint);
    // Validate only durable and persisted stores are persisted
    // This should be evoked twice, for checkpointV1 and checkpointV2
    verify(storageManagerUtil, times(2)).getTaskStoreDir(eq(durableStoreDir), eq("loggedPersistentStore"), eq(taskName), any());
    File checkpointPath = Paths.get(StorageManagerUtil.getCheckpointDirPath(durableStoreDir, newCheckpointId)).toFile();
    verify(storageManagerUtil).writeCheckpointV2File(eq(checkpointPath), eq(checkpoint));
  }

  @Test
  public void testPersistToFileSystemCheckpointV2Only() throws IOException {
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isDurableStore()).thenReturn(true);
    Path mockPath = mock(Path.class);
    when(mockLPStore.checkpoint(any())).thenReturn(Optional.of(mockPath));

    StorageEngine mockPStore = mock(StorageEngine.class);
    StoreProperties pStoreProps = mock(StoreProperties.class);
    when(mockPStore.getStoreProperties()).thenReturn(pStoreProps);
    when(pStoreProps.isPersistedToDisk()).thenReturn(true);
    when(pStoreProps.isDurableStore()).thenReturn(false);

    StorageEngine mockLIStore = mock(StorageEngine.class);
    StoreProperties liStoreProps = mock(StoreProperties.class);
    when(mockLIStore.getStoreProperties()).thenReturn(liStoreProps);
    when(liStoreProps.isPersistedToDisk()).thenReturn(false);
    when(liStoreProps.isDurableStore()).thenReturn(true);

    StorageEngine mockIStore = mock(StorageEngine.class);
    StoreProperties iStoreProps = mock(StoreProperties.class);
    when(mockIStore.getStoreProperties()).thenReturn(iStoreProps);
    when(iStoreProps.isPersistedToDisk()).thenReturn(false);
    when(iStoreProps.isDurableStore()).thenReturn(false);

    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of(
        "loggedPersistentStore", mockLPStore,
        "persistentStore", mockPStore,
        "loggedInMemStore", mockLIStore,
        "inMemStore", mockIStore
    );

    Partition changelogPartition = new Partition(0);
    SystemStream changelogSystemStream = new SystemStream("changelogSystem", "changelogStream");
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);
    java.util.Map<String, SystemStream> storeChangelogsStreams = ImmutableMap.of(
        "loggedPersistentStore", changelogSystemStream,
        "loggedInMemStore", new SystemStream("system", "stream")
    );

    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
    File durableStoreDir = new File("durableStorePath");
    when(storageManagerUtil.getTaskStoreDir(eq(durableStoreDir), eq("loggedPersistentStore"), any(), any()))
        .thenReturn(durableStoreDir);
    TaskName taskName = new TaskName("task");

    when(containerStorageManager.getAllStores(taskName)).thenReturn(taskStores);
    TaskStorageCommitManager commitManager = spy(new TaskStorageCommitManager(taskName,
        Collections.emptyMap(), containerStorageManager, storeChangelogsStreams, changelogPartition,
        null, null, ForkJoinPool.commonPool(), storageManagerUtil, durableStoreDir));
    doNothing().when(commitManager).writeChangelogOffsetFile(any(), any(), any(), any());

    CheckpointId newCheckpointId = CheckpointId.create();

    java.util.Map<String, String> storeSCM = ImmutableMap.of(
        "loggedPersistentStore", "system;loggedPersistentStoreStream;1",
        "persistentStore", "system;persistentStoreStream;1",
        "loggedInMemStore", "system;loggedInMemStoreStream;1",
        "inMemStore", "system;inMemStoreStream;1"
    );
    CheckpointV2 checkpoint = new CheckpointV2(newCheckpointId, Collections.emptyMap(), Collections.singletonMap("factory", storeSCM));

    commitManager.init();
    // invoke persist to file system
    commitManager.writeCheckpointToStoreDirectories(checkpoint);
    // Validate only durable and persisted stores are persisted
    verify(storageManagerUtil).getTaskStoreDir(eq(durableStoreDir), eq("loggedPersistentStore"), eq(taskName), any());
    File checkpointPath = Paths.get(StorageManagerUtil.getCheckpointDirPath(durableStoreDir, newCheckpointId)).toFile();
    verify(storageManagerUtil).writeCheckpointV2File(eq(checkpointPath), eq(checkpoint));
  }

  @Test
  public void testWriteChangelogOffsetFilesV1() throws IOException {
    Map<String, Map<SystemStreamPartition, String>> mockFileSystem = new HashMap<>();
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isDurableStore()).thenReturn(true);
    Path mockPath = mock(Path.class);
    when(mockLPStore.checkpoint(any())).thenReturn(Optional.of(mockPath));

    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of("loggedPersistentStore", mockLPStore);

    Partition changelogPartition = new Partition(0);
    SystemStream changelogSystemStream = new SystemStream("changelogSystem", "changelogStream");
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);
    java.util.Map<String, SystemStream> storeChangelogsStreams = ImmutableMap.of("loggedPersistentStore", changelogSystemStream);

    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
    File tmpTestPath = new File("store-checkpoint-test");
    when(storageManagerUtil.getTaskStoreDir(eq(tmpTestPath), eq("loggedPersistentStore"), any(), any())).thenReturn(tmpTestPath);
    TaskName taskName = new TaskName("task");

    when(containerStorageManager.getAllStores(taskName)).thenReturn(taskStores);
    TaskStorageCommitManager commitManager = spy(new TaskStorageCommitManager(taskName,
        Collections.emptyMap(), containerStorageManager, storeChangelogsStreams, changelogPartition,
        null, null, ForkJoinPool.commonPool(), storageManagerUtil, tmpTestPath));

    doAnswer(invocation -> {
      String fileDir = invocation.getArgumentAt(3, File.class).getName();
      SystemStreamPartition ssp = invocation.getArgumentAt(1, SystemStreamPartition.class);
      String offset = invocation.getArgumentAt(2, String.class);
      if (mockFileSystem.containsKey(fileDir)) {
        mockFileSystem.get(fileDir).put(ssp, offset);
      } else {
        Map<SystemStreamPartition, String> sspOffsets = new HashMap<>();
        sspOffsets.put(ssp, offset);
        mockFileSystem.put(fileDir, sspOffsets);
      }
      return null;
    }).when(commitManager).writeChangelogOffsetFile(any(), any(), any(), any());

    CheckpointId newCheckpointId = CheckpointId.create();

    String newestOffset = "1";
    KafkaChangelogSSPOffset kafkaChangelogSSPOffset = new KafkaChangelogSSPOffset(newCheckpointId, newestOffset);
    java.util.Map<SystemStreamPartition, String> offsetsJava = ImmutableMap.of(
        changelogSSP, kafkaChangelogSSPOffset.toString()
    );

    commitManager.init();
    // invoke persist to file system for v2 checkpoint
    commitManager.writeCheckpointToStoreDirectories(new CheckpointV1(offsetsJava));

    assertEquals(2, mockFileSystem.size());
    // check if v2 offsets are written correctly
    String v2FilePath = StorageManagerUtil
        .getCheckpointDirPath(tmpTestPath, newCheckpointId);
    assertTrue(mockFileSystem.containsKey(v2FilePath));
    assertTrue(mockFileSystem.get(v2FilePath).containsKey(changelogSSP));
    assertEquals(1, mockFileSystem.get(v2FilePath).size());
    assertEquals(newestOffset, mockFileSystem.get(v2FilePath).get(changelogSSP));
    // check if v1 offsets are written correctly
    String v1FilePath = tmpTestPath.getPath();
    assertTrue(mockFileSystem.containsKey(v1FilePath));
    assertTrue(mockFileSystem.get(v1FilePath).containsKey(changelogSSP));
    assertEquals(1, mockFileSystem.get(v1FilePath).size());
    assertEquals(newestOffset, mockFileSystem.get(v1FilePath).get(changelogSSP));
  }

  @Test
  public void testWriteChangelogOffsetFilesV2andV1() throws IOException {
    Map<String, Map<SystemStreamPartition, String>> mockFileSystem = new HashMap<>();
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    Map<String, CheckpointV2> mockCheckpointFileSystem = new HashMap<>();
    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isDurableStore()).thenReturn(true);
    Path mockPath = mock(Path.class);
    when(mockLPStore.checkpoint(any())).thenReturn(Optional.of(mockPath));

    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of("loggedPersistentStore", mockLPStore);

    Partition changelogPartition = new Partition(0);
    SystemStream changelogSystemStream = new SystemStream("changelogSystem", "changelogStream");
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);
    java.util.Map<String, SystemStream> storeChangelogsStreams = ImmutableMap.of("loggedPersistentStore", changelogSystemStream);

    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
    File tmpTestPath = new File("store-checkpoint-test");
    when(storageManagerUtil.getTaskStoreDir(eq(tmpTestPath), eq("loggedPersistentStore"), any(), any())).thenReturn(tmpTestPath);
    TaskName taskName = new TaskName("task");

    when(containerStorageManager.getAllStores(taskName)).thenReturn(taskStores);
    TaskStorageCommitManager commitManager = spy(new TaskStorageCommitManager(taskName,
        Collections.emptyMap(), containerStorageManager, storeChangelogsStreams, changelogPartition,
        null, null, ForkJoinPool.commonPool(), storageManagerUtil, tmpTestPath));

    doAnswer(invocation -> {
      String fileDir = invocation.getArgumentAt(3, File.class).getName();
      SystemStreamPartition ssp = invocation.getArgumentAt(1, SystemStreamPartition.class);
      String offset = invocation.getArgumentAt(2, String.class);
      if (mockFileSystem.containsKey(fileDir)) {
        mockFileSystem.get(fileDir).put(ssp, offset);
      } else {
        Map<SystemStreamPartition, String> sspOffsets = new HashMap<>();
        sspOffsets.put(ssp, offset);
        mockFileSystem.put(fileDir, sspOffsets);
      }
      return null;
    }).when(commitManager).writeChangelogOffsetFile(any(), any(), any(), any());

    doAnswer(invocation -> {
      String storeDir = invocation.getArgumentAt(0, File.class).getName();
      CheckpointV2 checkpointV2 = invocation.getArgumentAt(1, CheckpointV2.class);
      mockCheckpointFileSystem.put(storeDir, checkpointV2);
      return null;
    }).when(storageManagerUtil).writeCheckpointV2File(any(), any());

    CheckpointId newCheckpointId = CheckpointId.create();

    String newestOffset = "1";
    KafkaChangelogSSPOffset kafkaChangelogSSPOffset = new KafkaChangelogSSPOffset(newCheckpointId, newestOffset);
    java.util.Map<SystemStreamPartition, String> offsetsJava = ImmutableMap.of(
        changelogSSP, kafkaChangelogSSPOffset.toString()
    );

    commitManager.init();
    // invoke persist to file system for v1 checkpoint
    commitManager.writeCheckpointToStoreDirectories(new CheckpointV1(offsetsJava));

    assertEquals(2, mockFileSystem.size());
    // check if v2 offsets are written correctly
    String v2FilePath = StorageManagerUtil
        .getCheckpointDirPath(tmpTestPath, newCheckpointId);
    assertTrue(mockFileSystem.containsKey(v2FilePath));
    assertTrue(mockFileSystem.get(v2FilePath).containsKey(changelogSSP));
    assertEquals(1, mockFileSystem.get(v2FilePath).size());
    assertEquals(newestOffset, mockFileSystem.get(v2FilePath).get(changelogSSP));
    // check if v1 offsets are written correctly
    String v1FilePath = tmpTestPath.getPath();
    assertTrue(mockFileSystem.containsKey(v1FilePath));
    assertTrue(mockFileSystem.get(v1FilePath).containsKey(changelogSSP));
    assertEquals(1, mockFileSystem.get(v1FilePath).size());
    assertEquals(newestOffset, mockFileSystem.get(v1FilePath).get(changelogSSP));

    java.util.Map<String, String> storeSCM = ImmutableMap.of(
        "loggedPersistentStore", "system;loggedPersistentStoreStream;1",
        "persistentStore", "system;persistentStoreStream;1",
        "loggedInMemStore", "system;loggedInMemStoreStream;1",
        "inMemStore", "system;inMemStoreStream;1"
    );
    CheckpointV2 checkpoint = new CheckpointV2(newCheckpointId, Collections.emptyMap(), Collections.singletonMap("factory", storeSCM));

    // invoke persist to file system with checkpoint v2
    commitManager.writeCheckpointToStoreDirectories(checkpoint);

    assertTrue(mockCheckpointFileSystem.containsKey(v2FilePath));
    assertEquals(checkpoint, mockCheckpointFileSystem.get(v2FilePath));
    assertTrue(mockCheckpointFileSystem.containsKey(v1FilePath));
    assertEquals(checkpoint, mockCheckpointFileSystem.get(v1FilePath));
    assertEquals(2, mockCheckpointFileSystem.size());
  }

  @Test
  public void testWriteChangelogOffsetFilesWithEmptyChangelogTopic() throws IOException {
    Map<String, Map<SystemStreamPartition, String>> mockFileSystem = new HashMap<>();
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isDurableStore()).thenReturn(true);
    Path mockPath = mock(Path.class);
    when(mockLPStore.checkpoint(any())).thenReturn(Optional.of(mockPath));

    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of("loggedPersistentStore", mockLPStore);

    Partition changelogPartition = new Partition(0);
    SystemStream changelogSystemStream = new SystemStream("changelogSystem", "changelogStream");
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);
    java.util.Map<String, SystemStream> storeChangelogsStreams = ImmutableMap.of("loggedPersistentStore", changelogSystemStream);

    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
    File tmpTestPath = new File("store-checkpoint-test");
    when(storageManagerUtil.getTaskStoreDir(eq(tmpTestPath), any(), any(), any())).thenReturn(tmpTestPath);
    TaskName taskName = new TaskName("task");

    when(containerStorageManager.getAllStores(taskName)).thenReturn(taskStores);
    TaskStorageCommitManager commitManager = spy(new TaskStorageCommitManager(taskName,
        Collections.emptyMap(), containerStorageManager, storeChangelogsStreams, changelogPartition,
        null, null, ForkJoinPool.commonPool(), storageManagerUtil, tmpTestPath));

    doAnswer(invocation -> {
      String storeName = invocation.getArgumentAt(0, String.class);
      String fileDir = invocation.getArgumentAt(3, File.class).getName();
      String mockKey = storeName + fileDir;
      SystemStreamPartition ssp = invocation.getArgumentAt(1, SystemStreamPartition.class);
      String offset = invocation.getArgumentAt(2, String.class);
      if (mockFileSystem.containsKey(mockKey)) {
        mockFileSystem.get(mockKey).put(ssp, offset);
      } else {
        Map<SystemStreamPartition, String> sspOffsets = new HashMap<>();
        sspOffsets.put(ssp, offset);
        mockFileSystem.put(mockKey, sspOffsets);
      }
      return null;
    }).when(commitManager).writeChangelogOffsetFile(any(), any(), any(), any());

    CheckpointId newCheckpointId = CheckpointId.create();

    String newestOffset = null;
    KafkaChangelogSSPOffset kafkaChangelogSSPOffset = new KafkaChangelogSSPOffset(newCheckpointId, newestOffset);
    java.util.Map<SystemStreamPartition, String> offsetsJava = ImmutableMap.of(
        changelogSSP, kafkaChangelogSSPOffset.toString()
    );

    commitManager.init();
    // invoke persist to file system for v2 checkpoint
    commitManager.writeCheckpointToStoreDirectories(new CheckpointV1(offsetsJava));
    assertTrue(mockFileSystem.isEmpty());
    // verify that delete was called on current store dir offset file
    verify(storageManagerUtil, times(1)).deleteOffsetFile(eq(tmpTestPath));
  }

  @Test(expected = SamzaException.class)
  public void testThrowOnWriteCheckpointDirIfUnsuccessful() {
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isDurableStore()).thenReturn(true);
    Path mockPath = mock(Path.class);
    when(mockLPStore.checkpoint(any())).thenReturn(Optional.of(mockPath));

    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of("loggedPersistentStore", mockLPStore);

    java.util.Map<String, SystemStream> storeChangelogsStreams = ImmutableMap.of("loggedPersistentStore", mock(SystemStream.class));

    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
    File tmpTestPath = new File("store-checkpoint-test");
    when(storageManagerUtil.getTaskStoreDir(eq(tmpTestPath), eq("loggedPersistentStore"), any(), any())).thenReturn(tmpTestPath);

    TaskName taskName = new TaskName("task");

    TaskStorageCommitManager commitManager = spy(new TaskStorageCommitManager(taskName,
        Collections.emptyMap(), containerStorageManager, storeChangelogsStreams, mock(Partition.class),
        null, null, ForkJoinPool.commonPool(), storageManagerUtil, tmpTestPath));

    java.util.Map<String, String> storeSCM = ImmutableMap.of(
        "loggedPersistentStore", "system;loggedPersistentStoreStream;1",
        "persistentStore", "system;persistentStoreStream;1",
        "loggedInMemStore", "system;loggedInMemStoreStream;1",
        "inMemStore", "system;inMemStoreStream;1"
    );
    when(containerStorageManager.getAllStores(taskName)).thenReturn(taskStores);
    CheckpointV2 checkpoint = new CheckpointV2(CheckpointId.create(), Collections.emptyMap(), Collections.singletonMap("factory", storeSCM));
    doThrow(IOException.class).when(storageManagerUtil).writeCheckpointV2File(eq(tmpTestPath), eq(checkpoint));

    commitManager.init();
    // Should throw samza exception since writeCheckpointV2 failed
    commitManager.writeCheckpointToStoreDirectories(checkpoint);
  }

  @Test
  public void testRemoveOldCheckpointsWhenBaseDirContainsRegularFiles() {
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);
    File durableStoreDir = mock(File.class);

    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);

    TaskName taskName = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );

    when(containerStorageManager.getAllStores(taskName)).thenReturn(Collections.emptyMap());
    TaskStorageCommitManager cm = new TaskStorageCommitManager(taskName, backupManagers, containerStorageManager,
        Collections.emptyMap(), new Partition(1), checkpointManager, new MapConfig(),
        ForkJoinPool.commonPool(), storageManagerUtil, durableStoreDir);


    File mockStoreDir = mock(File.class);
    String mockStoreDirName = "notDirectory";
    when(durableStoreDir.listFiles()).thenReturn(new File[] {mockStoreDir});
    when(mockStoreDir.getName()).thenReturn(mockStoreDirName);
    when(storageManagerUtil.getTaskStoreDir(eq(durableStoreDir), eq(mockStoreDirName), eq(taskName), eq(TaskMode.Active))).thenReturn(mockStoreDir);
    // null here can happen if listFiles is called on a non-directory
    when(mockStoreDir.listFiles(any(FileFilter.class))).thenReturn(null);

    cm.cleanUp(CheckpointId.create(), new HashMap<>()).join();
    verify(durableStoreDir).listFiles();
    verify(mockStoreDir).listFiles(any(FileFilter.class));
    verify(storageManagerUtil).getTaskStoreDir(eq(durableStoreDir), eq(mockStoreDirName), eq(taskName), eq(TaskMode.Active));
  }
}
