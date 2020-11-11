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
import com.google.common.collect.ImmutableSet;
import java.io.FileFilter;
import scala.Option;
import scala.collection.immutable.Map;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Optional;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.ScalaJavaUtil;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTransactionalStateTaskStorageBackupManager {
  @Test
  public void testFlushOrder() {
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    StorageEngine mockStore = mock(StorageEngine.class);
    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of("mockStore", mockStore);
    when(csm.getAllStores(any())).thenReturn(taskStores);

    KafkaTransactionalStateTaskStorageBackupManager tsm = spy(buildTSM(csm, mock(Partition.class), new StorageManagerUtil()));
    // stub actual method call
    doReturn(mock(Map.class)).when(tsm).getNewestChangelogSSPOffsets(any(), any(), any(), any());

    // invoke Kafka flush
    tsm.snapshot();

    // ensure that stores are flushed before we get newest changelog offsets
    InOrder inOrder = inOrder(mockStore, tsm);
    inOrder.verify(mockStore).flush();
    inOrder.verify(tsm).getNewestChangelogSSPOffsets(any(), any(), any(), any());
  }

  @Test
  public void testGetNewestOffsetsReturnsCorrectOffset() {
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    KafkaTransactionalStateTaskStorageBackupManager tsm = buildTSM(csm, mock(Partition.class), new StorageManagerUtil());

    TaskName taskName = mock(TaskName.class);
    String changelogSystemName = "systemName";
    String storeName = "storeName";
    String changelogStreamName = "changelogName";
    String newestChangelogSSPOffset = "1";
    SystemStream changelogSystemStream = new SystemStream(changelogSystemName, changelogStreamName);
    Partition changelogPartition = new Partition(0);
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);

    Map<String, SystemStream> storeChangelogs =
        ScalaJavaUtil.toScalaMap(ImmutableMap.of(storeName, changelogSystemStream));

    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    SystemAdmin systemAdmin = mock(SystemAdmin.class);
    SystemStreamPartitionMetadata metadata = mock(SystemStreamPartitionMetadata.class);

    when(metadata.getNewestOffset()).thenReturn(newestChangelogSSPOffset);
    when(systemAdmins.getSystemAdmin(changelogSystemName)).thenReturn(systemAdmin);
    when(systemAdmin.getSSPMetadata(eq(ImmutableSet.of(changelogSSP)))).thenReturn(ImmutableMap.of(changelogSSP, metadata));

    // invoke the method
    Map<SystemStreamPartition, Option<String>> offsets =
        tsm.getNewestChangelogSSPOffsets(
            taskName, storeChangelogs, changelogPartition, systemAdmins);

    // verify results
    assertEquals(1, offsets.size());
    assertEquals(Option.apply(newestChangelogSSPOffset), offsets.apply(changelogSSP));
  }

  @Test
  public void testGetNewestOffsetsReturnsNoneForEmptyTopic() {
    // empty topic == null newest offset
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    KafkaTransactionalStateTaskStorageBackupManager tsm = buildTSM(csm, mock(Partition.class), new StorageManagerUtil());

    TaskName taskName = mock(TaskName.class);
    String changelogSystemName = "systemName";
    String storeName = "storeName";
    String changelogStreamName = "changelogName";
    String newestChangelogSSPOffset = null;
    SystemStream changelogSystemStream = new SystemStream(changelogSystemName, changelogStreamName);
    Partition changelogPartition = new Partition(0);
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);

    Map<String, SystemStream> storeChangelogs =
        ScalaJavaUtil.toScalaMap(ImmutableMap.of(storeName, changelogSystemStream));

    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    SystemAdmin systemAdmin = mock(SystemAdmin.class);
    SystemStreamPartitionMetadata metadata = mock(SystemStreamPartitionMetadata.class);

    when(metadata.getNewestOffset()).thenReturn(newestChangelogSSPOffset);
    when(systemAdmins.getSystemAdmin(changelogSystemName)).thenReturn(systemAdmin);
    when(systemAdmin.getSSPMetadata(eq(ImmutableSet.of(changelogSSP)))).thenReturn(ImmutableMap.of(changelogSSP, metadata));

    // invoke the method
    Map<SystemStreamPartition, Option<String>> offsets =
        tsm.getNewestChangelogSSPOffsets(
            taskName, storeChangelogs, changelogPartition, systemAdmins);

    // verify results
    assertEquals(1, offsets.size());
    assertEquals(Option.empty(), offsets.apply(changelogSSP));
  }

  @Test(expected = SamzaException.class)
  public void testGetNewestOffsetsThrowsIfNullMetadata() {
    // empty topic == null newest offset
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    KafkaTransactionalStateTaskStorageBackupManager tsm = buildTSM(csm, mock(Partition.class), new StorageManagerUtil());

    TaskName taskName = mock(TaskName.class);
    String changelogSystemName = "systemName";
    String storeName = "storeName";
    String changelogStreamName = "changelogName";
    String newestChangelogSSPOffset = null;
    SystemStream changelogSystemStream = new SystemStream(changelogSystemName, changelogStreamName);
    Partition changelogPartition = new Partition(0);
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);

    Map<String, SystemStream> storeChangelogs =
        ScalaJavaUtil.toScalaMap(ImmutableMap.of(storeName, changelogSystemStream));

    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    SystemAdmin systemAdmin = mock(SystemAdmin.class);
    SystemStreamPartitionMetadata metadata = mock(SystemStreamPartitionMetadata.class);

    when(metadata.getNewestOffset()).thenReturn(newestChangelogSSPOffset);
    when(systemAdmins.getSystemAdmin(changelogSystemName)).thenReturn(systemAdmin);
    when(systemAdmin.getSSPMetadata(eq(ImmutableSet.of(changelogSSP)))).thenReturn(null);

    // invoke the method
    Map<SystemStreamPartition, Option<String>> offsets =
        tsm.getNewestChangelogSSPOffsets(
            taskName, storeChangelogs, changelogPartition, systemAdmins);

    // verify results
    fail("Should have thrown an exception if admin didn't return any metadata");
  }

  @Test(expected = SamzaException.class)
  public void testGetNewestOffsetsThrowsIfNullSSPMetadata() {
    // empty topic == null newest offset
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    KafkaTransactionalStateTaskStorageBackupManager tsm = buildTSM(csm, mock(Partition.class), new StorageManagerUtil());

    TaskName taskName = mock(TaskName.class);
    String changelogSystemName = "systemName";
    String storeName = "storeName";
    String changelogStreamName = "changelogName";
    String newestChangelogSSPOffset = null;
    SystemStream changelogSystemStream = new SystemStream(changelogSystemName, changelogStreamName);
    Partition changelogPartition = new Partition(0);
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);

    Map<String, SystemStream> storeChangelogs =
        ScalaJavaUtil.toScalaMap(ImmutableMap.of(storeName, changelogSystemStream));

    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    SystemAdmin systemAdmin = mock(SystemAdmin.class);
    SystemStreamPartitionMetadata metadata = mock(SystemStreamPartitionMetadata.class);

    when(metadata.getNewestOffset()).thenReturn(newestChangelogSSPOffset);
    when(systemAdmins.getSystemAdmin(changelogSystemName)).thenReturn(systemAdmin);
    java.util.Map metadataMap = new HashMap() { {
        put(changelogSSP, null);
      } };
    when(systemAdmin.getSSPMetadata(eq(ImmutableSet.of(changelogSSP)))).thenReturn(metadataMap);

    // invoke the method
    Map<SystemStreamPartition, Option<String>> offsets =
        tsm.getNewestChangelogSSPOffsets(
            taskName, storeChangelogs, changelogPartition, systemAdmins);

    // verify results
    fail("Should have thrown an exception if admin returned null metadata for changelog SSP");
  }

  @Test(expected = SamzaException.class)
  public void testGetNewestOffsetsThrowsIfErrorGettingMetadata() {
    // empty topic == null newest offset
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    KafkaTransactionalStateTaskStorageBackupManager tsm = buildTSM(csm, mock(Partition.class), new StorageManagerUtil());

    TaskName taskName = mock(TaskName.class);
    String changelogSystemName = "systemName";
    String storeName = "storeName";
    String changelogStreamName = "changelogName";
    String newestChangelogSSPOffset = null;
    SystemStream changelogSystemStream = new SystemStream(changelogSystemName, changelogStreamName);
    Partition changelogPartition = new Partition(0);
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);

    Map<String, SystemStream> storeChangelogs =
        ScalaJavaUtil.toScalaMap(ImmutableMap.of(storeName, changelogSystemStream));

    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    SystemAdmin systemAdmin = mock(SystemAdmin.class);
    SystemStreamPartitionMetadata metadata = mock(SystemStreamPartitionMetadata.class);

    when(metadata.getNewestOffset()).thenReturn(newestChangelogSSPOffset);
    when(systemAdmins.getSystemAdmin(changelogSystemName)).thenThrow(new SamzaException("Error getting metadata"));
    when(systemAdmin.getSSPMetadata(eq(ImmutableSet.of(changelogSSP)))).thenReturn(null);

    // invoke the method
    Map<SystemStreamPartition, Option<String>> offsets =
        tsm.getNewestChangelogSSPOffsets(
            taskName, storeChangelogs, changelogPartition, systemAdmins);

    // verify results
    fail("Should have thrown an exception if admin had an error getting metadata");
  }

  @Test
  public void testCheckpoint() {
    ContainerStorageManager csm = mock(ContainerStorageManager.class);

    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isLoggedStore()).thenReturn(true);
    Path mockPath = mock(Path.class);
    when(mockLPStore.checkpoint(any())).thenReturn(Optional.of(mockPath));

    StorageEngine mockPStore = mock(StorageEngine.class);
    StoreProperties pStoreProps = mock(StoreProperties.class);
    when(mockPStore.getStoreProperties()).thenReturn(pStoreProps);
    when(pStoreProps.isPersistedToDisk()).thenReturn(true);
    when(pStoreProps.isLoggedStore()).thenReturn(false);

    StorageEngine mockLIStore = mock(StorageEngine.class);
    StoreProperties liStoreProps = mock(StoreProperties.class);
    when(mockLIStore.getStoreProperties()).thenReturn(liStoreProps);
    when(liStoreProps.isPersistedToDisk()).thenReturn(false);
    when(liStoreProps.isLoggedStore()).thenReturn(true);

    StorageEngine mockIStore = mock(StorageEngine.class);
    StoreProperties iStoreProps = mock(StoreProperties.class);
    when(mockIStore.getStoreProperties()).thenReturn(iStoreProps);
    when(iStoreProps.isPersistedToDisk()).thenReturn(false);
    when(iStoreProps.isLoggedStore()).thenReturn(false);

    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of(
        "loggedPersistentStore", mockLPStore,
        "persistentStore", mockPStore,
        "loggedInMemStore", mockLIStore,
        "inMemStore", mockIStore
    );
    when(csm.getAllStores(any())).thenReturn(taskStores);

    KafkaTransactionalStateTaskStorageBackupManager tsm = spy(buildTSM(csm, mock(Partition.class), new StorageManagerUtil()));
    // stub actual method call
    ArgumentCaptor<Map> checkpointPathsCaptor = ArgumentCaptor.forClass(Map.class);
    doNothing().when(tsm).writeChangelogOffsetFiles(any(), any(), any());

    Map<SystemStreamPartition, Option<String>> offsets = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(mock(SystemStreamPartition.class), Option.apply("1")));

    // invoke checkpoint
    tsm.checkpoint(CheckpointId.create(), offsets);

    // ensure that checkpoint is never called for non-logged persistent stores since they're
    // always cleared on restart.
    verify(mockPStore, never()).checkpoint(any());
    // ensure that checkpoint is never called for in-memory stores since they're not persistent.
    verify(mockIStore, never()).checkpoint(any());
    verify(mockLIStore, never()).checkpoint(any());
    verify(tsm).writeChangelogOffsetFiles(checkpointPathsCaptor.capture(), any(), eq(offsets));
    Map<String, Path> checkpointPaths = checkpointPathsCaptor.getValue();
    assertEquals(1, checkpointPaths.size());
    assertEquals(mockPath, checkpointPaths.apply("loggedPersistentStore"));
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckpointFailsIfErrorCreatingCheckpoint() {
    ContainerStorageManager csm = mock(ContainerStorageManager.class);

    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isLoggedStore()).thenReturn(true);
    when(mockLPStore.checkpoint(any())).thenThrow(new IllegalStateException());
    java.util.Map<String, StorageEngine> taskStores =
        ImmutableMap.of("loggedPersistentStore", mockLPStore);
    when(csm.getAllStores(any())).thenReturn(taskStores);

    KafkaTransactionalStateTaskStorageBackupManager tsm = spy(buildTSM(csm, mock(Partition.class), new StorageManagerUtil()));

    Map<SystemStreamPartition, Option<String>> offsets = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(mock(SystemStreamPartition.class), Option.apply("1")));

    // invoke checkpoint
    tsm.checkpoint(CheckpointId.create(), offsets);
    verify(tsm, never()).writeChangelogOffsetFiles(any(), any(), any());
    fail("Should have thrown an exception if error creating store checkpoint");
  }

  @Test(expected = SamzaException.class)
  public void testCheckpointFailsIfErrorWritingOffsetFiles() {
    ContainerStorageManager csm = mock(ContainerStorageManager.class);

    StorageEngine mockLPStore = mock(StorageEngine.class);
    StoreProperties lpStoreProps = mock(StoreProperties.class);
    when(mockLPStore.getStoreProperties()).thenReturn(lpStoreProps);
    when(lpStoreProps.isPersistedToDisk()).thenReturn(true);
    when(lpStoreProps.isLoggedStore()).thenReturn(true);
    Path mockPath = mock(Path.class);
    when(mockLPStore.checkpoint(any())).thenReturn(Optional.of(mockPath));
    java.util.Map<String, StorageEngine> taskStores =
        ImmutableMap.of("loggedPersistentStore", mockLPStore);
    when(csm.getAllStores(any())).thenReturn(taskStores);

    KafkaTransactionalStateTaskStorageBackupManager tsm = spy(buildTSM(csm, mock(Partition.class), new StorageManagerUtil()));
    doThrow(new SamzaException("Error writing offset file"))
        .when(tsm).writeChangelogOffsetFiles(any(), any(), any());

    Map<SystemStreamPartition, Option<String>> offsets = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(mock(SystemStreamPartition.class), Option.apply("1")));

    // invoke checkpoint
    tsm.checkpoint(CheckpointId.create(), offsets);

    fail("Should have thrown an exception if error writing offset file.");
  }

  @Test
  public void testWriteChangelogOffsetFiles() throws IOException {
    String storeName = "mockStore";
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    StorageEngine mockStore = mock(StorageEngine.class);
    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of(storeName, mockStore);
    when(csm.getAllStores(any())).thenReturn(taskStores);

    Partition changelogPartition = new Partition(0);
    SystemStream changelogSS = new SystemStream("system", "changelog");
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSS, changelogPartition);
    StorageManagerUtil smu = spy(new StorageManagerUtil());
    File mockCurrentStoreDir = mock(File.class);
    doReturn(mockCurrentStoreDir).when(smu).getTaskStoreDir(any(), eq(storeName), any(), any());
    doNothing().when(smu).writeOffsetFile(eq(mockCurrentStoreDir), any(), anyBoolean());
    KafkaTransactionalStateTaskStorageBackupManager tsm = spy(buildTSM(csm, changelogPartition, smu));

    String changelogNewestOffset = "1";
    Map<SystemStreamPartition, Option<String>> offsets = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(changelogSSP, Option.apply(changelogNewestOffset)));

    Path checkpointPath = Files.createTempDirectory("store-checkpoint-test").toAbsolutePath();

    Map<String, Path> checkpointPaths = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(storeName, checkpointPath));
    Map<String, SystemStream> storeChangelogs = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(storeName, changelogSS));

    // invoke method
    tsm.writeChangelogOffsetFiles(checkpointPaths, storeChangelogs, offsets);

    // verify that offset file was written to the checkpoint dir
    java.util.Map<SystemStreamPartition, String> fileOffsets = new StorageManagerUtil()
        .readOffsetFile(checkpointPath.toFile(), ImmutableSet.of(changelogSSP), false);
    assertEquals(1, fileOffsets.size());
    assertEquals(changelogNewestOffset, fileOffsets.get(changelogSSP));

    // verify that offset file write was called on the current dir
    verify(smu, times(1)).writeOffsetFile(eq(mockCurrentStoreDir), any(), anyBoolean());
  }

  @Test
  public void testWriteChangelogOffsetFilesWithEmptyChangelogTopic() throws IOException {
    String storeName = "mockStore";
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    StorageEngine mockStore = mock(StorageEngine.class);
    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of(storeName, mockStore);
    when(csm.getAllStores(any())).thenReturn(taskStores);

    Partition changelogPartition = new Partition(0);
    SystemStream changelogSS = new SystemStream("system", "changelog");
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSS, changelogPartition);
    StorageManagerUtil mockSMU = mock(StorageManagerUtil.class);
    File mockCurrentStoreDir = mock(File.class);
    when(mockSMU.getTaskStoreDir(any(), eq(storeName), any(), any())).thenReturn(mockCurrentStoreDir);
    KafkaTransactionalStateTaskStorageBackupManager tsm = spy(buildTSM(csm, changelogPartition, mockSMU));

    String changelogNewestOffset = null;
    Map<SystemStreamPartition, Option<String>> offsets = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(changelogSSP, Option.apply(changelogNewestOffset)));

    Path checkpointPath = Files.createTempDirectory("store-checkpoint-test").toAbsolutePath();

    Map<String, Path> checkpointPaths = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(storeName, checkpointPath));
    Map<String, SystemStream> storeChangelogs = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(storeName, changelogSS));

    // invoke method
    tsm.writeChangelogOffsetFiles(checkpointPaths, storeChangelogs, offsets);

    // verify that the offset files were not written to the checkpoint dir
    assertFalse(Files.exists(new File(checkpointPath.toFile(), StorageManagerUtil.OFFSET_FILE_NAME_LEGACY).toPath()));
    assertFalse(Files.exists(new File(checkpointPath.toFile(), StorageManagerUtil.OFFSET_FILE_NAME_NEW).toPath()));
    java.util.Map<SystemStreamPartition, String> fileOffsets = new StorageManagerUtil()
        .readOffsetFile(checkpointPath.toFile(), ImmutableSet.of(changelogSSP), false);
    assertEquals(0, fileOffsets.size());

    // verify that delete was called on current store dir offset file
    verify(mockSMU, times(1)).deleteOffsetFile(eq(mockCurrentStoreDir));
  }

  /**
   * This should never happen with CheckpointingTaskStorageManager. #getNewestChangelogSSPOffset must
   * return a key for every changelog SSP. If the SSP is empty, the value should be none. If it could
   * not fetch metadata, it should throw an exception instead of skipping the SSP.
   * If this contract is accidentally broken, ensure that we fail the commit
   */
  @Test(expected = SamzaException.class)
  public void testWriteChangelogOffsetFilesWithNoChangelogOffset() throws IOException {
    String storeName = "mockStore";
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    StorageEngine mockStore = mock(StorageEngine.class);
    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of(storeName, mockStore);
    when(csm.getAllStores(any())).thenReturn(taskStores);

    Partition changelogPartition = new Partition(0);
    SystemStream changelogSS = new SystemStream("system", "changelog");
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSS, changelogPartition);
    KafkaTransactionalStateTaskStorageBackupManager tsm = spy(buildTSM(csm, changelogPartition, new StorageManagerUtil()));

    // no mapping present for changelog newest offset
    Map<SystemStreamPartition, Option<String>> offsets = ScalaJavaUtil.toScalaMap(ImmutableMap.of());

    Path checkpointPath = Files.createTempDirectory("store-checkpoint-test").toAbsolutePath();
    Map<String, Path> checkpointPaths = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(storeName, checkpointPath));
    Map<String, SystemStream> storeChangelogs = ScalaJavaUtil.toScalaMap(
        ImmutableMap.of(storeName, changelogSS));

    // invoke method
    tsm.writeChangelogOffsetFiles(checkpointPaths, storeChangelogs, offsets);

    fail("Should have thrown an exception if no changelog offset found for checkpointed store");
  }

  @Test
  public void testRemoveOldCheckpointsWhenBaseDirContainsRegularFiles() {
    TaskName taskName = new TaskName("Partition 0");
    ContainerStorageManager containerStorageManager = mock(ContainerStorageManager.class);
    Map<String, SystemStream> changelogSystemStreams = mock(Map.class);
    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    File loggedStoreBaseDir = mock(File.class);
    Partition changelogPartition = new Partition(0);
    TaskMode taskMode = TaskMode.Active;
    StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);

    File mockStoreDir = mock(File.class);
    String mockStoreDirName = "notDirectory";

    when(loggedStoreBaseDir.listFiles()).thenReturn(new File[] {mockStoreDir});
    when(mockStoreDir.getName()).thenReturn(mockStoreDirName);
    when(storageManagerUtil.getTaskStoreDir(eq(loggedStoreBaseDir), eq(mockStoreDirName), eq(taskName), eq(taskMode))).thenReturn(mockStoreDir);
    // null here can happen if listFiles is called on a non-directory
    when(mockStoreDir.listFiles(any(FileFilter.class))).thenReturn(null);

    KafkaTransactionalStateTaskStorageBackupManager
        tsm = new KafkaTransactionalStateTaskStorageBackupManager(taskName, containerStorageManager,
        changelogSystemStreams, systemAdmins, loggedStoreBaseDir, changelogPartition, taskMode, storageManagerUtil);

    tsm.cleanUp(CheckpointId.create());
  }

  private KafkaTransactionalStateTaskStorageBackupManager buildTSM(ContainerStorageManager csm, Partition changelogPartition,
      StorageManagerUtil smu) {
    TaskName taskName = new TaskName("Partition 0");
    Map<String, SystemStream> changelogSystemStreams = mock(Map.class);
    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    File loggedStoreBaseDir = mock(File.class);
    TaskMode taskMode = TaskMode.Active;

    return new KafkaTransactionalStateTaskStorageBackupManager(
        taskName, csm, changelogSystemStreams, systemAdmins,
        loggedStoreBaseDir, changelogPartition, taskMode, smu);
  }
}