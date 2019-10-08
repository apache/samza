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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.storage.TransactionalStateTaskRestoreManager.StoreActions;
import org.apache.samza.storage.TransactionalStateTaskRestoreManager.RestoreOffsets;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestTransactionalStateTaskRestoreManager {
  @Test
  public void testGetCurrentChangelogOffsets() {
    // test gets metadata for all and only task store changelog SSPs
    // test all changelogs have same partition
    // test does not change returned ssp metadata
    TaskModel mockTaskModel = mock(TaskModel.class);
    when(mockTaskModel.getTaskName()).thenReturn(new TaskName("Partition 0"));
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    String store2Name = "store2";
    String changelog2SystemName = "system2";
    String changelog2StreamName = "store2Changelog";

    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStream changelog2SystemStream = new SystemStream(changelog2SystemName, changelog2StreamName);

    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(
        store1Name, changelog1SystemStream,
        store2Name, changelog2SystemStream);

    SSPMetadataCache mockSSPMetadataCache = mock(SSPMetadataCache.class);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "1", "2");
    when(mockSSPMetadataCache.getMetadata(eq(changelog1SSP))).thenReturn(changelog1SSPMetadata);
    SystemStreamPartition changelog2SSP = new SystemStreamPartition(changelog2SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog2SSPMetadata = new SystemStreamPartitionMetadata("1", "2", "3");
    when(mockSSPMetadataCache.getMetadata(eq(changelog2SSP))).thenReturn(changelog2SSPMetadata);

    Map<SystemStreamPartition, SystemStreamPartitionMetadata> currentChangelogOffsets =
        TransactionalStateTaskRestoreManager.getCurrentChangelogOffsets(
            mockTaskModel, mockStoreChangelogs, mockSSPMetadataCache);

    verify(mockSSPMetadataCache, times(1)).getMetadata(changelog1SSP);
    verify(mockSSPMetadataCache, times(1)).getMetadata(changelog2SSP);
    verifyNoMoreInteractions(mockSSPMetadataCache);

    assertEquals(2, currentChangelogOffsets.size());
    assertEquals(changelog1SSPMetadata, currentChangelogOffsets.get(changelog1SSP));
    assertEquals(changelog2SSPMetadata, currentChangelogOffsets.get(changelog2SSP));
  }

  @Test
  public void testGetStoreActionsForNonLoggedPersistentStore_AlwaysClearStore() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(false);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of();

    Map<SystemStreamPartition, String> mockCheckpointedChangelogOffset = ImmutableMap.of();
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets = ImmutableMap.of();

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    Config mockConfig = mock(Config.class);
    Clock mockClock = mock(Clock.class);

    File mockCurrentStoreDir = mock(File.class);
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockNonLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(mockCurrentStoreDir);

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    assertEquals(1, storeActions.storeDirsToDelete.get(store1Name).size());
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockCurrentStoreDir));
    // ensure that there is nothing to retain or restore.
    assertEquals(0, storeActions.storeDirsToRetain.size());
    assertEquals(0, storeActions.storesToRestore.size());
  }

  @Test
  public void testGetStoreActionsForLoggedNonPersistentStore_RestoreToCheckpointedOffset() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(false); // non-persistent (in memory) store
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = "5";
    ImmutableMap<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        ImmutableMap.of(changelog1SSP, changelog1CheckpointedOffset);
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    Config mockConfig = mock(Config.class);
    Clock mockClock = mock(Clock.class);

    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that there is nothing to delete or retain
    assertEquals(0, storeActions.storeDirsToDelete.size());
    // ensure that we restore from the oldest changelog offset to checkpointed changelog offset
    assertEquals("0", storeActions.storesToRestore.get(store1Name).startingOffset);
    assertEquals(changelog1CheckpointedOffset, storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedNonPersistentStore_FullRestoreIfCheckpointedOffsetNewerThanNewest() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(false); // non-persistent store
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    // checkpointed changelog offset > newest offset (e.g. changelog topic got changed)
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = "21";
    Map<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        new HashMap<SystemStreamPartition, String>() { {
          put(changelog1SSP, changelog1CheckpointedOffset);
        } };
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    Config mockConfig = mock(Config.class);
    Clock mockClock = mock(Clock.class);


    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that there is nothing to retain or delete
    assertEquals(0, storeActions.storeDirsToDelete.size());
    assertEquals(0, storeActions.storeDirsToRetain.size());
    // ensure that we mark the store for full restore (from current oldest to current newest)
    assertEquals("0", storeActions.storesToRestore.get(store1Name).startingOffset);
    assertEquals("10", storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedNonPersistentStore_FullRestoreIfCheckpointedOffsetOlderThanOldest() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(false); // non-persistent store
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    // checkpointed changelog offset > newest offset (e.g. changelog topic got changed)
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("10", "20", "21");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = "5";
    Map<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        new HashMap<SystemStreamPartition, String>() { {
          put(changelog1SSP, changelog1CheckpointedOffset);
        } };
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    Config mockConfig = mock(Config.class);
    Clock mockClock = mock(Clock.class);


    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that there is nothing to retain or delete
    assertEquals(0, storeActions.storeDirsToDelete.size());
    assertEquals(0, storeActions.storeDirsToRetain.size());
    // ensure that we mark the store for full restore (from current oldest to current newest)
    assertEquals("10", storeActions.storesToRestore.get(store1Name).startingOffset);
    assertEquals("20", storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedNonPersistentStore_FullTrimIfNullCheckpointedOffsetAndNotRetainExistingChangelogState() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(false); // non-persistent store
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = null;
    Map<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        new HashMap<SystemStreamPartition, String>() { {
          put(changelog1SSP, changelog1CheckpointedOffset);
        } };
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    HashMap<String, String> configMap = new HashMap<>();
    configMap.put(TaskConfig.TRANSACTIONAL_STATE_RETAIN_EXISTING_CHANGELOG_STATE, "false");
    Config mockConfig = new MapConfig(configMap);
    Clock mockClock = mock(Clock.class);

    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that there is nothing to delete or retain
    assertEquals(0, storeActions.storeDirsToDelete.size());
    assertEquals(0, storeActions.storeDirsToRetain.size());
    // ensure that we mark the store for restore (full trim == restore from oldest to null)
    assertEquals("0", storeActions.storesToRestore.get(store1Name).startingOffset);
    assertNull(storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedPersistentStore_RestoreToCheckpointedOffsetIfNoStoreCheckpoints() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = "5";
    ImmutableMap<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        ImmutableMap.of(changelog1SSP, changelog1CheckpointedOffset);
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    Config mockConfig = mock(Config.class);
    Clock mockClock = mock(Clock.class);

    File mockCurrentStoreDir = mock(File.class);
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(mockCurrentStoreDir);
    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    assertEquals(1, storeActions.storeDirsToDelete.get(store1Name).size());
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockCurrentStoreDir));
    // ensure that we restore from the oldest changelog offset to checkpointed changelog offset
    assertEquals("0", storeActions.storesToRestore.get(store1Name).startingOffset);
    assertEquals(changelog1CheckpointedOffset, storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedPersistentStore_RestoreToCheckpointedOffsetIfInvalidStoreCheckpoints() {
    // in these tests, stale == local offset within range of current oldest and newest, but not equal to checkpointed
    // invalid == store offset is corrupted / store is stale (older than delete retention) etc.
    // hence in these tests a store checkpoint can be not-stale yet invalid
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = "5";
    ImmutableMap<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        ImmutableMap.of(changelog1SSP, changelog1CheckpointedOffset);
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    Config mockConfig = mock(Config.class);
    Clock mockClock = mock(Clock.class);

    File mockCurrentStoreDir = mock(File.class);
    File mockStoreNewerCheckpointDir = mock(File.class);
    String newerCheckpointDirLocalOffset = "5"; // not stale
    File mockStoreOlderCheckpointDir = mock(File.class);
    String olderCheckpointDirLocalOffset = "3";
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(mockCurrentStoreDir);
    when(mockStorageManagerUtil.getTaskStoreCheckpointDirs(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(ImmutableList.of(mockStoreNewerCheckpointDir, mockStoreOlderCheckpointDir));
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreNewerCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(false); // invalid store
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreOlderCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(false); // invalid store
    Set<SystemStreamPartition> mockChangelogSSPs = ImmutableSet.of(changelog1SSP);
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreNewerCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, newerCheckpointDirLocalOffset));
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreOlderCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, olderCheckpointDirLocalOffset)); // less than checkpointed offset (5)

    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that all current dir and checkpoint dirs are marked for deletion
    assertEquals(3, storeActions.storeDirsToDelete.get(store1Name).size());
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockCurrentStoreDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreOlderCheckpointDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreNewerCheckpointDir));
    // ensure that no store checkpoint is marked for retention
    assertEquals(0, storeActions.storeDirsToRetain.size());
    // ensure that we mark the store for restore from oldest offset to checkpointed offset
    assertEquals("0", storeActions.storesToRestore.get(store1Name).startingOffset);
    assertEquals(changelog1CheckpointedOffset, storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedPersistentStore_RestoreDeltaIfStaleStoreCheckpoint() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = "5";
    ImmutableMap<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        ImmutableMap.of(changelog1SSP, changelog1CheckpointedOffset);
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    Config mockConfig = mock(Config.class);
    Clock mockClock = mock(Clock.class);

    File mockCurrentStoreDir = mock(File.class);
    File mockStoreNewerCheckpointDir = mock(File.class);
    String newerCheckpointDirLocalOffset = "4";
    File mockStoreOlderCheckpointDir = mock(File.class);
    String olderCheckpointDirLocalOffset = "3";
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(mockCurrentStoreDir);
    when(mockStorageManagerUtil.getTaskStoreCheckpointDirs(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(ImmutableList.of(mockStoreNewerCheckpointDir, mockStoreOlderCheckpointDir));
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreNewerCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreOlderCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    Set<SystemStreamPartition> mockChangelogSSPs = ImmutableSet.of(changelog1SSP);
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreNewerCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, newerCheckpointDirLocalOffset));
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreOlderCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, olderCheckpointDirLocalOffset)); // less than checkpointed offset (5)

    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that both the current dir and older stale checkpoint dir are marked for deletion
    assertEquals(2, storeActions.storeDirsToDelete.get(store1Name).size());
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockCurrentStoreDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreOlderCheckpointDir));
    // ensure that the newer (but still stale) store checkpoint is marked for retention
    assertEquals(mockStoreNewerCheckpointDir, storeActions.storeDirsToRetain.get(store1Name));
    // ensure that we mark the store for restore from newer (but still stale) local offset to checkpointed offset
    assertEquals(newerCheckpointDirLocalOffset, storeActions.storesToRestore.get(store1Name).startingOffset);
    assertEquals(changelog1CheckpointedOffset, storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedPersistentStore_NoRestoreButTrimIfUpToDateStoreCheckpoint() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = "5";
    ImmutableMap<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        ImmutableMap.of(changelog1SSP, changelog1CheckpointedOffset);
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    Config mockConfig = mock(Config.class);
    Clock mockClock = mock(Clock.class);

    File mockCurrentStoreDir = mock(File.class);
    File mockStoreUpToDateCheckpointDir = mock(File.class);
    File mockStoreOlderCheckpointDir = mock(File.class);
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(mockCurrentStoreDir);
    when(mockStorageManagerUtil.getTaskStoreCheckpointDirs(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(ImmutableList.of(mockStoreUpToDateCheckpointDir, mockStoreOlderCheckpointDir));
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreUpToDateCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreOlderCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    Set<SystemStreamPartition> mockChangelogSSPs = ImmutableSet.of(changelog1SSP);
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreUpToDateCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, changelog1CheckpointedOffset));
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreOlderCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, "3")); // less than checkpointed offset (5)

    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that both the current dir and older checkpoint dir are marked for deletion
    assertEquals(2, storeActions.storeDirsToDelete.get(store1Name).size());
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockCurrentStoreDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreOlderCheckpointDir));
    // ensure that the up-to-date store checkpoint is marked for retention
    assertEquals(mockStoreUpToDateCheckpointDir, storeActions.storeDirsToRetain.get(store1Name));
    // ensure that we mark the store for restore even if local offset == checkpointed offset
    // this is required even if there are no messages to restore, since there may be message we need to trim
    assertEquals(changelog1CheckpointedOffset, storeActions.storesToRestore.get(store1Name).startingOffset);
    assertEquals(changelog1CheckpointedOffset, storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedPersistentStore_FullRestoreIfNullCheckpointedOffsetAndRetainExistingChangelogState() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = null;
    Map<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        new HashMap<SystemStreamPartition, String>() { {
          put(changelog1SSP, changelog1CheckpointedOffset);
        } };
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    HashMap<String, String> configMap = new HashMap<>();
    configMap.put(TaskConfig.TRANSACTIONAL_STATE_RETAIN_EXISTING_CHANGELOG_STATE, "true");
    Config mockConfig = new MapConfig(configMap);
    Clock mockClock = mock(Clock.class);

    File mockCurrentStoreDir = mock(File.class);
    File mockStoreNewerCheckpointDir = mock(File.class);
    File mockStoreOlderCheckpointDir = mock(File.class);
    String olderCheckpointDirLocalOffset = "3";
    String newerCheckpointDirLocalOffset = "5";
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(mockCurrentStoreDir);
    when(mockStorageManagerUtil.getTaskStoreCheckpointDirs(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(ImmutableList.of(mockStoreNewerCheckpointDir, mockStoreOlderCheckpointDir));
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreNewerCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreOlderCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    Set<SystemStreamPartition> mockChangelogSSPs = ImmutableSet.of(changelog1SSP);
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreNewerCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, newerCheckpointDirLocalOffset));
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreOlderCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, olderCheckpointDirLocalOffset)); // less than checkpointed offset (5)

    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that all the store dirs (current or checkpoint) are marked for deletion
    assertEquals(3, storeActions.storeDirsToDelete.get(store1Name).size());
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockCurrentStoreDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreOlderCheckpointDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreNewerCheckpointDir));
    // ensure that no directories are retained
    assertEquals(0, storeActions.storeDirsToRetain.size());
    // ensure that we mark the store for full restore (from oldest to newest)
    assertEquals("0", storeActions.storesToRestore.get(store1Name).startingOffset);
    assertEquals("10", storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedPersistentStore_FullTrimIfNullCheckpointedOffsetAndNotRetainExistingState() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = null;
    Map<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        new HashMap<SystemStreamPartition, String>() { {
          put(changelog1SSP, changelog1CheckpointedOffset);
        } };
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    HashMap<String, String> configMap = new HashMap<>();
    configMap.put(TaskConfig.TRANSACTIONAL_STATE_RETAIN_EXISTING_CHANGELOG_STATE, "false");
    Config mockConfig = new MapConfig(configMap);
    Clock mockClock = mock(Clock.class);

    File mockCurrentStoreDir = mock(File.class);
    File mockStoreNewerCheckpointDir = mock(File.class);
    File mockStoreOlderCheckpointDir = mock(File.class);
    String olderCheckpointDirLocalOffset = "3";
    String newerCheckpointDirLocalOffset = "5";
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(mockCurrentStoreDir);
    when(mockStorageManagerUtil.getTaskStoreCheckpointDirs(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(ImmutableList.of(mockStoreNewerCheckpointDir, mockStoreOlderCheckpointDir));
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreNewerCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreOlderCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    Set<SystemStreamPartition> mockChangelogSSPs = ImmutableSet.of(changelog1SSP);
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreNewerCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, newerCheckpointDirLocalOffset));
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreOlderCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, olderCheckpointDirLocalOffset)); // less than checkpointed offset (5)

    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that all the store dirs (current or checkpoint) are marked for deletion
    assertEquals(3, storeActions.storeDirsToDelete.get(store1Name).size());
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockCurrentStoreDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreOlderCheckpointDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreNewerCheckpointDir));
    // ensure that no directories are retained
    assertEquals(0, storeActions.storeDirsToRetain.size());
    // ensure that we mark the store for restore (full trim == restore from oldest to null)
    assertEquals("0", storeActions.storesToRestore.get(store1Name).startingOffset);
    assertNull(storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedPersistentStore_FullRestoreIfNullCheckpointedAndOldestOffset() {
    // full restore == clear existing state
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    // SSPMetadata contract allows for (and recommends) null as the oldest offset for empty streams.
    // KafkaSystemAdmin does not follow this convention and returns 0 instead, but we should test for this
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata(null, null, null);
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = null;
    Map<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        new HashMap<SystemStreamPartition, String>() { {
          put(changelog1SSP, changelog1CheckpointedOffset);
        } };
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    HashMap<String, String> configMap = new HashMap<>();
    configMap.put(TaskConfig.TRANSACTIONAL_STATE_RETAIN_EXISTING_CHANGELOG_STATE, "true"); // should not matter
    Config mockConfig = new MapConfig(configMap);
    Clock mockClock = mock(Clock.class);

    File mockCurrentStoreDir = mock(File.class);
    File mockStoreNewerCheckpointDir = mock(File.class);
    File mockStoreOlderCheckpointDir = mock(File.class);
    String olderCheckpointDirLocalOffset = "3";
    String newerCheckpointDirLocalOffset = "5";
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(mockCurrentStoreDir);
    when(mockStorageManagerUtil.getTaskStoreCheckpointDirs(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(ImmutableList.of(mockStoreNewerCheckpointDir, mockStoreOlderCheckpointDir));
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreNewerCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreOlderCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    Set<SystemStreamPartition> mockChangelogSSPs = ImmutableSet.of(changelog1SSP);
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreNewerCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, newerCheckpointDirLocalOffset));
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreOlderCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, olderCheckpointDirLocalOffset)); // less than checkpointed offset (5)

    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            if (offset1 == null || offset2 == null) {
              return -1;
            }
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that all the store dirs (current or checkpoint) are marked for deletion
    assertEquals(3, storeActions.storeDirsToDelete.get(store1Name).size());
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockCurrentStoreDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreOlderCheckpointDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreNewerCheckpointDir));
    // ensure that no directories are retained
    assertEquals(0, storeActions.storeDirsToRetain.size());
    // ensure that we do a full restore (on the empty topic)
    assertNull(storeActions.storesToRestore.get(store1Name).startingOffset);
    assertNull(storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedPersistentStore_FullRestoreIfCheckpointedOffsetOlderThanOldest() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    // oldest offset > checkpointed changelog offset
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("11", "20", "21");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = "5";
    Map<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        new HashMap<SystemStreamPartition, String>() { {
          put(changelog1SSP, changelog1CheckpointedOffset);
        } };
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    Config mockConfig = mock(Config.class);
    Clock mockClock = mock(Clock.class);

    File mockCurrentStoreDir = mock(File.class);
    File mockStoreNewerCheckpointDir = mock(File.class);
    File mockStoreOlderCheckpointDir = mock(File.class);
    String olderCheckpointDirLocalOffset = "3";
    String newerCheckpointDirLocalOffset = "5";
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(mockCurrentStoreDir);
    when(mockStorageManagerUtil.getTaskStoreCheckpointDirs(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(ImmutableList.of(mockStoreNewerCheckpointDir, mockStoreOlderCheckpointDir));
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreNewerCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreOlderCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    Set<SystemStreamPartition> mockChangelogSSPs = ImmutableSet.of(changelog1SSP);
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreNewerCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, newerCheckpointDirLocalOffset));
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreOlderCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, olderCheckpointDirLocalOffset)); // less than checkpointed offset (5)

    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that all the store dirs (current or checkpoint) are marked for deletion
    assertEquals(3, storeActions.storeDirsToDelete.get(store1Name).size());
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockCurrentStoreDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreOlderCheckpointDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreNewerCheckpointDir));
    // ensure that no directories are retained
    assertEquals(0, storeActions.storeDirsToRetain.size());
    // ensure that we mark the store for full restore (from current oldest to current newest)
    assertEquals("11", storeActions.storesToRestore.get(store1Name).startingOffset);
    assertEquals("20", storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testGetStoreActionsForLoggedPersistentStore_FullRestoreIfCheckpointedOffsetNewerThanNewest() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(store1Name, store1Engine);

    String changelog1SystemName = "system1";
    String changelog1StreamName = "store1Changelog";
    SystemStream changelog1SystemStream = new SystemStream(changelog1SystemName, changelog1StreamName);
    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    // checkpointed changelog offset > newest offset (e.g. changelog topic got changed)
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    String changelog1CheckpointedOffset = "21";
    Map<SystemStreamPartition, String> mockCheckpointedChangelogOffset =
        new HashMap<SystemStreamPartition, String>() { {
          put(changelog1SSP, changelog1CheckpointedOffset);
        } };
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogOffsets =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(changelog1SSP.getSystem())).thenReturn(mockSystemAdmin);
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);
    Config mockConfig = mock(Config.class);
    Clock mockClock = mock(Clock.class);

    File mockCurrentStoreDir = mock(File.class);
    File mockStoreNewerCheckpointDir = mock(File.class);
    File mockStoreOlderCheckpointDir = mock(File.class);
    String olderCheckpointDirLocalOffset = "5";
    String newerCheckpointDirLocalOffset = "15";
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(mockCurrentStoreDir);
    when(mockStorageManagerUtil.getTaskStoreCheckpointDirs(eq(mockLoggedStoreBaseDir), eq(store1Name), eq(taskName), any()))
        .thenReturn(ImmutableList.of(mockStoreNewerCheckpointDir, mockStoreOlderCheckpointDir));
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreNewerCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    when(mockStorageManagerUtil.isLoggedStoreValid(eq(store1Name), eq(mockStoreOlderCheckpointDir), any(),
        eq(mockStoreChangelogs), eq(mockTaskModel), any(), eq(mockStoreEngines))).thenReturn(true);
    Set<SystemStreamPartition> mockChangelogSSPs = ImmutableSet.of(changelog1SSP);
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreNewerCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, newerCheckpointDirLocalOffset));
    when(mockStorageManagerUtil.readOffsetFile(eq(mockStoreOlderCheckpointDir), eq(mockChangelogSSPs), eq(false)))
        .thenReturn(ImmutableMap.of(changelog1SSP, olderCheckpointDirLocalOffset)); // less than checkpointed offset (5)

    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });

    StoreActions storeActions = TransactionalStateTaskRestoreManager.getStoreActions(
        mockTaskModel, mockStoreEngines, mockStoreChangelogs, mockCheckpointedChangelogOffset,
        mockCurrentChangelogOffsets, mockSystemAdmins, mockStorageManagerUtil,
        mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir, mockConfig, mockClock);

    // ensure that all the store dirs (current or checkpoint) are marked for deletion
    assertEquals(3, storeActions.storeDirsToDelete.get(store1Name).size());
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockCurrentStoreDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreOlderCheckpointDir));
    assertTrue(storeActions.storeDirsToDelete.get(store1Name).contains(mockStoreNewerCheckpointDir));
    // ensure that no directories are retained
    assertEquals(0, storeActions.storeDirsToRetain.size());
    // ensure that we mark the store for full restore (from current oldest to current newest)
    assertEquals("0", storeActions.storesToRestore.get(store1Name).startingOffset);
    assertEquals("10", storeActions.storesToRestore.get(store1Name).endingOffset);
  }

  @Test
  public void testSetupStoreDirs() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    StorageEngine store1Engine = mock(StorageEngine.class);
    StoreProperties mockStore1Properties = mock(StoreProperties.class);
    when(store1Engine.getStoreProperties()).thenReturn(mockStore1Properties);
    when(mockStore1Properties.isLoggedStore()).thenReturn(true);
    when(mockStore1Properties.isPersistedToDisk()).thenReturn(true);
    String store2Name = "store2";
    StorageEngine store2Engine = mock(StorageEngine.class);
    StoreProperties mockStore2Properties = mock(StoreProperties.class);
    when(store2Engine.getStoreProperties()).thenReturn(mockStore2Properties);
    when(mockStore2Properties.isLoggedStore()).thenReturn(false); // non-logged store
    when(mockStore2Properties.isPersistedToDisk()).thenReturn(true);

    Map<String, StorageEngine> mockStoreEngines = ImmutableMap.of(
        store1Name, store1Engine,
        store2Name, store2Engine);

    File mockStore1DirToRetain = mock(File.class);
    // there will be no dir to retain for non-logged persistent stores
    ImmutableMap<String, File> storeDirsToRetain = ImmutableMap.of(store1Name, mockStore1DirToRetain);
    ListMultimap<String, File> storeDirsToDelete = ArrayListMultimap.create();
    File mockStore1CurrentDir = mock(File.class);
    Path mockStore1CurrentDirPath = mock(Path.class);
    when(mockStore1CurrentDir.toPath()).thenReturn(mockStore1CurrentDirPath);
    File mockStore2CurrentDir = mock(File.class);
    Path mockStore2CurrentDirPath = mock(Path.class);
    when(mockStore2CurrentDir.toPath()).thenReturn(mockStore2CurrentDirPath);
    File mockStore1DirToDelete1 = mock(File.class);
    File mockStore1DirToDelete2 = mock(File.class);
    File mockStore2DirToDelete1 = mock(File.class);
    File mockStore2DirToDelete2 = mock(File.class);
    storeDirsToDelete.put(store1Name, mockStore1CurrentDir);
    storeDirsToDelete.put(store1Name, mockStore1DirToDelete1);
    storeDirsToDelete.put(store1Name, mockStore1DirToDelete2);
    storeDirsToDelete.put(store2Name, mockStore2CurrentDir);
    storeDirsToDelete.put(store2Name, mockStore2DirToDelete1);
    storeDirsToDelete.put(store2Name, mockStore2DirToDelete2);

    StoreActions storeActions = new StoreActions(storeDirsToRetain, storeDirsToDelete, ImmutableMap.of());
    StorageManagerUtil mockStorageManagerUtil = mock(StorageManagerUtil.class);
    FileUtil mockFileUtil = mock(FileUtil.class);
    File mockLoggedStoreBaseDir = mock(File.class);
    File mockNonLoggedStoreBaseDir = mock(File.class);

    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockLoggedStoreBaseDir), eq(store1Name), any(), any()))
        .thenReturn(mockStore1CurrentDir);
    when(mockStorageManagerUtil.getTaskStoreDir(eq(mockNonLoggedStoreBaseDir), eq(store2Name), any(), any()))
        .thenReturn(mockStore2CurrentDir);
    when(mockFileUtil.exists(eq(mockStore1CurrentDirPath)))
        .thenReturn(false);
    when(mockFileUtil.exists(eq(mockStore2CurrentDirPath)))
        .thenReturn(true); // will not be true in reality since current dir is always cleared, but return true for testing

    TransactionalStateTaskRestoreManager.setupStoreDirs(mockTaskModel, mockStoreEngines, storeActions,
        mockStorageManagerUtil, mockFileUtil, mockLoggedStoreBaseDir, mockNonLoggedStoreBaseDir);

    // verify that store directories to delete are deleted
    verify(mockFileUtil, times(1)).rm(mockStore1CurrentDir);
    verify(mockFileUtil, times(1)).rm(mockStore1DirToDelete1);
    verify(mockFileUtil, times(1)).rm(mockStore1DirToDelete2);
    verify(mockFileUtil, times(1)).rm(mockStore2CurrentDir);
    verify(mockFileUtil, times(1)).rm(mockStore2DirToDelete1);
    verify(mockFileUtil, times(1)).rm(mockStore2DirToDelete2);

    // verify that store checkpoint directories to retain are moved to (empty) current dirs only for store 1
    // setupStoreDirs doesn't guarantee that the dir is empty by itself, but the dir will be part of dirs to delete.
    verify(mockStorageManagerUtil, times(1)).moveCheckpointFiles(any(), any());
    verify(mockFileUtil, times(1)).exists(mockStore1CurrentDirPath);
    verify(mockFileUtil, times(1)).createDirectories(mockStore1CurrentDirPath);
    verify(mockFileUtil, times(1)).exists(mockStore2CurrentDirPath);
    verify(mockFileUtil, never()).createDirectories(mockStore2CurrentDirPath); // should not be called since exists == true

    verifyNoMoreInteractions(mockFileUtil);
  }

  @Test
  public void testRegisterStartingOffsets() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    String store1Name = "store1";
    String changelogSystemName = "system";
    String changelog1StreamName = "store1Changelog";
    String store2Name = "store2";
    String changelog2StreamName = "store2Changelog";
    String store3Name = "store3";
    String changelog3StreamName = "store3Changelog";
    String store4Name = "store4";
    String changelog4StreamName = "store4Changelog";

    // tests restore for store 1 and store 2 but not store 3
    Map<String, RestoreOffsets> mockRestoreOffsets = ImmutableMap.of(
        store1Name, new RestoreOffsets("0", "5"), // tests starting offset == oldest (i.e. restore is inclusive)
        store2Name, new RestoreOffsets("15", "20"), // tests starting offset != oldest (i.e. restore from next offset)
        store4Name, new RestoreOffsets("31", null)); // tests that null ending offsets are OK (should trim)
    StoreActions mockStoreActions = new StoreActions(ImmutableMap.of(), ArrayListMultimap.create(), mockRestoreOffsets);


    SystemStream changelog1SystemStream = new SystemStream(changelogSystemName, changelog1StreamName);
    SystemStream changelog2SystemStream = new SystemStream(changelogSystemName, changelog2StreamName);
    SystemStream changelog3SystemStream = new SystemStream(changelogSystemName, changelog3StreamName);
    SystemStream changelog4SystemStream = new SystemStream(changelogSystemName, changelog4StreamName);

    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(
        store1Name, changelog1SystemStream,
        store2Name, changelog2SystemStream,
        store3Name, changelog3SystemStream,
        store4Name, changelog4SystemStream);

    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    SystemStreamPartition changelog2SSP = new SystemStreamPartition(changelog2SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog2SSPMetadata = new SystemStreamPartitionMetadata("11", "20", "21");
    SystemStreamPartition changelog3SSP = new SystemStreamPartition(changelog3SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog3SSPMetadata = new SystemStreamPartitionMetadata("21", "30", "31");
    SystemStreamPartition changelog4SSP = new SystemStreamPartition(changelog4SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog4SSPMetadata = new SystemStreamPartitionMetadata("31", "40", "41");
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogSSPMetadata =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata,
            changelog2SSP, changelog2SSPMetadata,
            changelog3SSP, changelog3SSPMetadata,
            changelog4SSP, changelog4SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(eq(changelogSystemName))).thenReturn(mockSystemAdmin);
    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });
    Mockito.when(mockSystemAdmin.getOffsetsAfter(any()))
        .thenAnswer((Answer<Map<SystemStreamPartition, String>>) invocation -> {
            Map<SystemStreamPartition, String> offsets = (Map<SystemStreamPartition, String>) invocation.getArguments()[0];
            Map<SystemStreamPartition, String> nextOffsets = new HashMap<>();
            offsets.forEach((ssp, offset) -> nextOffsets.put(ssp, Long.toString(Long.valueOf(offset) + 1)));
            return nextOffsets;
          });

    SystemConsumer mockSystemConsumer = mock(SystemConsumer.class);
    Map<String, SystemConsumer> mockStoreConsumers = ImmutableMap.of(
        store1Name, mockSystemConsumer,
        store2Name, mockSystemConsumer,
        store3Name, mockSystemConsumer,
        store4Name, mockSystemConsumer);

    TransactionalStateTaskRestoreManager.registerStartingOffsets(
        mockTaskModel, mockStoreActions, mockStoreChangelogs, mockSystemAdmins,
        mockStoreConsumers, mockCurrentChangelogSSPMetadata);

    // verify that we first register upcoming offsets for each changelog ssp
    verify(mockSystemConsumer, times(1)).register(changelog1SSP, "11");
    verify(mockSystemConsumer, times(1)).register(changelog2SSP, "21");
    verify(mockSystemConsumer, times(1)).register(changelog3SSP, "31");
    verify(mockSystemConsumer, times(1)).register(changelog4SSP, "41");

    // then verify that we override the starting offsets for changelog 1 and 2
    verify(mockSystemConsumer, times(1)).register(changelog1SSP, "0"); // ensure that starting offset is inclusive if oldest
    verify(mockSystemConsumer, times(1)).register(changelog2SSP, "16"); // and that it is next offset if not oldest
    verify(mockSystemConsumer, times(1)).register(changelog4SSP, "31"); // and that null ending offset is ok
    verifyNoMoreInteractions(mockSystemConsumer);
  }

  @Test(expected = IllegalStateException.class)
  public void testRegisterStartingOffsetsThrowsIfStartingGreaterThanEnding() {
    TaskModel mockTaskModel = mock(TaskModel.class);
    TaskName taskName = new TaskName("Partition 0");
    when(mockTaskModel.getTaskName()).thenReturn(taskName);
    Partition taskChangelogPartition = new Partition(0);
    when(mockTaskModel.getChangelogPartition()).thenReturn(taskChangelogPartition);

    // tests starting offset > ending offset
    Map<String, RestoreOffsets> mockRestoreOffsets = ImmutableMap.of("store1", new RestoreOffsets("5", "0"));
    StoreActions mockStoreActions = new StoreActions(ImmutableMap.of(), ArrayListMultimap.create(), mockRestoreOffsets);

    String store1Name = "store1";
    String changelogSystemName = "system";
    String changelog1StreamName = "store1Changelog";

    SystemStream changelog1SystemStream = new SystemStream(changelogSystemName, changelog1StreamName);
    Map<String, SystemStream> mockStoreChangelogs = ImmutableMap.of(store1Name, changelog1SystemStream);

    SystemStreamPartition changelog1SSP = new SystemStreamPartition(changelog1SystemStream, taskChangelogPartition);
    SystemStreamPartitionMetadata changelog1SSPMetadata = new SystemStreamPartitionMetadata("0", "10", "11");
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> mockCurrentChangelogSSPMetadata =
        ImmutableMap.of(changelog1SSP, changelog1SSPMetadata);

    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    when(mockSystemAdmins.getSystemAdmin(eq(changelogSystemName))).thenReturn(mockSystemAdmin);
    Mockito.when(mockSystemAdmin.offsetComparator(anyString(), anyString()))
        .thenAnswer((Answer<Integer>) invocation -> {
            String offset1 = (String) invocation.getArguments()[0];
            String offset2 = (String) invocation.getArguments()[1];
            return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
          });
    Mockito.when(mockSystemAdmin.getOffsetsAfter(any()))
        .thenAnswer((Answer<Map<SystemStreamPartition, String>>) invocation -> {
            Map<SystemStreamPartition, String> offsets = (Map<SystemStreamPartition, String>) invocation.getArguments()[0];
            Map<SystemStreamPartition, String> nextOffsets = new HashMap<>();
            offsets.forEach((ssp, offset) -> nextOffsets.put(ssp, Long.toString(Long.valueOf(offset) + 1)));
            return nextOffsets;
          });

    SystemConsumer mockSystemConsumer = mock(SystemConsumer.class);
    Map<String, SystemConsumer> mockStoreConsumers = ImmutableMap.of("store1", mockSystemConsumer);

    TransactionalStateTaskRestoreManager.registerStartingOffsets(
        mockTaskModel, mockStoreActions, mockStoreChangelogs, mockSystemAdmins,
        mockStoreConsumers, mockCurrentChangelogSSPMetadata);

    fail("Should have thrown an exception since starting offset > ending offset");
  }
}