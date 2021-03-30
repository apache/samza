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
import org.apache.samza.checkpoint.kafka.KafkaStateCheckpointMarker;

import java.util.HashMap;
import java.util.concurrent.ForkJoinPool;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class TestTransactionalStateTaskBackupManager {
  @Test
  public void testFlushOrder() {
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    StorageEngine mockStore = mock(StorageEngine.class);
    java.util.Map<String, StorageEngine> taskStores = ImmutableMap.of("mockStore", mockStore);
    when(csm.getAllStores(any())).thenReturn(taskStores);
    when(mockStore.getStoreProperties()).thenReturn(new StoreProperties
        .StorePropertiesBuilder().setPersistedToDisk(true).setLoggedStore(true).build());

    KafkaTransactionalStateTaskBackupManager tsm = spy(buildTSM(csm, mock(Partition.class), new StorageManagerUtil()));
    TaskStorageCommitManager commitManager = new TaskStorageCommitManager(new TaskName("task"),
        ImmutableMap.of("kafka", tsm), csm, null, null, null, null,
        ForkJoinPool.commonPool(), new StorageManagerUtil(), null);
    // stub actual method call
    doReturn(mock(java.util.Map.class)).when(tsm).getNewestChangelogSSPOffsets(any(), any(), any(), any());

    // invoke Kafka flush
    commitManager.init();
    commitManager.snapshot(CheckpointId.create());

    // ensure that stores are flushed before we get newest changelog offsets
    InOrder inOrder = inOrder(mockStore, tsm);
    inOrder.verify(mockStore).flush();
    inOrder.verify(tsm).getNewestChangelogSSPOffsets(any(), any(), any(), any());
  }

  @Test
  public void testGetNewestOffsetsReturnsCorrectOffset() {
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    KafkaTransactionalStateTaskBackupManager tsm = buildTSM(csm, mock(Partition.class), new StorageManagerUtil());

    TaskName taskName = mock(TaskName.class);
    String changelogSystemName = "systemName";
    String storeName = "storeName";
    String changelogStreamName = "changelogName";
    String newestChangelogSSPOffset = "1";
    SystemStream changelogSystemStream = new SystemStream(changelogSystemName, changelogStreamName);
    Partition changelogPartition = new Partition(0);
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);

    java.util.Map<String, SystemStream> storeChangelogs = new HashMap<>();
    storeChangelogs.put(storeName, changelogSystemStream);

    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    SystemAdmin systemAdmin = mock(SystemAdmin.class);
    SystemStreamPartitionMetadata metadata = mock(SystemStreamPartitionMetadata.class);

    when(metadata.getNewestOffset()).thenReturn(newestChangelogSSPOffset);
    when(systemAdmins.getSystemAdmin(changelogSystemName)).thenReturn(systemAdmin);
    when(systemAdmin.getSSPMetadata(eq(ImmutableSet.of(changelogSSP)))).thenReturn(ImmutableMap.of(changelogSSP, metadata));

    // invoke the method
    java.util.Map<String, String> stateCheckpointMarkerMap =
        tsm.getNewestChangelogSSPOffsets(
            taskName, storeChangelogs, changelogPartition, systemAdmins);

    // verify results
    assertEquals(1, stateCheckpointMarkerMap.size());
    KafkaStateCheckpointMarker kscm = KafkaStateCheckpointMarker.fromString(stateCheckpointMarkerMap.get(storeName));
    assertEquals(newestChangelogSSPOffset, kscm.getChangelogOffset());
    assertEquals(changelogSSP, kscm.getChangelogSSP());
  }

  @Test
  public void testGetNewestOffsetsReturnsNoneForEmptyTopic() {
    // empty topic == null newest offset
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    KafkaTransactionalStateTaskBackupManager tsm = buildTSM(csm, mock(Partition.class), new StorageManagerUtil());

    TaskName taskName = mock(TaskName.class);
    String changelogSystemName = "systemName";
    String storeName = "storeName";
    String changelogStreamName = "changelogName";
    String newestChangelogSSPOffset = null;
    SystemStream changelogSystemStream = new SystemStream(changelogSystemName, changelogStreamName);
    Partition changelogPartition = new Partition(0);
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);

    java.util.Map<String, SystemStream> storeChangelogs = new HashMap<String, SystemStream>();
    storeChangelogs.put(storeName, changelogSystemStream);

    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    SystemAdmin systemAdmin = mock(SystemAdmin.class);
    SystemStreamPartitionMetadata metadata = mock(SystemStreamPartitionMetadata.class);

    when(metadata.getNewestOffset()).thenReturn(newestChangelogSSPOffset);
    when(systemAdmins.getSystemAdmin(changelogSystemName)).thenReturn(systemAdmin);
    when(systemAdmin.getSSPMetadata(eq(ImmutableSet.of(changelogSSP)))).thenReturn(ImmutableMap.of(changelogSSP, metadata));

    // invoke the method
    java.util.Map<String, String> stateCheckpointMarkerMap =
        tsm.getNewestChangelogSSPOffsets(
            taskName, storeChangelogs, changelogPartition, systemAdmins);

    // verify results
    assertEquals(1, stateCheckpointMarkerMap.size());
    KafkaStateCheckpointMarker kscm = KafkaStateCheckpointMarker.fromString(stateCheckpointMarkerMap.get(storeName));
    assertEquals(changelogSSP, kscm.getChangelogSSP());
    assertNull(kscm.getChangelogOffset());
  }

  @Test(expected = SamzaException.class)
  public void testGetNewestOffsetsThrowsIfNullMetadata() {
    // empty topic == null newest offset
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    KafkaTransactionalStateTaskBackupManager tsm = buildTSM(csm, mock(Partition.class), new StorageManagerUtil());

    TaskName taskName = mock(TaskName.class);
    String changelogSystemName = "systemName";
    String storeName = "storeName";
    String changelogStreamName = "changelogName";
    String newestChangelogSSPOffset = null;
    SystemStream changelogSystemStream = new SystemStream(changelogSystemName, changelogStreamName);
    Partition changelogPartition = new Partition(0);
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);

    java.util.Map<String, SystemStream> storeChangelogs = new HashMap<>();
    storeChangelogs.put(storeName, changelogSystemStream);

    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    SystemAdmin systemAdmin = mock(SystemAdmin.class);
    SystemStreamPartitionMetadata metadata = mock(SystemStreamPartitionMetadata.class);

    when(metadata.getNewestOffset()).thenReturn(newestChangelogSSPOffset);
    when(systemAdmins.getSystemAdmin(changelogSystemName)).thenReturn(systemAdmin);
    when(systemAdmin.getSSPMetadata(eq(ImmutableSet.of(changelogSSP)))).thenReturn(null);

    // invoke the method
    java.util.Map<String, String> offsets =
        tsm.getNewestChangelogSSPOffsets(
            taskName, storeChangelogs, changelogPartition, systemAdmins);

    // verify results
    fail("Should have thrown an exception if admin didn't return any metadata");
  }

  @Test(expected = SamzaException.class)
  public void testGetNewestOffsetsThrowsIfNullSSPMetadata() {
    // empty topic == null newest offset
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    KafkaTransactionalStateTaskBackupManager tsm = buildTSM(csm, mock(Partition.class), new StorageManagerUtil());

    TaskName taskName = mock(TaskName.class);
    String changelogSystemName = "systemName";
    String storeName = "storeName";
    String changelogStreamName = "changelogName";
    String newestChangelogSSPOffset = null;
    SystemStream changelogSystemStream = new SystemStream(changelogSystemName, changelogStreamName);
    Partition changelogPartition = new Partition(0);
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);

    java.util.Map<String, SystemStream> storeChangelogs = new HashMap<>();
    storeChangelogs.put(storeName, changelogSystemStream);

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
    java.util.Map<String, String> offsets =
        tsm.getNewestChangelogSSPOffsets(
            taskName, storeChangelogs, changelogPartition, systemAdmins);

    // verify results
    fail("Should have thrown an exception if admin returned null metadata for changelog SSP");
  }

  @Test(expected = SamzaException.class)
  public void testGetNewestOffsetsThrowsIfErrorGettingMetadata() {
    // empty topic == null newest offset
    ContainerStorageManager csm = mock(ContainerStorageManager.class);
    KafkaTransactionalStateTaskBackupManager tsm = buildTSM(csm, mock(Partition.class), new StorageManagerUtil());

    TaskName taskName = mock(TaskName.class);
    String changelogSystemName = "systemName";
    String storeName = "storeName";
    String changelogStreamName = "changelogName";
    String newestChangelogSSPOffset = null;
    SystemStream changelogSystemStream = new SystemStream(changelogSystemName, changelogStreamName);
    Partition changelogPartition = new Partition(0);
    SystemStreamPartition changelogSSP = new SystemStreamPartition(changelogSystemStream, changelogPartition);

    java.util.Map<String, SystemStream> storeChangelogs = new HashMap<>();
    storeChangelogs.put(storeName, changelogSystemStream);

    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    SystemAdmin systemAdmin = mock(SystemAdmin.class);
    SystemStreamPartitionMetadata metadata = mock(SystemStreamPartitionMetadata.class);

    when(metadata.getNewestOffset()).thenReturn(newestChangelogSSPOffset);
    when(systemAdmins.getSystemAdmin(changelogSystemName)).thenThrow(new SamzaException("Error getting metadata"));
    when(systemAdmin.getSSPMetadata(eq(ImmutableSet.of(changelogSSP)))).thenReturn(null);

    // invoke the method
    java.util.Map<String, String> offsets =
        tsm.getNewestChangelogSSPOffsets(
            taskName, storeChangelogs, changelogPartition, systemAdmins);

    // verify results
    fail("Should have thrown an exception if admin had an error getting metadata");
  }

  private KafkaTransactionalStateTaskBackupManager buildTSM(ContainerStorageManager csm, Partition changelogPartition,
      StorageManagerUtil smu) {
    TaskName taskName = new TaskName("Partition 0");
    java.util.Map<String, SystemStream> changelogSystemStreams = mock(java.util.Map.class);
    SystemAdmins systemAdmins = mock(SystemAdmins.class);

    return new KafkaTransactionalStateTaskBackupManager(
        taskName, changelogSystemStreams, systemAdmins, changelogPartition);
  }
}