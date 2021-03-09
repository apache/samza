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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.container.TaskName;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;


public class TestTaskStorageCommitManager {
  @Test
  public void testCommitManagerStart() {
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);
    Checkpoint checkpoint = mock(Checkpoint.class);

    TaskName task = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );
    TaskStorageCommitManager cm = new TaskStorageCommitManager(task, backupManagers, checkpointManager);

    when(checkpointManager.readLastCheckpoint(task)).thenReturn(checkpoint);
    cm.start();
    verify(taskBackupManager1).init(checkpoint);
    verify(taskBackupManager2).init(checkpoint);
  }

  @Test
  public void testCommitManagerStartNullCheckpointManager() {
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);

    TaskName task = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );
    TaskStorageCommitManager cm = new TaskStorageCommitManager(task, backupManagers, null);
    cm.start();
    verify(taskBackupManager1).init(null);
    verify(taskBackupManager2).init(null);
  }

  @Test
  public void testCommitAllFactories() {
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);
    Checkpoint checkpoint = mock(Checkpoint.class);

    TaskName taskName = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );
    TaskStorageCommitManager cm = new TaskStorageCommitManager(taskName, backupManagers, checkpointManager);
    when(checkpointManager.readLastCheckpoint(taskName)).thenReturn(checkpoint);
    cm.start();
    verify(taskBackupManager1).init(checkpoint);
    verify(taskBackupManager2).init(checkpoint);

    CheckpointId newCheckpointId = CheckpointId.create();
    Map<String, String> factory1Checkpoints = ImmutableMap.of(
        "store1", "system;stream;1",
        "store2", "system;stream;2"
    );
    Map<String, String> factory2Checkpoints = ImmutableMap.of(
        "store1", "blobId1",
        "store2", "blobId2"
    );

    when(taskBackupManager1.snapshot(newCheckpointId)).thenReturn(factory1Checkpoints);
    when(taskBackupManager1.upload(newCheckpointId, factory1Checkpoints))
        .thenReturn(CompletableFuture.completedFuture(factory1Checkpoints));
    when(taskBackupManager2.snapshot(newCheckpointId)).thenReturn(factory2Checkpoints);
    when(taskBackupManager2.upload(newCheckpointId, factory2Checkpoints))
        .thenReturn(CompletableFuture.completedFuture(factory2Checkpoints));

    cm.commit(taskName, newCheckpointId);

    // Test flow for factory 1
    verify(taskBackupManager1).snapshot(newCheckpointId);
    verify(taskBackupManager1).upload(newCheckpointId, factory1Checkpoints);

    // Test flow for factory 2
    verify(taskBackupManager2).snapshot(newCheckpointId);
    verify(taskBackupManager2).upload(newCheckpointId, factory2Checkpoints);
  }

  @Test
  public void testCleanupAllBackupManagers() {
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    TaskBackupManager taskBackupManager1 = mock(TaskBackupManager.class);
    TaskBackupManager taskBackupManager2 = mock(TaskBackupManager.class);
    Checkpoint checkpoint = mock(Checkpoint.class);

    TaskName taskName = new TaskName("task1");
    Map<String, TaskBackupManager> backupManagers = ImmutableMap.of(
        "factory1", taskBackupManager1,
        "factory2", taskBackupManager2
    );
    TaskStorageCommitManager cm = new TaskStorageCommitManager(taskName, backupManagers, checkpointManager);
    when(checkpointManager.readLastCheckpoint(taskName)).thenReturn(checkpoint);

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
        "factory2", factory2Checkpoints,
        "factory3", Collections.EMPTY_MAP // factory 3 should be ignored
    );

    CheckpointId newCheckpointId = CheckpointId.create();
    cm.cleanUp(newCheckpointId, factoryCheckpointsMap);

    verify(taskBackupManager1).cleanUp(newCheckpointId, factory1Checkpoints);
    verify(taskBackupManager2).cleanUp(newCheckpointId, factory2Checkpoints);
  }
}
