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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.StateCheckpointMarker;
import org.apache.samza.container.TaskName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskStorageCommitManager {

  private static final Logger LOG = LoggerFactory.getLogger(TaskStorageCommitManager.class);

  private final TaskName taskName;
  private final TaskBackupManager storageBackupManager;
  private final CheckpointManager checkpointManager;

  public TaskStorageCommitManager(TaskName taskName, TaskBackupManager storageBackupManager, CheckpointManager checkpointManager) {
    this.taskName = taskName;
    this.storageBackupManager = storageBackupManager;
    this.checkpointManager = checkpointManager;
  }

  public void start() {
    if (checkpointManager != null) {
      storageBackupManager.start(checkpointManager.readLastCheckpoint(taskName));
    } else {
      storageBackupManager.start(null);
    }
  }

  /**
   * Commits the local state on the remote backup implementation
   * @return Committed StoreName to StateCheckpointMarker mappings of the committed SSPs
   */
  public Map<String, List<StateCheckpointMarker>> commit(TaskName taskName, CheckpointId checkpointId) {
    Map<String, StateCheckpointMarker> snapshot = storageBackupManager.snapshot(checkpointId);
    CompletableFuture<Map<String, StateCheckpointMarker>>
        uploadFuture = storageBackupManager.upload(checkpointId, snapshot);

    try {
      // TODO: Make async with andThen and add thread management for concurrency and add timeouts
      Map<String, StateCheckpointMarker> uploadMap = uploadFuture.get();
      if (uploadMap != null) {
        LOG.trace("Persisting stores to file system for taskName: {}} with checkpoint id: {}", taskName, checkpointId);
        storageBackupManager.persistToFilesystem(checkpointId, uploadMap);
      }

      // TODO: call commit on multiple backup managers when available
      return mergeCheckpoints(taskName, Collections.singletonList(uploadMap));
    } catch (Exception e) {
      throw new SamzaException("Upload commit portion could not be completed for taskName", e);
    }
  }

  public void cleanUp(CheckpointId checkpointId) {
    if (storageBackupManager != null) {
      storageBackupManager.cleanUp(checkpointId);
    }
  }

  public void close() {
    if (storageBackupManager != null) {
      storageBackupManager.stop();
    }
  }

  private Map<String, List<StateCheckpointMarker>> mergeCheckpoints(TaskName taskName, List<Map<String, StateCheckpointMarker>> stateCheckpoints) {
    if (stateCheckpoints == null || stateCheckpoints.size() < 1) {
      return Collections.emptyMap();
    }
    Map<String, StateCheckpointMarker> firstCheckpoint = stateCheckpoints.get(0);
    if (firstCheckpoint == null) {
      return Collections.emptyMap();
    }
    Map<String, List<StateCheckpointMarker>> mergedCheckpoints = new HashMap<>();
    for (String store : firstCheckpoint.keySet()) {
      List<StateCheckpointMarker> markers = new ArrayList<>();
      for (Map<String, StateCheckpointMarker> stateCheckpoint : stateCheckpoints) {
        if (!stateCheckpoint.containsKey(store)) {
          throw new SamzaException(String.format("Store %s is not backed up in all remote backup systems for taskName: %s", store, taskName));
        }
        markers.add(stateCheckpoint.get(store));
      }
      mergedCheckpoints.put(store, markers);
    }
    return mergedCheckpoints;
  }
}
