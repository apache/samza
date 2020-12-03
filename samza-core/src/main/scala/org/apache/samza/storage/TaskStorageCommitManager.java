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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.StateCheckpointMarker;
import org.apache.samza.container.TaskName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskStorageCommitManager {

  private static final Logger LOG = LoggerFactory.getLogger(TaskStorageCommitManager.class);
  private static final long COMMIT_TIMEOUT_MS = 30000;
  private final TaskStorageBackupManager storageBackupManager;

  public TaskStorageCommitManager(TaskStorageBackupManager storageBackupManager) {
    this.storageBackupManager = storageBackupManager;
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
      // TODO: Make async with andThen and add thread management for concurrency
      Map<String, StateCheckpointMarker> uploadMap = uploadFuture.get(COMMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (uploadMap == null) {
        LOG.trace("Checkpointing stores for taskName: {}} with checkpoint id: {}", taskName, checkpointId);
        storageBackupManager.persistToFilesystem(checkpointId, uploadMap);
      }

      // TODO: call commit on multiple backup managers when available
      return mergeCheckpoints(uploadMap);
    } catch (TimeoutException e) {
      throw new SamzaException("Upload timed out, commitTimeoutMs: " + COMMIT_TIMEOUT_MS, e);
    } catch (Exception e) {
      throw new SamzaException("Upload commit portion could not be completed", e);
    }
  }

  public void cleanUp(CheckpointId checkpointId) {
    storageBackupManager.cleanUp(checkpointId);
  }

  private Map<String, List<StateCheckpointMarker>> mergeCheckpoints(Map<String, StateCheckpointMarker>... stateCheckpoints) {
    if (stateCheckpoints == null || stateCheckpoints.length < 1) {
      return null;
    }
    Map<String, StateCheckpointMarker> firstCheckpoint = stateCheckpoints[0];
    if (firstCheckpoint == null) {
      return null;
    }
    Map<String, List<StateCheckpointMarker>> mergedCheckpoints = new HashMap<>();
    for (String store : firstCheckpoint.keySet()) {
      List<StateCheckpointMarker> markers = new ArrayList<>();
      for (Map<String, StateCheckpointMarker> stateCheckpoint : stateCheckpoints) {
        if (!stateCheckpoint.containsKey(store)) {
          throw new SamzaException("Store %s is not backed up in all remote backup systems");
        }
        markers.add(stateCheckpoint.get(store));
      }
      mergedCheckpoints.put(store, markers);
    }
    return mergedCheckpoints;
  }
}
