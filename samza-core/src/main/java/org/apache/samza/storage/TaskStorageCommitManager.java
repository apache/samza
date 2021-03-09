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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.container.TaskName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the commit of the state stores of the task.
 */
public class TaskStorageCommitManager {

  private static final Logger LOG = LoggerFactory.getLogger(TaskStorageCommitManager.class);

  private final TaskName taskName;
  private final CheckpointManager checkpointManager;
  private final Map<String, TaskBackupManager> stateBackendToBackupManager;

  public TaskStorageCommitManager(TaskName taskName,
      Map<String, TaskBackupManager> stateBackendToBackupManager,
      CheckpointManager checkpointManager) {
    this.taskName = taskName;
    this.stateBackendToBackupManager = stateBackendToBackupManager;
    this.checkpointManager = checkpointManager;
  }

  public void start() {
    if (checkpointManager != null) {
      Checkpoint checkpoint = checkpointManager.readLastCheckpoint(taskName);
      LOG.debug("Last checkpoint on start for task: {} is: {}", taskName, checkpoint);
      stateBackendToBackupManager.values().forEach(storageBackupManager -> storageBackupManager.init(checkpoint));
    } else {
      stateBackendToBackupManager.values().forEach(storageBackupManager -> storageBackupManager.init(null));
    }
  }

  /**
   * Commits the local state on the remote backup implementation
   * @return Committed Map of FactoryName to (Map of StoreName to StateCheckpointMarker) mappings of the committed SSPs
   */
  public Map<String, Map<String, String>> commit(TaskName taskName, CheckpointId checkpointId) {
    // { state backend factory -> { store Name -> state checkpoint marker }}
    Map<String, Map<String, String>> backendFactoryStoreStateMarkers = new HashMap<>();

    // for each configured state backend factory, backup the state for all store in this task.
    stateBackendToBackupManager.forEach((stateBackendFactoryName, storageBackupManager) -> {
      Map<String, String> snapshotSCMs = storageBackupManager.snapshot(checkpointId);
      LOG.debug("Found snapshot SCMs for taskName: {}, checkpoint id: {}, storage backup manager: {} to be: {}",
          taskName, checkpointId, storageBackupManager, snapshotSCMs);

      CompletableFuture<Map<String, String>> uploadFuture = storageBackupManager.upload(checkpointId, snapshotSCMs);
      try {
        // TODO: HIGH dchen Make async with andThen and add thread management for concurrency and add timeouts,
        // need to make upload threads independent
        Map<String, String> uploadedStoreStateMarkers = uploadFuture.get();
        LOG.debug("Found upload SCMs for taskName: {}, checkpoint id: {}, storage backup manager: {} to be: {}",
            taskName, checkpointId, storageBackupManager, uploadedStoreStateMarkers);

        // TODO BLOCKER dchen move out to TaskInstanceManager and do this once per commit instead of once per
        //  state backend
        if (uploadedStoreStateMarkers != null) {
          LOG.debug("Persisting SCMs to store checkpoint directory for taskName: {} with checkpoint id: {}",
              taskName, checkpointId);
          storageBackupManager.persistToFilesystem(checkpointId, uploadedStoreStateMarkers);
        }

        backendFactoryStoreStateMarkers.put(stateBackendFactoryName, uploadedStoreStateMarkers);
      } catch (Exception e) {
        throw new SamzaException(
            "Error uploading StateCheckpointMarkers, state commit upload phase could not be completed for taskName", e);
      }
    });

    return backendFactoryStoreStateMarkers;
  }

  /**
   * Writes a copy of the persisted {@link Checkpoint} from {@link #commit(TaskName, CheckpointId)}
   * locally to the file system on disk
   * @param checkpoint the latest checkpoint to be persisted to local file system
   */
  public void persistToLocalFileSystem(Checkpoint checkpoint) {
    // TODO BLOCKER dchen refactor TaskBackupManager.persistToFileSystem() to be done here instead
  }

  /**
   * Cleanup the commit state for each of the task backup managers
   * @param checkpointId CheckpointId of the most recent successful commit
   * @param stateCheckpointMarkers map of map(stateBackendFactoryName to map(storeName to StateCheckpointMarkers) from
   *                              the latest commit
   */
  public void cleanUp(CheckpointId checkpointId, Map<String, Map<String, String>> stateCheckpointMarkers) {
    stateCheckpointMarkers.entrySet().forEach((factoryNameToSCM) -> {
      String factoryName = factoryNameToSCM.getKey();
      if (stateBackendToBackupManager.containsKey(factoryName)) {
        TaskBackupManager backupManager = stateBackendToBackupManager.get(factoryName);
        if (backupManager != null) {
          backupManager.cleanUp(checkpointId, factoryNameToSCM.getValue());
        }
      } else {
        LOG.warn("Ignored cleanup for scm: {} due to unknown factory: {} ",
            factoryNameToSCM.getValue(), factoryNameToSCM.getKey());
      }
    });
  }

  /**
   * Close all the state backup managers
   */
  public void close() {
    stateBackendToBackupManager.values().forEach(storageBackupManager -> {
      if (storageBackupManager != null) {
        storageBackupManager.close();
      }
    });
  }
}
