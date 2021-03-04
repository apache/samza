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
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.StateCheckpointMarker;
import org.apache.samza.container.TaskName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO HIGH dchen add javadocs for this class.
// TODO HIGH dchen add unit tests for this class.
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
   * TODO BLOCKER dchen add comments / docs for what all these Map keys and value are.
   * @return Committed StoreName to StateCheckpointMarker mappings of the committed SSPs
   */
  public Map<String, List<StateCheckpointMarker>> commit(TaskName taskName, CheckpointId checkpointId) {
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
        // need to make upload theads independent
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

    return getStoreSCMs(taskName, backendFactoryStoreStateMarkers);
  }

  // TODO HIGH dchen add javadocs explaining what the params are.
  public void cleanUp(CheckpointId checkpointId, Map<String, List<StateCheckpointMarker>> stateCheckpointMarkers) {
    // { state backend factory -> { store name -> state checkpoint marker)
    Map<String, Map<String, StateCheckpointMarker>> stateBackendToStoreSCMs = new HashMap<>();

    // The number of backend factories is equal to the length of the stateCheckpointMarker per store list
    stateBackendToBackupManager.keySet().forEach((stateBackendFactoryName) -> {
      stateBackendToStoreSCMs.put(stateBackendFactoryName, new HashMap<>());
    });

    stateCheckpointMarkers.forEach((storeName, scmList) -> {
      scmList.forEach(scm -> {
        if (stateBackendToStoreSCMs.containsKey(scm.getStateBackendFactoryName())) {
          stateBackendToStoreSCMs.get(scm.getStateBackendFactoryName()).put(storeName, scm);
        } else {
          LOG.warn("Ignored cleanup for scm: {} due to unknown factory: {} ", scm, scm.getStateBackendFactoryName());
        }
      });
    });

    stateBackendToStoreSCMs.forEach((backendFactoryName, storeSCMs) -> {
      TaskBackupManager storageBackupManager = stateBackendToBackupManager.get(backendFactoryName);
      if (storageBackupManager != null) {
        storageBackupManager.cleanUp(checkpointId, storeSCMs);
      }
    });
  }

  public void close() {
    stateBackendToBackupManager.values().forEach(storageBackupManager -> {
      if (storageBackupManager != null) {
        storageBackupManager.close();
      }
    });
  }

  // TODO HIGH dchen add javadocs for what this method is doing
  // TODO HIGH dchen add unit tests.
  private Map<String, List<StateCheckpointMarker>> getStoreSCMs(TaskName taskName,
      Map<String, Map<String, String>> stateBackendFactoryToStoreStateMarkers) {
    if (stateBackendFactoryToStoreStateMarkers == null || stateBackendFactoryToStoreStateMarkers.size() == 0) {
      return Collections.emptyMap();
    }

    Map<String, List<StateCheckpointMarker>> storeSCMs = new HashMap<>();

    for (Map.Entry<String, Map<String, String>> stateBackendFactoryToStoreStateMarker:
        stateBackendFactoryToStoreStateMarkers.entrySet()) {
      String stateBackendFactory = stateBackendFactoryToStoreStateMarker.getKey();
      Map<String, String> storeNameToStateCheckpointMarkers = stateBackendFactoryToStoreStateMarker.getValue();
      for (Map.Entry<String, String> storeNameToStateCheckpointMarker: storeNameToStateCheckpointMarkers.entrySet()) {
        String storeName = storeNameToStateCheckpointMarker.getKey();
        String stateCheckpointMarker = storeNameToStateCheckpointMarker.getValue();
        storeSCMs.computeIfAbsent(storeName, sn -> new ArrayList<>())
            .add(new StateCheckpointMarker(stateBackendFactory, stateCheckpointMarker));
      }
    }

    return storeSCMs;
  }
}
