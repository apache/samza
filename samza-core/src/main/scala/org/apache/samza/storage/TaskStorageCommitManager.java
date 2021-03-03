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


public class TaskStorageCommitManager {

  private static final Logger LOG = LoggerFactory.getLogger(TaskStorageCommitManager.class);

  private final TaskName taskName;
  private final CheckpointManager checkpointManager;
  private final Map<String, TaskBackupManager> backendFactoryBackupManagerMap;

  public TaskStorageCommitManager(TaskName taskName, Map<String, TaskBackupManager> backendFactoryBackupManagerMap, CheckpointManager checkpointManager) {
    this.taskName = taskName;
    this.backendFactoryBackupManagerMap = backendFactoryBackupManagerMap;
    this.checkpointManager = checkpointManager;
  }

  public void start() {
    if (checkpointManager != null) {
      Checkpoint checkpoint = checkpointManager.readLastCheckpoint(taskName);
      LOG.debug("Last checkpoint on start for task: {} is: {}", taskName, checkpoint);
      backendFactoryBackupManagerMap.values().forEach(storageBackupManager -> storageBackupManager.init(checkpoint));
    } else {
      backendFactoryBackupManagerMap.values().forEach(storageBackupManager -> storageBackupManager.init(null));
    }
  }

  /**
   * Commits the local state on the remote backup implementation
   * @return Committed StoreName to StateCheckpointMarker mappings of the committed SSPs
   */
  public Map<String, List<StateCheckpointMarker>> commit(TaskName taskName, CheckpointId checkpointId) {
    List<Map<String, StateCheckpointMarker>> stateCheckpoints = new ArrayList<>();
    backendFactoryBackupManagerMap.values().forEach(storageBackupManager -> {
      Map<String, StateCheckpointMarker> snapshotSCMs = storageBackupManager.snapshot(checkpointId);
      LOG.debug("Found snapshot SCMs for taskName: {} checkpoint id: {} to be: {}", taskName, checkpointId, snapshotSCMs);

      CompletableFuture<Map<String, StateCheckpointMarker>> uploadFuture = storageBackupManager.upload(checkpointId, snapshotSCMs);

      try {
        // TODO: HIGH dchen Make async with andThen and add thread management for concurrency and add timeouts,
        // need to make upload theads independent
        Map<String, StateCheckpointMarker> uploadSCMs = uploadFuture.get();
        LOG.debug("Found uplaod SCMs for taskName: {} checkpoint id: {} to be: {}", taskName, checkpointId, uploadSCMs);

        if (uploadSCMs != null) {
          LOG.debug("Persisting SCMs to store checkpoint directory for taskName: {} with checkpoint id: {}", taskName,
              checkpointId);
          storageBackupManager.persistToFilesystem(checkpointId, uploadSCMs);
        }

        stateCheckpoints.add(uploadSCMs);
      } catch (Exception e) {
        throw new SamzaException(
            "Error uploading StateCheckpointMarkers, state commit upload phase could not be completed for taskName", e);
      }
    });
    return mergeCheckpoints(taskName, stateCheckpoints);
  }

  public void cleanUp(CheckpointId checkpointId, Map<String, List<StateCheckpointMarker>> stateCheckpointMarkers) {
    Map<String, Map<String, StateCheckpointMarker>> factoryStateCheckpointsMap = new HashMap<>();
    // The number of backend factories is equal to the length of the stateCheckpointMarker per store list
    backendFactoryBackupManagerMap.keySet().forEach((backendFactory) -> {
      factoryStateCheckpointsMap.put(backendFactory, new HashMap<>());
    });
    stateCheckpointMarkers.forEach((storeName, scmList) -> {
      scmList.forEach(scm -> {
        if (factoryStateCheckpointsMap.containsKey(scm.getFactoryName())) {
          factoryStateCheckpointsMap.get(scm.getFactoryName()).put(storeName, scm);
        } else {
          LOG.warn("Ignored cleanup for scm: {} due to unknown factory {} ", scm, scm.getFactoryName());
        }
      });
    });

    factoryStateCheckpointsMap.forEach((backendFactoryName, stateCheckpointMarkerMap) -> {
      TaskBackupManager storageBackupManager = backendFactoryBackupManagerMap.get(backendFactoryName);
      if (storageBackupManager != null) {
        storageBackupManager.cleanUp(checkpointId, stateCheckpointMarkerMap);
      }
    });
  }

  public void close() {
    backendFactoryBackupManagerMap.values().forEach(storageBackupManager -> {
      if (storageBackupManager != null) {
        storageBackupManager.close();
      }
    });
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
