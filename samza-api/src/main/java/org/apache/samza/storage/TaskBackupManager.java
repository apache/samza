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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;


/**
 * <p>
 * TaskBackupManager is the interface that must be implemented for any remote system that Samza persists its state to
 * during the task commit operation.
 * {@link #snapshot(CheckpointId)} will be evoked synchronous to task processing and get a snapshot of the stores
 * state to be persisted for the commit. {@link #upload(CheckpointId, Map)} will then use the snapshotted state
 * to persist to the underlying backup system and will be asynchronous to task processing.
 * </p>
 * The interface will be evoked in the following way:
 * <ul>
 *   <li>Snapshot will be called before Upload.</li>
 *   <li>persistToFilesystem will be called after Upload is completed</li>
 *   <li>Cleanup is only called after Upload and persistToFilesystem has successfully completed</li>
 * </ul>
 */
public interface TaskBackupManager {

  /**
   * Initializes the TaskBackupManager instance.
   *
   * @param checkpoint last recorded checkpoint from the CheckpointManager or null if no last checkpoint was found
   */
  void init(@Nullable Checkpoint checkpoint);

  /**
   *  Snapshot is used to capture the current state of the stores in order to persist it to the backup manager in the
   *  {@link #upload(CheckpointId, Map)} (CheckpointId, Map)} phase. Performs the commit operation that is
   *  synchronous to processing. Returns the per store name state checkpoint markers to be used in upload.
   *
   * @param checkpointId {@link CheckpointId} of the current commit
   * @return a map of store name to state checkpoint markers for stores managed by this state backend
   */
  Map<String, String> snapshot(CheckpointId checkpointId);

  /**
   * Upload is used to persist the state provided by the {@link #snapshot(CheckpointId)} to the
   * underlying backup system. Commit operation that is asynchronous to message processing and returns a
   * {@link CompletableFuture} containing the successfully uploaded state checkpoint markers .
   *
   * @param checkpointId {@link CheckpointId} of the current commit
   * @param stateCheckpointMarkers the map of storename to state checkpoint markers returned by
   *                               {@link #snapshot(CheckpointId)}
   * @return a {@link CompletableFuture} containing a map of store name to state checkpoint markers
   *         after the upload is complete
   */
  CompletableFuture<Map<String, String>> upload(CheckpointId checkpointId, Map<String, String> stateCheckpointMarkers);

  /**
   * Cleanup any local or remote state for checkpoint information that is older than the provided checkpointId
   * This operation is required to be idempotent.
   *
   * @param checkpointId the {@link CheckpointId} of the last successfully committed checkpoint
   * @param stateCheckpointMarkers a map of store name to state checkpoint markers returned by
   *                               {@link #upload(CheckpointId, Map)} (CheckpointId, Map)} upload}
   */
  CompletableFuture<Void> cleanUp(CheckpointId checkpointId, Map<String, String> stateCheckpointMarkers);

  /**
   * Shutdown hook the backup manager to cleanup any allocated resources
   */
  void close();

}