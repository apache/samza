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

package org.apache.samza.storage

import java.util.concurrent.CompletableFuture
import java.util.{Map => JMap}

import org.apache.samza.checkpoint.{CheckpointId, StateCheckpointMarker}

/**
 * <p>
 * TaskStorageBackupManager is the interface that must be implemented for
 * any remote system that Samza persists its state to. The {@link TaskStorageCommitManager} evokes
 * the TaskStorageBackupManager in the following way:
 * </p>
 *
 * <ul>
 *   <li>Snapshot will be called before Upload.</li>
 *   <li>persistToFilesystem will be called after Upload is completed</li>
 *   <li>Cleanup is only called after Upload and persistToFilesystem has successfully completed</li>
 * </ul>
 */
trait TaskStorageBackupManager {

  /**
   * Commit operation that is synchronous to processing
   * @param checkpointId Checkpoint id of the current commit
   * @return The SSP to checkpoint map of the snapshotted local store
   */
  def snapshot(checkpointId: CheckpointId): JMap[String, StateCheckpointMarker]

  /**
   * Commit operation that is asynchronous to message processing,
   * @param checkpointId Checkpoint id of the current commit
   * @param stateCheckpointMarkers The map of storename to checkpoint makers returned by the snapshot
   * @return The future of storename to checkpoint map of the uploaded local store
   */
  def upload(checkpointId: CheckpointId, stateCheckpointMarkers: JMap[String, StateCheckpointMarker]): CompletableFuture[JMap[String, StateCheckpointMarker]]

  /**
   * Persist the state locally to the file system
   * @param checkpointId The id of the checkpoint to be committed
   * @param stateCheckpointMarkers Uploaded storename to checkpoints markers to be persisted locally
   */
  def persistToFilesystem(checkpointId: CheckpointId, stateCheckpointMarkers: JMap[String, StateCheckpointMarker]): Unit

  /**
   * Cleanup any local or remote state for obsolete checkpoint information that are older than checkpointId
   * @param checkpointId The id of the latest successfully committed checkpoint
   */
  def cleanUp(checkpointId: CheckpointId): Unit

  /**
   * Used for testing as a shutdown hook to cleanup any allocated resources
   */
  def stop(): Unit

}