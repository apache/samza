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

import org.apache.samza.checkpoint.CheckpointId
import org.apache.samza.system.SystemStreamPartition

import scala.concurrent.Future

trait TaskStorageBackupManager {
  
  /**
   * Commit operation that is synchronous to processing
   * @return The SSP to checkpoint map of the snapshotted local store
   */
  def snapshot(): Map[SystemStreamPartition, Option[String]]

  /**
   * Commit operation that is asynchronous to message processing,
   * @param snapshotCheckpointsMap The SSP to checkpoint map returned by the snapshot
   * @return The SSP to checkpoint map of the uploaded local store
   */
  def upload(snapshotCheckpointsMap: Map[SystemStreamPartition, Option[String]]): Future[Map[SystemStreamPartition, Option[String]]]

  def checkpoint(checkpointId: CheckpointId, newestChangelogOffsets: Map[SystemStreamPartition, Option[String]]): Unit

  /**
   * Cleanup any local or remote state for obselete checkpoint information
   * @param checkpointId The id of the latest succesfully committed checkpoint
   */
  def cleanUp(checkpointId: CheckpointId): Unit

  def stop(): Unit

}