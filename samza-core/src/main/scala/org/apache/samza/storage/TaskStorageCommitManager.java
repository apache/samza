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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.immutable.Map;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;


public class TaskStorageCommitManager {

  private static final Logger LOG = LoggerFactory.getLogger(TaskStorageCommitManager.class);
  private static final long COMMIT_TIMEOUT_MS = 30000;
  private static final Duration MAX_COMMIT_DURATION = Duration.create(COMMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  private final TaskStorageBackupManager storageBackupManager;

  public TaskStorageCommitManager(TaskStorageBackupManager storageBackupManager) {
    this.storageBackupManager = storageBackupManager;
  }

  /**
   * Commits the local state on the remote backup implementation
   * @return Committed SSP to checkpoint mappings of the committed SSPs
   */
  public Map<SystemStreamPartition, Option<String>> commit(TaskName taskName, CheckpointId checkpointId) {
    Map<SystemStreamPartition, Option<String>> snapshot = storageBackupManager.snapshot();
    Future<Map<SystemStreamPartition, Option<String>>> uploadFuture = storageBackupManager.upload(snapshot);

    try {
      // TODO: Make async with andThen and add thread management for concurrency
      Map<SystemStreamPartition, Option<String>> uploadMap = Await.result(uploadFuture, MAX_COMMIT_DURATION);

      if (uploadMap != null) {
        LOG.trace("Checkpointing stores for taskName: {}} with checkpoint id: {}", taskName, checkpointId);
        storageBackupManager.persistToFilesystem(checkpointId, uploadMap);
      }
      return uploadMap;
    } catch (TimeoutException e) {
      throw new SamzaException("Upload timed out, commitTimeoutMs: " + COMMIT_TIMEOUT_MS, e);
    } catch (Exception e) {
      throw new SamzaException("Upload commit portion could not be completed", e);
    }
  }

  public void cleanUp(CheckpointId checkpointId) {
    storageBackupManager.cleanUp(checkpointId);
  }
}
