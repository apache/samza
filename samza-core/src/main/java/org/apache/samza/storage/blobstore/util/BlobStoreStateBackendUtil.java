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

package org.apache.samza.storage.blobstore.util;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.samza.storage.blobstore.Metadata;
import org.apache.samza.storage.blobstore.exceptions.DeletedException;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;
import org.apache.samza.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper methods for blob store backup and restore managers.
 */
public class BlobStoreStateBackendUtil {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreStateBackendUtil.class);

  /**
   * Get the blob id of {@link SnapshotIndex} and {@link SnapshotIndex}es for the provided {@param task}
   * in the provided {@param checkpoint}.
   * @param jobName job name is used to build request metadata
   * @param jobId job id is used to build request metadata
   * @param taskName task name to get the store state checkpoint markers and snapshot indexes for
   * @param checkpoint {@link Checkpoint} instance to get the store state checkpoint markers from. Only
   *                   {@link CheckpointV2} and newer are supported for blob stores.
   * @return Map of store name to its blob id of snapshot indices and their corresponding snapshot indices for the task.
   */
  public static Map<String, Pair<String, SnapshotIndex>> getStoreSnapshotIndexes(
      String jobName, String jobId, String taskName, Checkpoint checkpoint, BlobStoreUtil blobStoreUtil) {
    if (checkpoint == null) {
      LOG.debug("No previous checkpoint found for taskName: {}", taskName);
      return ImmutableMap.of();
    }

    if (checkpoint.getVersion() == 1) {
      throw new SamzaException("Checkpoint version 1 is not supported for blob store backup and restore.");
    }

    Map<String, CompletableFuture<Pair<String, SnapshotIndex>>>
        storeSnapshotIndexFutures = new HashMap<>();

    CheckpointV2 checkpointV2 = (CheckpointV2) checkpoint;
    Map<String, Map<String, String>> factoryToStoreSCMs = checkpointV2.getStateCheckpointMarkers();
    Map<String, String> storeSnapshotIndexBlobIds = factoryToStoreSCMs.get(StorageConfig.BLOB_STORE_STATE_BACKEND_FACTORY);

    if (storeSnapshotIndexBlobIds != null) {
      storeSnapshotIndexBlobIds.forEach((storeName, snapshotIndexBlobId) -> {
        try {
          LOG.debug("Getting snapshot index for taskName: {} store: {} blobId: {}", taskName, storeName, snapshotIndexBlobId);
          Metadata requestMetadata =
              new Metadata(Metadata.PAYLOAD_PATH_SNAPSHOT_INDEX, Optional.empty(), jobName, jobId, taskName, storeName);
          CompletableFuture<SnapshotIndex> snapshotIndexFuture =
              blobStoreUtil.getSnapshotIndex(snapshotIndexBlobId, requestMetadata).toCompletableFuture();
          Pair<CompletableFuture<String>, CompletableFuture<SnapshotIndex>> pairOfFutures =
              Pair.of(CompletableFuture.completedFuture(snapshotIndexBlobId), snapshotIndexFuture);

          // save the future and block once in the end instead of blocking for each request.
          storeSnapshotIndexFutures.put(storeName, FutureUtil.toFutureOfPair(pairOfFutures));
        } catch (Exception e) {
          throw new SamzaException(
              String.format("Error getting SnapshotIndex for blobId: %s for taskName: %s store: %s",
                  snapshotIndexBlobId, taskName, storeName), e);
        }
      });
    } else {
      LOG.debug("No store SCMs found for blob store state backend in for taskName: {} in checkpoint {}",
          taskName, checkpointV2.getCheckpointId());
    }

    try {
      return FutureUtil.toFutureOfMap(t -> {
        Throwable unwrappedException = FutureUtil.unwrapExceptions(CompletionException.class, t);
        if (unwrappedException instanceof DeletedException) {
          LOG.warn("Ignoring already deleted snapshot index for taskName: {}", taskName, t);
          return true;
        } else {
          return false;
        }
      }, storeSnapshotIndexFutures).join();
    } catch (Exception e) {
      throw new SamzaException(
          String.format("Error while waiting to get store snapshot indexes for task %s", taskName), e);
    }
  }
}
