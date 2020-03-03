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

import com.google.common.annotations.VisibleForTesting;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * A storage manager for all side input stores. It is associated with each {@link org.apache.samza.container.TaskInstance}
 * and is responsible for handling directory management, offset tracking and offset file management for the side input stores.
 */
public class NonTransactionalTaskSideInputStorageManager implements TaskSideInputStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(NonTransactionalTaskSideInputStorageManager.class);
  private static final long STORE_DELETE_RETENTION_MS = TimeUnit.DAYS.toMillis(1); // same as changelog delete retention

  protected final Clock clock;
  protected final Map<String, StorageEngine> stores;
  protected final File storeBaseDir;
  protected final Map<String, Set<SystemStreamPartition>> storeToSSPs;
  protected final TaskName taskName;
  protected final TaskMode taskMode;
  protected final StorageManagerUtil storageManagerUtil = new StorageManagerUtil();

  public NonTransactionalTaskSideInputStorageManager(
      TaskName taskName,
      TaskMode taskMode,
      File storeBaseDir,
      Map<String, StorageEngine> sideInputStores,
      Map<String, Set<SystemStreamPartition>> storesToSSPs,
      Clock clock) {
    this.clock = clock;
    this.stores = sideInputStores;
    this.storeBaseDir = storeBaseDir;
    this.storeToSSPs = storesToSSPs;
    this.taskName = taskName;
    this.taskMode = taskMode;
  }

  /**
   * Initializes the side input storage manager.
   */
  @Override
  public void init() {
    LOG.info("Initializing side input stores.");

    Map<SystemStreamPartition, String> fileOffsets = getFileOffsets();
    LOG.info("File offsets for the task {}: {}", taskName, fileOffsets);

    initializeStoreDirectories();
  }

  /**
   * Flushes the contents of the underlying store and writes the offset file to disk.
   * Synchronized inorder to be exclusive with process()
   */
  @Override
  public synchronized void flush() {
    LOG.info("Flushing side inputs for task: {}", this.taskName);
    stores.values().forEach(StorageEngine::flush);
  }

  /**
   * Stops the storage engines for all the stores and writes the offset file to disk.
   */
  @Override
  public void stop() {
    LOG.info("Stopping the side input stores.");
    stores.values().forEach(StorageEngine::stop);
  }

  /**
   * Gets the {@link StorageEngine} associated with the input {@code storeName} if found, or null.
   *
   * @param storeName store name to get the {@link StorageEngine} for
   * @return the {@link StorageEngine} associated with {@code storeName} if found, or null
   */
  @Override
  public StorageEngine getStore(String storeName) {
    return stores.get(storeName);
  }

  @Override
  public Map<String, Path> checkpoint(CheckpointId checkpointId) {
    return Collections.emptyMap();
  }

  @Override
  public void removeOldCheckpoints(String latestCheckpointId) {
    // no-op
  }

  /**
   * Initializes the store directories for all the stores:
   *  1. Cleans up the directories for invalid stores.
   *  2. Ensures that the directories exist.
   */
  private void initializeStoreDirectories() {
    LOG.info("Initializing side input store directories.");

    stores.keySet().forEach(storeName -> {
        File storeLocation = getStoreLocation(storeName);
        String storePath = storeLocation.toPath().toString();
        if (!isValidSideInputStore(storeName, storeLocation)) {
          LOG.info("Cleaning up the store directory at {} for {}", storePath, storeName);
          new FileUtil().rm(storeLocation);
        }

        if (isPersistedStore(storeName) && !storeLocation.exists()) {
          LOG.info("Creating {} as the store directory for the side input store {}", storePath, storeName);
          storeLocation.mkdirs();
        }
      });
  }

  /**
   * Writes the offset files for all side input stores one by one. There is one offset file per store.
   * Its contents are a JSON encoded mapping from each side input SSP to its last processed offset, and a checksum.
   */
  @Override
  public void writeOffsetFiles(Map<SystemStreamPartition, String> lastProcessedOffsets, Map<String, Path> checkpointPaths) {
    storeToSSPs.entrySet().stream()
        .filter(entry -> isPersistedStore(entry.getKey())) // filter out in-memory side input stores
        .forEach((entry) -> {
            String storeName = entry.getKey();
            Map<SystemStreamPartition, String> offsets = entry.getValue().stream()
              .filter(lastProcessedOffsets::containsKey)
              .collect(Collectors.toMap(Function.identity(), lastProcessedOffsets::get));

            try {
              File taskStoreDir = storageManagerUtil.getTaskStoreDir(storeBaseDir, storeName, taskName, taskMode);
              storageManagerUtil.writeOffsetFile(taskStoreDir, offsets, true);
            } catch (Exception e) {
              throw new SamzaException("Failed to write offset file for side input store: " + storeName, e);
            }
          });
  }

  /**
   * Gets the side input SSP offsets for all stores from their local offset files.
   *
   * @return a {@link Map} of {@link SystemStreamPartition} to offset in the offset files.
   */
  @Override
  public Map<SystemStreamPartition, String> getFileOffsets() {
    LOG.info("Loading initial offsets from the file for side input stores.");
    Map<SystemStreamPartition, String> fileOffsets = new HashMap<>();

    stores.keySet().forEach(storeName -> {
        LOG.debug("Reading local offsets for store: {}", storeName);

        File storeLocation = getStoreLocation(storeName);
        if (isValidSideInputStore(storeName, storeLocation)) {
          try {

            Map<SystemStreamPartition, String> offsets =
                storageManagerUtil.readOffsetFile(storeLocation, storeToSSPs.get(storeName), true);
            fileOffsets.putAll(offsets);
          } catch (Exception e) {
            LOG.warn("Failed to load the offset file for side input store:" + storeName, e);
          }
        }
      });

    return fileOffsets;
  }

  @VisibleForTesting
  File getStoreLocation(String storeName) {
    return storageManagerUtil.getTaskStoreDir(storeBaseDir, storeName, taskName, taskMode);
  }

  private boolean isValidSideInputStore(String storeName, File storeLocation) {
    return isPersistedStore(storeName)
        && !storageManagerUtil.isStaleStore(storeLocation, STORE_DELETE_RETENTION_MS, clock.currentTimeMillis(), true)
        && storageManagerUtil.isOffsetFileValid(storeLocation, storeToSSPs.get(storeName), true);
  }

  private boolean isPersistedStore(String storeName) {
    return Optional.ofNullable(stores.get(storeName))
        .map(StorageEngine::getStoreProperties)
        .map(StoreProperties::isPersistedToDisk)
        .orElse(false);
  }
}