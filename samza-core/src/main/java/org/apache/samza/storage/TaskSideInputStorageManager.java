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
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A storage manager for all side input stores. It is associated with each {@link org.apache.samza.container.TaskInstance}
 * and is responsible for handling directory management, offset tracking and offset file management for the side input stores.
 */
public class TaskSideInputStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(TaskSideInputStorageManager.class);
  private static final long STORE_DELETE_RETENTION_MS = TimeUnit.DAYS.toMillis(1); // same as changelog delete retention

  private final Clock clock;
  private final Map<String, StorageEngine> stores;
  private final File storeBaseDir;
  private final Map<String, Set<SystemStreamPartition>> storeToSSps;
  private final TaskName taskName;
  private final TaskMode taskMode;
  private final StorageManagerUtil storageManagerUtil = new StorageManagerUtil();

  public TaskSideInputStorageManager(TaskName taskName, TaskMode taskMode, File storeBaseDir, Map<String, StorageEngine> sideInputStores,
      Map<String, Set<SystemStreamPartition>> storesToSSPs, Clock clock) {
    validateStoreConfiguration(sideInputStores);

    this.clock = clock;
    this.stores = sideInputStores;
    this.storeBaseDir = storeBaseDir;
    this.storeToSSps = storesToSSPs;
    this.taskName = taskName;
    this.taskMode = taskMode;
  }

  // Get the taskName associated with this instance.
  public TaskName getTaskName() {
    return this.taskName;
  }

  /**
   * Initializes the side input storage manager.
   */
  public void init() {
    LOG.info("Initializing side input stores.");

    initializeStoreDirectories();
  }

  /**
   * Flushes the contents of the underlying store and writes the offset file to disk.
   * Synchronized inorder to be exclusive with process()
   *
   * @param lastProcessedOffsets The last processed offsets for each SSP. These will be used when writing offsets files
   *                             for each store.
   */
  public void flush(Map<SystemStreamPartition, String> lastProcessedOffsets) {
    LOG.info("Flushing the side input stores.");
    stores.values().forEach(StorageEngine::flush);
    writeFileOffsets(lastProcessedOffsets);
  }

  /**
   * Stops the storage engines for all the stores and writes the offset file to disk.
   *
   * @param lastProcessedOffsets The last processed offsets for each SSP. These will be used when writing offsets files
   *                             for each store.
   */
  public void stop(Map<SystemStreamPartition, String> lastProcessedOffsets) {
    LOG.info("Stopping the side input stores.");
    stores.values().forEach(StorageEngine::stop);
    writeFileOffsets(lastProcessedOffsets);
  }

  /**
   * Gets the {@link StorageEngine} associated with the input {@code storeName} if found, or null.
   *
   * @param storeName store name to get the {@link StorageEngine} for
   * @return the {@link StorageEngine} associated with {@code storeName} if found, or null
   */
  public StorageEngine getStore(String storeName) {
    return stores.get(storeName);
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
   *
   * @param lastProcessedOffsets The offset per SSP to write
   */
  public void writeFileOffsets(Map<SystemStreamPartition, String> lastProcessedOffsets) {
    storeToSSps.entrySet().stream()
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
  public Map<SystemStreamPartition, String> getFileOffsets() {
    LOG.info("Loading initial offsets from the file for side input stores.");
    Map<SystemStreamPartition, String> fileOffsets = new HashMap<>();

    stores.keySet().forEach(storeName -> {
      LOG.debug("Reading local offsets for store: {}", storeName);

      File storeLocation = getStoreLocation(storeName);
      if (isValidSideInputStore(storeName, storeLocation)) {
        try {

          Map<SystemStreamPartition, String> offsets =
              storageManagerUtil.readOffsetFile(storeLocation, storeToSSps.get(storeName), true);
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
        && storageManagerUtil.isOffsetFileValid(storeLocation, storeToSSps.get(storeName), true);
  }

  private boolean isPersistedStore(String storeName) {
    return Optional.ofNullable(stores.get(storeName))
        .map(StorageEngine::getStoreProperties)
        .map(StoreProperties::isPersistedToDisk)
        .orElse(false);
  }

  private void validateStoreConfiguration(Map<String, StorageEngine> stores) {
    stores.forEach((storeName, storageEngine) -> {
      // Ensure that the side inputs store is NOT logged (they are durable)
      if (storageEngine.getStoreProperties().isLoggedStore()) {
        throw new SamzaException(
            String.format("Cannot configure both side inputs and a changelog for store: %s.", storeName));
      }
    });
  }
}