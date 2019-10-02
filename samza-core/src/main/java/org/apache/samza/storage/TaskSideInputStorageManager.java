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
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * A storage manager for all side input stores. It is associated with each {@link org.apache.samza.container.TaskInstance}
 * and is responsible for handling directory management, offset tracking and offset file management for the side input stores.
 */
public class TaskSideInputStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(TaskSideInputStorageManager.class);
  private static final long STORE_DELETE_RETENTION_MS = TimeUnit.DAYS.toMillis(1); // same as changelog delete retention

  private final Clock clock;
  private final Map<String, SideInputsProcessor> storeToProcessor;
  private final Map<String, StorageEngine> stores;
  private final File storeBaseDir;
  private final Map<String, Set<SystemStreamPartition>> storeToSSps;
  private final Map<SystemStreamPartition, Set<String>> sspsToStores;
  private final StreamMetadataCache streamMetadataCache;
  private final SystemAdmins systemAdmins;
  private final TaskName taskName;
  private final TaskMode taskMode;
  private final Map<SystemStreamPartition, String> lastProcessedOffsets = new ConcurrentHashMap<>();
  private final StorageManagerUtil storageManagerUtil = new StorageManagerUtil();

  private Map<SystemStreamPartition, String> startingOffsets;

  public TaskSideInputStorageManager(
      TaskName taskName,
      TaskMode taskMode,
      StreamMetadataCache streamMetadataCache,
      File storeBaseDir,
      Map<String, StorageEngine> sideInputStores,
      Map<String, SideInputsProcessor> storesToProcessor,
      Map<String, Set<SystemStreamPartition>> storesToSSPs,
      SystemAdmins systemAdmins,
      Config config,
      Clock clock) {
    this.clock = clock;
    this.stores = sideInputStores;
    this.storeBaseDir = storeBaseDir;
    this.storeToSSps = storesToSSPs;
    this.streamMetadataCache = streamMetadataCache;
    this.systemAdmins = systemAdmins;
    this.taskName = taskName;
    this.taskMode = taskMode;
    this.storeToProcessor = storesToProcessor;

    validateStoreConfiguration();

    this.sspsToStores = new HashMap<>();
    storesToSSPs.forEach((store, ssps) -> {
        for (SystemStreamPartition ssp: ssps) {
          sspsToStores.computeIfAbsent(ssp, key -> new HashSet<>());
          sspsToStores.computeIfPresent(ssp, (key, value) -> {
              value.add(store);
              return value;
            });
        }
      });
  }

  /**
   * Initializes the side input storage manager.
   */
  public void init() {
    LOG.info("Initializing side input stores.");

    Map<SystemStreamPartition, String> fileOffsets = getFileOffsets();
    LOG.info("File offsets for the task {}: ", taskName, fileOffsets);

    Map<SystemStreamPartition, String> oldestOffsets = getOldestOffsets();
    LOG.info("Oldest offsets for the task {}: ", taskName, fileOffsets);

    startingOffsets = getStartingOffsets(fileOffsets, oldestOffsets);
    LOG.info("Starting offsets for the task {}: {}", taskName, startingOffsets);

    lastProcessedOffsets.putAll(fileOffsets);
    LOG.info("Last processed offsets for the task {}: {}", taskName, lastProcessedOffsets);

    initializeStoreDirectories();
  }

  /**
   * Flushes the contents of the underlying store and writes the offset file to disk.
   * Synchronized inorder to be exclusive with process()
   */
  public synchronized void flush() {
    LOG.info("Flushing the side input stores.");
    stores.values().forEach(StorageEngine::flush);
    writeOffsetFiles();
  }

  /**
   * Stops the storage engines for all the stores and writes the offset file to disk.
   */
  public void stop() {
    LOG.info("Stopping the side input stores.");
    stores.values().forEach(StorageEngine::stop);
    writeOffsetFiles();
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
   * Gets the starting offset for the given side input {@link SystemStreamPartition}.
   *
   * Note: The method doesn't respect {@link org.apache.samza.config.StreamConfig#CONSUMER_OFFSET_DEFAULT} and
   * {@link org.apache.samza.config.StreamConfig#CONSUMER_RESET_OFFSET} configurations. It will use the local offset
   * file if it is valid, else it will fall back to oldest offset in the stream.
   *
   * @param ssp side input system stream partition to get the starting offset for
   * @return the starting offset
   */
  public String getStartingOffset(SystemStreamPartition ssp) {
    return startingOffsets.get(ssp);
  }

  // Get the taskName associated with this instance.
  public TaskName getTaskName() {
    return this.taskName;
  }

  /**
   * Gets the last processed offset for the given side input {@link SystemStreamPartition}.
   *
   * @param ssp side input system stream partition to get the last processed offset for
   * @return the last processed offset
   */
  public String getLastProcessedOffset(SystemStreamPartition ssp) {
    return lastProcessedOffsets.get(ssp);
  }

  /**
   * For unit testing only
   */
  @VisibleForTesting
  void updateLastProcessedOffset(SystemStreamPartition ssp, String offset) {
    lastProcessedOffsets.put(ssp, offset);
  }

  /**
   * Processes the incoming side input message envelope and updates the last processed offset for its SSP.
   * Synchronized inorder to be exclusive with flush().
   *
   * @param message incoming message to be processed
   */
  public synchronized void process(IncomingMessageEnvelope message) {
    SystemStreamPartition ssp = message.getSystemStreamPartition();
    Set<String> storeNames = sspsToStores.get(ssp);

    for (String storeName : storeNames) {
      SideInputsProcessor sideInputsProcessor = storeToProcessor.get(storeName);

      KeyValueStore keyValueStore = (KeyValueStore) stores.get(storeName);
      Collection<Entry<?, ?>> entriesToBeWritten = sideInputsProcessor.process(message, keyValueStore);

      // Iterate over the list to be written.
      // TODO: SAMZA-2255: Optimize value writes in TaskSideInputStorageManager
      for (Entry entry : entriesToBeWritten) {
        // If the key is null we ignore, if the value is null, we issue a delete, else we issue a put
        if (entry.getKey() != null) {
          if (entry.getValue() != null) {
            keyValueStore.put(entry.getKey(), entry.getValue());
          } else {
            keyValueStore.delete(entry.getKey());
          }
        }
      }
    }

    // update the last processed offset
    lastProcessedOffsets.put(ssp, message.getOffset());
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
  @VisibleForTesting
  void writeOffsetFiles() {
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
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  Map<SystemStreamPartition, String> getFileOffsets() {
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

  /**
   * Gets the starting offsets for the {@link SystemStreamPartition}s belonging to all the side input stores.
   * If the local file offset is available and is greater than the oldest available offset from source, uses it,
   * else falls back to oldest offset in the source.
   *
   * @param fileOffsets offsets from the local offset file
   * @param oldestOffsets oldest offsets from the source
   * @return a {@link Map} of {@link SystemStreamPartition} to offset
   */
  @VisibleForTesting
  Map<SystemStreamPartition, String> getStartingOffsets(
      Map<SystemStreamPartition, String> fileOffsets, Map<SystemStreamPartition, String> oldestOffsets) {
    Map<SystemStreamPartition, String> startingOffsets = new HashMap<>();

    sspsToStores.keySet().forEach(ssp -> {
        String fileOffset = fileOffsets.get(ssp);
        String oldestOffset = oldestOffsets.get(ssp);

        startingOffsets.put(ssp,
          storageManagerUtil.getStartingOffset(
            ssp, systemAdmins.getSystemAdmin(ssp.getSystem()), fileOffset, oldestOffset));
      });

    return startingOffsets;
  }

  /**
   * Gets the oldest offset for the {@link SystemStreamPartition}s associated with all the store side inputs.
   *   1. Groups the list of the SSPs based on system stream
   *   2. Fetches the {@link SystemStreamMetadata} from {@link StreamMetadataCache}
   *   3. Fetches the partition metadata for each system stream and fetch the corresponding partition metadata
   *      and populates the oldest offset for SSPs belonging to the system stream.
   *
   * @return a {@link Map} of {@link SystemStreamPartition} to their oldest offset.
   */
  @VisibleForTesting
  Map<SystemStreamPartition, String> getOldestOffsets() {
    Map<SystemStreamPartition, String> oldestOffsets = new HashMap<>();

    // Step 1
    Map<SystemStream, List<SystemStreamPartition>> systemStreamToSsp = sspsToStores.keySet().stream()
        .collect(Collectors.groupingBy(SystemStreamPartition::getSystemStream));

    // Step 2
    Map<SystemStream, SystemStreamMetadata> metadata = JavaConverters.mapAsJavaMapConverter(
        streamMetadataCache.getStreamMetadata(
            JavaConverters.asScalaSetConverter(systemStreamToSsp.keySet()).asScala().toSet(), false)).asJava();

    // Step 3
    metadata.forEach((systemStream, systemStreamMetadata) -> {
        // get the partition metadata for each system stream
        Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata =
          systemStreamMetadata.getSystemStreamPartitionMetadata();

        // For SSPs belonging to the system stream, use the partition metadata to get the oldest offset
        Map<SystemStreamPartition, String> offsets = systemStreamToSsp.get(systemStream).stream()
          .collect(
              Collectors.toMap(Function.identity(), ssp -> partitionMetadata.get(ssp.getPartition()).getOldestOffset()));

        oldestOffsets.putAll(offsets);
      });

    return oldestOffsets;
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

  private void validateStoreConfiguration() {
    stores.forEach((storeName, storageEngine) -> {
        if (!storeToProcessor.containsKey(storeName)) {
          throw new SamzaException(
              String.format("Side inputs processor missing for store: %s.", storeName));
        }

        if (storageEngine.getStoreProperties().isLoggedStore()) {
          throw new SamzaException(
              String.format("Cannot configure both side inputs and a changelog for store: %s.", storeName));
        }
      });
  }
}