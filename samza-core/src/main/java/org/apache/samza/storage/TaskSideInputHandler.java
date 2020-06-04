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
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


/**
 * This class encapsulates all processing logic / state for all side input SSPs within a task.
 */
public class TaskSideInputHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TaskSideInputHandler.class);

  private final StorageManagerUtil storageManagerUtil = new StorageManagerUtil();
  private final Map<SystemStreamPartition, String> lastProcessedOffsets = new ConcurrentHashMap<>();

  private final TaskName taskName;
  private final TaskSideInputStorageManager taskSideInputStorageManager;
  private final Map<SystemStreamPartition, Set<String>> sspToStores;
  private final Map<String, SideInputsProcessor> storeToProcessor;
  private final SystemAdmins systemAdmins;
  private final StreamMetadataCache streamMetadataCache;

  private Map<SystemStreamPartition, String> startingOffsets;

  public TaskSideInputHandler(TaskName taskName, TaskMode taskMode, File storeBaseDir,
      Map<String, StorageEngine> storeToStorageEngines, Map<String, Set<SystemStreamPartition>> storeToSSPs,
      Map<String, SideInputsProcessor> storeToProcessor, SystemAdmins systemAdmins,
      StreamMetadataCache streamMetadataCache, Clock clock) {
    validateProcessorConfiguration(storeToSSPs.keySet(), storeToProcessor);

    this.taskName = taskName;
    this.systemAdmins = systemAdmins;
    this.streamMetadataCache = streamMetadataCache;
    this.storeToProcessor = storeToProcessor;

    this.sspToStores = storeToSSPs.entrySet().stream()
        .flatMap(storeAndSSPs -> storeAndSSPs.getValue().stream()
            .map(ssp -> new AbstractMap.SimpleImmutableEntry<>(ssp, storeAndSSPs.getKey())))
        .collect(Collectors.groupingBy(
            Map.Entry::getKey,
            Collectors.mapping(Map.Entry::getValue, Collectors.toSet())));

    this.taskSideInputStorageManager = new TaskSideInputStorageManager(taskName,
        taskMode,
        storeBaseDir,
        storeToStorageEngines,
        storeToSSPs,
        clock);
  }

  /**
   * The {@link TaskName} associated with this {@link TaskSideInputHandler}
   *
   * @return the task name for this handler
   */
  public TaskName getTaskName() {
    return this.taskName;
  }

  /**
   * Initializes the underlying {@link TaskSideInputStorageManager} and determines starting offsets for each SSP.
   */
  public void init() {
    this.taskSideInputStorageManager.init();

    Map<SystemStreamPartition, String> fileOffsets = this.taskSideInputStorageManager.getFileOffsets();
    LOG.info("File offsets for the task {}: {}", taskName, fileOffsets);

    this.lastProcessedOffsets.putAll(fileOffsets);
    LOG.info("Last processed offsets for the task {}: {}", taskName, lastProcessedOffsets);

    this.startingOffsets = getStartingOffsets(fileOffsets, getOldestOffsets());
    LOG.info("Starting offsets for the task {}: {}", taskName, startingOffsets);
  }

  /**
   * Processes the incoming side input message envelope and updates the last processed offset for its SSP.
   * Synchronized inorder to be exclusive with flush().
   *
   * @param envelope incoming envelope to be processed
   */
  public synchronized void process(IncomingMessageEnvelope envelope) {
    SystemStreamPartition envelopeSSP = envelope.getSystemStreamPartition();
    String envelopeOffset = envelope.getOffset();

    for (String store: this.sspToStores.get(envelopeSSP)) {
      SideInputsProcessor storeProcessor = this.storeToProcessor.get(store);
      KeyValueStore keyValueStore = (KeyValueStore) this.taskSideInputStorageManager.getStore(store);
      Collection<Entry<?, ?>> entriesToBeWritten = storeProcessor.process(envelope, keyValueStore);

      // TODO: SAMZA-2255: optimize writes to side input stores
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

    this.lastProcessedOffsets.put(envelopeSSP, envelopeOffset);
  }

  /**
   * Flushes the underlying {@link TaskSideInputStorageManager}
   * Synchronized inorder to be exclusive with process()
   */
  public synchronized void flush() {
    this.taskSideInputStorageManager.flush(this.lastProcessedOffsets);
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
    return this.startingOffsets.get(ssp);
  }

  /**
   * Gets the last processed offset for the given side input {@link SystemStreamPartition}.
   *
   * @param ssp side input system stream partition to get the last processed offset for
   * @return the last processed offset
   */
  public String getLastProcessedOffset(SystemStreamPartition ssp) {
    return this.lastProcessedOffsets.get(ssp);
  }

  /**
   * Stops the underlying storage manager at the last processed offsets.
   */
  public void stop() {
    this.taskSideInputStorageManager.stop(this.lastProcessedOffsets);
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

    this.sspToStores.keySet().forEach(ssp -> {
        String fileOffset = fileOffsets.get(ssp);
        String oldestOffset = oldestOffsets.get(ssp);

        startingOffsets.put(ssp,
            this.storageManagerUtil.getStartingOffset(
                ssp, this.systemAdmins.getSystemAdmin(ssp.getSystem()), fileOffset, oldestOffset));
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
   * @return a {@link Map} of {@link SystemStreamPartition} to their oldest offset. If partitionMetadata could not be
   * obtained for any {@link SystemStreamPartition} the offset for it is populated as null.
   */
  @VisibleForTesting
  Map<SystemStreamPartition, String> getOldestOffsets() {
    Map<SystemStreamPartition, String> oldestOffsets = new HashMap<>();

    // Step 1
    Map<SystemStream, List<SystemStreamPartition>> systemStreamToSsp = this.sspToStores.keySet().stream()
        .collect(Collectors.groupingBy(SystemStreamPartition::getSystemStream));

    // Step 2
    Map<SystemStream, SystemStreamMetadata> metadata = JavaConverters.mapAsJavaMapConverter(
        this.streamMetadataCache.getStreamMetadata(
            JavaConverters.asScalaSetConverter(systemStreamToSsp.keySet()).asScala().toSet(), false)).asJava();

    // Step 3
    metadata.forEach((systemStream, systemStreamMetadata) -> {

        // get the partition metadata for each system stream
        Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata =
            systemStreamMetadata.getSystemStreamPartitionMetadata();

        // For SSPs belonging to the system stream, use the partition metadata to get the oldest offset
        // if partitionMetadata was not obtained for any SSP, populate oldest-offset as null
        // Because of https://bugs.openjdk.java.net/browse/JDK-8148463 using lambda will NPE when getOldestOffset() is null
        for (SystemStreamPartition ssp : systemStreamToSsp.get(systemStream)) {
          oldestOffsets.put(ssp, partitionMetadata.get(ssp.getPartition()).getOldestOffset());
        }
      });

    return oldestOffsets;
  }

  /**
   * Validates that each store has an associated {@link SideInputsProcessor}
   */
  private void validateProcessorConfiguration(Set<String> stores, Map<String, SideInputsProcessor> storeToProcessor) {
    stores.forEach(storeName -> {
        if (!storeToProcessor.containsKey(storeName)) {
          throw new SamzaException(
              String.format("Side inputs processor missing for store: %s.", storeName));
        }
      });
  }
}
