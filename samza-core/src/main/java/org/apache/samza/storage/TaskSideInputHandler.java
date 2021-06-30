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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.executors.KeyBasedExecutorService;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCallbackFactory;
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
  // indicates to ContainerStorageManager that all side input ssps in this task are caught up
  private final CountDownLatch taskCaughtUpLatch;
  // used to coordinate updates of checkpoint offsets by ContainerStorageManager
  private final Map<SystemStreamPartition, Object> sspLockObjects;
  // indicates the latest checkpoint per SSP. updated by ContainerStorageManager background thread
  private final Map<SystemStreamPartition, Optional<String>> checkpointedOffsets;
  private final KeyBasedExecutorService checkpointedSSPExecutor;
  private final KeyBasedExecutorService nonCheckpointedSSPExecutor;

  private Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> initialSideInputSSPMetadata;
  private Map<SystemStreamPartition, String> startingOffsets;


  public TaskSideInputHandler(TaskName taskName, TaskMode taskMode, File storeBaseDir,
      Map<String, StorageEngine> storeToStorageEngines, Map<String, Set<SystemStreamPartition>> storeToSSPs,
      Map<String, SideInputsProcessor> storeToProcessor, SystemAdmins systemAdmins,
      StreamMetadataCache streamMetadataCache, CountDownLatch taskCaughtUpLatch, Clock clock,
      Map<SystemStreamPartition, Object> sspLockObjects, Map<SystemStreamPartition, Optional<String>> checkpointOffsets) {
    validateProcessorConfiguration(storeToSSPs.keySet(), storeToProcessor);

    this.taskName = taskName;
    this.systemAdmins = systemAdmins;
    this.streamMetadataCache = streamMetadataCache;
    this.storeToProcessor = storeToProcessor;
    this.taskCaughtUpLatch = taskCaughtUpLatch;

    this.sspToStores = new HashMap<>();
    storeToSSPs.forEach((store, ssps) -> {
      for (SystemStreamPartition ssp: ssps) {
        this.sspToStores.computeIfAbsent(ssp, key -> new HashSet<>());
        this.sspToStores.computeIfPresent(ssp, (key, value) -> {
          value.add(store);
          return value;
        });
      }
    });

    this.taskSideInputStorageManager = new TaskSideInputStorageManager(taskName,
        taskMode,
        storeBaseDir,
        storeToStorageEngines,
        storeToSSPs,
        clock);

    this.sspLockObjects = Collections.unmodifiableMap(sspLockObjects);
    this.checkpointedOffsets = Collections.unmodifiableMap(checkpointOffsets);

    Set<String> checkpointedStores = this.sspToStores.entrySet().stream()
        .filter(sspAndStores -> this.checkpointedOffsets.containsKey(sspAndStores.getKey()))
        .flatMap(sspAndStores -> sspAndStores.getValue().stream())
        .collect(Collectors.toSet());

    Set<String> nonCheckpointedStores = this.sspToStores.entrySet().stream()
        .filter(sspAndStores -> !this.checkpointedOffsets.containsKey(sspAndStores.getKey()))
        .flatMap(sspAndStores -> sspAndStores.getValue().stream())
        .collect(Collectors.toSet());

    this.checkpointedSSPExecutor = new KeyBasedExecutorService(Math.max(1, checkpointedStores.size()));
    this.nonCheckpointedSSPExecutor = new KeyBasedExecutorService(Math.max(1, nonCheckpointedStores.size()));
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

    this.initialSideInputSSPMetadata = getInitialSideInputSSPMetadata();
    LOG.info("Task {} will catch up to offsets {}", this.taskName, this.initialSideInputSSPMetadata);

    this.startingOffsets.forEach((ssp, offset) -> checkCaughtUp(ssp, offset, SystemStreamMetadata.OffsetType.UPCOMING));
  }

  /**
   * Retrieves the newest offset for each SSP
   *
   * @return a map of SSP to metadata
   */
  private Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> getInitialSideInputSSPMetadata() {
    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> initialSideInputSSPMetadata = new HashMap<>();
    for (SystemStreamPartition ssp : this.sspToStores.keySet()) {
      boolean partitionsMetadataOnly = false;
      SystemStreamMetadata systemStreamMetadata = this.streamMetadataCache.getSystemStreamMetadata(ssp.getSystemStream(), partitionsMetadataOnly);
      if (systemStreamMetadata != null) {
        SystemStreamMetadata.SystemStreamPartitionMetadata sspMetadata =
            systemStreamMetadata.getSystemStreamPartitionMetadata().get(ssp.getPartition());
        initialSideInputSSPMetadata.put(ssp, sspMetadata);
      }
    }
    return initialSideInputSSPMetadata;
  }

  /**
   * Processes the incoming side input message envelope and updates the last processed offset for its SSP.
   * Synchronized inorder to be exclusive with flush().
   *
   * @param envelope incoming envelope to be processed
   * @param callbackFactory
   */
  public synchronized void process(IncomingMessageEnvelope envelope, TaskCallbackFactory callbackFactory) {
    TaskCallback callback = callbackFactory.createCallback();
    SystemStreamPartition envelopeSSP = envelope.getSystemStreamPartition();
    String envelopeOffset = envelope.getOffset();
    boolean isSspCheckpointed = this.checkpointedOffsets.containsKey(envelopeSSP);
    SystemAdmin systemAdmin = this.systemAdmins.getSystemAdmin(envelopeSSP.getSystem());
    KeyBasedExecutorService executorToUse = isSspCheckpointed
        ? this.checkpointedSSPExecutor
        : this.nonCheckpointedSSPExecutor;

    List<Future<?>> storeFutures = new LinkedList<>();

    sspToStores.get(envelopeSSP).forEach(store -> {
      Future<?> storeFuture = executorToUse.submitOrdered(store, () -> {
        // if the incoming envelope has an offset greater than the checkpoint, have this thread wait until
        // the checkpoint updating thread in ContainerStorageManager wakes it back up
        synchronized (this.sspLockObjects.get(envelopeSSP)) {
          while (isSspCheckpointed && this.checkpointedOffsets.get(envelopeSSP).isPresent()
              && systemAdmin.offsetComparator(envelopeOffset, this.checkpointedOffsets.get(envelopeSSP).get()) > 0) {
            try {
              this.sspLockObjects.get(envelopeSSP).wait();
            } catch (InterruptedException e) {
              LOG.error("Side input restore interrupted when waiting for new checkpoint. Task: " + this.taskName, e);
              // fail, return and do not process the envelope
              callback.failure(e);
              return;
            }
          }
        }

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
      });

      // collect the futures, in order to wait on them to coordinate callback invocation
      storeFutures.add(storeFuture);
    });

    // TODO this needs to be async to support task concurrency > 1
    // wait on the work submitted for other stores
    for (Future<?> future : storeFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        callback.failure(e);
        // callback should only be invoked once; return now and stop waiting
        return;
      }
    }
    this.lastProcessedOffsets.put(envelopeSSP, envelopeOffset);
    checkCaughtUp(envelopeSSP, envelopeOffset, SystemStreamMetadata.OffsetType.NEWEST);
    callback.complete();
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
   * Stops the underlying storage manager at the last processed offsets. Any pending and upcoming invocations
   * of {@link #process} and {@link #flush} are assumed to have completed or ceased prior to calling this method.
   */
  public void stop() {
    this.checkpointedSSPExecutor.shutdownNow();
    this.nonCheckpointedSSPExecutor.shutdownNow();
    this.taskSideInputStorageManager.stop(this.lastProcessedOffsets);
  }

  /**
   * Gets the starting offsets for the {@link SystemStreamPartition}s belonging to all the side input stores. See doc
   * of {@link StorageManagerUtil#getStartingOffset} for how file offsets and oldest offsets for each SSP are
   * reconciled.
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
   * An SSP is considered caught up once the offset indicated for it in {@link #initialSideInputSSPMetadata} has been
   * processed. Once the set of SSPs to catch up becomes empty, the latch for the task will count down, notifying
   * {@link ContainerStorageManager} that it is caught up.
   *
   * @param ssp The SSP to be checked
   * @param currentOffset The offset to be checked
   * @param offsetTypeToCheck The type offset to compare {@code currentOffset} to.
   */
  private void checkCaughtUp(SystemStreamPartition ssp, String currentOffset, SystemStreamMetadata.OffsetType offsetTypeToCheck) {
    SystemStreamMetadata.SystemStreamPartitionMetadata sspMetadata = this.initialSideInputSSPMetadata.get(ssp);
    String offsetToCheck = sspMetadata == null ? null : sspMetadata.getOffset(offsetTypeToCheck);

    LOG.trace("Checking offset {} against {} offset {} for {}.", currentOffset, offsetToCheck, offsetTypeToCheck, ssp);

    Integer comparatorResult;
    if (currentOffset == null || offsetToCheck == null) {
      comparatorResult = -1;
    } else {
      SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(ssp.getSystem());
      comparatorResult = systemAdmin.offsetComparator(currentOffset, offsetToCheck);
    }

    // The SSP is no longer lagging if the envelope's offset is greater than or equal to the
    // latest offset.
    if (comparatorResult != null && comparatorResult.intValue() >= 0) {
      LOG.info("Side input ssp {} has caught up to offset {}.", ssp, offsetToCheck);
      // if its caught up, we remove the ssp from the map
      this.initialSideInputSSPMetadata.remove(ssp);
      if (this.initialSideInputSSPMetadata.isEmpty()) {
        // if the metadata list is now empty, all SSPs in the task are caught up so count down the latch
        // this will only happen once, when the last ssp catches up
        this.taskCaughtUpLatch.countDown();
      }
    }
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
