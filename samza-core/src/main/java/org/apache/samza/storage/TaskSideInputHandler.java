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
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.executors.KeyBasedExecutorService;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

// TODO pick a better name for this class?

/**
 * This class encapsulates all processing logic / state for all side input SSPs within a task.
 */
public class TaskSideInputHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TaskSideInputHandler.class);

  private final StorageManagerUtil storageManagerUtil = new StorageManagerUtil();

  private final TaskName taskName;
  private final TaskSideInputStorageManager taskSideInputStorageManager;
  private final Map<SystemStreamPartition, Set<String>> sspToStores;
  private final Map<String, SideInputsProcessor> storeToProcessor;
  private final SystemAdmins systemAdmins;
  private final StreamMetadataCache streamMetadataCache;
  // marks the offsets per SSP that must be bootstrapped to, inclusive. container startup will block until these offsets have been processed
  private final Map<SystemStreamPartition, String> sspBootstrapOffsets;

  // these objects are SHARED WITH CONTAINER STORAGE MANAGER
  // used to coordinate updates of checkpoint offsets by ContainerStorageManager
  private final Map<SystemStreamPartition, Object> sspLockObjects;
  // indicates the latest checkpoint per SSP. updated by ContainerStorageManager background thread
  private final Map<SystemStreamPartition, Optional<String>> checkpointedOffsets;
  // used to block container until each SSP reaches its bootstrap offset
  private final CountDownLatch sideInputTasksCaughtUp;

  private final Map<SystemStreamPartition, String> startingOffsets;
  private final Map<SystemStreamPartition, String> lastProcessedOffsets;
  private final KeyBasedExecutorService checkpointedSSPExecutor;
  private final KeyBasedExecutorService nonCheckpointedSSPExecutor;

  public TaskSideInputHandler(
      TaskName taskName,
      TaskSideInputStorageManager taskSideInputStorageManager,
      Map<String, Set<SystemStreamPartition>> storeToSSPs,
      Map<String, SideInputsProcessor> storeToProcessor,
      SystemAdmins systemAdmins,
      StreamMetadataCache streamMetadataCache,
      CountDownLatch sideInputTasksCaughtUp,
      Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> initialSSPMetadata,
      Map<SystemStreamPartition, Object> sspLockObjects,
      Map<SystemStreamPartition, Optional<String>> checkpointOffsets) {
    this.taskName = taskName;
    this.taskSideInputStorageManager = taskSideInputStorageManager;
    this.systemAdmins = systemAdmins;
    this.streamMetadataCache = streamMetadataCache;
    this.sideInputTasksCaughtUp = sideInputTasksCaughtUp;
    this.storeToProcessor = storeToProcessor;
    this.sspLockObjects = Collections.unmodifiableMap(sspLockObjects);
    this.checkpointedOffsets = Collections.unmodifiableMap(checkpointOffsets);

    this.sspToStores = storeToSSPs.entrySet().stream()
        .flatMap(storeAndSSPs -> storeAndSSPs.getValue().stream()
            .map(ssp -> new AbstractMap.SimpleImmutableEntry<>(ssp, storeAndSSPs.getKey())))
        .collect(Collectors.groupingBy(
            Map.Entry::getKey,
            Collectors.mapping(Map.Entry::getValue, Collectors.toSet())));

    // for non-checkpointed SSPs, use their newest offset. for checkpointed SSPs, use their current checkpoint
    this.sspBootstrapOffsets = new HashMap<>();
    initialSSPMetadata.entrySet().stream()
        // only SSPs for this task
        .filter(entry -> this.sspToStores.containsKey(entry.getKey()))
        // that do not have checkpoints
        .filter(entry -> !this.checkpointedOffsets.containsKey(entry.getKey()))
        .forEach(entry -> this.sspBootstrapOffsets.put(entry.getKey(), entry.getValue().getNewestOffset()));
    this.checkpointedOffsets.entrySet().stream()
        .filter(entry -> entry.getValue().isPresent())
        .forEach(entry -> this.sspBootstrapOffsets.put(entry.getKey(), entry.getValue().get()));

    this.lastProcessedOffsets = taskSideInputStorageManager.getFileOffsets();
    this.startingOffsets = getStartingOffsets(this.lastProcessedOffsets, getOldestOffsets());
    this.startingOffsets.forEach((ssp, offset) -> checkCaughtUp(ssp, offset, true));


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

  public void process(IncomingMessageEnvelope envelope, TaskCallbackFactory callbackFactory) {
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
            // the checkpoint updating thread wakes it back up
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

    // TODO how to coordinate fairly
    // callback invocation should wait for all futures to finish, or for any to fail
    executorToUse.submit(() -> {
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
        checkCaughtUp(envelopeSSP, envelopeOffset, false);
        callback.complete();
      });
  }

  public String getStartingOffset(SystemStreamPartition ssp) {
    return this.startingOffsets.get(ssp);
  }

  public String getLastProcessedOffset(SystemStreamPartition ssp) {
    return this.lastProcessedOffsets.get(ssp);
  }

  public void stop() {
    this.checkpointedSSPExecutor.shutdownNow();
    this.nonCheckpointedSSPExecutor.shutdownNow();
  }

  /**
   * Checks if whether the given offset for the SSP has reached the latest offset (determined at class construction time),
   * removing it from the list of SSPs to catch up. Once the set of SSPs to catch up becomes empty, the latch shared
   * with {@link ContainerStorageManager} will be counted down exactly once.
   *
   * @param ssp The SSP to be checked
   * @param currentOffset The offset to be checked
   * @param isStartingOffset Indicates whether the offset being checked is the starting offset of the SSP (and thus has
   *                         not yet been processed). This will be set to true when each SSP's starting offset is checked
   *                         at startup.
   */
  private synchronized void checkCaughtUp(SystemStreamPartition ssp, String currentOffset, boolean isStartingOffset) {
    String caughtUpOffset = sspBootstrapOffsets.get(ssp);

    LOG.trace("Checking offset {} against {} for {}. isStartingOffset: {}", currentOffset, caughtUpOffset, ssp, isStartingOffset);

    Integer comparatorResult;
    if (currentOffset == null || caughtUpOffset == null) {
      comparatorResult = -1;
    } else {
      SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(ssp.getSystem());
      comparatorResult = systemAdmin.offsetComparator(currentOffset, caughtUpOffset);
    }

    // If the starting offset, it must be greater (since the envelope at the starting offset will not yet have been processed)
    // If not the starting offset, it must be greater than OR equal
    if (comparatorResult != null && ((isStartingOffset && comparatorResult > 0) || (!isStartingOffset && comparatorResult >= 0))) {
      LOG.info("Side input ssp {} has caught up to offset {}.", ssp, caughtUpOffset);
      // if its caught up, we remove the ssp from the map
      this.sspBootstrapOffsets.remove(ssp);
      if (this.sspBootstrapOffsets.isEmpty()) {
        // if the metadata list is now empty, all SSPs in the task are caught up so count down the latch
        // this will only happen once, when the last ssp catches up
        this.sideInputTasksCaughtUp.countDown();
      }
    }
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
   * @return a {@link Map} of {@link SystemStreamPartition} to their oldest offset.
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
        Map<SystemStreamPartition, String> offsets = systemStreamToSsp.get(systemStream).stream()
            .collect(
                Collectors.toMap(Function.identity(), ssp -> partitionMetadata.get(ssp.getPartition()).getOldestOffset()));

        oldestOffsets.putAll(offsets);
      });

    return oldestOffsets;
  }
}
