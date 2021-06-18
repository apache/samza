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
package org.apache.samza.startpoint;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.system.SystemStreamPartition;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The StartpointManager reads and writes {@link Startpoint} to the provided {@link MetadataStore}
 *
 * The intention for the StartpointManager is to maintain a strong contract between the caller
 * and how Startpoints are stored in the underlying MetadataStore.
 *
 * Startpoints are written in the MetadataStore using keys of two different formats:
 * 1) {@link SystemStreamPartition} only
 * 2) A combination of {@link SystemStreamPartition} and {@link TaskName}
 *
 * Startpoints are then fanned out to a fan out namespace in the MetadataStore by the
 * {@link org.apache.samza.clustermanager.ClusterBasedJobCoordinator} or the standalone
 * {@link org.apache.samza.coordinator.JobCoordinator} upon startup and the
 * {@link org.apache.samza.checkpoint.OffsetManager} gets the fan outs to set the starting offsets per task and per
 * {@link SystemStreamPartition}. The fan outs are deleted once the offsets are committed to the checkpoint.
 *
 * The read, write and delete methods are intended for external callers.
 * The fan out methods are intended to be used within a job coordinator.
 */
public class StartpointManager {
  private static final Integer VERSION = 1;
  public static final String NAMESPACE = "samza-startpoint-v" + VERSION;

  static final Duration DEFAULT_EXPIRATION_DURATION = Duration.ofHours(12);

  private static final Logger LOG = LoggerFactory.getLogger(StartpointManager.class);
  private static final String NAMESPACE_FAN_OUT = NAMESPACE + "-fan-out";

  private final NamespaceAwareCoordinatorStreamStore fanOutStore;
  private final NamespaceAwareCoordinatorStreamStore readWriteStore;
  private final ObjectMapper objectMapper = StartpointObjectMapper.getObjectMapper();

  private boolean stopped = true;

  /**
   *  Builds the StartpointManager based upon the provided {@link MetadataStore} that is instantiated.
   *  Setting up a metadata store instance is expensive which requires opening multiple connections
   *  and reading tons of information. Fully instantiated metadata store is passed in as a constructor argument
   *  to reuse it across different utility classes.
   *
   * @param metadataStore an instance of {@link MetadataStore} used to read/write the start-points.
   */
  public StartpointManager(MetadataStore metadataStore) {
    Preconditions.checkNotNull(metadataStore, "MetadataStore cannot be null");

    this.readWriteStore = new NamespaceAwareCoordinatorStreamStore(metadataStore, NAMESPACE);
    this.fanOutStore = new NamespaceAwareCoordinatorStreamStore(metadataStore, NAMESPACE_FAN_OUT);
    LOG.info("Startpoints are written to namespace: {} and fanned out to namespace: {} in the metadata store of type: {}",
        NAMESPACE, NAMESPACE_FAN_OUT, metadataStore.getClass().getCanonicalName());
  }

  /**
   * Perform startup operations. Method is idempotent.
   */
  public void start() {
    if (stopped) {
      LOG.info("starting");
      readWriteStore.init();
      fanOutStore.init();
      stopped = false;
    } else {
      LOG.warn("already started");
    }
  }

  /**
   * Perform teardown operations. Method is idempotent.
   */
  public void stop() {
    if (!stopped) {
      LOG.info("stopping");
      readWriteStore.close();
      fanOutStore.close();
      stopped = true;
    } else {
      LOG.warn("already stopped");
    }
  }

    /**
     * Writes a {@link Startpoint} that defines the start position for a {@link SystemStreamPartition}.
     * @param ssp The {@link SystemStreamPartition} to map the {@link Startpoint} against.
     * @param startpoint Reference to a Startpoint object.
     */
  public void writeStartpoint(SystemStreamPartition ssp, Startpoint startpoint) {
    writeStartpoint(ssp, null, startpoint);
  }

  /**
   * Writes a {@link Startpoint} that defines the start position for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp The {@link SystemStreamPartition} to map the {@link Startpoint} against.
   * @param taskName The {@link TaskName} to map the {@link Startpoint} against.
   * @param startpoint Reference to a Startpoint object.
   */
  public void writeStartpoint(SystemStreamPartition ssp, TaskName taskName, Startpoint startpoint) {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");
    Preconditions.checkNotNull(startpoint, "Startpoint cannot be null");

    try {
      readWriteStore.put(toReadWriteStoreKey(ssp, taskName), objectMapper.writeValueAsBytes(startpoint));
      readWriteStore.flush();
    } catch (Exception ex) {
      throw new SamzaException(String.format(
          "Startpoint for SSP: %s and task: %s may not have been written to the metadata store.", ssp, taskName), ex);
    }
  }

  /**
   * Returns the last {@link Startpoint} that defines the start position for a {@link SystemStreamPartition}.
   * @param ssp The {@link SystemStreamPartition} to fetch the {@link Startpoint} for.
   * @return {@link Optional} of {@link Startpoint} for the {@link SystemStreamPartition}.
   *         It is empty if it does not exist or if it is too stale.
   */
  @VisibleForTesting
  public Optional<Startpoint> readStartpoint(SystemStreamPartition ssp) {
    Map<String, byte[]> startpointBytes = readWriteStore.all();
    // there is no task-name to use as key for the startpoint in this case (only the ssp), so we use a null task-name
    return readStartpoint(startpointBytes, ssp, null);
  }

  /**
   * Returns the last {@link Startpoint} that defines the start position for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp The {@link SystemStreamPartition} to fetch the {@link Startpoint} for.
   * @param taskName the {@link TaskName} to fetch the {@link Startpoint} for.
   * @return {@link Optional} of {@link Startpoint} for the {@link SystemStreamPartition}.
   *         It is empty if it does not exist or if it is too stale.
   */
  @VisibleForTesting
  public Optional<Startpoint> readStartpoint(SystemStreamPartition ssp, TaskName taskName) {
    Map<String, byte[]> startpointBytes = readWriteStore.all();
    return readStartpoint(startpointBytes, ssp, taskName);
  }

  /**
   * Returns the {@link Startpoint} for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp The {@link SystemStreamPartition} to fetch the {@link Startpoint} for.
   * @param taskName The {@link TaskName} to fetch the {@link Startpoint} for.
   * @return {@link Optional} of {@link Startpoint} for the {@link SystemStreamPartition} and {@link TaskName}.
   *         It is empty if it does not exist or if it is too stale.
   */
  public Optional<Startpoint> readStartpoint(Map<String, byte[]> startpointMap, SystemStreamPartition ssp, TaskName taskName) {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");

    byte[] startpointBytes = startpointMap.get(toReadWriteStoreKey(ssp, taskName));

    if (ArrayUtils.isNotEmpty(startpointBytes)) {
      try {
        Startpoint startpoint = objectMapper.readValue(startpointBytes, Startpoint.class);
        if (Instant.now().minus(DEFAULT_EXPIRATION_DURATION).isBefore(Instant.ofEpochMilli(startpoint.getCreationTimestamp()))) {
          return Optional.of(startpoint); // return if deserializable and if not stale
        }
        LOG.warn("Creation timestamp: {} of startpoint: {} has crossed the expiration duration: {}. Ignoring it",
            startpoint.getCreationTimestamp(), startpoint, DEFAULT_EXPIRATION_DURATION);
      } catch (IOException ex) {
        throw new SamzaException(ex);
      }
    }

    return Optional.empty();
  }


  /**
   * Deletes the {@link Startpoint} for a {@link SystemStreamPartition}
   * @param ssp The {@link SystemStreamPartition} to delete the {@link Startpoint} for.
   */
  public void deleteStartpoint(SystemStreamPartition ssp) {
    deleteStartpoint(ssp, null);
  }

  /**
   * Deletes the {@link Startpoint} for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp ssp The {@link SystemStreamPartition} to delete the {@link Startpoint} for.
   * @param taskName ssp The {@link TaskName} to delete the {@link Startpoint} for.
   */
  public void deleteStartpoint(SystemStreamPartition ssp, TaskName taskName) {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");

    readWriteStore.delete(toReadWriteStoreKey(ssp, taskName));
    readWriteStore.flush();
  }

  /**
   * Deletes all {@link Startpoint}s
   */
  public void deleteAllStartpoints() {
    Set<String> readWriteKeys = readWriteStore.all().keySet();
    for (String key : readWriteKeys) {
      readWriteStore.delete(key);
    }
    readWriteStore.flush();
  }

  /**
   * The Startpoints that are written to with {@link #writeStartpoint(SystemStreamPartition, Startpoint)} and with
   * {@link #writeStartpoint(SystemStreamPartition, TaskName, Startpoint)} are moved from a "read-write" namespace
   * to a "fan out" namespace.
   * This method is not atomic nor thread-safe. The intent is for the Samza Processor's coordinator to use this
   * method to assign the Startpoints to the appropriate tasks.
   * @param taskToSSPs Determines which {@link TaskName} each {@link SystemStreamPartition} maps to.
   * @return The set of active {@link TaskName}s that were fanned out to.
   */
  public Map<TaskName, Map<SystemStreamPartition, Startpoint>> fanOut(Map<TaskName, Set<SystemStreamPartition>> taskToSSPs) throws IOException {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkArgument(MapUtils.isNotEmpty(taskToSSPs), "taskToSSPs cannot be null or empty");

    // construct fan out with the existing readWriteStore entries and mark the entries for deletion after fan out
    Instant now = Instant.now();
    HashMultimap<SystemStreamPartition, TaskName> deleteKeys = HashMultimap.create();
    HashMap<TaskName, StartpointFanOutPerTask> fanOuts = new HashMap<>();
    Map<String, byte[]> startpointMap = readWriteStore.all();

    for (TaskName taskName : taskToSSPs.keySet()) {
      Set<SystemStreamPartition> ssps = taskToSSPs.get(taskName);
      if (CollectionUtils.isEmpty(ssps)) {
        LOG.warn("No SSPs are mapped to taskName: {}", taskName.getTaskName());
        continue;
      }
      for (SystemStreamPartition ssp : ssps) {
        Optional<Startpoint> startpoint = readStartpoint(startpointMap, ssp, null); // Read SSP-only key
        startpoint.ifPresent(sp -> deleteKeys.put(ssp, null));

        Optional<Startpoint> startpointForTask = readStartpoint(startpointMap, ssp, taskName); // Read SSP+taskName key
        startpointForTask.ifPresent(sp -> deleteKeys.put(ssp, taskName));

        Optional<Startpoint> startpointWithPrecedence = resolveStartpointPrecendence(startpoint, startpointForTask);
        if (!startpointWithPrecedence.isPresent()) {
          continue;
        }

        fanOuts.putIfAbsent(taskName, new StartpointFanOutPerTask(now));
        fanOuts.get(taskName).getFanOuts().put(ssp, startpointWithPrecedence.get());
      }
    }

    if (fanOuts.isEmpty()) {
      LOG.debug("No fan outs created.");
      return ImmutableMap.of();
    }

    LOG.info("Fanning out to {} tasks", fanOuts.size());

    // Fan out to store
    for (TaskName taskName : fanOuts.keySet()) {
      String fanOutKey = toFanOutStoreKey(taskName);
      StartpointFanOutPerTask newFanOut = fanOuts.get(taskName);
      fanOutStore.put(fanOutKey, objectMapper.writeValueAsBytes(newFanOut));
    }
    fanOutStore.flush();

    for (SystemStreamPartition ssp : deleteKeys.keySet()) {
      for (TaskName taskName : deleteKeys.get(ssp)) {
        if (taskName != null) {
          deleteStartpoint(ssp, taskName);
        } else {
          deleteStartpoint(ssp);
        }
      }
    }

    return ImmutableMap.copyOf(fanOuts.entrySet().stream()
        .collect(Collectors.toMap(fo -> fo.getKey(), fo -> fo.getValue().getFanOuts())));
  }

  /**
   * Read the fanned out {@link Startpoint}s for the given {@link TaskName}
   * @param taskName to read the fan out Startpoints for
   * @return fanned out Startpoints
   */
  public Map<SystemStreamPartition, Startpoint> getFanOutForTask(TaskName taskName) throws IOException {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkNotNull(taskName, "TaskName cannot be null");

    byte[] fanOutBytes = fanOutStore.get(toFanOutStoreKey(taskName));
    if (ArrayUtils.isEmpty(fanOutBytes)) {
      return ImmutableMap.of();
    }
    StartpointFanOutPerTask startpointFanOutPerTask = objectMapper.readValue(fanOutBytes, StartpointFanOutPerTask.class);
    return ImmutableMap.copyOf(startpointFanOutPerTask.getFanOuts());
  }

  /**
   * Deletes the fanned out {@link Startpoint} for the given {@link TaskName}
   * @param taskName to delete the fan out Startpoints for
   */
  public void removeFanOutForTask(TaskName taskName) {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkNotNull(taskName, "TaskName cannot be null");

    fanOutStore.delete(toFanOutStoreKey(taskName));
    fanOutStore.flush();
  }

  /**
   * Deletes all fanned out {@link Startpoint}s
   */
  public void removeAllFanOuts() {
    Set<String> fanOutKeys = fanOutStore.all().keySet();
    for (String key : fanOutKeys) {
      fanOutStore.delete(key);
    }
    fanOutStore.flush();
  }

  @VisibleForTesting
  MetadataStore getReadWriteStore() {
    return readWriteStore;
  }

  @VisibleForTesting
  MetadataStore getFanOutStore() {
    return fanOutStore;
  }

  @VisibleForTesting
  ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  private static Optional<Startpoint> resolveStartpointPrecendence(Optional<Startpoint> startpoint1, Optional<Startpoint> startpoint2) {
    if (startpoint1.isPresent() && startpoint2.isPresent()) {
      // if SSP-only and SSP+taskName startpoints both exist, resolve to the one with the latest timestamp
      if (startpoint1.get().getCreationTimestamp() > startpoint2.get().getCreationTimestamp()) {
        return startpoint1;
      }
      return startpoint2;
    }
    return startpoint1.isPresent() ? startpoint1 : startpoint2;
  }

  private static String toReadWriteStoreKey(SystemStreamPartition ssp, TaskName taskName) {
    Preconditions.checkArgument(ssp != null, "SystemStreamPartition should be defined");
    Preconditions.checkArgument(StringUtils.isNotBlank(ssp.getSystem()), "System should be defined");
    Preconditions.checkArgument(StringUtils.isNotBlank(ssp.getStream()), "Stream should be defined");
    Preconditions.checkArgument(ssp.getPartition() != null, "Partition should be defined");

    String storeKey = ssp.getSystem() + "." + ssp.getStream() + "." + String.valueOf(ssp.getPartition().getPartitionId());
    if (taskName != null) {
      storeKey += "." + taskName.getTaskName();
    }
    return storeKey;
  }

  private static String toFanOutStoreKey(TaskName taskName) {
    Preconditions.checkArgument(taskName != null, "TaskName should be defined");
    Preconditions.checkArgument(StringUtils.isNotBlank(taskName.getTaskName()), "TaskName should not be blank");

    return taskName.getTaskName();
  }
}
