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
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The StartpointManager reads and writes {@link Startpoint} to the {@link MetadataStore} defined by
 * the configuration task.startpoint.metadata.store.factory.
 *
 * Startpoints are keyed in the MetadataStore by two different formats:
 * 1) Only by {@link SystemStreamPartition}
 * 2) A combination of {@link SystemStreamPartition} and {@link TaskName}
 *
 * The intention for the StartpointManager is to maintain a strong contract between the caller
 * and how Startpoints are stored in the underlying MetadataStore.
 */
public class StartpointManager {
  private static final Logger LOG = LoggerFactory.getLogger(StartpointManager.class);
  private static final String NAMESPACE = "samza-startpoint-ver1";
  private static final Duration DEFAULT_EXPIRATION_DURATION = Duration.ofHours(12);

  private final MetadataStore metadataStore;
  private final StartpointSerde startpointSerde;

  // Startpoints are considered to be stale if they were created before this duration.
  // TODO: Determine if it makes sense to make this configurable.
  private final Duration expirationDuration;

  private boolean started = false;

  StartpointManager(MetadataStore metadataStore, StartpointSerde startpointSerde, Duration expirationDuration) {
    this.metadataStore = metadataStore;
    this.startpointSerde = startpointSerde;
    this.expirationDuration = expirationDuration;

    // Register each Startpoint type with the serde
    startpointSerde.register(StartpointSpecific.class);
    startpointSerde.register(StartpointTimestamp.class);
    startpointSerde.register(StartpointEarliest.class);
    startpointSerde.register(StartpointLatest.class);
    startpointSerde.register(StartpointBootstrap.class);
  }

  /**
   * Creates a {@link StartpointManager} instance with the provided {@link MetadataStoreFactory}
   * @param metadataStoreFactory {@link MetadataStoreFactory} used to construct the underlying store.
   * @param config {@link Config} required for the underlying store.
   * @param metricsRegistry {@link MetricsRegistry} to hook into the underlying store.
   * @return {@link StartpointManager} instance.
   */
  public static StartpointManager getWithMetadataStore(MetadataStoreFactory metadataStoreFactory, Config config, MetricsRegistry metricsRegistry) {
    Preconditions.checkNotNull(metadataStoreFactory, "MetadataStoreFactory cannot be null");
    Preconditions.checkNotNull(config, "Config cannot be null");
    Preconditions.checkNotNull(metricsRegistry, "MetricsRegistry cannot be null");

    MetadataStore metadataStore = metadataStoreFactory.getMetadataStore(NAMESPACE, config, metricsRegistry);

    // TODO: Determine if it is worth it to make the duration expiration configurable
    StartpointManager startpointManager = new StartpointManager(metadataStore, new StartpointSerde(), DEFAULT_EXPIRATION_DURATION);

    LOG.info("StartpointManager created with metadata store: {}", metadataStore.getClass().getCanonicalName());

    return startpointManager;
  }

  /**
   * Starts the underlying {@link MetadataStore}
   */
  public void start() {
    if (!started) {
      metadataStore.init();
      started = true;
    } else {
      LOG.warn("StartpointManager already started");
    }
  }

  /**
   * Writes a {@link Startpoint} that defines the start position for a {@link SystemStreamPartition}.
   * @param ssp The {@link SystemStreamPartition} to map the {@link Startpoint} against.
   * @param startpoint Reference to a Startpoint object.
   */
  public void writeStartpoint(SystemStreamPartition ssp, Startpoint startpoint) {
    writeStartpointForTask(ssp, null, startpoint);
  }

  /**
   * Writes a {@link Startpoint} that defines the start position for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp The {@link SystemStreamPartition} to map the {@link Startpoint} against.
   * @param taskName The {@link TaskName} to map the {@link Startpoint} against.
   * @param startpoint Reference to a Startpoint object.
   */
  public void writeStartpointForTask(SystemStreamPartition ssp, TaskName taskName, Startpoint startpoint) {
    Preconditions.checkState(started, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");
    Preconditions.checkNotNull(startpoint, "Startpoint cannot be null");

    try {
      metadataStore.put(toStoreKey(ssp, taskName), startpointSerde.toBytes(startpoint));
    } catch (Exception ex) {
      throw new SamzaException(String.format(
          "Startpoint for SSP: %s and task: %s may not have been written to the metadata store.", ssp, taskName), ex);
    }
  }

  /**
   * Returns the last {@link Startpoint} that defines the start position for a {@link SystemStreamPartition}.
   * @param ssp The {@link SystemStreamPartition} to fetch the {@link Startpoint} for.
   * @return {@link Startpoint} for the {@link SystemStreamPartition}, or null if it does not exist or if it is too stale
   */
  public Startpoint readStartpoint(SystemStreamPartition ssp) {
    return readStartpointForTask(ssp, null);
  }

  /**
   * Returns the {@link Startpoint} for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp The {@link SystemStreamPartition} to fetch the {@link Startpoint} for.
   * @param taskName The {@link TaskName} to fetch the {@link Startpoint} for.
   * @return {@link Startpoint} for the {@link SystemStreamPartition}, or null if it does not exist or if it is too stale.
   */
  public Startpoint readStartpointForTask(SystemStreamPartition ssp, TaskName taskName) {
    Preconditions.checkState(started, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");

    byte[] startpointBytes = metadataStore.get(toStoreKey(ssp, taskName));

    if (Objects.nonNull(startpointBytes)) {
      Startpoint startpoint = startpointSerde.fromBytes(startpointBytes);
      if (Instant.now().minus(expirationDuration).isBefore(Instant.ofEpochMilli(startpoint.getCreatedTimestamp()))) {
        return startpoint; // return if deserializable and if not stale
      }
    }

    return null;
  }

  /**
   * Deletes the {@link Startpoint} for a {@link SystemStreamPartition}
   * @param ssp The {@link SystemStreamPartition} to delete the {@link Startpoint} for.
   */
  public void deleteStartpoint(SystemStreamPartition ssp) {
    deleteStartpointForTask(ssp, null);
  }

  /**
   * Deletes the {@link Startpoint} for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp ssp The {@link SystemStreamPartition} to delete the {@link Startpoint} for.
   * @param taskName ssp The {@link TaskName} to delete the {@link Startpoint} for.
   */
  public void deleteStartpointForTask(SystemStreamPartition ssp, TaskName taskName) {
    Preconditions.checkState(started, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");

    metadataStore.delete(toStoreKey(ssp, taskName));
  }

  /**
   * For {@link Startpoint}s keyed only by {@link SystemStreamPartition}, this method remaps the Startpoints for the specified
   * SystemStreamPartition to SystemStreamPartition+{@link TaskName} for all tasks provided by the {@link JobModel}
   * This method is not atomic or thread-safe. The intent is for the Samza Processor's coordinator to use this
   * method to assign the Startpoints to the appropriate tasks.
   * @param ssp The {@link SystemStreamPartition} that a {@link Startpoint} is keyed to.
   * @param jobModel The {@link JobModel} is used to determine which {@link TaskName} each {@link SystemStreamPartition} maps to.
   * @return The list of {@link TaskName}s mapped to the ssp.
   */
  public Set<TaskName> groupStartpointsPerTask(SystemStreamPartition ssp, JobModel jobModel) {
    Preconditions.checkState(started, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");
    Preconditions.checkNotNull(jobModel, "JobModel cannot be null");

    Startpoint startpoint = readStartpoint(ssp);
    HashMap<TaskName, Startpoint> result = new HashMap<>();

    LOG.info("Grouping Startpoint keyed on SSP: {} to tasks determined by the job model.", ssp);

    // Re-map startpoints from SSP-only keys to SSP+TaskName keys
    HashSet<TaskName> tasksWithSSP = new HashSet<>();

    // Inspect the job model for TaskName to SSPs mapping and re-map startpoints from SSP-only keys to SSP+TaskName keys.
    jobModel.getContainers().values().forEach(containerModel ->
        containerModel.getTasks().forEach((taskName, taskModel) -> {
            if (taskModel.getSystemStreamPartitions().contains(ssp)) {
              tasksWithSSP.add(taskName);
              Startpoint startpointForTask = readStartpointForTask(ssp, taskName);

              // Existing startpoint keyed by SSP+TaskName takes precedence and will not be overwritten.
              if (Objects.isNull(startpointForTask)) {
                writeStartpointForTask(ssp, taskName, startpoint);
                result.put(taskName, startpoint);
                LOG.info("Startpoint for SSP: {} remapped with task: {}.", ssp, taskName);
              } else {
                LOG.info("Startpoint for SSP: {} and task: {} already exists and will not be overwritten.", ssp, taskName);
              }
            }
          })
    );

    // Delete startpoint keyed by only this SSP
    deleteStartpoint(ssp);

    return ImmutableSet.copyOf(tasksWithSSP);
  }

  /**
   * Relinquish resources held by the underlying {@link MetadataStore}
   */
  public void stop() {
    if (started) {
      metadataStore.close();
      started = false;
    } else {
      LOG.warn("StartpointManager already stopped.");
    }
  }

  @VisibleForTesting
  MetadataStore getMetadataStore() {
    return metadataStore;
  }

  @VisibleForTesting
  Duration getExpirationDuration() {
    return expirationDuration;
  }

  private static String toStoreKey(SystemStreamPartition ssp, TaskName taskName) {
    return new StartpointKey(ssp, taskName).toMetadataStoreKey();
  }
}
