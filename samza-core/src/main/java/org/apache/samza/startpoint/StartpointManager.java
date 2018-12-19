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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.JsonSerdeV2;
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
  private static final String NAMESPACE = "samza-startpoint-v1";

  static final Duration DEFAULT_EXPIRATION_DURATION = Duration.ofHours(12);

  private final MetadataStore metadataStore;
  private final StartpointSerde startpointSerde = new StartpointSerde();

  private boolean started = false;

  /**
   * Constructs a {@link StartpointManager} instance with the provided {@link MetadataStoreFactory}
   * @param metadataStoreFactory {@link MetadataStoreFactory} used to construct the underlying store.
   * @param config {@link Config} required for the underlying store.
   * @param metricsRegistry {@link MetricsRegistry} to hook into the underlying store.
   */
  public StartpointManager(MetadataStoreFactory metadataStoreFactory, Config config, MetricsRegistry metricsRegistry) {
    Preconditions.checkNotNull(metadataStoreFactory, "MetadataStoreFactory cannot be null");
    Preconditions.checkNotNull(config, "Config cannot be null");
    Preconditions.checkNotNull(metricsRegistry, "MetricsRegistry cannot be null");

    this.metadataStore = metadataStoreFactory.getMetadataStore(NAMESPACE, config, metricsRegistry);
    LOG.info("StartpointManager created with metadata store: {}", metadataStore.getClass().getCanonicalName());
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
    writeStartpoint(ssp, null, startpoint);
  }

  /**
   * Writes a {@link Startpoint} that defines the start position for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp The {@link SystemStreamPartition} to map the {@link Startpoint} against.
   * @param taskName The {@link TaskName} to map the {@link Startpoint} against.
   * @param startpoint Reference to a Startpoint object.
   */
  public void writeStartpoint(SystemStreamPartition ssp, TaskName taskName, Startpoint startpoint) {
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
    return readStartpoint(ssp, null);
  }

  /**
   * Returns the {@link Startpoint} for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp The {@link SystemStreamPartition} to fetch the {@link Startpoint} for.
   * @param taskName The {@link TaskName} to fetch the {@link Startpoint} for.
   * @return {@link Startpoint} for the {@link SystemStreamPartition}, or null if it does not exist or if it is too stale.
   */
  public Startpoint readStartpoint(SystemStreamPartition ssp, TaskName taskName) {
    Preconditions.checkState(started, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");

    byte[] startpointBytes = metadataStore.get(toStoreKey(ssp, taskName));

    if (Objects.nonNull(startpointBytes)) {
      Startpoint startpoint = startpointSerde.fromBytes(startpointBytes);
      if (Instant.now().minus(DEFAULT_EXPIRATION_DURATION).isBefore(Instant.ofEpochMilli(startpoint.getCreationTimestamp()))) {
        return startpoint; // return if deserializable and if not stale
      }
      LOG.warn("Stale Startpoint: {} was read. Ignoring.", startpoint);
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
   * For {@link Startpoint}s keyed only by {@link SystemStreamPartition}, this method re-maps the Startpoints from
   * SystemStreamPartition to SystemStreamPartition+{@link TaskName} for all tasks provided by the {@link JobModel}
   * This method is not atomic or thread-safe. The intent is for the Samza Processor's coordinator to use this
   * method to assign the Startpoints to the appropriate tasks.
   * @param jobModel The {@link JobModel} is used to determine which {@link TaskName} each {@link SystemStreamPartition} maps to.
   * @return The list of {@link SystemStreamPartition}s that were fanned out to SystemStreamPartition+TaskName.
   */
  public Set<SystemStreamPartition> fanOutStartpointsToTasks(JobModel jobModel) {
    Preconditions.checkState(started, "Underlying metadata store not available");
    Preconditions.checkNotNull(jobModel, "JobModel cannot be null");

    HashSet<SystemStreamPartition> sspsToDelete = new HashSet<>();

    // Inspect the job model for TaskName-to-SSPs mapping and re-map startpoints from SSP-only keys to SSP+TaskName keys.
    for (ContainerModel containerModel: jobModel.getContainers().values()) {
      for (TaskModel taskModel : containerModel.getTasks().values()) {
        TaskName taskName = taskModel.getTaskName();
        for (SystemStreamPartition ssp : taskModel.getSystemStreamPartitions()) {
          Startpoint startpoint = readStartpoint(ssp); // Read SSP-only key
          if (startpoint == null) {
            LOG.debug("No Startpoint for SSP: {} in task: {}", ssp, taskName);
            continue;
          }

          LOG.info("Grouping Startpoint keyed on SSP: {} to tasks determined by the job model.", ssp);
          Startpoint startpointForTask = readStartpoint(ssp, taskName);
          if (startpointForTask == null || startpointForTask.getCreationTimestamp() < startpoint.getCreationTimestamp()) {
            writeStartpoint(ssp, taskName, startpoint);
            sspsToDelete.add(ssp); // Mark for deletion
            LOG.info("Startpoint for SSP: {} remapped with task: {}.", ssp, taskName);
          } else {
            LOG.info("Startpoint for SSP: {} and task: {} already exists and will not be overwritten.", ssp, taskName);
          }

        }
      }
    }

    // Delete SSP-only keys
    sspsToDelete.forEach(ssp -> {
        deleteStartpoint(ssp);
        LOG.info("All Startpoints for SSP: {} have been grouped to the appropriate tasks and the SSP was deleted.");
      });

    return ImmutableSet.copyOf(sspsToDelete);
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

  private static String toStoreKey(SystemStreamPartition ssp, TaskName taskName) {
    return new String(new JsonSerdeV2<>().toBytes(new StartpointKey(ssp, taskName)));
  }
}
