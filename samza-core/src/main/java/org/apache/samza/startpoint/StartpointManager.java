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
import com.google.common.collect.Sets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
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
@InterfaceStability.Unstable
public class StartpointManager {
  private static final Logger LOG = LoggerFactory.getLogger(StartpointManager.class);
  private static final String NAMESPACE = "samza-startpoint-v1";

  private final StartpointSerde startpointSerde = new StartpointSerde();

  private MetadataStore metadataStore;

  private StartpointManager(MetadataStore metadataStore) {
    this.metadataStore = metadataStore;
  }

  /**
   * Creates a {@link StartpointManager} instance.
   * @param config {@link Config} required for the underlying store.
   * @param metricsRegistry {@link MetricsRegistry} to hook into the underlying store.
   * @return {@link StartpointManager} instance.
   */
  public static StartpointManager getInstance(Config config, MetricsRegistry metricsRegistry) {
    Preconditions.checkNotNull(config, "Config cannot be null");
    Preconditions.checkNotNull(metricsRegistry, "MetricsRegistry cannot be null");

    String metadataStoreFactoryName = new JobConfig(config).getStartpointMetadataStoreFactory();
    MetadataStore metadataStore;
    try {
      MetadataStoreFactory metadataStoreFactory = Util.getObj(metadataStoreFactoryName, MetadataStoreFactory.class);
      metadataStore = metadataStoreFactory.getMetadataStore(NAMESPACE, config, metricsRegistry);
      metadataStore.init();
    } catch (ConfigException | NullPointerException ex) {
      LOG.error("StartpointManager cannot be created because the underlying metadata store could not be configured or created.", ex);
      return null;
    }

    StartpointManager startpointManager = new StartpointManager(metadataStore);
    LOG.info("StartpointManager created with metadata store: {}",
        startpointManager.metadataStore.getClass().getCanonicalName());

    return startpointManager;
  }

  /**
   * Writes a {@link Startpoint} that defines the start position for a {@link SystemStreamPartition}.
   * @param ssp The {@link SystemStreamPartition} to map the {@link Startpoint} against.
   * @param startpoint Reference to a Startpoint object.
   */
  public void writeStartpoint(SystemStreamPartition ssp, Startpoint startpoint) {
    Preconditions.checkNotNull(metadataStore, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");
    Preconditions.checkNotNull(startpoint, "Startpoint cannot be null");

    try {
      metadataStore.put(toStoreKey(ssp),
          startpointSerde.toBytes(startpoint.copyWithStoredAt(Instant.now().toEpochMilli())));
    } catch (Exception ex) {
      throw new SamzaException(String.format(
          "Startpoint for SSP: %s may not have been written to the metadata store.", ssp), ex);
    }
  }

  /**
   * Writes a {@link Startpoint} that defines the start position for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp The {@link SystemStreamPartition} to map the {@link Startpoint} against.
   * @param taskName The {@link TaskName} to map the {@link Startpoint} against.
   * @param startpoint Reference to a Startpoint object.
   */
  public void writeStartpointForTask(SystemStreamPartition ssp, TaskName taskName, Startpoint startpoint) {
    Preconditions.checkNotNull(metadataStore, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");
    Preconditions.checkNotNull(taskName, "TaskName cannot be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(taskName.getTaskName()), "TaskName cannot be blank");
    Preconditions.checkNotNull(startpoint, "Startpoint cannot be null");

    try {
      metadataStore.put(toStoreKey(ssp, taskName),
          startpointSerde.toBytes(startpoint.copyWithStoredAt(Instant.now().toEpochMilli())));
    } catch (Exception ex) {
      throw new SamzaException(String.format(
          "Startpoint for SSP: %s and task: %s may not have been written to the metadata store.", ssp, taskName), ex);
    }
  }

  /**
   * Returns the last {@link Startpoint} that defines the start position for a {@link SystemStreamPartition}.
   * @param ssp The {@link SystemStreamPartition} to fetch the {@link Startpoint} for.
   * @return {@link Startpoint} for the {@link SystemStreamPartition}, or null if it does not exist.
   */
  public Startpoint readStartpoint(SystemStreamPartition ssp) {
    Preconditions.checkNotNull(metadataStore, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");

    byte[] startpointBytes = metadataStore.get(toStoreKey(ssp));

    return Objects.isNull(startpointBytes) ? null : startpointSerde.fromBytes(startpointBytes);
  }

  /**
   * Returns the {@link Startpoint} for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp The {@link SystemStreamPartition} to fetch the {@link Startpoint} for.
   * @param taskName The {@link TaskName} to fetch the {@link Startpoint} for.
   * @return {@link Startpoint} for the {@link SystemStreamPartition}, or null if it does not exist.
   */
  public Startpoint readStartpointForTask(SystemStreamPartition ssp, TaskName taskName) {
    Preconditions.checkNotNull(metadataStore, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");
    Preconditions.checkNotNull(taskName, "TaskName cannot be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(taskName.getTaskName()), "TaskName cannot be blank");

    byte[] startpointBytes = metadataStore.get(toStoreKey(ssp, taskName));

    return Objects.isNull(startpointBytes) ? null : startpointSerde.fromBytes(startpointBytes);
  }

  /**
   * Deletes the {@link Startpoint} for a {@link SystemStreamPartition}
   * @param ssp The {@link SystemStreamPartition} to delete the {@link Startpoint} for.
   */
  public void deleteStartpoint(SystemStreamPartition ssp) {
    Preconditions.checkNotNull(metadataStore, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");

    metadataStore.delete(toStoreKey(ssp));
  }

  /**
   * Deletes the {@link Startpoint} for a {@link SystemStreamPartition} and {@link TaskName}.
   * @param ssp ssp The {@link SystemStreamPartition} to delete the {@link Startpoint} for.
   * @param taskName ssp The {@link TaskName} to delete the {@link Startpoint} for.
   */
  public void deleteStartpointForTask(SystemStreamPartition ssp, TaskName taskName) {
    Preconditions.checkNotNull(metadataStore, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");
    Preconditions.checkNotNull(taskName, "TaskName cannot be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(taskName.getTaskName()), "TaskName cannot be blank");

    metadataStore.delete(toStoreKey(ssp, taskName));
  }

  /**
   * For {@link Startpoint}s keyed only by {@link SystemStreamPartition}, this method remaps the Startpoints for the specified
   * SystemStreamPartition to SystemStreamPartition+{@link TaskName} for all tasks provided by the {@link SystemStreamPartitionGrouper}
   * This method is not atomic or thread-safe. The intent is for the Samza Processor's coordinator to use this
   * method to assign the Startpoints to the appropriate tasks.
   * @param ssp The {@link SystemStreamPartition} that a {@link Startpoint} is keyed to.
   * @param grouper The {@link SystemStreamPartitionGrouper} is used to determine what the task names are.
   * @return The list of {@link TaskName}s
   */
  public Set<TaskName> groupStartpointsPerTask(SystemStreamPartition ssp, SystemStreamPartitionGrouper grouper) {
    Preconditions.checkNotNull(metadataStore, "Underlying metadata store not available");
    Preconditions.checkNotNull(ssp, "SystemStreamPartition cannot be null");
    Preconditions.checkNotNull(grouper, "SystemStreamPartitionGrouper cannot be null");

    Startpoint startpoint = readStartpoint(ssp);
    HashMap<TaskName, Startpoint> result = new HashMap<>();

    LOG.info("Grouping Startpoint keyed on SSP: {} to tasks determined by the grouper: {}", ssp, grouper.getClass().getCanonicalName());

    // Re-map startpoints from SSP-only keys to SSP+TaskName keys
    Map<TaskName, Set<SystemStreamPartition>> group = grouper.group(Sets.newHashSet(ssp));
    group.forEach((taskName, sspSet) -> {
        if (sspSet.size() != 1) {
          throw new SamzaException(String.format("Expected only one SSP per task from grouper: %s",
              grouper.getClass().getCanonicalName()));
        }

        SystemStreamPartition sspFromGrouper = sspSet.iterator().next();
        if (!Objects.equals(ssp, sspFromGrouper)) {
          throw new SamzaException(String.format("Expected SSP: %s, but got: %s from grouper: %s", ssp, sspFromGrouper,
              grouper.getClass().getCanonicalName()));
        }

        Startpoint startpointForTask = readStartpointForTask(ssp, taskName);

        // Existing startpoint keyed by SSP+TaskName takes precedence and will not be overwritten.
        if (Objects.isNull(startpointForTask)) {
          writeStartpointForTask(ssp, taskName, startpoint);
          result.put(taskName, startpoint);
          LOG.info("Startpoint for SSP: {} remapped with task: {}.", ssp, taskName);
        } else {
          LOG.info("Startpoint for SSP: {} and task: {} already exists and will not be overwritten.", ssp, taskName);
        }
      });

    // Delete startpoint keyed by only this SSP
    deleteStartpoint(ssp);

    return ImmutableSet.copyOf(group.keySet());
  }

  /**
   * Relinquish resources held by the underlying {@link MetadataStore}
   */
  public void stop() {
    if (Objects.nonNull(metadataStore)) {
      metadataStore.flush();
      metadataStore.close();
      metadataStore = null;
      LOG.info("StartpointManager has stopped and this instance can no longer be used.");
    } else {
      LOG.warn("StartpointManager has already stopped");
    }
  }

  @VisibleForTesting
  MetadataStore getMetadataStore() {
    return metadataStore;
  }

  private static String toStoreKey(SystemStreamPartition ssp) {
    return toStoreKey(ssp, null);
  }

  private static String toStoreKey(SystemStreamPartition ssp, TaskName taskName) {
    return new StartpointKey(ssp, taskName).toString();
  }
}
