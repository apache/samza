/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 .* with the License.  You may obtain a copy of the License at
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
package org.apache.samza.drain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.UUID;
import joptsimple.internal.Strings;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DrainUtils provides utility methods for managing {@link DrainNotification} in the the provided {@link MetadataStore}.
 * */
public class DrainUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DrainUtils.class);
  private static final Integer VERSION = 1;
  // namespace for the underlying metadata store
  public static final String DRAIN_METADATA_STORE_NAMESPACE = "samza-drain-v" + VERSION;

  private DrainUtils() {
  }

  /**
   * Writes a {@link DrainNotification} to the underlying metastore. This method should be used by external controllers
   * to issue a DrainNotification to the JobCoordinator and Samza Containers.
   * @param metadataStore Metadata store to write drain notification to.
   *
   * @return generated uuid for the DrainNotification
   */
  public static UUID writeDrainNotification(MetadataStore metadataStore) {
    Preconditions.checkArgument(metadataStore != null, "MetadataStore cannot be null.");
    final Config config = CoordinatorStreamUtil.readConfigFromCoordinatorStream(metadataStore);
    final ApplicationConfig applicationConfig = new ApplicationConfig(config);
    final String runId = applicationConfig.getRunId();
    if (Strings.isNullOrEmpty(runId)) {
      throw new SamzaException("Unable to retrieve runId from metadata store. DrainNotification will not be "
          + "written to the metadata store.");
    }
    LOG.info("Received runId {}", runId);
    LOG.info("Attempting to write DrainNotification to metadata-store for the deployment ID {}", runId);
    final UUID uuid = UUID.randomUUID();
    final DrainNotification message = DrainNotification.create(uuid, runId);
    return writeDrainNotification(metadataStore, message);
  }

  /**
   * Writes a {@link DrainNotification} to an underlying metadata store.
   * */
  @VisibleForTesting
  static UUID writeDrainNotification(MetadataStore metadataStore, DrainNotification drainNotification) {
    Preconditions.checkArgument(metadataStore != null, "MetadataStore cannot be null.");
    Preconditions.checkArgument(drainNotification != null, "DrainNotification cannot be null.");
    final NamespaceAwareCoordinatorStreamStore drainMetadataStore =
        new NamespaceAwareCoordinatorStreamStore(metadataStore, DRAIN_METADATA_STORE_NAMESPACE);
    final ObjectMapper objectMapper = DrainNotificationObjectMapper.getObjectMapper();
    try {
      drainMetadataStore.put(drainNotification.getUuid().toString(), objectMapper.writeValueAsBytes(drainNotification));
      drainMetadataStore.flush();
      LOG.info("DrainNotification with id {} written to metadata-store for the deployment ID {}", drainNotification.getUuid(), drainNotification.getUuid());
    } catch (Exception ex) {
      throw new SamzaException(
          String.format("DrainNotification might have been not written to metastore %s", drainNotification), ex);
    }
    return drainNotification.getUuid();
  }

  /**
   * Cleans up DrainNotifications for the current deployment from the underlying metadata store.
   * The current runId is extracted from the config.
   *
   * @param metadataStore underlying metadata store
   * @param config Config for the job. Used to extract the runId of the job.
   * */
  public static void cleanup(MetadataStore metadataStore, Config config) {
    Preconditions.checkArgument(metadataStore != null, "MetadataStore cannot be null.");
    Preconditions.checkNotNull(config, "Config parameter cannot be null.");

    final ApplicationConfig applicationConfig = new ApplicationConfig(config);
    final String runId = applicationConfig.getRunId();
    final ObjectMapper objectMapper = DrainNotificationObjectMapper.getObjectMapper();
    final NamespaceAwareCoordinatorStreamStore drainMetadataStore =
        new NamespaceAwareCoordinatorStreamStore(metadataStore, DRAIN_METADATA_STORE_NAMESPACE);

    if (DrainMonitor.shouldDrain(drainMetadataStore, runId)) {
      LOG.info("Attempting to clean up DrainNotifications from the metadata-store for the current deployment {}", runId);
      drainMetadataStore.all()
          .values()
          .stream()
          .map(bytes -> {
            try {
              return objectMapper.readValue(bytes, DrainNotification.class);
            } catch (IOException e) {
              LOG.error("Unable to deserialize DrainNotification from the metadata store", e);
              throw new SamzaException(e);
            }
          })
          .filter(notification -> runId.equals(notification.getRunId()))
          .forEach(notification -> drainMetadataStore.delete(notification.getUuid().toString()));

      drainMetadataStore.flush();
      LOG.info("Successfully cleaned up DrainNotifications from the metadata-store for the current deployment {}", runId);
    } else {
      LOG.info("No DrainNotification found in the metadata-store for the current deployment {}. No need to cleanup.",
          runId);
    }
  }

  /**
   * Cleans up all DrainNotifications irrespective of the runId.
   * */
  public static void cleanupAll(MetadataStore metadataStore) {
    Preconditions.checkArgument(metadataStore != null, "MetadataStore cannot be null.");
    final NamespaceAwareCoordinatorStreamStore drainMetadataStore =
        new NamespaceAwareCoordinatorStreamStore(metadataStore, DRAIN_METADATA_STORE_NAMESPACE);
    LOG.info("Attempting to cleanup all DrainNotifications from the metadata-store.");
    drainMetadataStore.all()
        .keySet()
        .forEach(drainMetadataStore::delete);
    drainMetadataStore.flush();
    LOG.info("Successfully cleaned up all DrainNotifications from the metadata-store.");
  }
}
