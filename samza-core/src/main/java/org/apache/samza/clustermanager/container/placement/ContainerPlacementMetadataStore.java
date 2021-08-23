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
package org.apache.samza.clustermanager.container.placement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.container.placement.ContainerPlacementMessage;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.apache.samza.container.placement.ContainerPlacementResponseMessage;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.metadatastore.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entity managing read writes to the metastore for {@link org.apache.samza.container.placement.ContainerPlacementRequestMessage}
 * and {@link org.apache.samza.container.placement.ContainerPlacementResponseMessage}
 *
 * ContainerPlacement control messages are written to {@link MetadataStore} as a KV under the namespace CONTAINER_PLACEMENT_STORE_NAMESPACE
 * Key is combination of {@link UUID} and message type (either {@link ContainerPlacementRequestMessage} or {@link ContainerPlacementResponseMessage})
 * and the value is the actual request or response message
 */
public class ContainerPlacementMetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerPlacementMetadataStore.class);

  private static final Integer VERSION = 1;
  public static final String CONTAINER_PLACEMENT_STORE_NAMESPACE = "samza-place-container-v" + VERSION;

  private final NamespaceAwareCoordinatorStreamStore containerPlacementMessageStore;
  private final ObjectMapper objectMapper = ContainerPlacementMessageObjectMapper.getObjectMapper();

  private boolean stopped = true;

  public ContainerPlacementMetadataStore(MetadataStore containerPlacementMessageStore) {
    Preconditions.checkNotNull(containerPlacementMessageStore, "MetadataStore cannot be null");
    this.containerPlacementMessageStore =
        new NamespaceAwareCoordinatorStreamStore(containerPlacementMessageStore, CONTAINER_PLACEMENT_STORE_NAMESPACE);
  }

  /**
   * Perform startup operations. Method is idempotent.
   */
  public void start() {
    if (stopped) {
      LOG.info("Starting ContainerPlacementStore");
      containerPlacementMessageStore.init();
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
      containerPlacementMessageStore.close();
      stopped = true;
    } else {
      LOG.warn("already stopped");
    }
  }

  /**
   * Checks if ContainerPlacementMetadataStore is running
   */
  public boolean isRunning() {
    return !stopped;
  }

  /**
   * Writes a {@link ContainerPlacementRequestMessage} to the underlying metastore. This method should be used by external controllers
   * to issue a request to JobCoordinator
   *
   * @param deploymentId identifier of the deployment
   * @param processorId logical id of the samza container 0,1,2
   * @param destinationHost host where the container is desired to move
   * @param requestExpiry optional per request expiry timeout for requests to cluster manager
   * @param timestamp timestamp of the request
   * @return uuid generated for the request
   */
  public UUID writeContainerPlacementRequestMessage(String deploymentId, String processorId, String destinationHost,
      Duration requestExpiry, long timestamp) {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    UUID uuid = UUID.randomUUID();
    ContainerPlacementRequestMessage message =
        new ContainerPlacementRequestMessage(uuid, deploymentId, processorId, destinationHost, requestExpiry,
            timestamp);
    try {
      containerPlacementMessageStore.put(toContainerPlacementMessageKey(message.getUuid(), message.getClass()),
          objectMapper.writeValueAsBytes(message));
      containerPlacementMessageStore.flush();
    } catch (Exception ex) {
      throw new SamzaException(
          String.format("ContainerPlacementRequestMessage might have been not written to metastore %s", message), ex);
    }
    return uuid;
  }

  /**
   * Writes a {@link ContainerPlacementResponseMessage} to the underlying metastore. This method should be used by Job Coordinator
   * only to write responses to Container Placement Action
   * @param message
   */
  public void writeContainerPlacementResponseMessage(ContainerPlacementResponseMessage message) {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkNotNull(message);
    try {
      containerPlacementMessageStore.put(toContainerPlacementMessageKey(message.getUuid(), message.getClass()),
          objectMapper.writeValueAsBytes(message));
      containerPlacementMessageStore.flush();
    } catch (Exception ex) {
      throw new SamzaException(
          String.format("ContainerPlacementResponseMessage might have been not written to metastore %s", message), ex);
    }
  }

  /**
   * Reads a {@link ContainerPlacementRequestMessage} from the underlying metastore
   * @param uuid uuid of the request
   * @return ContainerPlacementRequestMessage if present
   */
  public Optional<ContainerPlacementRequestMessage> readContainerPlacementRequestMessage(UUID uuid) {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkNotNull(uuid, "uuid cannot be null");

    byte[] messageBytes = containerPlacementMessageStore.get(
        toContainerPlacementMessageKey(uuid, ContainerPlacementRequestMessage.class));
    if (ArrayUtils.isNotEmpty(messageBytes)) {
      try {
        ContainerPlacementRequestMessage requestMessage =
            (ContainerPlacementRequestMessage) objectMapper.readValue(messageBytes, ContainerPlacementMessage.class);
        return Optional.of(requestMessage);
      } catch (IOException e) {
        throw new SamzaException(
            String.format("Error reading the ContainerPlacementResponseMessage for uuid: %s", uuid), e);
      }
    }
    return Optional.empty();
  }

  /**
   * Reads a {@link ContainerPlacementResponseMessage} from the underlying metastore
   * @param uuid uuid of the response message
   * @return ContainerPlacementResponseMessage if present
   */
  public Optional<ContainerPlacementResponseMessage> readContainerPlacementResponseMessage(UUID uuid) {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkNotNull(uuid, "uuid cannot be null");

    byte[] messageBytes = containerPlacementMessageStore.get(
        toContainerPlacementMessageKey(uuid, ContainerPlacementResponseMessage.class));
    if (ArrayUtils.isNotEmpty(messageBytes)) {
      try {
        ContainerPlacementResponseMessage requestMessage =
            (ContainerPlacementResponseMessage) objectMapper.readValue(messageBytes, ContainerPlacementMessage.class);
        return Optional.of(requestMessage);
      } catch (IOException e) {
        throw new SamzaException(
            String.format("Error reading the ContainerPlacementResponseMessage for uuid: %s", uuid), e);
      }
    }
    return Optional.empty();
  }

  /**
   * Deletes a {@link ContainerPlacementRequestMessage} if present identified by the key {@code uuid}
   * @param uuid uuid of the request
   */
  public void deleteContainerPlacementRequestMessage(UUID uuid) {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkNotNull(uuid, "uuid cannot be null");
    containerPlacementMessageStore.delete(toContainerPlacementMessageKey(uuid, ContainerPlacementRequestMessage.class));
    containerPlacementMessageStore.flush();
  }

  /**
   * Deletes a {@link ContainerPlacementResponseMessage} if present identified by the key {@code processorId}
   * @param uuid uuid of the request
   */
  public void deleteContainerPlacementResponseMessage(UUID uuid) {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Preconditions.checkNotNull(uuid, "uuid cannot be null");
    containerPlacementMessageStore.delete(toContainerPlacementMessageKey(uuid, ContainerPlacementResponseMessage.class));
    containerPlacementMessageStore.flush();
  }

  /**
   * Deletes both {@link ContainerPlacementRequestMessage} and {@link ContainerPlacementResponseMessage} identified by
   * uuid
   * @param uuid uuid of request and response message
   */
  public void deleteAllContainerPlacementMessages(UUID uuid) {
    deleteContainerPlacementRequestMessage(uuid);
    deleteContainerPlacementResponseMessage(uuid);
  }

  /**
   * Deletes all {@link ContainerPlacementMessage}
   */
  public void deleteAllContainerPlacementMessages() {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    Set<String> requestKeys = containerPlacementMessageStore.all().keySet();
    for (String key : requestKeys) {
      containerPlacementMessageStore.delete(key);
    }
    containerPlacementMessageStore.flush();
  }

  static String toContainerPlacementMessageKey(UUID uuid, Class<?> messageType) {
    Preconditions.checkNotNull(uuid, "UUID should not be null");
    Preconditions.checkNotNull(messageType, "messageType should not be null");
    Preconditions.checkArgument(
        messageType == ContainerPlacementRequestMessage.class || messageType == ContainerPlacementResponseMessage.class,
        "messageType should be either ContainerPlacementRequestMessage or ContainerPlacementResponseMessage");
    if (messageType == ContainerPlacementRequestMessage.class) {
      return uuid.toString() + "." + ContainerPlacementRequestMessage.class.getSimpleName();
    }
    return uuid.toString() + "." + ContainerPlacementResponseMessage.class.getSimpleName();
  }

  @VisibleForTesting
  List<ContainerPlacementRequestMessage> readAllContainerPlacementRequestMessages() {
    Preconditions.checkState(!stopped, "Underlying metadata store not available");
    List<ContainerPlacementRequestMessage> newActions = new ArrayList<>();
    Map<String, byte[]> messageBytes = containerPlacementMessageStore.all();
    for (byte[] action : messageBytes.values()) {
      try {
        ContainerPlacementMessage message = objectMapper.readValue(action, ContainerPlacementMessage.class);
        if (message instanceof ContainerPlacementRequestMessage) {
          newActions.add((ContainerPlacementRequestMessage) message);
        }
      } catch (IOException e) {
        throw new SamzaException(e);
      }
    }
    // Sort the actions in order of timestamp
    newActions.sort(Comparator.comparingLong(ContainerPlacementRequestMessage::getTimestamp));
    return newActions;
  }

  @VisibleForTesting
  MetadataStore getContainerPlacementStore() {
    return containerPlacementMessageStore;
  }
}
