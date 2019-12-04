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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.ContainerProcessManager;
import org.apache.samza.container.placement.ContainerPlacementMessage;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.apache.samza.container.placement.ContainerPlacementResponseMessage;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.metadatastore.MetadataStore;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless handler that dispatches {@link ContainerPlacementRequestMessage} read from {@link MetadataStore} to Job Coordinator
 * and writes responses {@link org.apache.samza.container.placement.ContainerPlacementResponseMessage} from Job Coordinator
 * to Metastore.
 *
 * {@link ContainerPlacementRequestMessage} are written under namespace {@code REQUEST_STORE_NAMESPACE}
 * {@link ContainerPlacementResponseMessage} are written under namespace {@code RESPONSE_STORE_NAMESPACE}
 */
public class ContainerPlacementHandler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerPlacementHandler.class);
  private static final int DEFAULT_CLUSTER_MANAGER_CONTAINER_PLACEMENT_HANDLER_SLEEP_MS = 5000;

  private static final Integer VERSION = 1;
  public static final String REQUEST_STORE_NAMESPACE = "samza-request-place-container-v" + VERSION;
  public static final String RESPONSE_STORE_NAMESPACE = "samza-response-place-container-v" + VERSION;

  private final NamespaceAwareCoordinatorStreamStore requestStore;
  private final NamespaceAwareCoordinatorStreamStore responseStore;
  /**
   * {@link ContainerProcessManager} needs to intercept container placement actions between ContainerPlacementHandler and
   * {@link org.apache.samza.clustermanager.ContainerManager} to avoid cyclic dependency between
   * {@link org.apache.samza.clustermanager.ContainerManager} and {@link org.apache.samza.clustermanager.ContainerAllocator}
   */
  private final ContainerProcessManager containerProcessManager;

  private final ObjectMapper objectMapper = ContainerPlacementMessageObjectMapper.getObjectMapper();

  /**
   * State that controls the lifecycle of the ContainerPlacementHandler thread
   */
  private volatile boolean isRunning;

  public ContainerPlacementHandler(MetadataStore metadataStore, ContainerProcessManager manager) {
    Preconditions.checkNotNull(metadataStore, "MetadataStore cannot be null");
    Preconditions.checkNotNull(manager, "ContainerProcessManager cannot be null");
    this.containerProcessManager = manager;
    this.requestStore = new NamespaceAwareCoordinatorStreamStore(metadataStore, REQUEST_STORE_NAMESPACE);
    this.responseStore = new NamespaceAwareCoordinatorStreamStore(metadataStore, RESPONSE_STORE_NAMESPACE);
    this.requestStore.init();
    this.responseStore.init();
    this.isRunning = true;
  }

  @Override
  public void run() {
    while (isRunning) {
      try {
        for (ContainerPlacementRequestMessage message : readAllContainerPlacementRequestMessages()) {
          // We do not need to dispatch ContainerPlacementResponseMessage because they are written from JobCoordinator
          // in response to a Container Placement Action
          LOG.info("Received a container placement message {}", message);
          containerProcessManager.registerContainerPlacementAction(message);
        }
        Thread.sleep(DEFAULT_CLUSTER_MANAGER_CONTAINER_PLACEMENT_HANDLER_SLEEP_MS);
      } catch (InterruptedException e) {
        LOG.warn("Got InterruptedException in ContainerPlacementHandler thread.", e);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.error(
            "Got unknown Exception while registering ContainerPlacement actions in ContainerPlacementHandler thread.",
            e);
      }
    }
  }

  /**
   * Writes a {@link ContainerPlacementRequestMessage} to the underlying metastore. This method should be used by external controllers
   * to issue a request to JobCoordinator
   * @param message container placement request
   */
  public void writeContainerPlacementRequestMessage(ContainerPlacementRequestMessage message) {
    Preconditions.checkState(isRunning, "Underlying metadata store not available");
    Preconditions.checkNotNull(message);
    try {
      requestStore.put(message.getProcessorId(), objectMapper.writeValueAsBytes(message));
    } catch (Exception ex) {
      throw new SamzaException(
          String.format("ContainerPlacementRequestMessage might have been not written to metastore %s", message), ex);
    }
  }

  /**
   * Writes a {@link ContainerPlacementResponseMessage} to the underlying metastore. This method should be used by Job Coordinator
   * only to write responses to Container Placement Action
   * @param message
   */
  void writeContainerPlacementResponseMessage(ContainerPlacementResponseMessage message) {
    Preconditions.checkState(isRunning, "Underlying metadata store not available");
    Preconditions.checkNotNull(message);
    try {
      responseStore.put(message.getProcessorId(), objectMapper.writeValueAsBytes(message));
    } catch (Exception ex) {
      throw new SamzaException(
          String.format("ContainerPlacementResponseMessage might have been not written to metastore %s", message), ex);
    }
  }

  /**
   * Reads a {@link ContainerPlacementRequestMessage} from the underlying metastore
   * @param processorId key of the message, logical processor id of a samza container 0,1,2
   * @return ContainerPlacementRequestMessage is its present
   */
  public Optional<ContainerPlacementRequestMessage> readContainerPlacementRequestMessage(String processorId) {
    Preconditions.checkState(isRunning, "Underlying metadata store not available");
    Preconditions.checkNotNull(processorId, "processorId cannot be null");

    byte[] messageBytes = requestStore.get(processorId);
    if (ArrayUtils.isNotEmpty(messageBytes)) {
      try {
        ContainerPlacementRequestMessage requestMessage =
            (ContainerPlacementRequestMessage) objectMapper.readValue(messageBytes, ContainerPlacementMessage.class);
        return Optional.of(requestMessage);
      } catch (IOException e) {
        throw new SamzaException(
            String.format("Error reading the ContainerPlacementResponseMessage for processorId {}", processorId), e);
      }
    }
    return Optional.empty();
  }

  /**
   * Reads a {@link ContainerPlacementResponseMessage} from the underlying metastore
   * @param processorId key of the message, logical processor id of a samza container 0,1,2
   * @return ContainerPlacementResponseMessage is its present
   */
  public Optional<ContainerPlacementResponseMessage> readContainerPlacementResponseMessage(String processorId) {
    Preconditions.checkState(isRunning, "Underlying metadata store not available");
    Preconditions.checkNotNull(processorId, "processorId cannot be null");

    byte[] messageBytes = responseStore.get(processorId);
    if (ArrayUtils.isNotEmpty(messageBytes)) {
      try {
        ContainerPlacementResponseMessage requestMessage =
            (ContainerPlacementResponseMessage) objectMapper.readValue(messageBytes, ContainerPlacementMessage.class);
        return Optional.of(requestMessage);
      } catch (IOException e) {
        throw new SamzaException(
            String.format("Error reading the ContainerPlacementResponseMessage for processorId {}", processorId), e);
      }
    }
    return Optional.empty();
  }

  /**
   * Deletes a {@link ContainerPlacementRequestMessage} if present identified by the key {@code processorId}
   * @param processorId logical processor id 0,1,2
   */
  public void deleteContainerPlacementRequestMessage(String processorId) {
    Preconditions.checkState(isRunning, "Underlying metadata store not available");
    Preconditions.checkNotNull(processorId, "processorId cannot be null");
    requestStore.delete(processorId);
  }

  /**
   * Deletes a {@link ContainerPlacementResponseMessage} if present identified by the key {@code processorId}
   * @param processorId logical processor id 0,1,2
   */
  public void deleteContainerPlacementResponseMessage(String processorId) {
    Preconditions.checkState(isRunning, "Underlying metadata store not available");
    Preconditions.checkNotNull(processorId, "processorId cannot be null");
    responseStore.delete(processorId);
  }

  /**
   * Deletes all {@link ContainerPlacementRequestMessage} present in underlying metastore
   */
  public void deleteAllContainerPlacementRequestMessages() {
    Preconditions.checkState(isRunning, "Underlying metadata store not available");
    Set<String> requestKeys = requestStore.all().keySet();
    for (String key : requestKeys) {
      requestStore.delete(key);
    }
  }

  /**
   * Deletes all {@link ContainerPlacementResponseMessage} present in underlying metastore
   */
  public void deleteAllContainerPlacementResponseMessages() {
    Preconditions.checkState(isRunning, "Underlying metadata store not available");
    Set<String> responseKeys = responseStore.all().keySet();
    for (String key : responseKeys) {
      responseStore.delete(key);
    }
  }

  /**
   * Deletes all {@link ContainerPlacementMessage}
   */
  public void deleteAllContainerPlacementMessages() {
    Preconditions.checkState(isRunning, "Underlying metadata store not available");
    deleteAllContainerPlacementRequestMessages();
    deleteAllContainerPlacementResponseMessages();
  }

  public void stop() {
    if (isRunning) {
      requestStore.close();
      responseStore.close();
      isRunning = false;
    } else {
      LOG.warn("ContainerPlacementHandler metastore already stopped");
    }
  }

  @VisibleForTesting
  List<ContainerPlacementRequestMessage> readAllContainerPlacementRequestMessages() {
    Preconditions.checkState(isRunning, "Underlying metadata store not available");
    List<ContainerPlacementRequestMessage> newActions = new ArrayList<>();
    Map<String, byte[]> messageBytes = requestStore.all();
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
    return newActions;
  }

  @VisibleForTesting
  MetadataStore getRequestStore() {
    return requestStore;
  }

  @VisibleForTesting
  MetadataStore getResponseStore() {
    return responseStore;
  }

}
