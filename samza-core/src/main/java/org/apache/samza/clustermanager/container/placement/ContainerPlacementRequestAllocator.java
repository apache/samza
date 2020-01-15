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

import com.google.common.base.Preconditions;
import org.apache.samza.clustermanager.ContainerProcessManager;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless handler that periodically dispatches {@link ContainerPlacementRequestMessage} read from Metadata store to Job Coordinator
 */
public class ContainerPlacementRequestAllocator implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerPlacementRequestAllocator.class);
  private static final int DEFAULT_CLUSTER_MANAGER_CONTAINER_PLACEMENT_HANDLER_SLEEP_MS = 5000;

  /**
   * {@link ContainerProcessManager} needs to intercept container placement actions between ContainerPlacementRequestAllocator and
   * {@link org.apache.samza.clustermanager.ContainerManager} to avoid cyclic dependency between
   * {@link org.apache.samza.clustermanager.ContainerManager} and {@link org.apache.samza.clustermanager.ContainerAllocator}
   */
  private final ContainerProcessManager containerProcessManager;
  private final ContainerPlacementMetadataStore containerPlacementMetadataStore;
  /**
   * State that controls the lifecycle of the ContainerPlacementRequestAllocator thread
   */
  private volatile boolean isRunning;

  public ContainerPlacementRequestAllocator(ContainerPlacementMetadataStore containerPlacementMetadataStore, ContainerProcessManager manager) {
    Preconditions.checkNotNull(containerPlacementMetadataStore, "containerPlacementMetadataStore cannot be null");
    Preconditions.checkNotNull(manager, "ContainerProcessManager cannot be null");
    this.containerProcessManager = manager;
    this.containerPlacementMetadataStore = containerPlacementMetadataStore;
    this.isRunning = true;
  }

  @Override
  public void run() {
    while (isRunning && containerPlacementMetadataStore.isRunning()) {
      try {
        for (ContainerPlacementRequestMessage message : containerPlacementMetadataStore.readAllContainerPlacementRequestMessages()) {
          // We do not need to dispatch ContainerPlacementResponseMessage because they are written from JobCoordinator
          // in response to a Container Placement Action
          LOG.info("Received a container placement message {}", message);
          containerProcessManager.registerContainerPlacementAction(message);
        }
        Thread.sleep(DEFAULT_CLUSTER_MANAGER_CONTAINER_PLACEMENT_HANDLER_SLEEP_MS);
      } catch (InterruptedException e) {
        LOG.warn("Got InterruptedException in ContainerPlacementRequestAllocator thread.", e);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.error(
            "Got unknown Exception while reading ContainerPlacementRequestMessage in ContainerPlacementRequestAllocator thread",
            e);
      }
    }
  }

  public void stop() {
    if (isRunning) {
      isRunning = false;
    } else {
      LOG.warn("ContainerPlacementRequestAllocator already stopped");
    }
  }
}
