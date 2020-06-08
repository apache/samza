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
import org.apache.samza.clustermanager.ContainerProcessManager;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless handler that periodically dispatches {@link ContainerPlacementRequestMessage} read from Metadata store to Job Coordinator
 * Container placement requests from the previous deployment are deleted from the metadata store, ContainerPlacementRequestAllocatorThread
 * does this cleanup
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
  /**
   * RunId of the app
   */
  private final String appRunId;
  /**
   * Sleep time for container placement handler thread
   */
  private final int containerPlacementHandlerSleepMs;
  public ContainerPlacementRequestAllocator(ContainerPlacementMetadataStore containerPlacementMetadataStore, ContainerProcessManager manager, ApplicationConfig config) {
    Preconditions.checkNotNull(containerPlacementMetadataStore, "containerPlacementMetadataStore cannot be null");
    Preconditions.checkNotNull(manager, "ContainerProcessManager cannot be null");
    this.containerProcessManager = manager;
    this.containerPlacementMetadataStore = containerPlacementMetadataStore;
    this.isRunning = true;
    this.appRunId = config.getRunId();
    this.containerPlacementHandlerSleepMs = DEFAULT_CLUSTER_MANAGER_CONTAINER_PLACEMENT_HANDLER_SLEEP_MS;
  }

  @VisibleForTesting
  /**
   * Should only get used for testing, cannot make it package private because end to end integeration test
   * need package private methods which live in org.apache.samza.clustermanager
   */
  public ContainerPlacementRequestAllocator(ContainerPlacementMetadataStore containerPlacementMetadataStore, ContainerProcessManager manager, ApplicationConfig config, int clusterManagerContainerPlacementHandlerSleepMs) {
    Preconditions.checkNotNull(containerPlacementMetadataStore, "containerPlacementMetadataStore cannot be null");
    Preconditions.checkNotNull(manager, "ContainerProcessManager cannot be null");
    this.containerProcessManager = manager;
    this.containerPlacementMetadataStore = containerPlacementMetadataStore;
    this.isRunning = true;
    this.appRunId = config.getRunId();
    this.containerPlacementHandlerSleepMs = clusterManagerContainerPlacementHandlerSleepMs;
  }

  @Override
  public void run() {
    while (isRunning && containerPlacementMetadataStore.isRunning()) {
      try {
        for (ContainerPlacementRequestMessage message : containerPlacementMetadataStore.readAllContainerPlacementRequestMessages()) {
          // We do not need to dispatch ContainerPlacementResponseMessage because they are written from JobCoordinator
          // in response to a Container Placement Action
          if (message.getDeploymentId().equals(appRunId)) {
            LOG.debug("Received a container placement message {}", message);
            containerProcessManager.registerContainerPlacementAction(message);
          } else {
            // Delete the ContainerPlacementMessages from the previous deployment
            containerPlacementMetadataStore.deleteAllContainerPlacementMessages(message.getUuid());
          }
        }
        Thread.sleep(containerPlacementHandlerSleepMs);
      } catch (InterruptedException e) {
        LOG.warn("Got InterruptedException in ContainerPlacementRequestAllocator thread.", e);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.error(
            "Got an exception while reading ContainerPlacementRequestMessage in ContainerPlacementRequestAllocator thread",
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
