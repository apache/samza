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
package org.apache.samza.clustermanager;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ContainerManager is a centralized entity that manages control actions like start, stop for both active and standby containers
 * ContainerManager acts as a brain for validating and issuing any actions on containers in the Job Coordinator.
 *
 * The requests to allocate resources resources made by {@link ContainerAllocator} can either expire or succeed.
 * When the requests succeeds the ContainerManager validates those requests before starting the container
 * When the requests expires the ContainerManager decides the next set of actions for the pending request.
 *
 * Callbacks issued from  {@link ClusterResourceManager} aka {@link ContainerProcessManager} are intercepted
 * by ContainerManager to handle container failure and completions for both active and standby containers
 */
public class ContainerManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);

  /**
   * Resource-manager, used to stop containers
   */
  private ClusterResourceManager clusterResourceManager;
  private final SamzaApplicationState samzaApplicationState;

  private Optional<StandbyContainerManager> standbyContainerManager;

  public ContainerManager(SamzaApplicationState samzaApplicationState, ClusterResourceManager clusterResourceManager,
      Boolean standByEnabled) {
    this.samzaApplicationState = samzaApplicationState;
    this.clusterResourceManager = clusterResourceManager;
    // Enable standby container manager if required
    if (standByEnabled) {
      this.standbyContainerManager =
          Optional.of(new StandbyContainerManager(samzaApplicationState, clusterResourceManager));
    } else {
      this.standbyContainerManager = Optional.empty();
    }
  }

  /**
   * Handles the container start action for both active & standby containers.
   *
   * @param request pending request for the preferred host
   * @param preferredHost preferred host to start the container
   * @param allocatedResource resource allocated from {@link ClusterResourceManager}
   * @param resourceRequestState state of request in {@link ContainerAllocator}
   * @param allocator to request resources from @{@link ClusterResourceManager}
   */
  void handleContainerLaunch(SamzaResourceRequest request, String preferredHost, SamzaResource allocatedResource,
      ResourceRequestState resourceRequestState, ContainerAllocator allocator) {
    if (this.standbyContainerManager.isPresent()) {
      standbyContainerManager.get()
          .checkStandbyConstraintsAndRunStreamProcessor(request, preferredHost, allocatedResource, allocator,
              resourceRequestState);
    } else {
      allocator.runStreamProcessor(request, preferredHost);
    }
  }

  /**
   * Handles the action to be taken after the container has been stopped.
   * Case 1. When standby is enabled, refer to {@link StandbyContainerManager#handleContainerStop} to check constraints.
   * Case 2. When standby is disabled there are two cases according to host-affinity being enabled
   *    Case 2.1. When host-affinity is enabled resources are requested on host where container was last seen
   *    Case 2.2. When host-affinity is disabled resources are requested for ANY_HOST
   *
   * @param processorId logical id of the container
   * @param containerId last known id of the container deployed
   * @param preferredHost host on which container was last deployed
   * @param exitStatus exit code returned by the container
   * @param preferredHostRetryDelay delay to be incurred before requesting resources
   * @param containerAllocator allocator for requesting resources
   */
  void handleContainerStop(String processorId, String containerId, String preferredHost, int exitStatus,
      Duration preferredHostRetryDelay, ContainerAllocator containerAllocator) {
    if (standbyContainerManager.isPresent()) {
      standbyContainerManager.get()
          .handleContainerStop(processorId, containerId, preferredHost, exitStatus, containerAllocator,
              preferredHostRetryDelay);
    } else {
      // If StandbyTasks are not enabled, we simply make a request for the preferredHost
      containerAllocator.requestResourceWithDelay(processorId, preferredHost, preferredHostRetryDelay);
    }
  }

  /**
   * Handle the container launch failure for active containers and standby (if enabled).
   * Case 1. When standby is enabled, refer to {@link StandbyContainerManager#handleContainerLaunchFail} to check behavior
   * Case 2. When standby is disabled the allocator issues a request for ANY_HOST resources
   *
   * @param processorId logical id of the container
   * @param containerId last known id of the container deployed
   * @param preferredHost host on which container is requested to be deployed
   * @param containerAllocator allocator for requesting resources
   */
  void handleContainerLaunchFail(String processorId, String containerId, String preferredHost,
      ContainerAllocator containerAllocator) {
    if (processorId != null && standbyContainerManager.isPresent()) {
      standbyContainerManager.get().handleContainerLaunchFail(processorId, containerId, containerAllocator);
    } else if (processorId != null) {
      LOG.info("Falling back to ANY_HOST for Processor ID: {} since launch failed for Container ID: {} on host: {}",
          processorId, containerId, preferredHost);
      containerAllocator.requestResource(processorId, ResourceRequestState.ANY_HOST);
    } else {
      LOG.warn("Did not find a pending Processor ID for Container ID: {} on host: {}. "
          + "Ignoring invalid/redundant notification.", containerId, preferredHost);
    }
  }

  /**
   * Handles an expired resource request for both active and standby containers. Since a preferred host cannot be obtained
   * this method checks the availability of surplus ANY_HOST resources and launches the container if available. Otherwise
   * issues an ANY_HOST request. This behavior holds regardless of host-affinity enabled or not.
   *
   * @param processorId logical id of the container
   * @param preferredHost host on which container is requested to be deployed
   * @param request pending request for the preferred host
   * @param allocator allocator for requesting resources
   * @param resourceRequestState state of request in {@link ContainerAllocator}
   */
  @VisibleForTesting
  void handleExpiredResourceRequest(String processorId, String preferredHost,
      SamzaResourceRequest request, ContainerAllocator allocator, ResourceRequestState resourceRequestState) {
    boolean resourceAvailableOnAnyHost = allocator.hasAllocatedResource(ResourceRequestState.ANY_HOST);
    if (standbyContainerManager.isPresent()) {
      standbyContainerManager.get()
          .handleExpiredResourceRequest(processorId, request,
              Optional.ofNullable(allocator.peekAllocatedResource(ResourceRequestState.ANY_HOST)), allocator,
              resourceRequestState);
    } else if (resourceAvailableOnAnyHost) {
      LOG.info("Request for Processor ID: {} on host: {} has expired. Running on ANY_HOST", processorId, preferredHost);
      allocator.runStreamProcessor(request, ResourceRequestState.ANY_HOST);
    } else {
      LOG.info("Request for Processor ID: {} on host: {} has expired. Requesting additional resources on ANY_HOST.",
          processorId, preferredHost);
      resourceRequestState.cancelResourceRequest(request);
      allocator.requestResource(processorId, ResourceRequestState.ANY_HOST);
    }
  }
}
