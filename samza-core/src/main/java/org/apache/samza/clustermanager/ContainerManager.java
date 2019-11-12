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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.clustermanager.container.placements.ContainerPlacementMetadata;
import org.apache.samza.clustermanager.container.placements.ContainerPlacementStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ContainerManager is a centralized entity that manages container actions like start, stop for both active and standby containers
 * ContainerManager acts as a brain for validating and issuing any actions on containers in the Job Coordinator.
 *
 * The requests to allocate resources resources made by {@link ContainerAllocator} can either expire or succeed.
 * When the requests succeeds the ContainerManager validates those requests before starting the container
 * When the requests expires the ContainerManager decides the next set of actions for the pending request.
 *
 * Callbacks issued from  {@link ClusterResourceManager} aka {@link ContainerProcessManager} are intercepted
 * by ContainerManager to handle container failure and completions for both active and standby containers
 *
 * ContainerManager encapsulates logic and state related to container placement actions like move, restarts for active container
 * if issued externally.
 *
 * TODO SAMZA-2378: Container Placements for Standby containers enabled jobs
 *      SAMZA-2379: Container Placements for job running in degraded state
 */
public class ContainerManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);
  private static final String ANY_HOST = ResourceRequestState.ANY_HOST;

  /**
   * Default timeout for request to the cluster manager for stopping an active container & checking expiry for
   * requested resources
   */
  private static final Long DEFAULT_CONTROL_ACTION_EXPIRY = Duration.ofSeconds(10).toMillis();
  /**
   * Resource-manager, used to stop containers
   */
  private final ClusterResourceManager clusterResourceManager;
  private final SamzaApplicationState samzaApplicationState;
  private final boolean hostAffinityEnabled;

  /**
   * Map maintaining active container placement action meta data indexed by containers processorId eg 0, 1, 2
   */
  private final ConcurrentHashMap<String, ContainerPlacementMetadata> actions;

  private Optional<StandbyContainerManager> standbyContainerManager;

  public ContainerManager(SamzaApplicationState samzaApplicationState, ClusterResourceManager clusterResourceManager,
      Boolean hostAffinityEnabled, Boolean standByEnabled) {
    this.samzaApplicationState = samzaApplicationState;
    this.clusterResourceManager = clusterResourceManager;
    this.actions = new ConcurrentHashMap<>();
    this.hostAffinityEnabled = hostAffinityEnabled;
    // Enable standby container manager if required
    if (standByEnabled) {
      this.standbyContainerManager =
          Optional.of(new StandbyContainerManager(samzaApplicationState, clusterResourceManager));
    } else {
      this.standbyContainerManager = Optional.empty();
    }
  }

  /**
   * Handles the container start action for both active & standby containers. If the container launch request is due to
   * an existing container placement action, issue a stop on active container & wait for the active container to be stopped before
   * issuing a start.
   *
   * @param request pending request for the preferred host
   * @param preferredHost preferred host to start the container
   * @param allocatedResource resource allocated from {@link ClusterResourceManager}
   * @param resourceRequestState state of request in {@link ContainerAllocator}
   * @param allocator to request resources from @{@link ClusterResourceManager}
   */
  void handleContainerLaunch(SamzaResourceRequest request, String preferredHost, SamzaResource allocatedResource,
      ResourceRequestState resourceRequestState, ContainerAllocator allocator) {
    if (getControlActionMetadata(request.getProcessorId()).isPresent()) {
      handleContainerAllocationForExistingControlAction(request.getProcessorId(), allocator, resourceRequestState,
          request, preferredHost);
    } else if (this.standbyContainerManager.isPresent()) {
      standbyContainerManager.get()
          .checkStandbyConstraintsAndRunStreamProcessor(request, preferredHost, allocatedResource, allocator,
              resourceRequestState);
    } else {
      allocator.runStreamProcessor(request, preferredHost);
    }
  }

  /**
   * Handles the action to be taken after the container has been stopped. If stop was issued due to existing control
   * action, mark the container as stopped, otherwise
   *
   * Case 1. When standby is enabled, refer to {@link StandbyContainerManager#handleContainerStop} to check constraints.
   * Case 2. When standby is disabled there are two cases according to host-affinity being enabled
   *    Case 1.1. When host-affinity is enabled resources are requested on host where container was last seen
   *    Case 2.2. When host-affinity is disabled resources are requested for ANY_HOST
   *
   * @param processorId logical id of the container eg 1,2,3
   * @param containerId last known id of the container deployed
   * @param preferredHost host on which container was last deployed
   * @param exitStatus exit code returned by the container
   * @param preferredHostRetryDelay delay to be incurred before requesting resources
   * @param containerAllocator allocator for requesting resources
   */
  void handleContainerStop(String processorId, String containerId, String preferredHost, int exitStatus,
      Duration preferredHostRetryDelay, ContainerAllocator containerAllocator) {
    if (getControlActionMetadata(processorId).isPresent()) {
      LOG.info("Setting the container with processorId {} stopped to be true because of existing container placement action", processorId);
      Object activeContainerStopped = getControlActionMetadata(processorId).get().getActiveContainerStopped();
      synchronized (activeContainerStopped) {
        activeContainerStopped.notifyAll();
      }
    } else if (standbyContainerManager.isPresent()) {
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
   *
   * Case 1. If this launch was issued due to an existing container placement action update the metadata to report failure and issue
   *         a request for source host where the container was last seen and mark the container placement failed
   * Case 2. When standby is enabled, refer to {@link StandbyContainerManager#handleContainerLaunchFail} to check behavior
   * Case 3. When standby is disabled the allocator issues a request for ANY_HOST resources
   *
   * @param processorId logical id of the container eg 1,2,3
   * @param containerId last known id of the container deployed
   * @param preferredHost host on which container is requested to be deployed
   * @param containerAllocator allocator for requesting resources
   */
  void handleContainerLaunchFail(String processorId, String containerId, String preferredHost,
      ContainerAllocator containerAllocator) {
    if (processorId != null && getControlActionMetadata(processorId).isPresent()) {
      ContainerPlacementMetadata metaData = getControlActionMetadata(processorId).get();
      // Issue a request to start the container on source host
      String sourceHost = hostAffinityEnabled ? metaData.getSourceHost() : ResourceRequestState.ANY_HOST;
      containerAllocator.requestResource(processorId, sourceHost);
      handleControlActionFailure(processorId, metaData, "failed to start container on destination host");
    } else if (processorId != null && standbyContainerManager.isPresent()) {
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
   * Handles the state update on successful launch of a container, if this launch is due to a container placement action updates the
   * related metadata to report success
   *
   * @param processorId logical processor id of container 0,1,2
   */
  void handleContainerLaunchSuccess(String processorId) {
    Optional<ContainerPlacementMetadata> metaData = getControlActionMetadata(processorId);
    if (metaData.isPresent()) {
      metaData.get().setActionStatus(ContainerPlacementStatus.StatusCode.SUCCEEDED, "Successfully completed the container placement action");
      LOG.info("Marking the container placement action sucesss {}", metaData);
      this.actions.remove(processorId);
    }
  }

  /**
   * Handles an expired resource request for both active and standby containers.
   *
   * Case 1. If this expired request is due to a container placement action mark the request as failed and return
   * Case 2. Otherwise, expired request are only handled for host affinity enabled jobs. Since a preferred host cannot be
   *         obtained this method checks the availability of surplus ANY_HOST resources and launches the container if available.
   *         Otherwise issues an ANY_HOST request.
   *
   * @param processorId logical id of the container
   * @param preferredHost host on which container is requested to be deployed
   * @param request pending request for the preferred host
   * @param allocator allocator for requesting resources
   * @param resourceRequestState state of request in {@link ContainerAllocator}
   */
  @VisibleForTesting
  void handleExpiredRequestForControlActionOrHostAffinityEnabled(String processorId, String preferredHost,
      SamzaResourceRequest request, ContainerAllocator allocator, ResourceRequestState resourceRequestState) {
    boolean resourceAvailableOnAnyHost = allocator.hasAllocatedResource(ResourceRequestState.ANY_HOST);

    if (getControlActionMetadata(processorId).isPresent()) {
      resourceRequestState.cancelResourceRequest(request);
      handleControlActionFailure(processorId, getControlActionMetadata(processorId).get(),
          "failed the Control action because request for resources to ClusterManager expired");
      return;
    }

    // Host affinity disabled wait for Resource manager to return resources
    if (!hostAffinityEnabled) {
      return;
    }

    // Handle expired requests for host affinity enabled case
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

  /**
   * Registers a container placement action to move the running container to destination host, if destination host is same as the
   * host on which container is running, container placement action is treated as a restart.
   *
   * When host affinity is disabled a move / restart is only allowed on ANY_HOST
   * When host affinity is enabled move / restart is allowed on specific or ANY_HOST
   * TODO: SAMZA-2378: Container Placements for Standby containers enabled jobs
   *
   * @param processorId logical id of the container 0, 1, 2
   * @param destinationHost host where container is desired to be moved, acceptable values of this param are any valid
   *                        hostname or "ANY_HOST"(in this case the request is sent to resource manager for any host)
   * @param containerAllocator to request physical resources
   */
  public ContainerPlacementStatus registerContainerPlacementAction(String processorId, String destinationHost,
      ContainerAllocator containerAllocator, Optional<Long> requestExpiry) {
    LOG.info("Received ControlAction request to move or restart container with processor id {} to host {}", processorId, destinationHost);
    ContainerPlacementStatus actionStatus = checkValidControlAction(processorId, destinationHost);
    if (actionStatus.status == ContainerPlacementStatus.StatusCode.BAD_REQUEST) {
      return actionStatus;
    }

    SamzaResource currentResource = samzaApplicationState.runningProcessors.get(processorId);
    LOG.info("Processor ID: {} matched a active container with deployment ID: {} running on host: {}", processorId,
        currentResource.getContainerId(), currentResource.getHost());

    if (destinationHost.equals(ANY_HOST) || !hostAffinityEnabled) {
      LOG.info("Changing the requested host to {} because either it is requested or host affinity is disabled",
          ResourceRequestState.ANY_HOST);
      destinationHost = ANY_HOST;
    }

    SamzaResourceRequest resourceRequest = containerAllocator.getResourceRequest(processorId, destinationHost);
    ContainerPlacementMetadata actionMetaData =
        new ContainerPlacementMetadata(processorId, currentResource.getContainerId(), currentResource.getHost(),
            destinationHost, actionStatus, requestExpiry.isPresent() ? requestExpiry.get() : DEFAULT_CONTROL_ACTION_EXPIRY);

    // Record the resource request for monitoring
    actionMetaData.setActionStatus(ContainerPlacementStatus.StatusCode.IN_PROGRESS);
    actionMetaData.recordResourceRequest(resourceRequest);
    actions.put(processorId, actionMetaData);
    // note this also updates state.preferredHost count
    containerAllocator.issueResourceRequest(resourceRequest);
    LOG.info("Control action with metadata {} and issued a request for resources in progress", actionMetaData);
    return actionStatus;
  }

  public Optional<Long> getActionExpiryTimeout(String processorId) {
    return this.actions.containsKey(processorId) ? Optional.of(
        this.actions.get(processorId).getRequestActionExpiryTimeout()) : Optional.empty();
  }

  /**
   * Handles the container allocation for an existing container placement action by issuing a stop on the active container and
   * waiting for the active container to shutdown for a timeout of {@code actionMetaData#getRequestActionExpiryTimeout}.
   *
   * Case 1. If active container fails to stop mark the container placement action failed.
   * Case 2. Otherwise once active container shuts down then issue a start for the container on the preferred host.
   *
   * This method is invoked by the allocator thread which waits on notify from the thread issuing
   * callbacks i.e {@link ContainerProcessManager}
   */
  private void handleContainerAllocationForExistingControlAction(String processorId, ContainerAllocator allocator,
      ResourceRequestState state, SamzaResourceRequest request, String preferredhost) {
    // check if container is already dead without issuing a stop here, fail the move request
    ContainerPlacementMetadata actionMetaData = getControlActionMetadata(processorId).get();
    if (samzaApplicationState.runningProcessors.containsKey(processorId)) {
      LOG.info("Requesting running container to shutdown due to existing container placement action {}",
          getControlActionMetadata(processorId).get());
      clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(processorId));
      Object activeContainerStopped = actionMetaData.getActiveContainerStopped();
      try {
        synchronized (activeContainerStopped) {
          activeContainerStopped.wait(actionMetaData.getRequestActionExpiryTimeout());
        }
        LOG.info("Active container is stopped issuing a run on the allocated resource");
        allocator.runStreamProcessor(request, preferredhost);
      } catch (InterruptedException e) {
        e.printStackTrace();
        state.cancelResourceRequest(request);
        handleControlActionFailure(processorId, actionMetaData,
            "timed out waiting for active container to stop with a timeout (ms) of: "
                + actionMetaData.getRequestActionExpiryTimeout());
      }
    }
  }

  private void handleControlActionFailure(String processorId, ContainerPlacementMetadata metaData, String failureMessage) {
    metaData.setActionStatus(ContainerPlacementStatus.StatusCode.FAILED, failureMessage);
    LOG.info("Control Action failed with metadata {}", metaData);
    this.actions.remove(processorId);
  }

  /**
   * Check if a activeContainerResource has control-action-metadata associated with it
   */
  private Optional<ContainerPlacementMetadata> getControlActionMetadata(String processorId) {
    return this.actions.containsKey(processorId) ? Optional.of(this.actions.get(processorId)) : Optional.empty();
  }

  /**
   * A valid container placement action is only issued for a running processor with a valid processor id
   */
  private ContainerPlacementStatus checkValidControlAction(String processorId, String destinationHost) {
    String errorMessagePrefix =
        String.format("ControlAction to move or restart container with processor id %s to host %s is rejected due to",
            processorId, destinationHost);
    Boolean invalidAction = false;
    String errorMessage = null;
    if (standbyContainerManager.isPresent()) {
      errorMessage = String.format("%s not supported for host standby enabled", errorMessagePrefix);
      invalidAction = true;
    } else if (processorId == null || destinationHost == null) {
      errorMessage = String.format("%s either processor id or the host argument is null", errorMessagePrefix);
      invalidAction = true;
    } else if (Integer.parseInt(processorId) >= samzaApplicationState.processorCount.get()) {
      errorMessage = String.format("%s invalid processor id", errorMessagePrefix);
      invalidAction = true;
    } else if (actions.containsKey(processorId)) {
      errorMessage = String.format("%s existing container placement action on container with metadata %s", errorMessagePrefix,
          actions.get(processorId));
      invalidAction = true;
    } else if (!samzaApplicationState.runningProcessors.containsKey(processorId)
        || samzaApplicationState.pendingProcessors.containsKey(processorId)) {
      errorMessage = String.format("%s container is either is not running or is in pending state", errorMessagePrefix);
      invalidAction = true;
    }

    if (invalidAction) {
      LOG.info(errorMessage);
      return new ContainerPlacementStatus(ContainerPlacementStatus.StatusCode.BAD_REQUEST, errorMessage);
    }

    return new ContainerPlacementStatus(ContainerPlacementStatus.StatusCode.ACCEPTED);
  }

}
