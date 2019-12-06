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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javafx.util.Pair;
import org.apache.samza.clustermanager.container.placements.ContainerPlacementMetadata;
import org.apache.samza.container.placements.ContainerPlacementMessage;
import org.apache.samza.container.placements.ContainerPlacementRequestMessage;
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
   * Resource-manager, used to stop containers
   */
  private final ClusterResourceManager clusterResourceManager;
  private final SamzaApplicationState samzaApplicationState;
  private final boolean hostAffinityEnabled;

  /**
   * Map maintaining active container placement action meta data indexed by container's processorId eg 0, 1, 2
   * Key is chosen to be processorId since at a time only one placement action can be in progress on a container.
   */
  private final ConcurrentHashMap<String, ContainerPlacementMetadata> actions;

  private final Optional<StandbyContainerManager> standbyContainerManager;

  public ContainerManager(SamzaApplicationState samzaApplicationState, ClusterResourceManager clusterResourceManager,
      boolean hostAffinityEnabled, boolean standByEnabled) {
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
   * Handles the container start action for both active & standby containers. This method is invoked by the allocator thread
   *
   * Case 1. If the container launch request is due to an existing container placement action, issue a stop on active
   *         container & wait for the active container to be stopped before issuing a start.
   * Case 2. If StandbyContainer is present refer to {@code StandbyContainerManager#checkStandbyConstraintsAndRunStreamProcessor}
   * Case 3. Otherwise just invoke a container start on the allocated resource for the pending request
   *
   * TODO: SAMZA-2399: Investigate & configure a timeout for container stop if needed
   *
   * @param request pending request for the preferred host
   * @param preferredHost preferred host to start the container
   * @param allocatedResource resource allocated from {@link ClusterResourceManager}
   * @param resourceRequestState state of request in {@link ContainerAllocator}
   * @param allocator to request resources from @{@link ClusterResourceManager}
   *
   * @return true if the container launch is complete, false if the container launch is in progress. A container launch
   *         might be in progress when it is waiting for the previous container incarnation to stop in case of container
   *         placement actions
   */
  boolean handleContainerLaunch(SamzaResourceRequest request, String preferredHost, SamzaResource allocatedResource,
      ResourceRequestState resourceRequestState, ContainerAllocator allocator) {
    LOG.info("Found an available container for Processor ID: {} on the host: {}", request.getProcessorId(), preferredHost);
    if (hasActiveContainerPlacementAction(request.getProcessorId())) {
      String processorId = request.getProcessorId();
      ContainerPlacementMetadata actionMetaData = getPlacementActionMetadata(processorId).get();
      ContainerPlacementMetadata.ContainerStatus actionStatus = actionMetaData.getContainerStatus();
      if (samzaApplicationState.runningProcessors.containsKey(processorId) && actionStatus == ContainerPlacementMetadata.ContainerStatus.RUNNING) {
        LOG.info("Requesting running container to shutdown due to existing container placement action {}",
            actionMetaData);
        actionMetaData.setContainerStatus(ContainerPlacementMetadata.ContainerStatus.STOP_IN_PROGRESS);
        clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(processorId));
        return false;
      } else if (actionStatus == ContainerPlacementMetadata.ContainerStatus.STOP_IN_PROGRESS) {
        LOG.info("Waiting for running container to shutdown due to existing container placement action {}", actionMetaData);
        return false;
      } else if (actionStatus == ContainerPlacementMetadata.ContainerStatus.STOPPED) {
        allocator.runStreamProcessor(request, preferredHost);
        return true;
      }
    }

    if (this.standbyContainerManager.isPresent()) {
      standbyContainerManager.get()
          .checkStandbyConstraintsAndRunStreamProcessor(request, preferredHost, allocatedResource, allocator,
              resourceRequestState);
    } else {
      allocator.runStreamProcessor(request, preferredHost);
    }
    return true;
  }

  /**
   * Handles the action to be taken after the container has been stopped. If stop was issued due to existing control
   * action, mark the container as stopped, otherwise
   *
   * Case 1. When standby is enabled, refer to {@link StandbyContainerManager#handleContainerStop} to check constraints.
   * Case 2. When standby is disabled there are two cases according to host-affinity being enabled
   *    Case 2.1. When host-affinity is enabled resources are requested on host where container was last seen
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
    if (hasActiveContainerPlacementAction(processorId)) {
      LOG.info("Setting the for container state with processorId {} to be stopped because of existing container placement action", processorId);
      getPlacementActionMetadata(processorId).get().setContainerStatus(ContainerPlacementMetadata.ContainerStatus.STOPPED);
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
    if (processorId != null && hasActiveContainerPlacementAction(processorId)) {
      ContainerPlacementMetadata metaData = getPlacementActionMetadata(processorId).get();
      // Issue a request to start the container on source host
      String sourceHost = hostAffinityEnabled ? metaData.getSourceHost() : ResourceRequestState.ANY_HOST;
      markContainerPlacementActionFailed(metaData,
          String.format("failed to start container on destination host %s, attempting to start on source host %s",
              preferredHost, sourceHost));
      containerAllocator.requestResource(processorId, sourceHost);
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
    if (hasActiveContainerPlacementAction(processorId)) {
      ContainerPlacementMetadata metadata = getPlacementActionMetadata(processorId).get();
      metadata.setActionStatus(ContainerPlacementMessage.StatusCode.SUCCEEDED,
              "Successfully completed the container placement action");
      LOG.info("Marking the container placement action success {}", metadata);
    }
  }

  /**
   * Handles an expired resource request for both active and standby containers.
   *
   * Case 1. If this expired request is due to a container placement action mark the request as failed and return
   * Case 2: Otherwise for a normal resource request following cases are possible
   *    Case 2.1  If StandbyContainer is present refer to {@code StandbyContainerManager#handleExpiredResourceRequest}
   *    Case 2.2: host-affinity is enabled, allocator thread looks for allocated resources on ANY_HOST and issues a
   *              container start if available, otherwise issue an ANY_HOST request
   *    Case 2.2: host-affinity is disabled, allocator thread does not handle expired requests, it waits for cluster
   *              manager to return resources on ANY_HOST
   *
   * @param processorId logical id of the container
   * @param preferredHost host on which container is requested to be deployed
   * @param request pending request for the preferred host
   * @param allocator allocator for requesting resources
   * @param resourceRequestState state of request in {@link ContainerAllocator}
   */
  @VisibleForTesting
  void handleExpiredRequest(String processorId, String preferredHost,
      SamzaResourceRequest request, ContainerAllocator allocator, ResourceRequestState resourceRequestState) {
    boolean resourceAvailableOnAnyHost = allocator.hasAllocatedResource(ResourceRequestState.ANY_HOST);


    // Case 1. Container placement actions can be taken in either cases of host affinity being set, in both cases
    // mark the control action failed
    if (hasActiveContainerPlacementAction(processorId)) {
      resourceRequestState.cancelResourceRequest(request);
      markContainerPlacementActionFailed(getPlacementActionMetadata(processorId).get(),
          "failed the Control action because request for resources to ClusterManager expired");
      return;
    }

    // Case 2. When host affinity is disabled wait for cluster resource manager return resources
    if (!hostAffinityEnabled) {
      return;
    }

    // Case 2. When host affinity is enabled handle the expired requests
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
   * @param requestMessage request containing logical processor id 0,1,2 and host where container is desired to be moved,
   *                       acceptable values of this param are any valid hostname or "ANY_HOST"(in this case the request
   *                       is sent to resource manager for any host)
   * @param containerAllocator to request physical resources
   */
  public void registerContainerPlacementAction(ContainerPlacementRequestMessage requestMessage, ContainerAllocator containerAllocator) {
    LOG.info("Received ControlAction request to move or restart container with processor id {} to host {}",
        requestMessage.getProcessorId(), requestMessage.getDestinationHost());
    String processorId = requestMessage.getProcessorId();
    String destinationHost = requestMessage.getDestinationHost();
    Pair<ContainerPlacementMessage.StatusCode, String> actionStatus =
        checkValidControlAction(processorId, destinationHost, requestMessage.getUuid());

    // Request is bad just update the response on message & return
    if (actionStatus.getKey() == ContainerPlacementMessage.StatusCode.BAD_REQUEST) {
      return;
    }

    SamzaResource currentResource = samzaApplicationState.runningProcessors.get(processorId);
    LOG.info("Processor ID: {} matched an active container with deployment ID: {} running on host: {}", processorId,
        currentResource.getContainerId(), currentResource.getHost());

    if (!hostAffinityEnabled) {
      LOG.info("Changing the requested host to {} because either it is requested or host affinity is disabled",
          ResourceRequestState.ANY_HOST);
      destinationHost = ANY_HOST;
    }

    SamzaResourceRequest resourceRequest = containerAllocator.getResourceRequest(processorId, destinationHost);
    ContainerPlacementMetadata actionMetaData = new ContainerPlacementMetadata(requestMessage, currentResource.getHost());
    // Record the resource request for monitoring
    actionMetaData.setActionStatus(ContainerPlacementMessage.StatusCode.IN_PROGRESS, "Preferred Resources requested");
    actionMetaData.recordResourceRequest(resourceRequest);
    actions.put(processorId, actionMetaData);
    containerAllocator.issueResourceRequest(resourceRequest);
    LOG.info("Container Placement action with metadata {} and issued a request for resources in progress",
        actionMetaData);
  }

  /**
   * This method is only used for Testing the Container Placement actions to get a hold of {@link ContainerPlacementMetadata}
   * for assertions. Not intended to be used in src
   */
  @VisibleForTesting
  ContainerPlacementMetadata registerContainerPlacementActionForTest(ContainerPlacementRequestMessage requestMessage,
      ContainerAllocator containerAllocator) {
    registerContainerPlacementAction(requestMessage, containerAllocator);
    if (hasActiveContainerPlacementAction(requestMessage.getProcessorId())) {
      return getPlacementActionMetadata(requestMessage.getProcessorId()).get();
    }
    return null;
  }

  public Optional<Duration> getActionExpiryTimeout(SamzaResourceRequest resourceRequest) {
    for (ContainerPlacementMetadata actionMetadata : actions.values()) {
      if (actionMetadata.containsResourceRequest(resourceRequest)
          && actionMetadata.getActionStatus() == ContainerPlacementMessage.StatusCode.IN_PROGRESS) {
        return actionMetadata.getRequestActionExpiryTimeout();
      }
    }
    return Optional.empty();
  }

  private void markContainerPlacementActionFailed(ContainerPlacementMetadata metaData, String failureMessage) {
    metaData.setActionStatus(ContainerPlacementMessage.StatusCode.FAILED, failureMessage);
    LOG.info("Container Placement action failed with metadata {}", metaData);
  }

  /**
   * A ContainerPlacementAction is only active if it is either CREATED, ACCEPTED or IN_PROGRESS
   */
  private boolean hasActiveContainerPlacementAction(String processorId) {
    Optional<ContainerPlacementMetadata> metadata = getPlacementActionMetadata(processorId);
    if (metadata.isPresent()) {
      switch (metadata.get().getActionStatus()) {
        case ACCEPTED:
        case CREATED:
        case IN_PROGRESS:
          return true;
        default:
          return false;
      }
    }
    return false;
  }

  /**
   * Check if a activeContainerResource has control-action-metadata associated with it
   */
  private Optional<ContainerPlacementMetadata> getPlacementActionMetadata(String processorId) {
    return Optional.ofNullable(this.actions.get(processorId));
  }

  /**
   * A valid container placement action is only issued for a running processor with a valid processor id which has no
   * in flight container requests. Duplicate actions are handled by deduping on uuid
   *
   * TODO: SAMZA-2402: Disallow pending Container Placement actions in metastore on job restarts
   */
  private Pair<ContainerPlacementMessage.StatusCode, String> checkValidControlAction(String processorId, String destinationHost, UUID uuid) {
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
    } else if (hasActiveContainerPlacementAction(processorId)) {
      errorMessage = String.format("%s existing container placement action on container with metadata %s", errorMessagePrefix,
          actions.get(processorId));
      invalidAction = true;
    } else if (actions.get(processorId) != null && actions.get(processorId).getUuid().equals(uuid)) {
      errorMessage = String.format("%s duplicate Container Placement request received, previous action taken: %s", errorMessagePrefix,
          actions.get(processorId));
      invalidAction = true;
    } else if (!samzaApplicationState.runningProcessors.containsKey(processorId)
        || samzaApplicationState.pendingProcessors.containsKey(processorId)) {
      errorMessage = String.format("%s container is either is not running or is in pending state", errorMessagePrefix);
      invalidAction = true;
    }

    if (invalidAction) {
      LOG.info(errorMessage);
      return new Pair<>(ContainerPlacementMessage.StatusCode.BAD_REQUEST, errorMessage);
    }

    return new Pair<>(ContainerPlacementMessage.StatusCode.ACCEPTED, "Request is accepted");
  }

}
