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
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementMetadataStore;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementMetadata;
import org.apache.samza.config.Config;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.placement.ContainerPlacementMessage;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.apache.samza.container.placement.ContainerPlacementResponseMessage;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.util.BoundedLinkedHashSet;
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
 */
public class ContainerManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);
  private static final String ANY_HOST = ResourceRequestState.ANY_HOST;
  private static final String LAST_SEEN = "LAST_SEEN";
  private static final String FORCE_RESTART_LAST_SEEN = "FORCE_RESTART_LAST_SEEN";
  private static final int UUID_CACHE_SIZE = 20000;

  /**
   * Container placement metadata store to write responses to control actions
   */
  private final ContainerPlacementMetadataStore containerPlacementMetadataStore;
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
  /**
   * In-memory cache of placement requests UUIDs de-queued from the metadata store. Used to de-dup requests with the same
   * request UUID. Sized using max tolerable memory footprint and max likely duplicate-spacing.
   */
  private final BoundedLinkedHashSet<UUID> placementRequestsCache;

  private final Optional<StandbyContainerManager> standbyContainerManager;

  private final LocalityManager localityManager;

  public ContainerManager(ContainerPlacementMetadataStore containerPlacementMetadataStore,
      SamzaApplicationState samzaApplicationState, ClusterResourceManager clusterResourceManager,
      boolean hostAffinityEnabled, boolean standByEnabled, LocalityManager localityManager, FaultDomainManager faultDomainManager, Config config) {
    Preconditions.checkNotNull(localityManager, "Locality manager cannot be null");
    Preconditions.checkNotNull(faultDomainManager, "Fault domain manager cannot be null");
    this.samzaApplicationState = samzaApplicationState;
    this.clusterResourceManager = clusterResourceManager;
    this.actions = new ConcurrentHashMap<>();
    this.placementRequestsCache = new BoundedLinkedHashSet<UUID>(UUID_CACHE_SIZE);
    this.hostAffinityEnabled = hostAffinityEnabled;
    this.containerPlacementMetadataStore = containerPlacementMetadataStore;
    this.localityManager = localityManager;
    // Enable standby container manager if required
    if (standByEnabled) {
      this.standbyContainerManager =
          Optional.of(new StandbyContainerManager(samzaApplicationState, clusterResourceManager, localityManager, config, faultDomainManager));
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
    if (hasActiveContainerPlacementAction(request.getProcessorId())) {
      String processorId = request.getProcessorId();
      ContainerPlacementMetadata actionMetaData = getPlacementActionMetadata(processorId).get();
      ContainerPlacementMetadata.ContainerStatus actionStatus = actionMetaData.getContainerStatus();
      if (samzaApplicationState.runningProcessors.containsKey(processorId) && actionStatus == ContainerPlacementMetadata.ContainerStatus.RUNNING) {
        LOG.debug("Requesting running container to shutdown due to existing ContainerPlacement action {}", actionMetaData);
        actionMetaData.setContainerStatus(ContainerPlacementMetadata.ContainerStatus.STOP_IN_PROGRESS);
        updateContainerPlacementActionStatus(actionMetaData, ContainerPlacementMessage.StatusCode.IN_PROGRESS,
            "Active container stop in progress");
        clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(processorId));
        return false;
      } else if (actionStatus == ContainerPlacementMetadata.ContainerStatus.STOP_IN_PROGRESS) {
        LOG.info("Waiting for running container to shutdown due to existing ContainerPlacement action {}", actionMetaData);
        return false;
      } else if (actionStatus == ContainerPlacementMetadata.ContainerStatus.STOP_FAILED) {
        LOG.info("Shutdown on running container failed for action {}", actionMetaData);
        markContainerPlacementActionFailed(actionMetaData,
            String.format("failed to stop container on current host %s", actionMetaData.getSourceHost()));
        resourceRequestState.cancelResourceRequest(request);
        return true;
      } else if (actionStatus == ContainerPlacementMetadata.ContainerStatus.STOPPED) {
        // If the job has standby containers enabled, always check standby constraints before issuing a start on container
        // Note: Always check constraints against allocated resource, since preferred host can be ANY_HOST as well
        if (standbyContainerManager.isPresent() && !standbyContainerManager.get().checkStandbyConstraints(request.getProcessorId(), allocatedResource.getHost())) {
          LOG.info(
              "Starting container {} on host {} does not meet standby constraints, falling back to source host placement metadata: {}",
              request.getProcessorId(), preferredHost, actionMetaData);
          // Release unstartable container
          standbyContainerManager.get().releaseUnstartableContainer(request, allocatedResource, preferredHost, resourceRequestState);
          // Fallback to source host since the new allocated resource does not meet standby constraints
          allocator.requestResource(processorId, actionMetaData.getSourceHost());
          markContainerPlacementActionFailed(actionMetaData,
              String.format("allocated resource %s does not meet standby constraints now, falling back to source host", allocatedResource));
        } else {
          LOG.info("Status updated for ContainerPlacement action: ", actionMetaData);
          allocator.runStreamProcessor(request, preferredHost);
        }
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
      ContainerPlacementMetadata metadata = getPlacementActionMetadata(processorId).get();
      LOG.info("Setting the container state with Processor ID: {} to be stopped because of existing ContainerPlacement action: {}",
          processorId, metadata);
      metadata.setContainerStatus(ContainerPlacementMetadata.ContainerStatus.STOPPED);
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
   * Handle the container stop failure for active containers and standby (if enabled).
   * @param processorId logical id of the container eg 1,2,3
   * @param containerId last known id of the container deployed
   * @param containerHost host on which container is requested to be deployed
   * @param containerAllocator allocator for requesting resources
   * TODO: SAMZA-2512 Add integ test for handleContainerStopFail
   */
  void handleContainerStopFail(String processorId, String containerId, String containerHost,
      ContainerAllocator containerAllocator) {
    if (processorId != null && hasActiveContainerPlacementAction(processorId)) {
      // Assuming resource acquired on destination host will be relinquished by the containerAllocator,
      // We mark the placement action as failed, and return.
      ContainerPlacementMetadata metaData = getPlacementActionMetadata(processorId).get();
      metaData.setContainerStatus(ContainerPlacementMetadata.ContainerStatus.STOP_FAILED);
    } else if (processorId != null && standbyContainerManager.isPresent()) {
      standbyContainerManager.get().handleContainerStopFail(processorId, containerId, containerAllocator);
    } else {
      LOG.warn("Did not find a running Processor ID for Container ID: {} on host: {}. "
          + "Ignoring invalid/redundant notification.", containerId, containerHost);
    }
  }

  /**
   * Handles the state update on successful launch of a container, if this launch is due to a container placement action updates the
   * related metadata to report success
   *
   * @param processorId logical processor id of container 0,1,2
   */
  void handleContainerLaunchSuccess(String processorId, String containerHost) {
    if (hasActiveContainerPlacementAction(processorId)) {
      ContainerPlacementMetadata metadata = getPlacementActionMetadata(processorId).get();
      // Mark the active container running again and dispatch a response
      metadata.setContainerStatus(ContainerPlacementMetadata.ContainerStatus.RUNNING);
      updateContainerPlacementActionStatus(metadata, ContainerPlacementMessage.StatusCode.SUCCEEDED,
          "Successfully completed the container placement action started container on host " + containerHost);
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
    // mark the container placement action failed
    if (hasActiveContainerPlacementAction(processorId)) {
      resourceRequestState.cancelResourceRequest(request);
      markContainerPlacementActionFailed(getPlacementActionMetadata(processorId).get(),
          "failed the ContainerPlacement action because request for resources to ClusterManager expired");
      return;
    }

    // Case 2. When host affinity is disabled wait for cluster resource manager return resources
    // TODO: SAMZA-2330: Handle expired request for host affinity disabled case by retying request for getting ANY_HOST
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
   * Handles expired allocated resource by requesting the same resource again and release the expired allocated resource
   *
   * @param request pending request for the preferred host
   * @param resource resource allocated from {@link ClusterResourceManager} which has expired
   * @param preferredHost host on which container is requested to be deployed
   * @param resourceRequestState state of request in {@link ContainerAllocator}
   * @param allocator allocator for requesting resources
   */
  void handleExpiredResource(SamzaResourceRequest request, SamzaResource resource, String preferredHost,
      ResourceRequestState resourceRequestState, ContainerAllocator allocator) {
    LOG.info("Allocated resource {} has expired for Processor ID: {} request: {}. Re-requesting resource again",
        resource, request.getProcessorId(), request);
    resourceRequestState.releaseUnstartableContainer(resource, preferredHost);
    resourceRequestState.cancelResourceRequest(request);
    SamzaResourceRequest newResourceRequest = allocator.getResourceRequest(request.getProcessorId(), request.getPreferredHost());
    if (hasActiveContainerPlacementAction(newResourceRequest.getProcessorId())) {
      ContainerPlacementMetadata metadata = getPlacementActionMetadata(request.getProcessorId()).get();
      metadata.recordResourceRequest(newResourceRequest);
    }
    allocator.issueResourceRequest(newResourceRequest);
  }

  /**
   * Registers a container placement action to move the running container to destination host, if destination host is same as the
   * host on which container is running, container placement action is treated as a restart.
   *
   * When host affinity is disabled a move / restart is only allowed on ANY_HOST
   * When host affinity is enabled move / restart is allowed on specific or ANY_HOST
   *
   * Container placement requests are tied to deploymentId which is currently {@link org.apache.samza.config.ApplicationConfig#APP_RUN_ID}
   * On job restarts container placement requests queued for the previous deployment are deleted using this
   *
   * All kinds of container placement request except for when destination host is "FORCE_RESTART_LAST_SEEN" work with
   * a RESERVE - STOP - START policy, which means resources are accrued first before issuing a container stop, failure to
   * do so will leave the running container untouched. Requests with destination host "FORCE_RESTART_LAST_SEEN" works with
   * STOP - RESERVE - START policy, which means running container is stopped first then resource request are issued, this case
   * is equivalent to doing a kill -9 on a container
   *
   * @param requestMessage request containing logical processor id 0,1,2 and host where container is desired to be moved,
   *                       acceptable values of this param are
   *                       - valid hostname
   *                       - "ANY_HOST" in this case the request is sent to resource manager for any host
   *                       - "LAST_SEEN" in this case request is sent to resource manager for last seen host
   *                       - "FORCE_RESTART_LAST_SEEN" in this case request is sent to resource manager for last seen host
   * @param containerAllocator to request physical resources
   */
  public void registerContainerPlacementAction(ContainerPlacementRequestMessage requestMessage, ContainerAllocator containerAllocator) {
    String processorId = requestMessage.getProcessorId();
    String destinationHost = requestMessage.getDestinationHost();
    // Is the action ready to be de-queued and taken or it needs to wait to be executed in future
    if (!deQueueAction(requestMessage)) {
      return;
    }
    LOG.info("ContainerPlacement action is de-queued metadata: {}", requestMessage);
    Pair<ContainerPlacementMessage.StatusCode, String> actionStatus = validatePlacementAction(requestMessage);
    // Action is de-queued upon so we record it in the cache
    placementRequestsCache.put(requestMessage.getUuid());
    // Remove the request message from metastore since this message is already acted upon
    containerPlacementMetadataStore.deleteContainerPlacementRequestMessage(requestMessage.getUuid());
    // Request is bad just update the response on message & return
    if (actionStatus.getKey() == ContainerPlacementMessage.StatusCode.BAD_REQUEST) {
      LOG.info("Status updated for ContainerPlacement action request: {} response: {}", requestMessage, actionStatus.getValue());
      writeContainerPlacementResponseMessage(requestMessage, actionStatus.getKey(), actionStatus.getValue());
      return;
    }

    /*
     * When destination host is {@code FORCE_RESTART_LAST_SEEN} its treated as eqvivalent to kill -9 operation for the container
     * In this scenario container is stopped first and we fallback to normal restart path so the policy here is
     * stop - reserve - move
     */
    if (destinationHost.equals(FORCE_RESTART_LAST_SEEN)) {
      LOG.info("Issuing a force restart for Processor ID: {} for ContainerPlacement action request {}", processorId, requestMessage);
      clusterResourceManager.stopStreamProcessor(samzaApplicationState.runningProcessors.get(processorId));
      writeContainerPlacementResponseMessage(requestMessage, ContainerPlacementMessage.StatusCode.SUCCEEDED,
          "Successfully issued a stop container request falling back to normal restart path");
      return;
    }

    /**
     * When destination host is {@code LAST_SEEN} its treated as a restart request on the host where container is running
     * on or has been seen last, but in this policy would be reserve - stop - move, which means reserve resources first
     * only if resources are accrued stop the active container and issue a start on it on resource acquired
     */
    if (destinationHost.equals(LAST_SEEN)) {
      String lastSeenHost = getSourceHostForContainer(requestMessage);
      LOG.info("Changing the requested host for placement action to {} because requested host is LAST_SEEN", lastSeenHost);
      destinationHost = lastSeenHost;
    }

    // TODO: SAMZA-2457: Allow host affinity disabled jobs to move containers to specific host
    if (!hostAffinityEnabled) {
      LOG.info("Changing the requested host for placement action to {} because host affinity is disabled", ResourceRequestState.ANY_HOST);
      destinationHost = ANY_HOST;
    }

    // Register metadata
    ContainerPlacementMetadata actionMetaData = new ContainerPlacementMetadata(requestMessage, getSourceHostForContainer(requestMessage));
    actions.put(processorId, actionMetaData);

    // If the job is running in a degraded state then the container is already stopped
    if (samzaApplicationState.failedProcessors.containsKey(requestMessage.getProcessorId())) {
      actionMetaData.setContainerStatus(ContainerPlacementMetadata.ContainerStatus.STOPPED);
    }

    SamzaResourceRequest resourceRequest = containerAllocator.getResourceRequest(processorId, destinationHost);
    // Record the resource request for monitoring
    actionMetaData.recordResourceRequest(resourceRequest);
    actions.put(processorId, actionMetaData);
    updateContainerPlacementActionStatus(actionMetaData, ContainerPlacementMessage.StatusCode.IN_PROGRESS, "Preferred Resources requested");
    containerAllocator.issueResourceRequest(resourceRequest);
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
    samzaApplicationState.failedContainerPlacementActions.incrementAndGet();
    updateContainerPlacementActionStatus(metaData, ContainerPlacementMessage.StatusCode.FAILED, failureMessage);
  }

  /**
   * A ContainerPlacementAction is only active if it is either CREATED, ACCEPTED or IN_PROGRESS
   */
  private boolean hasActiveContainerPlacementAction(String processorId) {
    Optional<ContainerPlacementMetadata> metadata = getPlacementActionMetadata(processorId);
    if (metadata.isPresent()) {
      switch (metadata.get().getActionStatus()) {
        case ACCEPTED:
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

  private void updateContainerPlacementActionStatus(ContainerPlacementMetadata metadata,
      ContainerPlacementMessage.StatusCode statusCode, String responseMessage) {
    metadata.setActionStatus(statusCode, responseMessage);
    writeContainerPlacementResponseMessage(metadata.getRequestMessage(), statusCode, responseMessage);
    LOG.info("Status updated for ContainerPlacement action: {}", metadata);
  }

  private void writeContainerPlacementResponseMessage(ContainerPlacementRequestMessage requestMessage,
      ContainerPlacementMessage.StatusCode statusCode, String responseMessage) {
    containerPlacementMetadataStore.writeContainerPlacementResponseMessage(
        ContainerPlacementResponseMessage.fromContainerPlacementRequestMessage(requestMessage, statusCode,
            responseMessage, System.currentTimeMillis()));
  }

  /**
   * Gets the hostname on which container is either currently running or was last seen on if it is not running
   * TODO SAMZA-2480: Move logic related to onResourcesCompleted from ContainerProcessManager to ContainerManager
   */
  private String getSourceHostForContainer(ContainerPlacementRequestMessage requestMessage) {
    String sourceHost = null;
    String processorId = requestMessage.getProcessorId();
    if (samzaApplicationState.runningProcessors.containsKey(processorId)) {
      SamzaResource currentResource = samzaApplicationState.runningProcessors.get(processorId);
      LOG.info("Processor ID: {} matched a running container with containerId ID: {} is running on host: {} for ContainerPlacement action: {}",
          processorId, currentResource.getContainerId(), currentResource.getHost(), requestMessage);
      sourceHost = currentResource.getHost();
    } else {
      sourceHost = Optional.ofNullable(localityManager.readLocality().getProcessorLocality(processorId))
          .map(ProcessorLocality::host)
          .orElse(null);
      LOG.info("Processor ID: {} is not running and was last seen on host: {} for ContainerPlacement action: {}",
          processorId, sourceHost, requestMessage);
    }
    return sourceHost;
  }

  /**
   * These are specific scenarios in which a placement action should wait for existing action to complete before it is executed
   * 1. If there is an placement request in progress on active container
   * 2. If there is an placement request is progress on any of its standby container
   * 3. If the container itself is pending a start
   *
   * @param requestMessage container placement request message
   * @return true if action should be taken right now, false if it needs to wait to be taken in future
   */
  private boolean deQueueAction(ContainerPlacementRequestMessage requestMessage) {
    // Do not dequeue action wait for the in-flight action to complete
    if (checkIfActiveOrStandbyContainerHasActivePlacementAction(requestMessage)) {
      return false;
    }

    if (samzaApplicationState.failedProcessors.containsKey(requestMessage.getProcessorId())) {
      LOG.info("ContainerPlacement request: {} is de-queued, container with Processor ID: {} has exhausted all retries and is in failed state",
          requestMessage, requestMessage.getProcessorId());
      return true;
    }

    // Do not dequeue the action wait for the container to come to a running state
    if (!samzaApplicationState.runningProcessors.containsKey(requestMessage.getProcessorId())
        || samzaApplicationState.pendingProcessors.containsKey(requestMessage.getProcessorId())) {
      LOG.info("ContainerPlacement request: {} is en-queued because container is pending start", requestMessage);
      return false;
    }
    return true;
  }

  /**
   * A valid container placement action needs a valid processor id. Duplicate actions are handled by de-duping on uuid.
   * If standby containers are enabled destination host requested must meet standby constraints
   *
   * @param requestMessage container placement request message
   * @return Pair<ContainerPlacementMessage.StatusCode, String> which is status code & response suggesting if the request is valid
   */
  private Pair<ContainerPlacementMessage.StatusCode, String> validatePlacementAction(ContainerPlacementRequestMessage requestMessage) {
    String errorMessagePrefix = ContainerPlacementMessage.StatusCode.BAD_REQUEST + " reason: %s";
    Boolean invalidAction = false;
    String errorMessage = null;

    boolean isRunning = samzaApplicationState.runningProcessors.containsKey(requestMessage.getProcessorId());
    boolean isPending = samzaApplicationState.pendingProcessors.containsKey(requestMessage.getProcessorId());
    boolean isFailed = samzaApplicationState.failedProcessors.containsKey(requestMessage.getProcessorId());

    if (!isRunning && !isPending && !isFailed) {
      errorMessage = String.format(errorMessagePrefix, "invalid processor id neither in running, pending or failed processors");
      invalidAction = true;
    } else if (placementRequestsCache.containsKey(requestMessage.getUuid())) {
      errorMessage = String.format(errorMessagePrefix, "duplicate UUID of the request, please retry");
      invalidAction = true;
    } else if (standbyContainerManager.isPresent() && !standbyContainerManager.get()
        .checkStandbyConstraints(requestMessage.getProcessorId(), requestMessage.getDestinationHost())) {
      errorMessage = String.format(errorMessagePrefix, "destination host does not meet standby constraints");
      invalidAction = true;
    }

    if (invalidAction) {
      return new ImmutablePair<>(ContainerPlacementMessage.StatusCode.BAD_REQUEST, errorMessage);
    }

    return new ImmutablePair<>(ContainerPlacementMessage.StatusCode.ACCEPTED, "Request is accepted");
  }

  /**
   * Checks if there are any active container placement action on container itself or on any of its standby containers
   * (if enabled)
   */
  private boolean checkIfActiveOrStandbyContainerHasActivePlacementAction(ContainerPlacementRequestMessage request) {
    String processorId = request.getProcessorId();
    // Container itself has active container placement actions
    if (hasActiveContainerPlacementAction(processorId)) {
      LOG.info("ContainerPlacement request: {} is en-queued because container has an in-progress placement action", request);
      return true;
    }

    if (standbyContainerManager.isPresent()) {
      // If requested placement action is on a standby container and its active container has a placement request,
      // this request shall not be de-queued until in-flight action on active container is complete
      if (StandbyTaskUtil.isStandbyContainer(processorId) && hasActiveContainerPlacementAction(
          StandbyTaskUtil.getActiveContainerId(processorId))) {
        LOG.info("ContainerPlacement request: {} is en-queued because its active container has an in-progress placement action", request);
        return true;
      }
      // If requested placement action is on a standby container and its active container has a placement request,
      // this request shall not be de-queued until in-flight action on active container is complete
      if (!StandbyTaskUtil.isStandbyContainer(processorId)) {
        for (String standby : standbyContainerManager.get().getStandbyList(processorId)) {
          if (hasActiveContainerPlacementAction(standby)) {
            LOG.info("ContainerPlacement request: {} is en-queued because one of its standby replica has an in-progress placement action", request);
            return true;
          }
        }
      }
    }
    return false;
  }

}
