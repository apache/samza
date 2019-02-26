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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.storage.kv.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Encapsulates logic and state concerning standby-containers.
 */
public class StandbyContainerManager {

  private static final Logger log = LoggerFactory.getLogger(StandbyContainerManager.class);

  private final SamzaApplicationState samzaApplicationState;

  // Map of samza containerIDs to their corresponding active and standby containers, e.g., 0 -> {0-0, 0-1}, 0-0 -> {0, 0-1}
  // This is used for checking no two standbys or active-standby-pair are started on the same host
  private final Map<String, List<String>> standbyContainerConstraints;

  // Map of active containers that are in failover, indexed by the active container's resourceID (at the time of failure)
  private final Map<String, FailoverMetadata> failovers;

  // Resource-manager, used to stop containers
  private ClusterResourceManager clusterResourceManager;

  public StandbyContainerManager(SamzaApplicationState samzaApplicationState,
      ClusterResourceManager clusterResourceManager) {
    this.failovers = new ConcurrentHashMap<>();
    this.standbyContainerConstraints = new HashMap<>();
    this.samzaApplicationState = samzaApplicationState;
    JobModel jobModel = samzaApplicationState.jobModelManager.jobModel();

    // populate the standbyContainerConstraints map by iterating over all containers
    jobModel.getContainers()
        .keySet()
        .forEach(containerId -> standbyContainerConstraints.put(containerId,
            StandbyTaskUtil.getStandbyContainerConstraints(containerId, jobModel)));
    this.clusterResourceManager = clusterResourceManager;

    log.info("Populated standbyContainerConstraints map {}", standbyContainerConstraints);
  }

  /**
   * We handle the stopping of a container depending on the case which is decided using the exit-status:
   *    Case 1. an Active-Container which has stopped for an "unknown" reason, then we start it on the given preferredHost
   *    Case 2. Active container has stopped because of node failure, thene we initiate a failover
   *    Case 3. StandbyContainer has stopped after it was chosen for failover, see {@link StandbyContainerManager#handleStandbyContainerStop}
   *    Case 4. StandbyContainer has stopped but not because of a failover, see {@link StandbyContainerManager#handleStandbyContainerStop}
   *
   * @param containerID containerID of the stopped container
   * @param resourceID last resourceID of the stopped container
   * @param preferredHost the host on which the container was running
   * @param exitStatus the exit status of the failed container
   * @param containerAllocator the container allocator
   */
  public void handleContainerStop(String containerID, String resourceID, String preferredHost, int exitStatus,
      AbstractContainerAllocator containerAllocator) {

    if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      handleStandbyContainerStop(containerID, resourceID, preferredHost, containerAllocator);
    } else {
      // initiate failover for the active container based on the exitStatus
      switch (exitStatus) {
        case SamzaResourceStatus.DISK_FAIL:
        case SamzaResourceStatus.ABORTED:
        case SamzaResourceStatus.PREEMPTED:
          initiateActiveContainerFailover(containerID, resourceID, containerAllocator);
          break;
      // in all other cases, request resource for the failed container
        default:
          log.info("Requesting resource for active-container {} on host {}", containerID, preferredHost);
          containerAllocator.requestResource(containerID, preferredHost);
          break;
      }
    }
  }

  /**
   * Handle the failed launch of a container, based on
   *    Case 1. If it is an active container, then initiate a failover for it.
   *    Case 2. If it is standby container, request a new resource on AnyHost.
   * @param containerID the ID of the container that has failed
   */
  public void handleContainerLaunchFail(String containerID, String resourceID,
      AbstractContainerAllocator containerAllocator) {

    if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      log.info("Handling launch fail for standby-container {}, requesting resource on any host {}", containerID);
      containerAllocator.requestResource(containerID, ResourceRequestState.ANY_HOST);
    } else {
      initiateActiveContainerFailover(containerID, resourceID, containerAllocator);
    }
  }

  /**
   *  If a standby container has stopped, then there are two possible cases
   *    Case 1. during a failover, the standby container was stopped for an active's start, then we
   *       1. request a resource on the standby's host to place the activeContainer, and
   *       2. request anyhost to place this standby
   *
   *    Case 2. independent of a failover, the standby container stopped, in which proceed with its resource-request
   * @param standbyContainerID SamzaContainerID of the standby container
   * @param preferredHost Preferred host of the standby container
   */
  private void handleStandbyContainerStop(String standbyContainerID, String resourceID, String preferredHost,
      AbstractContainerAllocator containerAllocator) {

    // if this standbyContainerResource was stopped for a failover, we will find a metadata entry
    Optional<StandbyContainerManager.FailoverMetadata> failoverMetadata = this.checkIfUsedForFailover(resourceID);

    if (failoverMetadata.isPresent()) {
      String activeContainerID = failoverMetadata.get().activeContainerID;
      String standbyContainerHostname = failoverMetadata.get().getStandbyContainerHostname(resourceID);

      log.info("Requesting resource for active container {} on host {}, and backup container {} on any host",
          activeContainerID, standbyContainerHostname, standbyContainerID);

      containerAllocator.requestResource(activeContainerID,
          standbyContainerHostname); // request standbycontainer's host for active-container
      containerAllocator.requestResource(standbyContainerID,
          ResourceRequestState.ANY_HOST); // request anyhost for standby container
      return;
    } else {
      log.info("Issuing request for standby container {} on host {}, since this is not for a failover",
          standbyContainerID, preferredHost);
      containerAllocator.requestResource(standbyContainerID, preferredHost);
      return;
    }
  }

  /** Method to handle failover for a container.
   *  If it is an active container,
   *  We try to find a standby for the active container, and issue a stop on it.
   *  If we do not find a standby container, we simply issue an anyhost request to place it.
   *  If it is standby container, we simply forward the request to the containerAllocator
   *
   * @param containerID the samzaContainerID of the active-container
   * @param resourceID  the samza-resource-ID of the container when it failed (used to index failover-state)
   */
  private void initiateActiveContainerFailover(String containerID, String resourceID,
      AbstractContainerAllocator containerAllocator) {
    Optional<Entry<String, SamzaResource>> standbyContainer = this.selectStandby(containerID, resourceID);

    // If we find a standbyContainer, we initiate a failover
    if (standbyContainer.isPresent()) {

      // ResourceID of the active container at the time of its last failure
      String standbyContainerId = standbyContainer.get().getKey();
      SamzaResource standbyResource = standbyContainer.get().getValue();
      String standbyResourceID = standbyResource.getResourceID();
      String standbyHost = standbyResource.getHost();

      // update the failover state
      this.registerFailover(containerID, resourceID, standbyResourceID, standbyHost);
      log.info("Initiating failover and stopping standby container, found standbyContainer {} = resource {}, "
          + "for active container {}", standbyContainerId, standbyResourceID, containerID);
      samzaApplicationState.failoversToStandby.incrementAndGet();

      this.clusterResourceManager.stopStreamProcessor(standbyResource);
      return;
    } else {

      // If we dont find a standbyContainer, we proceed with the ANYHOST request
      log.info("No standby container found for active container {}, making a request for {}", containerID,
          ResourceRequestState.ANY_HOST);
      samzaApplicationState.failoversToAnyHost.incrementAndGet();
      containerAllocator.requestResource(containerID, ResourceRequestState.ANY_HOST);
      return;
    }
  }

  /**
   * Method to select a standby container for a given active container that has stopped.
   * TODO: enrich this method to select standby's intelligently based on lag, timestamp, load-balencing, etc.
   * @param activeContainerID Samza containerID of the active container
   * @param activeContainerResourceID ResourceID of the active container at the time of its last failure
   * @return
   */
  private Optional<Entry<String, SamzaResource>> selectStandby(String activeContainerID,
      String activeContainerResourceID) {

    log.info("Standby containers {} for active container {}", this.standbyContainerConstraints.get(activeContainerID), activeContainerID);

    // obtain any existing failover metadata
    Optional<StandbyContainerManager.FailoverMetadata> failoverMetadata =
        activeContainerResourceID == null ? Optional.empty() : this.getFailoverMetadata(activeContainerResourceID);

    // Iterate over the list of running standby containers, to find a standby resource that we have not already
    // used for a failover for this active resoruce
    for (String standbyContainerID : this.standbyContainerConstraints.get(activeContainerID)) {

      if (samzaApplicationState.runningContainers.containsKey(standbyContainerID)) {
        SamzaResource standbyContainerResource = samzaApplicationState.runningContainers.get(standbyContainerID);

        // use this standby if there was no previous failover or if this standbyResource was not used for it
        if (!failoverMetadata.isPresent() || !failoverMetadata.get()
            .isStandbyResourceUsed(standbyContainerResource.getResourceID())) {

          log.info("Returning standby container {} in running state for active container {}", standbyContainerID,
              activeContainerID);
          return Optional.of(new Entry<>(standbyContainerID, standbyContainerResource));
        }
      }
    }

    log.info("Did not find any running standby container for active container {}", activeContainerID);
    return Optional.empty();
  }

  /**
   * Register a new failover that has been initiated for the active container resource (identified by its resource ID).
   */
  private void registerFailover(String activeContainerID, String activeContainerResourceID,
      String selectedStandbyContainerResourceID, String standbyContainerHost) {

    // this active container's resource ID is already registered, in which case update the metadata
    if (failovers.containsKey(activeContainerResourceID)) {
      FailoverMetadata failoverMetadata = failovers.get(activeContainerResourceID);
      failoverMetadata.updateStandbyContainer(selectedStandbyContainerResourceID, standbyContainerHost);
    } else {
      FailoverMetadata failoverMetadata =
          new FailoverMetadata(activeContainerID, activeContainerResourceID, selectedStandbyContainerResourceID,
              standbyContainerHost);
      this.failovers.put(activeContainerResourceID, failoverMetadata);
    }
  }

  /**
   * Check if this standbyContainerResource is present in the failoverState for an active container.
   * This is used to determine if we requested a stop a container.
   */
  private Optional<FailoverMetadata> checkIfUsedForFailover(String standbyContainerResourceId) {

    if (standbyContainerResourceId == null) {
      return Optional.empty();
    }

    for (FailoverMetadata failoverMetadata : failovers.values()) {
      if (failoverMetadata.isStandbyResourceUsed(standbyContainerResourceId)) {
        log.info("Standby container with resource id {} was selected for failover of active container {}",
            standbyContainerResourceId, failoverMetadata.activeContainerID);
        return Optional.of(failoverMetadata);
      }
    }
    return Optional.empty();
  }

  /**
   * Check if matching this SamzaResourceRequest to the given resource, meets all standby-container container constraints.
   *
   * @param request The resource request to match.
   * @param samzaResource The samzaResource to potentially match the resource to.
   * @return
   */
  private boolean checkStandbyConstraints(SamzaResourceRequest request, SamzaResource samzaResource) {
    String containerIDToStart = request.getContainerID();
    String host = samzaResource.getHost();
    List<String> containerIDsForStandbyConstraints = this.standbyContainerConstraints.get(containerIDToStart);

    // Check if any of these conflicting containers are running/launching on host
    for (String containerID : containerIDsForStandbyConstraints) {
      SamzaResource resource = samzaApplicationState.pendingContainers.get(containerID);

      // return false if a conflicting container is pending for launch on the host
      if (resource != null && resource.getHost().equals(host)) {
        log.info("Container {} cannot be started on host {} because container {} is already scheduled on this host",
            containerIDToStart, samzaResource.getHost(), containerID);
        return false;
      }

      // return false if a conflicting container is running on the host
      resource = samzaApplicationState.runningContainers.get(containerID);
      if (resource != null && resource.getHost().equals(host)) {
        log.info("Container {} cannot be started on host {} because container {} is already running on this host",
            containerIDToStart, samzaResource.getHost(), containerID);
        return false;
      }
    }

    return true;
  }

  /**
   *  Attempt to the run a container on the given candidate resource, if doing so meets the standby container constraints.
   * @param request The Samza container request
   * @param preferredHost the preferred host associated with the container
   * @param samzaResource the resource candidate
   */
  public void checkStandbyConstraintsAndRunStreamProcessor(SamzaResourceRequest request, String preferredHost,
      SamzaResource samzaResource, AbstractContainerAllocator containerAllocator,
      ResourceRequestState resourceRequestState) {
    String containerID = request.getContainerID();

    if (checkStandbyConstraints(request, samzaResource)) {
      // This resource can be used to launch this container
      log.info("Running container {} on {} meets standby constraints, preferredHost = {}", containerID, samzaResource.getHost(), preferredHost);
      containerAllocator.runStreamProcessor(request, preferredHost);
      samzaApplicationState.successfulStandbyAllocations.incrementAndGet();

    } else if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      // This resource cannot be used to launch this standby container, so we make a new anyhost request
      log.info("Running standby container {} on host {} does not meet standby constraints, cancelling resource request, releasing resource, and making a new ANY_HOST request",
          containerID, samzaResource.getHost());
      resourceRequestState.releaseUnstartableContainer(samzaResource, preferredHost);
      resourceRequestState.cancelResourceRequest(request);
      containerAllocator.requestResource(containerID, ResourceRequestState.ANY_HOST);
      samzaApplicationState.failedStandbyAllocations.incrementAndGet();
    } else {
      // This resource cannot be used to launch this active container container, so we initiate a failover
      log.warn("Running active container {} on host {} does not meet standby constraints, cancelling resource request, releasing resource",
          containerID, samzaResource.getHost());
      resourceRequestState.releaseUnstartableContainer(samzaResource, preferredHost);
      resourceRequestState.cancelResourceRequest(request);

      // if this active-container has never failed, then simple request anyhost
      if (!samzaApplicationState.failedContainersStatus.containsKey(containerID)) {
        log.info("Requesting ANY_HOST for active container {}", containerID);
        containerAllocator.requestResource(containerID, ResourceRequestState.ANY_HOST);
      } else {
        log.info("Initiating failover for active container {}", containerID);
        // we use the activeContainer's last known resourceID to initiate the failover
        String lastKnownResourceID = samzaApplicationState.failedContainersStatus.get(containerID).getResourceID();
        initiateActiveContainerFailover(containerID, lastKnownResourceID, containerAllocator);
      }

      samzaApplicationState.failedStandbyAllocations.incrementAndGet();
    }
  }

  public void handleExpiredResourceRequest(String containerID, SamzaResourceRequest request,
      Optional<SamzaResource> alternativeResource, AbstractContainerAllocator containerAllocator,
      ResourceRequestState resourceRequestState) {

    if (StandbyTaskUtil.isStandbyContainer(containerID) && alternativeResource.isPresent()) {
      // A standby container can be started on the anyhost-alternative-resource rightaway provided it passes all the
      // standby constraints

      log.info("Handling expired request, standby container {} can be started on alternative resource {}", containerID, alternativeResource.get());

      checkStandbyConstraintsAndRunStreamProcessor(request, ResourceRequestState.ANY_HOST, alternativeResource.get(),
          containerAllocator, resourceRequestState);

    } else if (StandbyTaskUtil.isStandbyContainer(containerID) && !alternativeResource.isPresent()) {
      // If there is no alternative-resource for the standby container we make a new anyhost request

      log.info("Handling expired request, requesting anyHost resource for standby container {}", containerID);

      resourceRequestState.cancelResourceRequest(request);
      containerAllocator.requestResource(containerID, ResourceRequestState.ANY_HOST);

    } else if (!StandbyTaskUtil.isStandbyContainer(containerID) && alternativeResource.isPresent()
        && !samzaApplicationState.failedContainersStatus.containsKey(containerID)) {
      // An active container can be started on the alternative-any-host resource rightaway, if it has no prior failure,
      // that is, it has no recorded failure status

      log.info("Handling expired request, trying to run active container {} on alternative resource {}", containerID, alternativeResource.get());

      checkStandbyConstraintsAndRunStreamProcessor(request, ResourceRequestState.ANY_HOST, alternativeResource.get(),
          containerAllocator, resourceRequestState);

    } else if (!StandbyTaskUtil.isStandbyContainer(containerID) &&
        !samzaApplicationState.failedContainersStatus.containsKey(containerID) && !alternativeResource.isPresent()) {
      // An active container has no prior failure, and there is no-alternative-anyhost resource, so we make a new anyhost
      // request

      log.info("Handling expired request, requesting anyHost resource for active container {} because this active container has never failed", containerID);

      resourceRequestState.cancelResourceRequest(request);
      containerAllocator.requestResource(containerID, ResourceRequestState.ANY_HOST);

    } else if (!StandbyTaskUtil.isStandbyContainer(containerID) &&
        samzaApplicationState.failedContainersStatus.containsKey(containerID)) {
      // An active container that had failed, and whose subsequent resource request has expired, needs to be failed over to
      // a new standby-candidate, so we initiate a failover

      log.info("Handling expired request, initiating failover for active container {}", containerID);

      resourceRequestState.cancelResourceRequest(request);

      // we use the activeContainer's last known resourceID to initiate the failover
      String lastKnownResourceID = samzaApplicationState.failedContainersStatus.get(containerID).getResourceID();
      initiateActiveContainerFailover(containerID, lastKnownResourceID, containerAllocator);

    } else {
      log.error("Handling expired request, invalid state containerID {}, resource request {}", containerID, request);
    }
  }

  /**
   * Check if this activeContainerResource has failover-metadata associated with it
   */
  private Optional<FailoverMetadata> getFailoverMetadata(String activeContainerResourceID) {
    return this.failovers.containsKey(activeContainerResourceID) ? Optional.of(
        this.failovers.get(activeContainerResourceID)) : Optional.empty();
  }

  @Override
  public String toString() {
    return this.failovers.toString();
  }

  /**
   * Encapsulates metadata concerning the failover of an active container.
   */
  public class FailoverMetadata {
    public final String activeContainerID;
    public final String activeContainerResourceID;

    // Map of samza-container-resource ID to host, for each standby container selected for failover of the activeContainer
    public final Map<String, String> selectedStandbyContainers;

    public FailoverMetadata(String activeContainerID, String activeContainerResourceID,
        String selectedStandbyContainerResourceID, String standbyContainerHost) {
      this.activeContainerID = activeContainerID;
      this.activeContainerResourceID = activeContainerResourceID;
      this.selectedStandbyContainers = new HashMap<>();
      this.selectedStandbyContainers.put(selectedStandbyContainerResourceID, standbyContainerHost);
    }

    // Check if this standbyContainerResourceID was used in this failover
    public synchronized boolean isStandbyResourceUsed(String standbyContainerResourceID) {
      return this.selectedStandbyContainers.keySet().contains(standbyContainerResourceID);
    }

    // Get the hostname corresponding to the standby resourceID
    public synchronized String getStandbyContainerHostname(String standbyContainerResourceID) {
      return selectedStandbyContainers.get(standbyContainerResourceID);
    }

    // Add the standbyContainer resource to the list of standbyContainers used in this failover
    public synchronized void updateStandbyContainer(String standbyContainerResourceID, String standbyContainerHost) {
      this.selectedStandbyContainers.put(standbyContainerResourceID, standbyContainerHost);
    }

    @Override
    public String toString() {
      return "[activeContainerID: " + this.activeContainerID + " activeContainerResourceID: "
          + this.activeContainerResourceID + " selectedStandbyContainers:" + selectedStandbyContainers + "]";
    }
  }
}
