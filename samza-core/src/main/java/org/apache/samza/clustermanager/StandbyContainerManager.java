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

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.JobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Encapsulates logic and state concerning standby-containers.
 */
public class StandbyContainerManager {

  private static final Logger log = LoggerFactory.getLogger(StandbyContainerManager.class);

  private final SamzaApplicationState samzaApplicationState;

  private final LocalityManager localityManager;

  // Map of samza containerIDs to their corresponding active and standby containers, e.g., 0 -> {0-0, 0-1}, 0-0 -> {0, 0-1}
  // This is used for checking no two standbys or active-standby-pair are started on the same host
  private final Map<String, List<String>> standbyContainerConstraints;

  // Map of active containers that are in failover, indexed by the active container's resourceID (at the time of failure)
  private final Map<String, FailoverMetadata> failovers;

  // Resource-manager, used to stop containers
  private ClusterResourceManager clusterResourceManager;

  // FaultDomainManager, used to get fault domain information of different hosts from the cluster manager.
  private final FaultDomainManager faultDomainManager;

  private final boolean isFaultDomainAwareStandbyEnabled;

  public StandbyContainerManager(SamzaApplicationState samzaApplicationState, ClusterResourceManager clusterResourceManager,
                                 LocalityManager localityManager, Config config, FaultDomainManager faultDomainManager) {
    this.failovers = new ConcurrentHashMap<>();
    this.localityManager = localityManager;
    this.standbyContainerConstraints = new HashMap<>();
    this.samzaApplicationState = samzaApplicationState;
    JobModel jobModel = samzaApplicationState.jobModelManager.jobModel();

    // populate the standbyContainerConstraints map by iterating over all containers
    jobModel.getContainers()
        .keySet()
        .forEach(containerId -> standbyContainerConstraints.put(containerId,
            StandbyTaskUtil.getStandbyContainerConstraints(containerId, jobModel)));
    this.clusterResourceManager = clusterResourceManager;
    this.faultDomainManager = faultDomainManager;
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.isFaultDomainAwareStandbyEnabled = clusterManagerConfig.getFaultDomainAwareStandbyEnabled();

    log.info("Populated standbyContainerConstraints map {}", standbyContainerConstraints);
  }

  /**
   * We handle the stopping of a container depending on the case which is decided using the exit-status:
   *    Case 1. an Active-Container which has stopped for an "unknown" reason, then we start it on the given preferredHost (but we record the resource-request)
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
      ContainerAllocator containerAllocator, Duration preferredHostRetryDelay) {

    if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      handleStandbyContainerStop(containerID, resourceID, preferredHost, containerAllocator, preferredHostRetryDelay);
    } else {
      // initiate failover for the active container based on the exitStatus
      switch (exitStatus) {
        case SamzaResourceStatus.DISK_FAIL:
        case SamzaResourceStatus.ABORTED:
        case SamzaResourceStatus.PREEMPTED:
          initiateStandbyAwareAllocation(containerID, resourceID, containerAllocator);
          break;
        // in all other cases, request-resource for the failed container, but record the resource-request, so that
        // if this request expires, we can do a failover -- select a standby to stop & start the active on standby's host
        default:
          log.info("Requesting resource for active-container {} on host {}", containerID, preferredHost);
          SamzaResourceRequest resourceRequest = containerAllocator.getResourceRequestWithDelay(containerID, preferredHost, preferredHostRetryDelay);

          FailoverMetadata failoverMetadata = registerActiveContainerFailure(containerID, resourceID);
          failoverMetadata.recordResourceRequest(resourceRequest);
          containerAllocator.issueResourceRequest(resourceRequest);
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
      ContainerAllocator containerAllocator) {

    if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      log.info("Handling launch fail for standby-container {}, requesting resource on any host {}", containerID);
      String activeContainerHost = getActiveContainerHost(containerID)
              .orElse(null);
      requestResource(containerAllocator, containerID, ResourceRequestState.ANY_HOST, Duration.ZERO, activeContainerHost);
    } else {
      initiateStandbyAwareAllocation(containerID, resourceID, containerAllocator);
    }
  }

  /**
   *  Handle the failed stop for a container, based on
   *  Case 1. If it is standby container, continue the failover
   *  Case 2. If it is an active container, then this is in invalid state and throw an exception to alarm/restart.
   * @param containerID the ID (e.g., 0, 1, 2) of the container that has failed
   * @param resourceID id of the resource used for the failed container
   */
  public void handleContainerStopFail(String containerID, String resourceID,
      ContainerAllocator containerAllocator) {
    if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      log.info("Handling stop fail for standby-container {}, continuing the failover (if present)", containerID);

      // if this standbyContainerResource was stopped for a failover, we will find a metadata entry
      Optional<StandbyContainerManager.FailoverMetadata> failoverMetadata = this.checkIfUsedForFailover(resourceID);

      // if we find a metadata entry, we continue with the failover (select another standby or any-host appropriately)
      failoverMetadata.ifPresent(
        metadata -> initiateStandbyAwareAllocation(metadata.activeContainerID, metadata.activeContainerResourceID,
            containerAllocator));
    } else {
      // If this class receives a callback for stop-fail on an active container, throw an exception
      throw new SamzaException("Invalid State. Received stop container fail for container Id: " + containerID);
    }
  }

  /**
   * This method removes the fault domain of the host passed as an argument, from the set of fault domains, and then returns it.
   * The set of fault domains returned is based on the set difference between all the available fault domains in the
   * cluster and the fault domain associated with the host that is passed as input.
   * @param hostToAvoid hostname whose fault domains are excluded
   * @return The set of fault domains which excludes the fault domain that the given host is on
   */
  public Set<FaultDomain> getAllowedFaultDomainsGivenHostToAvoid(String hostToAvoid) {
    Set<FaultDomain> allFaultDomains = faultDomainManager.getAllFaultDomains();
    Set<FaultDomain> faultDomainToAvoid = Optional.ofNullable(hostToAvoid)
            .map(faultDomainManager::getFaultDomainsForHost)
            .orElse(Collections.emptySet());
    allFaultDomains.removeAll(faultDomainToAvoid);
    return allFaultDomains;
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
      ContainerAllocator containerAllocator, Duration preferredHostRetryDelay) {

    // if this standbyContainerResource was stopped for a failover, we will find a metadata entry
    Optional<StandbyContainerManager.FailoverMetadata> failoverMetadata = this.checkIfUsedForFailover(resourceID);

    if (failoverMetadata.isPresent()) {
      String activeContainerID = failoverMetadata.get().activeContainerID;
      String standbyContainerHostname = failoverMetadata.get().getStandbyContainerHostname(resourceID);

      log.info("Requesting resource for active container {} on host {}, and backup container {} on any host",
          activeContainerID, standbyContainerHostname, standbyContainerID);

      // request standbycontainer's host for active-container
      SamzaResourceRequest resourceRequestForActive =
        containerAllocator.getResourceRequestWithDelay(activeContainerID, standbyContainerHostname, preferredHostRetryDelay);
      // record the resource request, before issuing it to avoid race with allocation-thread
      failoverMetadata.get().recordResourceRequest(resourceRequestForActive);
      containerAllocator.issueResourceRequest(resourceRequestForActive);

      // request any-host for standby container
      requestResource(containerAllocator, standbyContainerID, ResourceRequestState.ANY_HOST, Duration.ZERO, standbyContainerHostname);
    } else {
      log.info("Issuing request for standby container {} on host {}, since this is not for a failover",
          standbyContainerID, preferredHost);
      String activeContainerHost = getActiveContainerHost(standbyContainerID)
              .orElse(null);
      requestResource(containerAllocator, standbyContainerID, preferredHost, preferredHostRetryDelay, activeContainerHost);
    }
  }

  /** Method to handle standby-aware allocation for an active container.
   *  We try to find a standby host for the active container, and issue a stop on any standby-containers running on it,
   *  request resource to place the active on the standby's host, and one to place the standby elsewhere.
   *  When requesting for resources,
   *  NOTE: When rack awareness is turned on, we always pass the <code>hostToAvoid</> parameter as null for the {@link #requestResource} method used here
   *  because the hostname of the previous active processor that died does not exist in the running or pending container list anymore.
   *  However, different racks will always be guaranteed through {@link #checkStandbyConstraintsAndRunStreamProcessor}.
   * @param activeContainerID the samzaContainerID of the active-container
   * @param resourceID  the samza-resource-ID of the container when it failed (used to index failover-state)
   */
  private void initiateStandbyAwareAllocation(String activeContainerID, String resourceID,
      ContainerAllocator containerAllocator) {

    String standbyHost = this.selectStandbyHost(activeContainerID, resourceID);

    // if the standbyHost returned is anyhost, we proceed with the request directly
    if (standbyHost.equals(ResourceRequestState.ANY_HOST)) {
      log.info("No standby container found for active container {}, making a resource-request for placing {} on {}, active's resourceID: {}",
          activeContainerID, activeContainerID, ResourceRequestState.ANY_HOST, resourceID);
      samzaApplicationState.failoversToAnyHost.incrementAndGet();
      containerAllocator.requestResource(activeContainerID, ResourceRequestState.ANY_HOST);

    } else {

      // Check if there is a running standby-container on that host that needs to be stopped
      List<String> standbySamzaContainerIds = this.standbyContainerConstraints.get(activeContainerID);

      Map<String, SamzaResource> runningStandbyContainersOnHost = new HashMap<>();
      this.samzaApplicationState.runningProcessors.forEach((samzaContainerId, samzaResource) -> {
        if (standbySamzaContainerIds.contains(samzaContainerId) && samzaResource.getHost().equals(standbyHost)) {
          runningStandbyContainersOnHost.put(samzaContainerId, samzaResource);
        }
      });

      if (runningStandbyContainersOnHost.isEmpty()) {
        // if there are no running standby-containers on the standbyHost, we proceed to directly make a resource request

        log.info("No running standby container to stop on host {}, making a resource-request for placing {} on {}, active's resourceID: {}",
            standbyHost, activeContainerID, standbyHost, resourceID);
        FailoverMetadata failoverMetadata = this.registerActiveContainerFailure(activeContainerID, resourceID);

        // record the resource request, before issuing it to avoid race with allocation-thread
        SamzaResourceRequest resourceRequestForActive =
                containerAllocator.getResourceRequest(activeContainerID, standbyHost);
        failoverMetadata.recordResourceRequest(resourceRequestForActive);
        containerAllocator.issueResourceRequest(resourceRequestForActive);
        samzaApplicationState.failoversToStandby.incrementAndGet();
      } else {
        // if there is a running standby-container on the standbyHost, we issue a stop (the stopComplete callback completes the remainder of the flow)
        FailoverMetadata failoverMetadata = this.registerActiveContainerFailure(activeContainerID, resourceID);

        runningStandbyContainersOnHost.forEach((standbyContainerID, standbyResource) -> {
          log.info("Initiating failover and stopping standby container, found standbyContainer {} = resource {}, "
                  + "for active container {}", runningStandbyContainersOnHost.keySet(),
              runningStandbyContainersOnHost.values(), activeContainerID);
          failoverMetadata.updateStandbyContainer(standbyResource.getContainerId(), standbyResource.getHost());
          samzaApplicationState.failoversToStandby.incrementAndGet();
          this.clusterResourceManager.stopStreamProcessor(standbyResource);
        });

        // if multiple standbys are on the same host, we are in an invalid state, so we fail the deploy and retry
        if (runningStandbyContainersOnHost.size() > 1) {
          throw new SamzaException(
              "Invalid State. Multiple standby containers found running on one host:" + runningStandbyContainersOnHost);
        }
      }
    }
  }

  /**
   * Method to select a standby host for a given active container.
   * 1. We first try to select a host which has a running standby-container, that we haven't already selected for failover.
   * 2. If we dont any such host, we iterate over last-known standbyHosts, if we haven't already selected it for failover.
   * 3. If still dont find a host, we fall back to AnyHost.
   *
   * See https://issues.apache.org/jira/browse/SAMZA-2140
   *
   * @param activeContainerID Samza containerID of the active container
   * @param activeContainerResourceID ResourceID of the active container at the time of its last failure
   * @return standby host for the active container (if found), Any-host otherwise.
   */
  private String selectStandbyHost(String activeContainerID, String activeContainerResourceID) {

    log.info("Standby containers {} for active container {} (resourceID {})", this.standbyContainerConstraints.get(activeContainerID), activeContainerID, activeContainerResourceID);

    // obtain any existing failover metadata
    Optional<FailoverMetadata> failoverMetadata = getFailoverMetadata(activeContainerResourceID);

    // Iterate over the list of running standby containers, to find a standby resource that we have not already
    // used for a failover for this active resource
    for (String standbyContainerID : this.standbyContainerConstraints.get(activeContainerID)) {

      if (samzaApplicationState.runningProcessors.containsKey(standbyContainerID)) {
        SamzaResource standbyContainerResource = samzaApplicationState.runningProcessors.get(standbyContainerID);

        // use this standby if there was no previous failover for which this standbyResource was used
        if (!(failoverMetadata.isPresent() && failoverMetadata.get()
            .isStandbyResourceUsed(standbyContainerResource.getContainerId()))) {

          log.info("Returning standby container {} in running state on host {} for active container {}",
              standbyContainerID, standbyContainerResource.getHost(), activeContainerID);
          return standbyContainerResource.getHost();
        }
      }
    }
    log.info("Did not find any running standby container for active container {}", activeContainerID);

    // We iterate over the list of last-known standbyHosts to check if anyone of them has not already been tried
    for (String standbyContainerID : this.standbyContainerConstraints.get(activeContainerID)) {

      String standbyHost =
          Optional.ofNullable(localityManager.readLocality().getProcessorLocality(standbyContainerID))
              .map(ProcessorLocality::host)
              .orElse(null);

      if (StringUtils.isBlank(standbyHost)) {
        log.info("No last known standbyHost for container {}", standbyContainerID);
      } else if (failoverMetadata.isPresent() && failoverMetadata.get().isStandbyHostUsed(standbyHost)) {

        log.info("Not using standby host {} for active container {} because it had already been selected", standbyHost,
            activeContainerID);
      } else {
        // standbyHost is valid and has not already been selected
        log.info("Returning standby host {} for active container {}", standbyHost, activeContainerID);
        return standbyHost;
      }
    }

    log.info("Did not find any standby host for active container {}, returning any-host", activeContainerID);
    return ResourceRequestState.ANY_HOST;
  }

  /**
   * Register the failure of an active container (identified by its resource ID).
   */
  private FailoverMetadata registerActiveContainerFailure(String activeContainerID, String activeContainerResourceID) {

    // this active container's resource ID is already registered, in which case update the metadata
    FailoverMetadata failoverMetadata;
    if (failovers.containsKey(activeContainerResourceID)) {
      failoverMetadata = failovers.get(activeContainerResourceID);
    } else {
      failoverMetadata = new FailoverMetadata(activeContainerID, activeContainerResourceID);
    }
    this.failovers.put(activeContainerResourceID, failoverMetadata);
    return failoverMetadata;
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
   * This method checks from the config if standby allocation is fault domain aware or not, and requests resources accordingly.
   *
   * @param containerAllocator ContainerAllocator object that requests for resources from the resource manager
   * @param containerID Samza container ID that will be run when a resource is allocated for this request
   * @param preferredHost name of the host that you prefer to run the processor on
   * @param preferredHostRetryDelay the {@link Duration} to add to the request timestamp
   * @param hostToAvoid The hostname to avoid requesting this resource on if fault domain aware standby allocation is enabled
   */
  void requestResource(ContainerAllocator containerAllocator, String containerID, String preferredHost, Duration preferredHostRetryDelay, String hostToAvoid) {
    if (StandbyTaskUtil.isStandbyContainer(containerID) && isFaultDomainAwareStandbyEnabled) {
      containerAllocator.requestResourceWithDelay(containerID, preferredHost, preferredHostRetryDelay, getAllowedFaultDomainsGivenHostToAvoid(hostToAvoid));
    } else {
      containerAllocator.requestResourceWithDelay(containerID, preferredHost, preferredHostRetryDelay, new HashSet<>());
    }
  }

  /**
   * This method returns the active container host given a standby or active container ID.
   *
   * @param containerID Standby or active container container ID
   * @return The active container host
   */
  Optional<String> getActiveContainerHost(String containerID) {
    String activeContainerId = containerID;
    if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      activeContainerId = StandbyTaskUtil.getActiveContainerId(containerID);
    }
    SamzaResource resource = samzaApplicationState.pendingProcessors.get(activeContainerId);
    if (resource == null) {
      resource = samzaApplicationState.runningProcessors.get(activeContainerId);
    }
    return Optional.ofNullable(resource)
            .map(SamzaResource::getHost);
  }

  /**
   * Check if matching this SamzaResourceRequest to the given resource, meets all standby-container container constraints.
   * This includes the check that a standby and its active should not be on the same fault domain or the same host.
   * @param containerIdToStart logical id of the container to start
   * @param host potential host to start the container on
   * @return
   */
  boolean checkStandbyConstraints(String containerIdToStart, String host) {
    List<String> containerIDsForStandbyConstraints = this.standbyContainerConstraints.get(containerIdToStart);

    // Check if any of these conflicting containers are running/launching on host
    for (String containerID : containerIDsForStandbyConstraints) {
      SamzaResource resource = samzaApplicationState.pendingProcessors.get(containerID);

      // return false if a conflicting container is pending for launch on the host
      if (resource != null && isFaultDomainAwareStandbyEnabled
              && faultDomainManager.hasSameFaultDomains(host, resource.getHost())) {
        log.info("Container {} cannot be started on host {} because container {} is already scheduled on this fault domain",
                containerIdToStart, host, containerID);
        if (StandbyTaskUtil.isStandbyContainer(containerIdToStart)) {
          samzaApplicationState.failedFaultDomainAwareContainerAllocations.incrementAndGet();
        }
        return false;
      } else if (resource != null && resource.getHost().equals(host)) {
        log.info("Container {} cannot be started on host {} because container {} is already scheduled on this host",
                containerIdToStart, host, containerID);
        return false;
      }

      // return false if a conflicting container is running on the host
      resource = samzaApplicationState.runningProcessors.get(containerID);
      if (resource != null && isFaultDomainAwareStandbyEnabled
              && faultDomainManager.hasSameFaultDomains(host, resource.getHost())) {
        log.info("Container {} cannot be started on host {} because container {} is already running on this fault domain",
                containerIdToStart, host, containerID);
        if (StandbyTaskUtil.isStandbyContainer(containerIdToStart)) {
          samzaApplicationState.failedFaultDomainAwareContainerAllocations.incrementAndGet();
        }
        return false;
      } else if (resource != null && resource.getHost().equals(host)) {
        log.info("Container {} cannot be started on host {} because container {} is already running on this host",
                containerIdToStart, host, containerID);
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
      SamzaResource samzaResource, ContainerAllocator containerAllocator,
      ResourceRequestState resourceRequestState) {
    String containerID = request.getProcessorId();

    if (checkStandbyConstraints(containerID, samzaResource.getHost())) {
      // This resource can be used to launch this container
      log.info("Running container {} on {} meets standby constraints, preferredHost = {}", containerID,
          samzaResource.getHost(), preferredHost);
      containerAllocator.runStreamProcessor(request, preferredHost);
      if (isFaultDomainAwareStandbyEnabled && StandbyTaskUtil.isStandbyContainer(containerID)) {
        samzaApplicationState.faultDomainAwareContainersStarted.incrementAndGet();
      }
    } else if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      // This resource cannot be used to launch this standby container, so we make a new anyhost request
      log.info(
          "Running standby container {} on host {} does not meet standby constraints, cancelling resource request, releasing resource, and making a new ANY_HOST request",
          containerID, samzaResource.getHost());
      releaseUnstartableContainer(request, samzaResource, preferredHost, resourceRequestState);
      String activeContainerHost = getActiveContainerHost(containerID)
              .orElse(null);
      requestResource(containerAllocator, containerID, ResourceRequestState.ANY_HOST, Duration.ZERO, activeContainerHost);
      samzaApplicationState.failedStandbyAllocations.incrementAndGet();
    } else {
      // This resource cannot be used to launch this active container, so we initiate a failover
      log.warn(
          "Running active container {} on host {} does not meet standby constraints, cancelling resource request, releasing resource",
          containerID, samzaResource.getHost());
      releaseUnstartableContainer(request, samzaResource, preferredHost, resourceRequestState);
      Optional<FailoverMetadata> failoverMetadata = getFailoverMetadata(request);
      String lastKnownResourceID =
          failoverMetadata.isPresent() ? failoverMetadata.get().activeContainerResourceID : "unknown-" + containerID;
      initiateStandbyAwareAllocation(containerID, lastKnownResourceID, containerAllocator);
      samzaApplicationState.failedStandbyAllocations.incrementAndGet();
    }
  }

  /**
   * Handle an expired resource request
   * @param containerID the containerID for which the resource request was made
   * @param request the expired resource request
   * @param alternativeResource an alternative, already-allocated, resource (if available)
   * @param containerAllocator the container allocator (used to issue any required subsequent resource requests)
   * @param resourceRequestState used to cancel resource requests if required.
   */
  public void handleExpiredResourceRequest(String containerID, SamzaResourceRequest request,
      Optional<SamzaResource> alternativeResource, ContainerAllocator containerAllocator,
      ResourceRequestState resourceRequestState) {

    if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      handleExpiredRequestForStandbyContainer(containerID, request, alternativeResource, containerAllocator,
          resourceRequestState);
    } else {
      handleExpiredRequestForActiveContainer(containerID, request, containerAllocator, resourceRequestState);
    }
  }

  /**
   * Fetches a list of standby container for an active container
   * @param activeContainerId logical id of the container ex: 0,1,2
   * @return list of standby containers ex: for active container 0: {0-0, 0-1}
   */
  List<String> getStandbyList(String activeContainerId) {
    return this.standbyContainerConstraints.get(activeContainerId);
  }

  /**
   * Release un-startable resources immediately and deletes requests corresponsing to it
   */
  void releaseUnstartableContainer(SamzaResourceRequest request, SamzaResource resource, String preferredHost,
      ResourceRequestState resourceRequestState) {
    resourceRequestState.releaseUnstartableContainer(resource, preferredHost);
    resourceRequestState.cancelResourceRequest(request);
  }

  // Handle an expired resource request that was made for placing a standby container
  private void handleExpiredRequestForStandbyContainer(String containerID, SamzaResourceRequest request,
      Optional<SamzaResource> alternativeResource, ContainerAllocator containerAllocator,
      ResourceRequestState resourceRequestState) {

    if (alternativeResource.isPresent()) {
      // A standby container can be started on the anyhost-alternative-resource rightaway provided it passes all the
      // standby constraints
      log.info("Handling expired request, standby container {} can be started on alternative resource {}", containerID,
          alternativeResource.get());
      checkStandbyConstraintsAndRunStreamProcessor(request, ResourceRequestState.ANY_HOST, alternativeResource.get(),
          containerAllocator, resourceRequestState);
    } else {
      // If there is no alternative-resource for the standby container we make a new anyhost request
      log.info("Handling expired request, requesting anyHost resource for standby container {}", containerID);
      resourceRequestState.cancelResourceRequest(request);
      String activeContainerHost = getActiveContainerHost(containerID)
              .orElse(null);
      requestResource(containerAllocator, containerID, ResourceRequestState.ANY_HOST, Duration.ZERO, activeContainerHost);
    }
  }

  // Handle an expired resource request that was made for placing an active container
  private void handleExpiredRequestForActiveContainer(String containerID, SamzaResourceRequest request,
      ContainerAllocator containerAllocator, ResourceRequestState resourceRequestState) {

    log.info("Handling expired request for active container {}", containerID);
    Optional<FailoverMetadata> failoverMetadata = getFailoverMetadata(request);
    resourceRequestState.cancelResourceRequest(request);

    // we use the activeContainer's resourceID to initiate the failover and index failover-state
    // if there is no prior failure for this active-Container, we use "unknown"
    String lastKnownResourceID =
        failoverMetadata.isPresent() ? failoverMetadata.get().activeContainerResourceID : "unknown-" + containerID;
    log.info("Handling expired request for active container {}, lastKnownResourceID is {}", containerID, lastKnownResourceID);
    initiateStandbyAwareAllocation(containerID, lastKnownResourceID, containerAllocator);
  }

  /**
   * Check if a activeContainerResource has failover-metadata associated with it
   */
  private Optional<FailoverMetadata> getFailoverMetadata(String activeContainerResourceID) {
    return this.failovers.containsKey(activeContainerResourceID) ? Optional.of(
        this.failovers.get(activeContainerResourceID)) : Optional.empty();
  }

  /**
   * Check if a SamzaResourceRequest was issued for a failover.
   */
  private Optional<FailoverMetadata> getFailoverMetadata(SamzaResourceRequest resourceRequest) {
    for (FailoverMetadata failoverMetadata : this.failovers.values()) {
      if (failoverMetadata.containsResourceRequest(resourceRequest)) {
        return Optional.of(failoverMetadata);
      }
    }
    return Optional.empty();
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
    private final Map<String, String> selectedStandbyContainers;

    // Resource requests issued during this failover
    private final Set<SamzaResourceRequest> resourceRequests;

    public FailoverMetadata(String activeContainerID, String activeContainerResourceID) {
      this.activeContainerID = activeContainerID;
      this.activeContainerResourceID = activeContainerResourceID;
      this.selectedStandbyContainers = new HashMap<>();
      resourceRequests = new HashSet<>();
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

    // Add the samzaResourceRequest to the list of resource requests associated with this failover
    public synchronized void recordResourceRequest(SamzaResourceRequest samzaResourceRequest) {
      this.resourceRequests.add(samzaResourceRequest);
    }

    // Check if this resource request has been issued in this failover
    public synchronized boolean containsResourceRequest(SamzaResourceRequest samzaResourceRequest) {
      return this.resourceRequests.contains(samzaResourceRequest);
    }

    // Check if this standby-host has been used in this failover, that is, if this host matches the host of a selected-standby,
    // or if we have made a resourceRequest for this host
    public boolean isStandbyHostUsed(String standbyHost) {
      return this.selectedStandbyContainers.values().contains(standbyHost)
          || this.resourceRequests.stream().filter(request -> request.getPreferredHost().equals(standbyHost)).count() > 0;
    }

    @Override
    public String toString() {
      return "[activeContainerID: " + this.activeContainerID + " activeContainerResourceID: "
          + this.activeContainerResourceID + " selectedStandbyContainers:" + selectedStandbyContainers
          + " resourceRequests: " + resourceRequests + "]";
    }
  }
}
