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
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.storage.kv.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the allocator thread that will be used by ContainerProcessManager when host-affinity is enabled for a job. It is similar
 * to {@link ContainerAllocator}, except that it considers locality for allocation.
 *
 * In case of host-affinity, each request ({@link SamzaResourceRequest} encapsulates the identifier of the container
 * to be run and a "preferredHost". preferredHost is determined by the locality mappings in the coordinator stream.
 * This thread periodically wakes up and makes the best-effort to assign a container to the preferredHost. If the
 * preferredHost is not returned by the cluster manager before the corresponding container expires, the thread
 * assigns the container to any other host that is allocated next.
 *
 * The container expiry is determined by CONTAINER_REQUEST_TIMEOUT and is configurable on a per-job basis.
 *
 * If there aren't enough containers, it waits by sleeping for allocatorSleepIntervalMs milliseconds.
 */
//This class is used in the refactored code path as called by run-jc.sh

public class HostAwareContainerAllocator extends AbstractContainerAllocator {
  private static final Logger log = LoggerFactory.getLogger(HostAwareContainerAllocator.class);
  /**
   * Tracks the expiration of a request for resources.
   */
  private final int requestTimeout;

  // Map of activeContainerIDs to its corresponding standby containers, and standbyContainerIDs to its corresponding
  // active container and other corresponding standbyContainers
  private final Map<String, List<String>> standbyContainerConstraints = new HashMap<>();

  public HostAwareContainerAllocator(ClusterResourceManager manager ,
      int timeout, Config config, SamzaApplicationState state) {
    super(manager, new ResourceRequestState(true, manager), config, state);
    this.requestTimeout = timeout;

    // if standbys are enabled, populate the standbyContainerConstraints map
    if (new JobConfig(config).getStandbyTasksEnabled()) {
      JobModel jobModel = state.jobModelManager.jobModel();
      jobModel.getContainers().keySet().forEach(containerId -> standbyContainerConstraints.put(containerId,
          StandbyTaskUtil.getStandbyContainerConstraints(containerId, jobModel)));
      log.info("Populated standbyContainerConstraints map {}", standbyContainerConstraints);
    }
  }

  /**
   * Since host-affinity is enabled, all allocated resources are buffered in the list keyed by "preferredHost".
   *
   * If the requested host is not available, the thread checks to see if the request has expired.
   * If it has expired, it runs the container with expectedContainerID on one of the available resources from the
   * allocatedContainers buffer keyed by "ANY_HOST".
   */
  @Override
  public void assignResourceRequests()  {
    while (hasPendingRequest()) {
      SamzaResourceRequest request = peekPendingRequest();
      log.info("Handling request: " + request.getContainerID() + " " + request.getRequestTimestampMs() + " " + request.getPreferredHost());
      String preferredHost = request.getPreferredHost();
      String containerID = request.getContainerID();

      if (hasAllocatedResource(preferredHost)) {
        // Found allocated container at preferredHost
        log.info("Found a matched-container {} on the preferred host. Running on {}", containerID, preferredHost);
        // Try to launch streamProcessor on this preferredHost if it all standby constraints are met
        checkStandbyTaskConstraintsAndRunStreamProcessor(request, preferredHost, peekAllocatedResource(preferredHost), state);
        state.matchedResourceRequests.incrementAndGet();
      } else {
        log.info("Did not find any allocated resources on preferred host {} for running container id {}",
            preferredHost, containerID);

        boolean expired = requestExpired(request);
        boolean resourceAvailableOnAnyHost = hasAllocatedResource(ResourceRequestState.ANY_HOST);

        if (expired) {
          updateExpiryMetrics(request);
          if (resourceAvailableOnAnyHost) {
            // if standby is not enabled, request a anyhost request
            if (!new JobConfig(config).getStandbyTasksEnabled()) {
              log.info("Request for container: {} on {} has expired. Running on ANY_HOST", request.getContainerID(),
                  request.getPreferredHost());
              runStreamProcessor(request, ResourceRequestState.ANY_HOST);
            } else if (StandbyTaskUtil.isStandbyContainer(containerID)) {
              // only standby resources can be on anyhost rightaway
              checkStandbyTaskConstraintsAndRunStreamProcessor(request, ResourceRequestState.ANY_HOST,
                  peekAllocatedResource(ResourceRequestState.ANY_HOST), state);
            } else {
              // re-requesting resource for active container
              resourceRequestState.cancelResourceRequest(request);
              requestResourceDueToLaunchFailOrExpiredRequest(containerID);
            }
          } else {
            log.info("Request for container: {} on {} has expired. Requesting additional resources on ANY_HOST.", request.getContainerID(), request.getPreferredHost());
            resourceRequestState.cancelResourceRequest(request);
            requestResourceDueToLaunchFailOrExpiredRequest(containerID);
          }
        } else {
          log.info("Request for container: {} on {} has not yet expired. Request creation time: {}. Request timeout: {}",
              new Object[]{request.getContainerID(), request.getPreferredHost(), request.getRequestTimestampMs(), requestTimeout});
          break;
        }
      }
    }
  }


  /**
   * Checks if a request has expired.
   * @param request
   * @return
   */
  private boolean requestExpired(SamzaResourceRequest request) {
    long currTime = System.currentTimeMillis();
    boolean requestExpired =  currTime - request.getRequestTimestampMs() > requestTimeout;
    if (requestExpired) {
      log.info("Request {} with currTime {} has expired", request, currTime);
    }
    return requestExpired;
  }

  private void updateExpiryMetrics(SamzaResourceRequest request) {
    String preferredHost = request.getPreferredHost();
    if (ResourceRequestState.ANY_HOST.equals(preferredHost)) {
      state.expiredAnyHostRequests.incrementAndGet();
    } else {
      state.expiredPreferredHostRequests.incrementAndGet();
    }
  }

  // Method to run a container on the given resource if it meets all standby constraints. If not, we re-request resource
  // for the container (similar to the case when we re-request for a launch-fail or request expiry).
  private boolean checkStandbyTaskConstraintsAndRunStreamProcessor(SamzaResourceRequest request, String preferredHost,
      SamzaResource samzaResource, SamzaApplicationState state) {

    // If standby tasks are not enabled run streamprocessor and return true
    if (!new JobConfig(config).getStandbyTasksEnabled()) {
      runStreamProcessor(request, preferredHost);
      return true;
    }

    String containerID = request.getContainerID();

    if (checkStandbyConstraints(request, samzaResource, state)) {
      // This resource can be used to launch this container
      log.info("Running container {} on preferred host {} meets standby constraints, launching on {}", containerID,
          preferredHost, samzaResource.getHost());
      runStreamProcessor(request, preferredHost);
      state.successfulStandbyAllocations.incrementAndGet();
      return true;
    } else {
      // This resource cannot be used to launch this container, so we treat it like a launch fail, and issue an ANY_HOST request
      log.info("Running container {} on host {} does not meet standby constraints, cancelling resource request, releasing resource, and making a new ANY_HOST request",
          containerID, samzaResource.getHost());
      resourceRequestState.releaseUnstartableContainer(samzaResource, preferredHost);
      resourceRequestState.cancelResourceRequest(request);
      requestResourceDueToLaunchFailOrExpiredRequest(containerID);
      state.failedStandbyAllocations.incrementAndGet();
      return false;
    }
  }

  // Helper method to check if this SamzaResourceRequest for a container can be met on this resource, given standby
  // container constraints, and the current set of pending and running containers
  private boolean checkStandbyConstraints(SamzaResourceRequest request, SamzaResource samzaResource,
      SamzaApplicationState samzaApplicationState) {
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
   * Intercept resource requests, which are due to either a launch-failure or resource-request expired or standby
   * 1. a standby container, we proceed to make a anyhost request
   * 2. an activeContainer, we try to fail-it-over to a standby
   * @param containerID Identifier of the container that will be run when a resource is allocated
   */
  @Override
  public void requestResourceDueToLaunchFailOrExpiredRequest(String containerID) {
    if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      log.info("Handling rerequesting for container {} using an any host request");
      super.requestResource(containerID, ResourceRequestState.ANY_HOST); // proceed with a the anyhost request
    } else {
      requestResource(containerID, ResourceRequestState.ANY_HOST); // invoke local method & select a new standby if possible
    }
  }

  // Intercept issuing of resource requests from the CPM
  // 1. for ActiveContainers and instead choose a StandbyContainer to stop
  // 2. for a StandbyContainer (after it has been chosen for failover), to put the active on the standby's host
  // and request another resource for the standby
  // 3. for a standbyContainer (if not for a failover)
  @Override
  public void requestResource(String containerID, String preferredHost) {

    // If StandbyTasks are not enabled, we simply forward the resource requests
    if (!new JobConfig(config).getStandbyTasksEnabled()) {
      super.requestResource(containerID, preferredHost);
      return;
    }

    // If its an anyhost request for an active container, then we select a standby container to stop and place this activeContainer on that standby's host
    // we may have already chosen a standby (which didnt work for a failover)
    if (!StandbyTaskUtil.isStandbyContainer(containerID) && preferredHost.equals(ResourceRequestState.ANY_HOST)) {
      initiateActiveContainerFailover(containerID);
    } else if (StandbyTaskUtil.isStandbyContainer(containerID)) {

    // If resource request is for a standby container, we check if we stopped the container for failover
      handleStandbyContainerStop(containerID, preferredHost);
    } else {

    // If its a preferred-host request for an active container, we proceed with it asis
      log.info("Requesting resource for active-container {} on host {}", containerID, preferredHost);
      super.requestResource(containerID, preferredHost);
      return;
    }
  }

  /** Method to handle failover for an active container.
   *  We try to find a standby for the active container, and issue a stop on it.
   *  If we do not find a standby container, we simply issue an anyhost request to place it.
   *
    * @param containerID the containerID of the active container
   */
  private void initiateActiveContainerFailover(String containerID) {
    Optional<Entry<String, SamzaResource>> standbyContainer =
        StandbyTaskUtil.selectStandby(containerID, this.standbyContainerConstraints.get(containerID), this.state);

    // If we find a standbyContainer, we initiate a failover
    if (standbyContainer.isPresent()) {

      // ResourceID of the active container at the time of its last failure
      String activeContainerResourceID = state.failedContainersStatus.get(containerID).getLast().getResourceID();
      String standbyContainerId = standbyContainer.get().getKey();
      SamzaResource standbyResource = standbyContainer.get().getValue();
      String standbyResourceID = standbyResource.getResourceID();
      String standbyHost = standbyResource.getHost();

      // update the failover state
      ContainerFailoverState failoverState = state.failovers.get(activeContainerResourceID);
      if (failoverState == null) {
        failoverState = new ContainerFailoverState(containerID, activeContainerResourceID, standbyResourceID, standbyHost);
        this.state.failovers.put(activeContainerResourceID, failoverState);
      } else {
        failoverState.updateStandbyContainer(standbyResourceID, standbyHost);
        failoverState.setStandbyContainerStatus(ContainerFailoverState.ContainerStatus.StopIssued);
      }

      log.info("initiating failover and stopping standby container, found standbyContainer {} = resource {}, "
          + "for active container {}", standbyContainerId, standbyResourceID, containerID);
      state.failoversToStandby.incrementAndGet();

      this.clusterResourceManager.stopStreamProcessor(standbyResource);
      return;

    } else {

      // If we dont find a standbyContainer, we proceed with the ANYHOST request
      log.info("No standby container found for active container {}, making a request for {}", containerID,
          ResourceRequestState.ANY_HOST);
      state.failoversToAnyHost.incrementAndGet();
      super.requestResource(containerID, ResourceRequestState.ANY_HOST);
      return;
    }
  }

  /**
   *  If the resource request is for a standby container, then either
   *    a. during a failover, the standby container was stopped for an active's start, then
   *       1. request a resource on the standby's host to place the activeContainer, and
   *       2. request anyhost to place this standby
   *
   *    b. independent of a failover, the standby container stopped for some reason, in which proceed with its resource-request
   * @param containerID
   * @param preferredHost
   */
  private void handleStandbyContainerStop(String containerID, String preferredHost) {
    String containerResourceId = state.failedContainersStatus.get(containerID) == null ? null : state.failedContainersStatus.get(containerID).getLast().getResourceID();
    Optional<ContainerFailoverState> containerFailoverState = checkIfUsedForFailover(containerResourceId, state.failovers);

    if (containerResourceId != null && containerFailoverState.isPresent()) {

      String activeContainerID = containerFailoverState.get().activeContainerID;
      String standbyContainerHostname = containerFailoverState.get().getStandbyContainerHostname(containerResourceId);

      containerFailoverState.get().setStandbyContainerStatus(ContainerFailoverState.ContainerStatus.Stopped);
      log.info("Requesting resource for active container {} on host {}, and backup container {} on any host",
          activeContainerID, standbyContainerHostname, containerID);

      containerFailoverState.get().setStandbyContainerStatus(ContainerFailoverState.ContainerStatus.ResourceRequested);
      containerFailoverState.get().setActiveContainerStatus(ContainerFailoverState.ContainerStatus.ResourceRequested);
      state.standbyStopsComplete.incrementAndGet();

      super.requestResource(activeContainerID, standbyContainerHostname); // request standbycontainer's host for active-container
      super.requestResource(containerID, ResourceRequestState.ANY_HOST); // request anyhost for standby container
      return;
    } else {
      log.info("Issuing request for standby container {} on host {}, since this is not for a failover", containerID, preferredHost);
      super.requestResource(containerID, preferredHost);
      return;
    }
  }

  // Helper method to check if this standbyContainerResource is present in the failoverState for an active container.
  // This is used to determine if we requested a stop a container.
  private static Optional<ContainerFailoverState> checkIfUsedForFailover(String standbyContainerResourceId,
      Map<String, ContainerFailoverState> failovers) {

    if (standbyContainerResourceId == null) return Optional.empty();

    for (ContainerFailoverState failoverState : failovers.values()) {
      if (failoverState.isStandbyResourceUsed(standbyContainerResourceId)) {
        log.info("Standby container with resource id {} was selected for failover of active container {}",
            standbyContainerResourceId, failoverState.activeContainerID);
        return Optional.of(failoverState);
      }
    }
    return Optional.empty();
  }



}