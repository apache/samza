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
        // Try to launch streamProcessor on this preferredHost
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
            log.info("Request for container: {} on {} has expired. Running on ANY_HOST", request.getContainerID(), request.getPreferredHost());
            checkStandbyTaskConstraintsAndRunStreamProcessor(request, ResourceRequestState.ANY_HOST,
                peekAllocatedResource(ResourceRequestState.ANY_HOST), state);
          } else {
            log.info("Request for container: {} on {} has expired. Requesting additional resources on ANY_HOST.", request.getContainerID(), request.getPreferredHost());
            resourceRequestState.cancelResourceRequest(request);
            handleReRequesting(containerID);
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
      // This resource cannot be used to launch this container, so we make a ANY_HOST request for another container
      log.info("Running container {} on host {} does not meet standby constraints, cancelling resource request, releasing resource, and making a new ANY_HOST request",
          containerID, samzaResource.getHost());
      resourceRequestState.releaseUnstartableContainer(samzaResource, preferredHost);
      resourceRequestState.cancelResourceRequest(request);
      handleReRequesting(containerID);
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

  // Intercept resource requests from the allocator, which are due to either a resource-request expired or standby
  // constraints not met, if it is for a
  // 1. a standby container, we proceed to make a anyhost request
  // 2. an activeContainer, we try to fail-it-over to a standby
  private void handleReRequesting(String containerID) {
    if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      log.info("Handling rerequesting for container {} using an any host request");
      super.requestResource(containerID, ResourceRequestState.ANY_HOST); // proceed with a the anyhost request
    } else {
      requestResource(containerID, ResourceRequestState.ANY_HOST); // invoke local method to attempt a failover
    }
  }

  // Intercept issuing of resource requests from the CPM
  // 1. for ActiveContainers and instead choose a StandbyContainer to stop
  // 2. for a StandbyContainer (after it has been chosen for failover), to put the active on the standby's host
  // and request another resource for the standby
  // 3. for a standbyContainer
  @Override
  public void requestResource(String containerID, String preferredHost) {

    // If StandbyTasks are not enabled, we simply forward the resource requests
    if (!new JobConfig(config).getStandbyTasksEnabled()) {
      super.requestResource(containerID, preferredHost);
      return;
    }

    // If anyHost is being requested for an active container, then we first select a standby container to stop and place
    // this activeContainer on that standby's host, we may not find any running standbyContainerID
    if (!StandbyTaskUtil.isStandbyContainer(containerID) && preferredHost.equals(ResourceRequestState.ANY_HOST)) {

      Optional<Entry<String, SamzaResource>> standbyContainer =
          StandbyTaskUtil.selectStandby(containerID, this.standbyContainerConstraints.get(containerID), this.state);

      // If we find a standbyContainer, we initiate a failover
      if (standbyContainer.isPresent()) {
        String standbyContainerId = standbyContainer.get().getKey();
        SamzaResource standbyContainerResource = standbyContainer.get().getValue();

        // ResourceID of the active container at the time of failover
        String activeContainerResourceID = state.failedContainersStatus.get(containerID).getLast().getResourceID();

        log.info("initiating failover and stopping standby container, found standbyContainer {} = resource {}, "
            + "for active container {}", standbyContainerId, standbyContainerResource, containerID);

        this.state.failovers.put(activeContainerResourceID,
            new ContainerFailoverState(containerID, standbyContainerId, standbyContainerResource));
        this.clusterResourceManager.stopStreamProcessor(standbyContainer.get().getValue());
        this.state.failovers.get(activeContainerResourceID)
            .setStandbyContainerStatus(ContainerFailoverState.ContainerStatus.StopIssued);
        return;
      } else {

        // If we dont find a standbyContainer, we proceed with the ANYHOST request
        log.info("No standby container found for active container {}, making a request for {}", containerID,
            preferredHost);
        super.requestResource(containerID, preferredHost);
        return;
      }
    } else if (StandbyTaskUtil.isStandbyContainer(containerID)) {
      // if the resource request is for a standby container, then either
      // a. during a failover, the standby container was stopped for an active's start, then
      //    1. request a resource on the standby's host to place the activeContainer, and
      //    2. request anyhost to place this standby
      //
      // b. independent of a failover, the standby container stopped for some reason, in which proceed with its resource-request


      String containerResourceId = state.failedContainersStatus.get(containerID) == null ? null
          : state.failedContainersStatus.get(containerID).getLast().getResourceID();

      if (containerResourceId != null && checkIfStopRequested(state.failovers, containerResourceId).isPresent()) {

        ContainerFailoverState containerFailoverState = checkIfStopRequested(state.failovers, containerResourceId).get();
        containerFailoverState.setStandbyContainerStatus(ContainerFailoverState.ContainerStatus.Stopped);

        containerFailoverState.setStandbyContainerStatus(ContainerFailoverState.ContainerStatus.ResourceRequested);
        containerFailoverState.setActiveContainerStatus(ContainerFailoverState.ContainerStatus.ResourceRequested);

        log.info("Requesting resource for active container {} on host {}, and backup container {} on any host",
            containerFailoverState.activeContainerID, containerFailoverState.standbyContainerResource.getHost(),
            containerID);

        super.requestResource(containerFailoverState.activeContainerID, containerFailoverState.standbyContainerResource.getHost());
        super.requestResource(containerID, ResourceRequestState.ANY_HOST);
        return;

      } else {
        log.info("Issuing request for standby container {} on host {}, since this is not for a failover", containerID, preferredHost);
        super.requestResource(containerID, preferredHost);
        return;
      }

    } else {
      log.info("Requesting resource for active-container {} on host {}", containerID, preferredHost);
      super.requestResource(containerID, preferredHost);
      return;
    }
  }

  public static Optional<ContainerFailoverState> checkIfStopRequested(Map<String, ContainerFailoverState> failovers,
      String standbyContainerResourceId) {
    for (ContainerFailoverState failoverState : failovers.values()) {
      if (failoverState.getStandbyContainerResource().getResourceID().equals(standbyContainerResourceId)) {
        log.info("Standby container with resource id {} was selected for failover of active container {}",
            standbyContainerResourceId, failoverState.activeContainerID);
        return Optional.of(failoverState);
      }
    }
    return Optional.empty();
  }


}