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
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.job.model.JobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.jetty.http.HttpParser.*;


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
    if (!new JobConfig(config).getStandbyTasksEnabled()) {
      JobModel jobModel = state.jobModelManager.jobModel();
      jobModel.getContainers().keySet().forEach(containerId -> standbyContainerConstraints.put(containerId,
          StandbyTaskUtil.getStandbyContainerConstraints(containerId, jobModel)));
      LOG.info("Populated standbyContainerConstraints map {}", standbyContainerConstraints);
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
            requestResource(containerID, ResourceRequestState.ANY_HOST);
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
      resourceRequestState.cancelResourceRequest(request);
      resourceRequestState.releaseUnstartableContainer(samzaResource, preferredHost);
      requestResource(containerID, ResourceRequestState.ANY_HOST);
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

}