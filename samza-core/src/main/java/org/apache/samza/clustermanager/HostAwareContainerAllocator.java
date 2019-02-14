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

import java.util.Optional;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
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
  private final Optional<StandbyContainerManager> standbyContainerManager;

  public HostAwareContainerAllocator(ClusterResourceManager manager ,
      int timeout, Config config, Optional<StandbyContainerManager> standbyContainerManager, SamzaApplicationState state) {
    super(manager, new ResourceRequestState(true, manager), config, state);
    this.requestTimeout = timeout;
    this.standbyContainerManager = standbyContainerManager;
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

    if (state.standbyContainerState.checkStandbyConstraints(request, samzaResource, state.runningContainers, state.pendingContainers)) {
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
}