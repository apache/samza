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
import java.util.Map;
import org.apache.samza.config.Config;
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

  public HostAwareContainerAllocator(ClusterResourceManager manager,
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
        checkStandbyConstraintsAndRunStreamProcessor(request, preferredHost, peekAllocatedResource(preferredHost));
        state.matchedResourceRequests.incrementAndGet();
      } else {
        log.info("Did not find any allocated resources on preferred host {} for running container id {}",
            preferredHost, containerID);

        boolean expired = requestExpired(request);
        boolean resourceAvailableOnAnyHost = hasAllocatedResource(ResourceRequestState.ANY_HOST);

        if (expired) {
          updateExpiryMetrics(request);

          if (standbyContainerManager.isPresent()) {
            standbyContainerManager.get().handleExpiredResourceRequest(containerID, request,
                Optional.ofNullable(peekAllocatedResource(ResourceRequestState.ANY_HOST)), this, resourceRequestState);

          } else if (resourceAvailableOnAnyHost) {
            log.info("Request for container: {} on {} has expired. Running on ANY_HOST", request.getContainerID(), request.getPreferredHost());
            runStreamProcessor(request, ResourceRequestState.ANY_HOST);

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
   * Since host-affinity is enabled, all container processes will be requested on their preferred host. If the job is
   * run for the first time, it will get matched to any available host.
   *
   * @param resourceToHostMapping A Map of [containerId, hostName] containerId is the ID of the container process
   *                               to run on the resource. hostName is the host on which the resource must be allocated.
   *                                The hostName value is null when host-affinity is enabled and job is run for the
   *                                first time
   */
  @Override
  public void requestResources(Map<String, String> resourceToHostMapping) {
    for (Map.Entry<String, String> entry : resourceToHostMapping.entrySet()) {
      String containerId = entry.getKey();
      String preferredHost = entry.getValue();
      if (preferredHost == null) {
        log.info("Preferred host not found for container id: {}", containerId);
        preferredHost = ResourceRequestState.ANY_HOST;
      }
      requestResource(containerId, preferredHost);
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

  private void checkStandbyConstraintsAndRunStreamProcessor(SamzaResourceRequest request, String preferredHost, SamzaResource samzaResource) {
    // If standby tasks are not enabled run streamprocessor on the given host
    if (!this.standbyContainerManager.isPresent()) {
      runStreamProcessor(request, preferredHost);
      return;
    }

    this.standbyContainerManager.get().checkStandbyConstraintsAndRunStreamProcessor(request, preferredHost,
        samzaResource, this, resourceRequestState);
  }
}