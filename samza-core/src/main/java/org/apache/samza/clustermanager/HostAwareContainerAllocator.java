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

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the allocator thread that will be used by ContainerProcessManager when host-affinity is enabled for a job. It is similar
 * to {@link ContainerAllocator}, except that it considers locality for allocation.
 *
 * In case of host-affinity, each request ({@link SamzaResourceRequest} encapsulates the identifier of the processor
 * to be run and a "preferredHost". preferredHost is determined by the locality mappings in the coordinator stream.
 * This thread periodically wakes up and makes the best-effort to assign a processor to the preferredHost. If a
 * resource on the preferredHost is not returned by the cluster manager before the corresponding request expires, it
 * assigns the processor to any other host that is allocated next.
 *
 * The resource expiry timeout is determined by CONTAINER_REQUEST_TIMEOUT and is configurable on a per-job basis.
 *
 * If there aren't enough resources, it waits by sleeping for allocatorSleepIntervalMs milliseconds.
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
      int timeout,
      Config config,
      Optional<StandbyContainerManager> standbyContainerManager,
      SamzaApplicationState state,
      ClassLoader pluginClassLoader) {
    super(manager, new ResourceRequestState(true, manager), config, state, pluginClassLoader);
    this.requestTimeout = timeout;
    this.standbyContainerManager = standbyContainerManager;
  }

  /**
   * Since host-affinity is enabled, all allocated resources are buffered in the list keyed by "preferredHost".
   *
   * If the requested host is not available, the thread checks to see if the request has expired.
   * If it has expired, it runs the processor on one of the available resources from the
   * allocatedContainers buffer keyed by "ANY_HOST".
   */
  @Override
  public void assignResourceRequests()  {
    for (Optional<SamzaResourceRequest> requestOptional = peekReadyPendingRequest();
        requestOptional.isPresent();
        requestOptional = peekReadyPendingRequest()) {
      SamzaResourceRequest request = requestOptional.get();
      String processorId = request.getProcessorId();
      String preferredHost = request.getPreferredHost();
      Instant requestCreationTime = request.getRequestTimestamp();
      log.info("Handling assignment request for Processor ID: {} on preferred host: {}.", processorId, preferredHost);

      if (hasAllocatedResource(preferredHost)) {
        // Found allocated container on preferredHost
        log.info("Found an available container for Processor ID: {} on the preferred host: {}", processorId, preferredHost);
        // Try to launch processor on this preferredHost if it all standby constraints are met
        checkStandbyConstraintsAndRunStreamProcessor(request, preferredHost, peekAllocatedResource(preferredHost));
        state.matchedResourceRequests.incrementAndGet();
      } else {
        log.info("Did not find any allocated containers for running Processor ID: {} on the preferred host: {}.", processorId, preferredHost);

        boolean expired = isRequestExpired(request);
        boolean resourceAvailableOnAnyHost = hasAllocatedResource(ResourceRequestState.ANY_HOST);

        if (expired) {
          updateExpiryMetrics(request);

          if (standbyContainerManager.isPresent()) {
            standbyContainerManager.get().handleExpiredResourceRequest(processorId, request,
                Optional.ofNullable(peekAllocatedResource(ResourceRequestState.ANY_HOST)), this, resourceRequestState);

          } else if (resourceAvailableOnAnyHost) {
            log.info("Request for Processor ID: {} on host: {} has expired. Running on ANY_HOST", processorId, preferredHost);
            runStreamProcessor(request, ResourceRequestState.ANY_HOST);

          } else {
            log.info("Request for Processor ID: {} on host: {} has expired. Requesting additional resources on ANY_HOST.", processorId, preferredHost);
            resourceRequestState.cancelResourceRequest(request);
            requestResource(processorId, ResourceRequestState.ANY_HOST);
          }

        } else {
          log.info("Request for Processor ID: {} on host: {} has not expired yet." +
                  "Request creation time: {}. Current Time: {}. Request timeout: {} ms",
              processorId, preferredHost, requestCreationTime, System.currentTimeMillis(), requestTimeout);
          break;
        }
      }
    }
  }


  /**
   * Since host-affinity is enabled, containers for all processors will be requested on their preferred host. If the job is
   * run for the first time, it will get matched to any available host.
   *
   * @param processorToHostMapping A Map of [processorId, hostName] where processorId is the ID of the Samza processor
   *                               to run on the resource. hostName is the host on which the resource must be allocated.
   *                               The hostName value is null when host-affinity is enabled and job is run for the
   *                               first time, or when the number of containers has been increased.
   */
  @Override
  public void requestResources(Map<String, String> processorToHostMapping) {
    for (Map.Entry<String, String> entry : processorToHostMapping.entrySet()) {
      String processorId = entry.getKey();
      String preferredHost = entry.getValue();
      if (preferredHost == null) {
        log.info("No preferred host mapping found for Processor ID: {}. Requesting resource on ANY_HOST", processorId);
        preferredHost = ResourceRequestState.ANY_HOST;
      }
      requestResource(processorId, preferredHost);
    }
  }

  /**
   * Checks if a request has expired.
   * @param request the request to check
   * @return true if request has expired
   */
  private boolean isRequestExpired(SamzaResourceRequest request) {
    long currTime = Instant.now().toEpochMilli();
    boolean requestExpired =  currTime - request.getRequestTimestamp().toEpochMilli() > requestTimeout;
    if (requestExpired) {
      log.info("Request for Processor ID: {} on host: {} with creation time: {} has expired at current time: {} after timeout: {} ms.",
          request.getProcessorId(), request.getPreferredHost(), request.getRequestTimestamp(), currTime, requestTimeout);
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