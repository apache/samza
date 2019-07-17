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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ResourceRequestState} maintains the state variables for all the resource requests and the allocated resources returned
 * by the cluster manager.
 *
 * This class is thread-safe, and can safely support concurrent accesses without any form of external synchronization. Currently,
 * this state is shared across both the Allocator Thread, and the Callback handler thread.
 *
 */
public class ResourceRequestState {
  private static final Logger log = LoggerFactory.getLogger(ResourceRequestState.class);
  public static final String ANY_HOST = "ANY_HOST";

  /**
   * Maintain a map of hostname to a list of resources allocated on this host
   */
  private final Map<String, List<SamzaResource>> allocatedResources = new HashMap<>();
  /**
   * Represents the queue of resource requests made by the {@link ContainerProcessManager}
   */
  private final PriorityQueue<SamzaResourceRequest> requestsQueue = new PriorityQueue<SamzaResourceRequest>();
  /**
   * Maintain a map of hostname to the number of requests made for resources on this host
   * This state variable is used to look-up whether an allocated resource on a host was ever requested in the past.
   * This map is not updated when host-affinity is not enabled
   */
  private final Map<String, AtomicInteger> hostRequestCounts = new HashMap<>();
  /**
   * Indicates whether host-affinity is enabled or not
   */
  private final boolean hostAffinityEnabled;

  private final ClusterResourceManager manager;

  private final Object lock = new Object();

  public ResourceRequestState(boolean hostAffinityEnabled, ClusterResourceManager manager) {
    this.hostAffinityEnabled = hostAffinityEnabled;
    this.manager = manager;
  }

  /**
   * Enqueues a {@link SamzaResourceRequest} to be sent to a {@link ClusterResourceManager}.
   *
   * @param request {@link SamzaResourceRequest} to be queued
   */
  public void addResourceRequest(SamzaResourceRequest request) {
    synchronized (lock) {
      requestsQueue.add(request);
      String preferredHost = request.getPreferredHost();

      // if host affinity is enabled, update state.
      if (hostAffinityEnabled) {
        //increment # of requests on the host.
        if (hostRequestCounts.containsKey(preferredHost)) {
          hostRequestCounts.get(preferredHost).incrementAndGet();
        } else {
          hostRequestCounts.put(preferredHost, new AtomicInteger(1));
        }
        /**
         * The following is important to correlate allocated resource data with the requestsQueue made before. If
         * the preferredHost is requested for the first time, the state should reflect that the allocatedResources
         * list is empty and NOT null.
         */

        if (!allocatedResources.containsKey(preferredHost)) {
          allocatedResources.put(preferredHost, new ArrayList<>());
        }
      }
      manager.requestResources(request);
    }
  }

  /**
   * Cancels a {@link SamzaResourceRequest} previously submitted to the {@link ClusterResourceManager}
   *
   * @param request {@link SamzaResourceRequest} to cancel
   */
  public void cancelResourceRequest(SamzaResourceRequest request) {
    log.info("Canceling resource request for Processor ID: {} on host: {}", request.getProcessorId(), request.getPreferredHost());
    synchronized (lock) {
      requestsQueue.remove(request);
      if (hostAffinityEnabled) {
        // assignedHost may not always be the preferred host.
        // Hence, we should safely decrement the counter for the preferredHost
        hostRequestCounts.get(request.getPreferredHost()).decrementAndGet();
      }
      manager.cancelResourceRequest(request);
    }
  }

  /**
   * Invoked each time a resource is returned from a {@link ClusterResourceManager}.
   * @param samzaResource The resource that was returned from the {@link ClusterResourceManager}
   */
  public void addResource(SamzaResource samzaResource) {
    synchronized (lock) {
      String containerId = samzaResource.getContainerId();
      if (hostAffinityEnabled) {
        String hostName = samzaResource.getHost();
        AtomicInteger requestCount = hostRequestCounts.get(hostName);
        // Check if this host was requested for any of the resources
        if (requestCount == null || requestCount.get() == 0) {
          log.info("Saving Container ID: {} in the buffer for ANY_HOST since its host: {} has not been requested specifically.",
              containerId, hostName);
          addToAllocatedResourceList(ANY_HOST, samzaResource);
        } else {
          // This host was indeed requested.
          int requestCountOnThisHost = requestCount.get();
          List<SamzaResource> allocatedResourcesOnThisHost = allocatedResources.get(hostName);
          if (requestCountOnThisHost > 0) {
            //there are pending requests for resources on this host.
            if (allocatedResourcesOnThisHost == null || allocatedResourcesOnThisHost.size() < requestCountOnThisHost) {
              log.info("Saving Container ID: {} in the buffer for host: {}.", containerId, hostName);
              addToAllocatedResourceList(hostName, samzaResource);
            } else {
              /**
               * The RM may allocate more containers on a given host than requested. In such a case, even though the
               * requestCount != 0, it will be greater than the total request count for that host. Hence, it should be
               * assigned to ANY_HOST
               */
              log.info("Saving Container ID: {} in the buffer for ANY_HOST since the number of containers already " +
                      "allocated on its host: {} ({}) is greater than what has been requested: {}.",
                  containerId, hostName, requestCountOnThisHost, containerId);
              addToAllocatedResourceList(ANY_HOST, samzaResource);
            }
          }
        }
      } else {
        log.info("Saving Container ID: {} in the buffer for ANY_HOST since host affinity is not enabled.", containerId);
        addToAllocatedResourceList(ANY_HOST, samzaResource);
      }
    }
  }

  // Appends a samzaResource to the list of allocated resources
  private void addToAllocatedResourceList(String host, SamzaResource samzaResource) {
    List<SamzaResource> samzaResources = allocatedResources.get(host);
    if (samzaResources != null) {
      samzaResources.add(samzaResource);
    } else {
      samzaResources = new ArrayList<>();
      samzaResources.add(samzaResource);
      allocatedResources.put(host, samzaResources);
    }
  }

  /**
   * This method updates the state after a request is fulfilled and a resource starts running on a host
   * Needs to be synchronized because the state buffers are populated by the AMRMCallbackHandler, whereas it is
   * drained by the allocator thread
   *
   * @param request {@link SamzaResourceRequest} that was fulfilled
   * @param assignedHost  Host to which the samzaResource was assigned
   * @param samzaResource Allocated samzaResource resource that was used to satisfy this request
   */
  public void updateStateAfterAssignment(SamzaResourceRequest request, String assignedHost, SamzaResource samzaResource) {
    synchronized (lock) {
      requestsQueue.remove(request);
      // A reference for the resource could either be held in the preferred host buffer or in the ANY_HOST buffer.
      if (allocatedResources.get(assignedHost) != null) {
        allocatedResources.get(assignedHost).remove(samzaResource);
      }
      if (allocatedResources.get(ANY_HOST) != null) {
        allocatedResources.get(ANY_HOST).remove(samzaResource);
      }
      if (hostAffinityEnabled) {
        // assignedHost may not always be the preferred host.
        // Hence, we should safely decrement the counter for the preferredHost
        hostRequestCounts.get(request.getPreferredHost()).decrementAndGet();
      }
      // To avoid getting back excess resources
      manager.cancelResourceRequest(request);
    }
  }

  /**
   * If requestQueue is empty, all extra resources in the buffer should be released and update the entire system's state
   * Needs to be synchronized because it is modifying shared state buffers
   * @return the number of resources released.
   */
  public int releaseExtraResources() {
    synchronized (lock) {
      int numReleasedResources = 0;
      if (requestsQueue.isEmpty()) {
        log.debug("Resource Requests Queue is empty.");
        if (hostAffinityEnabled) {
          List<String> allocatedHosts = getAllocatedHosts();
          for (String host : allocatedHosts) {
            log.info("Releasing extra resources on host: {}", host);
            numReleasedResources += releaseResourcesForHost(host);
          }
        } else {
          numReleasedResources += releaseResourcesForHost(ANY_HOST);
        }
        clearState();
      }
      return numReleasedResources;
    }
  }

  /**
   * Releases a single resource based on containerId.
   * @param containerId Yarn container ID
   */
  public void releaseResource(String containerId) {
    if (StringUtils.isEmpty(containerId)) {
      log.warn("ContainerId is not specified");
      return;
    }

    synchronized (lock) {
      allocatedResources.values().forEach(resources -> {
          if (resources != null) {
            resources.removeIf(r -> containerId.equals(r.getContainerId()));
          }
        });
    }
  }

  /**
   * Releases a container that was allocated and assigned but could not be started.
   * e.g. because of a ConnectException while trying to communicate with the NM.
   * This method assumes the specified container and associated request have already
   * been removed from their respective queues.
   *
   * @param resource the {@link SamzaResource} to release.
   */
  public void releaseUnstartableContainer(SamzaResource resource, String preferredHost) {
    synchronized (lock) {
      String containerId = resource.getContainerId();
      log.info("Releasing unstartable Container ID: {} on host: {}", containerId, resource.getHost());
      manager.releaseResources(resource);

      // A reference for the resource could either be held in the preferred host buffer or in the ANY_HOST buffer.
      if (allocatedResources.get(preferredHost) != null) {
        allocatedResources.get(preferredHost).remove(resource);
        log.info("Unstartable Container ID: {} removed from the buffer for host: {}", containerId, preferredHost);
      }
      if (allocatedResources.get(ANY_HOST) != null) {
        allocatedResources.get(ANY_HOST).remove(resource);
        log.info("Unstartable Container ID: {} removed from the buffer for ANY_HOST", containerId);
      }
    }
  }


  /**
   * Releases all allocated resources for the specified host.
   * @param host  the host for which the resources should be released.
   * @return      the number of resources released.
   */
  private int releaseResourcesForHost(String host) {
    int numReleasedResources = 0;
    List<SamzaResource> samzaResources = allocatedResources.get(host);
    if (samzaResources != null) {
      for (SamzaResource resource : samzaResources) {
        log.info("Releasing Container ID: {} on host: {}", resource.getContainerId(), host);
        manager.releaseResources(resource);
        numReleasedResources++;
      }
    }
    return numReleasedResources;
  }


  /**
   * Clears all the state variables
   * Performed when there are no more unfulfilled requests
   */
  private void clearState() {
    allocatedResources.clear();
    hostRequestCounts.clear();
    requestsQueue.clear();
  }

  /**
   * Returns the list of hosts which has at least 1 allocated Resource in the buffer
   * @return list of host names
   */
  private List<String> getAllocatedHosts() {
    List<String> hostKeys = new ArrayList<>();
    for (Map.Entry<String, List<SamzaResource>> entry: allocatedResources.entrySet()) {
      if (entry.getValue().size() > 0) {
        hostKeys.add(entry.getKey());
      }
    }
    return hostKeys;
  }

  /**
   * Retrieves, but does not remove, the first allocated resource on the specified host.
   *
   * @param host the host for which a resource is needed.
   * @return a {@link SamzaResource} allocated for the specified host or {@code null} if there isn't one.
   */
  public SamzaResource peekResource(String host) {
    synchronized (lock) {
      List<SamzaResource> resourcesOnPreferredHostBuffer = this.allocatedResources.get(host);
      List<SamzaResource> resourcesOnAnyHostBuffer = this.allocatedResources.get(ANY_HOST);

      // First search for the preferred host (may be ANY_HOST too)
      if (resourcesOnPreferredHostBuffer != null && !resourcesOnPreferredHostBuffer.isEmpty()) {
        SamzaResource resource = resourcesOnPreferredHostBuffer.get(0);
        log.info("Found Container ID: {} for host: {} in the buffer.", resource.getContainerId(), host);
        return resource;
      } else if (resourcesOnAnyHostBuffer != null && !resourcesOnAnyHostBuffer.isEmpty()) {
        // If no resources for preferred host int he buffer, look for ANY_HOST
        log.debug("No resources on preferred host buffer. Scanning ANY_HOST buffer");
        SamzaResource resource = resourcesOnAnyHostBuffer.stream()
            .filter(resrc -> resrc.getHost().equals(host))
            .findAny().orElse(null);
        if (resource != null) {
          log.info("Found Container ID: {} for host: {} in the buffer.", resource.getContainerId(), host);
        }
        return resource;
      } else {
        log.debug("Could not find any containers on host: {}. Both preferred host and ANY_HOST buffers are empty", host);
        return null;
      }
    }
  }

  /**
   * Retrieves, but does not remove, the next pending request in the queue.
   *
   * @return  the pending request or {@code null} if there is no pending request.
   */
  public SamzaResourceRequest peekPendingRequest() {
    synchronized (lock) {
      return this.requestsQueue.peek();
    }
  }

  /**
   * Returns the number of pending SamzaResource requests in the queue.
   * @return the number of pending requests
   */
  public int numPendingRequests() {
    synchronized (lock) {
      return this.requestsQueue.size();
    }
  }


  /**
   * Returns the list of resources allocated on a given host. If no resources were ever allocated on
   * the given host, it returns null. This method makes a defensive shallow copy. A shallow copy is
   * sufficient because the SamzaResource class does not expose setters.
   *
   * @param host hostname
   * @return list of resources allocated on the given host, or null
   */
  public List<SamzaResource> getResourcesOnAHost(String host) {
    synchronized (lock) {
      List<SamzaResource> samzaResourceList = allocatedResources.get(host);
      if (samzaResourceList == null)
        return null;

      return new ArrayList<>(samzaResourceList);
    }
  }

  // Package private, used only in tests.
  Map<String, AtomicInteger> getHostRequestCounts() {
    return Collections.unmodifiableMap(hostRequestCounts);
  }
}