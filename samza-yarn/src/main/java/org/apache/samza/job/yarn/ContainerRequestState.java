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
package org.apache.samza.job.yarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class maintains the state variables for all the container requests and the allocated containers returned
 * by the RM
 * Important: Even though we use concurrent data structures, this class is not thread-safe. Thread safety has to be
 * handled by the caller.
 */
public class ContainerRequestState {
  private static final Logger log = LoggerFactory.getLogger(ContainerRequestState.class);
  public static final String ANY_HOST = "ANY_HOST";

  /**
   * Maintain a map of hostname to a list of containers allocated on this host
   */
  private final ConcurrentHashMap<String, List<Container>> allocatedContainers = new ConcurrentHashMap<String, List<Container>>();
  /**
   * Represents the queue of container requests made by the {@link org.apache.samza.job.yarn.SamzaTaskManager}
   */
  private final PriorityBlockingQueue<SamzaContainerRequest> requestsQueue = new PriorityBlockingQueue<SamzaContainerRequest>();
  /**
   * Maintain a map of hostname to the number of requests made for containers on this host
   * This state variable is used to look-up whether an allocated container on a host was ever requested in the past.
   * This map is not updated when host-affinity is not enabled
   */
  private final ConcurrentHashMap<String, AtomicInteger> requestsToCountMap = new ConcurrentHashMap<String, AtomicInteger>();
  /**
   * Indicates whether host-affinity is enabled or not
   */
  private final boolean hostAffinityEnabled;

  private final AMRMClientAsync<AMRMClient.ContainerRequest> amClient;

  // TODO: Refactor such that the state class for host-affinity enabled allocator is a subclass of a generic state class
  public ContainerRequestState(AMRMClientAsync<AMRMClient.ContainerRequest> amClient,
                               boolean hostAffinityEnabled) {
    this.amClient = amClient;
    this.hostAffinityEnabled = hostAffinityEnabled;
  }

  /**
   * This method is called every time {@link org.apache.samza.job.yarn.SamzaTaskManager} requestsQueue for a container
   * Adds {@link org.apache.samza.job.yarn.SamzaContainerRequest} to the requestsQueue queue.
   * If host-affinity is enabled, it updates the requestsToCountMap as well.
   *
   * @param request {@link org.apache.samza.job.yarn.SamzaContainerRequest} that was sent to the RM
   */
  public synchronized void updateRequestState(SamzaContainerRequest request) {

    log.info("Requesting a container for {} at {}", request.getExpectedContainerId(), request.getPreferredHost());
    amClient.addContainerRequest(request.getIssuedRequest());

    requestsQueue.add(request);
    String preferredHost = request.getPreferredHost();
    if (hostAffinityEnabled) {
      if (requestsToCountMap.containsKey(preferredHost)) {
        requestsToCountMap.get(preferredHost).incrementAndGet();
      } else {
        requestsToCountMap.put(preferredHost, new AtomicInteger(1));
      }
      /**
       * The following is important to correlate allocated container data with the requestsQueue made before. If
       * the preferredHost is requested for the first time, the state should reflect that the allocatedContainers
       * list is empty and NOT null.
       */
      if (!allocatedContainers.containsKey(preferredHost)) {
        allocatedContainers.put(preferredHost, new ArrayList<Container>());
      }
    }
  }

  /**
   * This method is called every time the RM returns an allocated container.
   * Adds the allocated container resource to the correct allocatedContainers buffer
   * @param container Container resource that was returned by the RM
   */
  public synchronized void addContainer(Container container) {
    if(hostAffinityEnabled) {
      String hostName = container.getNodeHttpAddress().split(":")[0];
      AtomicInteger requestCount = requestsToCountMap.get(hostName);
      // Check if this host was requested for any of the containers
      if (requestCount == null || requestCount.get() == 0) {
        log.debug(
            "Request count for the allocatedContainer on {} is null or 0. This means that the host was not requested " +
                "for running containers.Hence, saving the container {} in the buffer for ANY_HOST",
            hostName,
            container.getId()
        );
        addToAllocatedContainerList(ANY_HOST, container);
      } else {
        int requestCountOnThisHost = requestCount.get();
        List<Container> allocatedContainersOnThisHost = allocatedContainers.get(hostName);
        if (requestCountOnThisHost > 0) {
          if (allocatedContainersOnThisHost == null) {
            log.debug("Saving the container {} in the buffer for {}", container.getId(), hostName);
            addToAllocatedContainerList(hostName, container);
          } else {
            if (allocatedContainersOnThisHost.size() < requestCountOnThisHost) {
              log.debug("Saving the container {} in the buffer for {}", container.getId(), hostName);
              addToAllocatedContainerList(hostName, container);
            } else {
              /**
               * The RM may allocate more containers on a given host than requested. In such a case, even though the
               * requestCount != 0, it will be greater than the total request count for that host. Hence, it should be
               * assigned to ANY_HOST
               */
              log.debug(
                  "The number of containers already allocated on {} is greater than what was " +
                      "requested, which is {}. Hence, saving the container {} in the buffer for ANY_HOST",
                  new Object[]{
                      hostName,
                      requestCountOnThisHost,
                      container.getId()
                  }
              );
              addToAllocatedContainerList(ANY_HOST, container);
            }
          }
        } else {
          log.debug(
              "This host was never requested. Hence, saving the container {} in the buffer for ANY_HOST",
              new Object[]{
                  hostName,
                  requestCountOnThisHost,
                  container.getId()
              }
          );
          addToAllocatedContainerList(ANY_HOST, container);
        }
      }
    } else {
      log.debug("Saving the container {} in the buffer for ANY_HOST", container.getId());
      addToAllocatedContainerList(ANY_HOST, container);
    }
  }

  // Update the allocatedContainers list
  private void addToAllocatedContainerList(String host, Container container) {
    List<Container> list = allocatedContainers.get(host);
    if (list != null) {
      list.add(container);
    } else {
      list = new ArrayList<Container>();
      list.add(container);
      allocatedContainers.put(host, list);
    }
  }

  /**
   * This method updates the state after a request is fulfilled and a container starts running on a host
   * Needs to be synchronized because the state buffers are populated by the AMRMCallbackHandler, whereas it is drained by the allocator thread
   *
   * @param request {@link org.apache.samza.job.yarn.SamzaContainerRequest} that was fulfilled
   * @param assignedHost  Host to which the container was assigned
   * @param container Allocated container resource that was used to satisfy this request
   */
  public synchronized void updateStateAfterAssignment(SamzaContainerRequest request, String assignedHost, Container container) {
    requestsQueue.remove(request);
    allocatedContainers.get(assignedHost).remove(container);
    if (hostAffinityEnabled) {
      // assignedHost may not always be the preferred host.
      // Hence, we should safely decrement the counter for the preferredHost
      requestsToCountMap.get(request.getPreferredHost()).decrementAndGet();
    }
    // To avoid getting back excess containers
    amClient.removeContainerRequest(request.getIssuedRequest());
  }

  /**
   * If requestQueue is empty, all extra containers in the buffer should be released and update the entire system's state
   * Needs to be synchronized because it is modifying shared state buffers
   * @return the number of containers released.
   */
  public synchronized int releaseExtraContainers() {
    int numReleasedContainers = 0;

    if (hostAffinityEnabled) {
      if (requestsQueue.isEmpty()) {
        log.info("Requests Queue is empty. Should clear up state.");

        List<String> allocatedHosts = getAllocatedHosts();
        for (String host : allocatedHosts) {
          List<Container> containers = getContainersOnAHost(host);
          if (containers != null) {
            for (Container c : containers) {
              log.info("Releasing extra container {} allocated on {}", c.getId(), host);
              amClient.releaseAssignedContainer(c.getId());
              numReleasedContainers++;
            }
          }
        }
        clearState();
      }
    } else {
      if (requestsQueue.isEmpty()) {
        log.info("No more pending requests in queue.");

        List<Container> availableContainers = getContainersOnAHost(ANY_HOST);
        while(availableContainers != null && !availableContainers.isEmpty()) {
          Container c = availableContainers.remove(0);
          log.info("Releasing extra allocated container - {}", c.getId());
          amClient.releaseAssignedContainer(c.getId());
          numReleasedContainers++;
        }
        clearState();
      }
    }
    return numReleasedContainers;
  }

  /**
   * Clears all the state variables
   * Performed when there are no more unfulfilled requests
   */
  private void clearState() {
    allocatedContainers.clear();
    requestsToCountMap.clear();
    requestsQueue.clear();
  }

  /**
   * Returns the list of hosts which has at least 1 allocatedContainer in the buffer
   * @return list of host names
   */
  private List<String> getAllocatedHosts() {
    List<String> hostKeys = new ArrayList<String>();
    for(Map.Entry<String, List<Container>> entry: allocatedContainers.entrySet()) {
      if(entry.getValue().size() > 0) {
        hostKeys.add(entry.getKey());
      }
    }
    return hostKeys;
  }

  /**
   * Returns the list of containers allocated on a given host
   * If no containers were ever allocated on the given host, it returns null.
   * @param host hostname
   * @return list of containers allocated on the given host, or null
   */
  public List<Container> getContainersOnAHost(String host) {
    return allocatedContainers.get(host);
  }

  public PriorityBlockingQueue<SamzaContainerRequest> getRequestsQueue() {
    return requestsQueue;
  }

  public ConcurrentHashMap<String, AtomicInteger> getRequestsToCountMap() {
    return requestsToCountMap;
  }
}
