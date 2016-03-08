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

import java.util.List;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.samza.SamzaException;
import org.apache.samza.config.YarnConfig;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is responsible for making requests for containers to the AM and also, assigning a container to run on an allocated resource.
 *
 * Since we are using a simple thread based allocation of a container to an allocated resource, the subclasses should implement {@link java.lang.Runnable} interface.
 * The allocator thread follows the lifecycle of the {@link org.apache.samza.job.yarn.SamzaTaskManager}. Depending on whether host-affinity is enabled or not, the allocation model varies.
 *
 * See {@link org.apache.samza.job.yarn.ContainerAllocator} and {@link org.apache.samza.job.yarn.HostAwareContainerAllocator}
 */
public abstract class AbstractContainerAllocator implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(AbstractContainerAllocator.class);

  public static final String ANY_HOST = ContainerRequestState.ANY_HOST;
  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_CONTAINER_MEM = 1024;
  public static final int DEFAULT_CPU_CORES = 1;

  protected final AMRMClientAsync<AMRMClient.ContainerRequest> amClient;
  protected final int ALLOCATOR_SLEEP_TIME;
  protected final ContainerUtil containerUtil;
  protected final int containerMaxMemoryMb;
  protected final int containerMaxCpuCore;

  // containerRequestState indicate the state of all unfulfilled container requests and allocated containers
  private final ContainerRequestState containerRequestState;

  // state that controls the lifecycle of the allocator thread
  private AtomicBoolean isRunning = new AtomicBoolean(true);

  public AbstractContainerAllocator(AMRMClientAsync<AMRMClient.ContainerRequest> amClient,
                            ContainerUtil containerUtil,
                            ContainerRequestState containerRequestState,
                            YarnConfig yarnConfig) {
    this.amClient = amClient;
    this.containerUtil = containerUtil;
    this.ALLOCATOR_SLEEP_TIME = yarnConfig.getAllocatorSleepTime();
    this.containerRequestState = containerRequestState;
    this.containerMaxMemoryMb = yarnConfig.getContainerMaxMemoryMb();
    this.containerMaxCpuCore = yarnConfig.getContainerMaxCpuCores();
  }

  /**
   * Continuously assigns requested containers to the allocated containers provided by the cluster manager.
   * The loop frequency is governed by thread sleeps for ALLOCATOR_SLEEP_TIME ms.
   *
   * Terminates when the isRunning flag is cleared.
   */
  @Override
  public void run() {
    while(isRunning.get()) {
      try {
        assignContainerRequests();

        // Release extra containers and update the entire system's state
        containerRequestState.releaseExtraContainers();

        Thread.sleep(ALLOCATOR_SLEEP_TIME);
      } catch (InterruptedException e) {
        log.info("Got InterruptedException in AllocatorThread.", e);
      } catch (Exception e) {
        log.error("Got unknown Exception in AllocatorThread.", e);
      }
    }
  }

  /**
   * Assigns the container requests from the queue to the allocated containers from the cluster manager and
   * runs them.
   */
  protected abstract void assignContainerRequests();

  /**
   * Updates the request state and runs the container on the specified host. Assumes a container
   * is available on the preferred host, so the caller must verify that before invoking this method.
   *
   * @param request             the {@link SamzaContainerRequest} which is being handled.
   * @param preferredHost       the preferred host on which the container should be run or
   *                            {@link ContainerRequestState#ANY_HOST} if there is no host preference.
   */
  protected void runContainer(SamzaContainerRequest request, String preferredHost) {
    // Get the available container
    Container container = peekAllocatedContainer(preferredHost);
    if (container == null)
      throw new SamzaException("Expected container was unavailable on host " + preferredHost);

    // Update state
    containerRequestState.updateStateAfterAssignment(request, preferredHost, container);
    int expectedContainerId = request.expectedContainerId;

    // Cancel request and run container
    log.info("Found available containers on {}. Assigning request for container_id {} with "
            + "timestamp {} to container {}",
        new Object[]{preferredHost, String.valueOf(expectedContainerId), request.getRequestTimestamp(), container.getId()});
    try {
      if (preferredHost.equals(ANY_HOST)) {
        containerUtil.runContainer(expectedContainerId, container);
      } else {
        containerUtil.runMatchedContainer(expectedContainerId, container);
      }
    } catch (SamzaContainerLaunchException e) {
      log.warn(String.format("Got exception while starting container %s. Requesting a new container on any host", container), e);
      containerRequestState.releaseUnstartableContainer(container);
      requestContainer(expectedContainerId, ContainerAllocator.ANY_HOST);
    }
  }

  /**
   * Called during initial request for containers
   *
   * @param containerToHostMappings Map of containerId to its last seen host (locality).
   *                                The locality value is null, either
   *                                - when host-affinity is not enabled, or
   *                                - when host-affinity is enabled and job is run for the first time
   */
  public void requestContainers(Map<Integer, String> containerToHostMappings) {
    for (Map.Entry<Integer, String> entry : containerToHostMappings.entrySet()) {
      int containerId = entry.getKey();
      String preferredHost = entry.getValue();
      if (preferredHost == null)
        preferredHost = ANY_HOST;

      requestContainer(containerId, preferredHost);
    }
  }
  /**
   * Method to request a container resource from yarn
   *
   * @param expectedContainerId Identifier of the container that will be run when a container resource is allocated for
   *                            this request
   * @param preferredHost Name of the host that you prefer to run the container on
   */
  public final void requestContainer(int expectedContainerId, String preferredHost) {
    SamzaContainerRequest request = new SamzaContainerRequest(
        containerMaxMemoryMb,
        containerMaxCpuCore,
        DEFAULT_PRIORITY,
        expectedContainerId,
        preferredHost);
    containerRequestState.updateRequestState(request);
    containerUtil.incrementContainerRequests();
  }

  /**
   * @return {@code true} if there is a pending request, {@code false} otherwise.
   */
  protected boolean hasPendingRequest() {
    return peekPendingRequest() != null;
  }

  /**
   * Retrieves, but does not remove, the next pending request in the queue.
   *
   * @return  the pending request or {@code null} if there is no pending request.
   */
  protected SamzaContainerRequest peekPendingRequest() {
    return containerRequestState.getRequestsQueue().peek();
  }

  /**
   * Method that adds allocated container to a synchronized buffer of allocated containers list
   * See allocatedContainers in {@link org.apache.samza.job.yarn.ContainerRequestState}
   *
   * @param container Container resource returned by the RM
   */
  public final void addContainer(Container container) {
    containerRequestState.addContainer(container);
  }

  /**
   * @param host  the host for which a container is needed.
   * @return      {@code true} if there is a container allocated for the specified host, {@code false} otherwise.
   */
  protected boolean hasAllocatedContainer(String host) {
    return peekAllocatedContainer(host) != null;
  }

  /**
   * Retrieves, but does not remove, the first allocated container on the specified host.
   *
   * @param host  the host for which a container is needed.
   * @return      the first {@link Container} allocated for the specified host or {@code null} if there isn't one.
   */
  protected Container peekAllocatedContainer(String host) {
    List<Container> allocatedContainers = containerRequestState.getContainersOnAHost(host);
    if (allocatedContainers == null || allocatedContainers.isEmpty()) {
      return null;
    }

    return allocatedContainers.get(0);
  }

  public final void setIsRunning(boolean state) {
    isRunning.set(state);
  }

}
