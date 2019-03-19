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

import org.apache.samza.SamzaException;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.job.ShellCommandBuilder;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * {@link AbstractContainerAllocator} makes requests for physical resources to the resource manager and also runs
 * a container process on an allocated physical resource. Sub-classes should override the assignResourceRequests()
 * method to assign resource requests according to some strategy.
 *
 * See {@link ContainerAllocator} and {@link HostAwareContainerAllocator} for two such strategies
 *
 * This class is not thread-safe.
 */

//This class is used in the refactored code path as called by run-jc.sh

public abstract class AbstractContainerAllocator implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(AbstractContainerAllocator.class);

  /* State that controls the lifecycle of the allocator thread*/
  private volatile boolean isRunning = true;

  /**
   * Config and derived config objects
   */
  private final TaskConfig taskConfig;

  private final Config config;

  /**
   * A ClusterResourceManager for the allocator to request for resources.
   */
  protected final ClusterResourceManager clusterResourceManager;
  /**
   * The allocator sleeps for allocatorSleepIntervalMs before it polls its queue for the next request
   */
  protected final int allocatorSleepIntervalMs;
  /**
   * Each container currently has the same configuration - memory, and numCpuCores.
   */
  protected final int containerMemoryMb;
  protected final int containerNumCpuCores;

  /**
   * State corresponding to num failed containers, running containers etc.
   */
  protected final SamzaApplicationState state;

  /**
   * ContainerRequestState indicates the state of all unfulfilled container requests and allocated containers
   */
  protected final ResourceRequestState resourceRequestState;

  public AbstractContainerAllocator(ClusterResourceManager containerProcessManager,
                                    ResourceRequestState resourceRequestState,
                                    Config config,
                                    SamzaApplicationState state) {
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.clusterResourceManager = containerProcessManager;
    this.allocatorSleepIntervalMs = clusterManagerConfig.getAllocatorSleepTime();
    this.resourceRequestState = resourceRequestState;
    this.containerMemoryMb = clusterManagerConfig.getContainerMemoryMb();
    this.containerNumCpuCores = clusterManagerConfig.getNumCores();
    this.taskConfig = new TaskConfig(config);
    this.state = state;
    this.config = config;
  }

  /**
   * Continually schedule StreamProcessors to run on resources obtained from the cluster manager.
   * The loop frequency is governed by thread sleeps for allocatorSleepIntervalMs ms.
   *
   * Terminates when the isRunning flag is cleared.
   */
  @Override
  public void run() {
    while (isRunning) {
      try {
        assignResourceRequests();
        // Release extra resources and update the entire system's state
        resourceRequestState.releaseExtraResources();

        Thread.sleep(allocatorSleepIntervalMs);
      } catch (InterruptedException e) {
        log.warn("Got InterruptedException in AllocatorThread.", e);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        log.error("Got unknown Exception in AllocatorThread.", e);
      }
    }
  }

  /**
   * Assign resources from the cluster manager and matches them to run container processes on them.
   *
   */
  protected abstract void assignResourceRequests();

  /**
   * Updates the request state and runs a container process on the specified host. Assumes a resource
   * is available on the preferred host, so the caller must verify that before invoking this method.
   *
   * @param request             the {@link SamzaResourceRequest} which is being handled.
   * @param preferredHost       the preferred host on which the StreamProcessor process should be run or
   *                            {@link ResourceRequestState#ANY_HOST} if there is no host preference.
   * @throws
   * SamzaException if there is no allocated resource in the specified host.
   */
  protected void runStreamProcessor(SamzaResourceRequest request, String preferredHost) {
    CommandBuilder builder = getCommandBuilder(request.getContainerID());
    // Get the available resource
    SamzaResource resource = peekAllocatedResource(preferredHost);
    if (resource == null)
      throw new SamzaException("Expected resource was unavailable on host " + preferredHost);

    // Update state
    resourceRequestState.updateStateAfterAssignment(request, preferredHost, resource);
    String containerID = request.getContainerID();

    //run container on resource
    log.info("Found available resources on {}. Assigning request for container_id {} with "
            + "timestamp {} to resource {}",
        new Object[]{preferredHost, String.valueOf(containerID), request.getRequestTimestampMs(), resource.getResourceID()});

    // Update container state as "pending" and then issue a request to launch it. It's important to perform the state-update
    // prior to issuing the request. Otherwise, there's a race where the response callback may arrive sooner and not see
    // the container as "pending" (SAMZA-2117)

    state.pendingContainers.put(containerID, resource);

    clusterResourceManager.launchStreamProcessor(resource, builder);
  }

  /**
   * Called during initial request for resources
   *
   * @param resourceToHostMapping A Map of [containerId, hostName] containerId is the ID of the container process
   *                               to run on the resource. hostName is the host on which the resource must be allocated.
   *                                The hostName value is null, either
   *                                - when host-affinity has never been enabled, or
   *                                - when host-affinity is enabled and job is run for the first time
   */
  public abstract void requestResources(Map<String, String> resourceToHostMapping);

  /**
   * Checks if this allocator has a pending resource request.
   * @return {@code true} if there is a pending request, {@code false} otherwise.
   */
  protected final boolean hasPendingRequest() {
    return peekPendingRequest() != null;
  }

  /**
   * Retrieves, but does not remove, the next pending request in the queue.
   *
   * @return  the pending request or {@code null} if there is no pending request.
   */
  protected final SamzaResourceRequest peekPendingRequest() {
    return resourceRequestState.peekPendingRequest();
  }

  /**
   * Method to request a resource from the cluster manager
   *
   * @param containerID Identifier of the container that will be run when a resource is allocated for
   *                            this request
   * @param preferredHost Name of the host that you prefer to run the container on
   */
  public final void requestResource(String containerID, String preferredHost) {
    SamzaResourceRequest request = getResourceRequest(containerID, preferredHost);
    issueResourceRequest(request);
  }

  public final SamzaResourceRequest getResourceRequest(String containerID, String preferredHost) {
    return new SamzaResourceRequest(this.containerNumCpuCores, this.containerMemoryMb,
        preferredHost, containerID);
  }

  public final void issueResourceRequest(SamzaResourceRequest request) {
    resourceRequestState.addResourceRequest(request);
    state.containerRequests.incrementAndGet();
    if (ResourceRequestState.ANY_HOST.equals(request.getPreferredHost())) {
      state.anyHostRequests.incrementAndGet();
    } else {
      state.preferredHostRequests.incrementAndGet();
    }
  }

  /**
   * Returns true if there are resources allocated on a host.
   * @param host  the host for which a resource is needed.
   * @return      {@code true} if there is a resource allocated for the specified host, {@code false} otherwise.
   */
  protected boolean hasAllocatedResource(String host) {
    return peekAllocatedResource(host) != null;
  }

  /**
   * Retrieves, but does not remove, the first allocated resource on the specified host.
   *
   * @param host  the host on which a resource is needed.
   * @return      the first {@link SamzaResource} allocated for the specified host or {@code null} if there isn't one.
   */
  protected SamzaResource peekAllocatedResource(String host) {
    return resourceRequestState.peekResource(host);
  }

  /**
   * Returns a command builder with the build environment configured with the containerId.
   * @param samzaContainerId to configure the builder with.
   * @return the constructed builder object
   */
  private CommandBuilder getCommandBuilder(String samzaContainerId) {
    String cmdBuilderClassName = taskConfig.getCommandClass(ShellCommandBuilder.class.getName());
    CommandBuilder cmdBuilder = Util.getObj(cmdBuilderClassName, CommandBuilder.class);

    cmdBuilder.setConfig(config).setId(samzaContainerId).setUrl(state.jobModelManager.server().getUrl());
    return cmdBuilder;
  }
  /**
   * Adds allocated samzaResource to a synchronized buffer of allocated resources.
   * See allocatedResources in {@link ResourceRequestState}
   *
   * @param samzaResource returned by the ContainerManager
   */
  public final void addResource(SamzaResource samzaResource) {
    resourceRequestState.addResource(samzaResource);
  }

  /**
   * Stops the Allocator. Setting this flag to false, exits the allocator loop.
   */
  public void stop() {
    isRunning = false;
  }

}
