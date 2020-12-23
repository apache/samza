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

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.job.ShellCommandBuilder;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link ContainerAllocator} makes requests for physical resources to the resource manager and also runs
 * a processor on an allocated physical resource.
 *
 * <ul>
 *  <li>
 *    In case of host-affinity enabled, each request ({@link SamzaResourceRequest} contains a processorId which
 *    identifies the processor the request is for and a "preferredHost" which is determined by the locality mappings
 *    in the coordinator stream
 *  </li>
 *  <li>
 *    This thread periodically matches outstanding resource requests with allocated resources.
 *    Its period is controlled using the {@code allocatorSleepIntervalMs} parameter
 *  </li>
 *  <li>
 *    When host-affinity is enabled, the resource-request's preferredHost param is set to the host the processor
 *    was last seen on
 *  </li>
 *  <li>
 *    When host-affinity is disabled, the resource-request's preferredHost param is set to {@link ResourceRequestState#ANY_HOST}
 *  </li>
 *  <li>
 *    When host-affinity is enabled and a preferred resource has not been obtained after {@code requestExpiryTimeout}
 *    milliseconds of the request being made, the resource is declared expired. The expired request are handled by
 *    allocating them to *ANY* allocated resource if available. If no surplus resources are available the current preferred
 *    resource-request is cancelled and resource-request for ANY_HOST is issued
 *  </li>
 *  <li>
 *    When host-affinity is not enabled, this periodically wakes up to assign a processor to *ANY* allocated resource.
 *    If there aren't enough resources, it waits by sleeping for {@code allocatorSleepIntervalMs} milliseconds.
 *  </li>
 * </ul>
 *
 * This class is not thread-safe. This class is used in the refactored code path as called by run-jc.sh
 */
public class ContainerAllocator implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerAllocator.class);

  /* State that controls the lifecycle of the allocator thread */
  private volatile boolean isRunning = true;

  /**
   * Flag for affine host requests
   */
  private final boolean hostAffinityEnabled;

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
   * State corresponding to num failed containers, running processors etc.
   */
  protected final SamzaApplicationState state;

  /**
   * ResourceRequestState indicates the state of all unfulfilled and allocated container requests
   */
  protected final ResourceRequestState resourceRequestState;
  /**
   * Tracks the configured expiration of a resource request defaults to {@code ClusterManagerConfig#CLUSTER_MANAGER_REQUEST_TIMEOUT_MS}
   * if specified or {@code ClusterManagerConfig#DEFAULT_CONTAINER_REQUEST_TIMEOUT_MS} otherwise
   */
  private final int configuredRequestExpiryTimeout;

  private final ContainerManager containerManager;

  public ContainerAllocator(ClusterResourceManager clusterResourceManager,
      Config config,
      SamzaApplicationState state,
      boolean hostAffinityEnabled,
      ContainerManager containerManager) {
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.clusterResourceManager = clusterResourceManager;
    this.allocatorSleepIntervalMs = clusterManagerConfig.getAllocatorSleepTime();
    this.resourceRequestState = new ResourceRequestState(hostAffinityEnabled, this.clusterResourceManager);
    this.containerMemoryMb = clusterManagerConfig.getContainerMemoryMb();
    this.containerNumCpuCores = clusterManagerConfig.getNumCores();
    this.taskConfig = new TaskConfig(config);
    this.state = state;
    this.config = config;
    this.hostAffinityEnabled = hostAffinityEnabled;
    this.containerManager = containerManager;
    this.configuredRequestExpiryTimeout = clusterManagerConfig.getContainerRequestTimeout();
  }

  /**
   * Continually schedule processors to run on resources obtained from the cluster manager.
   * The loop frequency is governed by thread sleeps for allocatorSleepIntervalMs ms.
   *
   * Terminates when the isRunning flag is cleared.
   */
  @Override
  public void run() {
    while (isRunning) {
      try {
        assignResourceRequests();

        // Move delayed requests that are ready to the active request queue
        resourceRequestState.sendPendingDelayedResourceRequests();

        // Release extra resources and update the entire system's state
        resourceRequestState.releaseExtraResources();

        Thread.sleep(allocatorSleepIntervalMs);
      } catch (InterruptedException e) {
        LOG.warn("Got InterruptedException in AllocatorThread.", e);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.error("Got unknown Exception in AllocatorThread.", e);
      }
    }
  }

  /**
   * Assigns resources received from the cluster manager to processors.
   *
   * During the run() method, the thread sleeps for allocatorSleepIntervalMs ms. It then invokes assignResourceRequests,
   * and tries to allocate any unsatisfied request that is still in the request queue {@link ResourceRequestState})
   * with allocated resources.
   *
   * When host-affinity is disabled, all allocated resources are buffered by the key "ANY_HOST"
   * When host-affinity is enabled, all allocated resources are buffered by the hostName as key
   *
   * If the requested host is not available, the thread checks to see if the request has expired. If it has expired
   * then following cases are possible
   *
   * Case 1. If this expired request is due to a container placement action mark the request as failed and return
   * Case 2: Otherwise for a normal resource request following cases are possible
   *    Case 2.1  If StandbyContainer is present refer to {@code StandbyContainerManager#handleExpiredResourceRequest}
   *    Case 2.2: host-affinity is enabled, allocator thread looks for allocated resources on ANY_HOST and issues a
   *              container start if available, otherwise issue an ANY_HOST request
   *    Case 2.2: host-affinity is disabled, allocator thread does not handle expired requests, it waits for cluster
   *              manager to return resources on ANY_HOST
   *         TODO: SAMZA-2330 Handle expired request for host affinity disabled case
   *
   * If the requested host is available then following cases are possible
   * Case 1. If the container launch request is due to an existing container placement action, issue a stop on active
   *         container & wait for the active container to be stopped before issuing a start.
   * Case 2. If StandbyContainer is present refer to {@code StandbyContainerManager#checkStandbyConstraintsAndRunStreamProcessor}
   * Case 3. Otherwise just invoke a container start on the allocated resource for the pending request
   *
   * When host-affinity is enabled and a {@code StandbyContainerManager} is present, the allocator transfers the request
   * to it for checking StandByConstraints before launching a processor
   */
  void assignResourceRequests() {
    while (hasReadyPendingRequest()) {
      SamzaResourceRequest request = peekReadyPendingRequest().get();
      String processorId = request.getProcessorId();
      String preferredHost = hostAffinityEnabled ? request.getPreferredHost() : ResourceRequestState.ANY_HOST;
      Instant requestCreationTime = request.getRequestTimestamp();

      LOG.info("Handling assignment for Processor ID: {} with request {}", processorId, request);
      if (hasAllocatedResource(preferredHost)) {

        // Found allocated container on preferredHost
        LOG.info("Found an available container for Processor ID: {} on the host: {}", processorId, preferredHost);

        // Needs to be only updated when host affinity is enabled
        if (hostAffinityEnabled) {
          state.matchedResourceRequests.incrementAndGet();
        }

        boolean containerLaunchComplete =
            containerManager.handleContainerLaunch(request, preferredHost, peekAllocatedResource(preferredHost),
                resourceRequestState, this);

        /**
         * Some Container launch requests are due to Container Placement actions like move, restarts. Under those
         * circumstances a container launch needs to wait for the previous container incarnation to stop. In this scenario
         * the allocator thread needs to sleep and recheck the stop again
         */
        if (!containerLaunchComplete) {
          break;
        }

      } else {

        LOG.info("Did not find any allocated containers for running Processor ID: {} on the host: {}.",
            processorId, preferredHost);

        if (isRequestExpired(request)) {
          updateExpiryMetrics(request);
          containerManager.handleExpiredRequest(processorId, preferredHost, request, this, resourceRequestState);
          // SAMZA-2601: to prevent infinite looping and logs filling up the disk, when host affinity is disabled,
          // we explicitly break the loop here and the whole process gets retried in run() after allocatorSleepIntervalMs
          if (!hostAffinityEnabled) {
            LOG.info("Waiting for resources to get allocated for request {},"
                + " no retries will be issued since host affinity is disabled", request);
            break;
          }
        } else {
          LOG.info("Request for Processor ID: {} on preferred host {} has not expired yet."
                  + "Request creation time: {}. Current Time: {}. Request timeout: {} ms", processorId, preferredHost,
              requestCreationTime, System.currentTimeMillis(), getRequestTimeout(request).toMillis());
          break;
        }
      }
    }
  }

  /**
   * Updates the request state and runs a processor on the specified host. Assumes a resource
   * is available on the preferred host, so the caller must verify that before invoking this method.
   *
   * @param request             the {@link SamzaResourceRequest} which is being handled.
   * @param preferredHost       the preferred host on which the processor should be run or
   *                            {@link ResourceRequestState#ANY_HOST} if there is no host preference.
   * @throws                    SamzaException if there is no allocated resource in the specified host.
   */
  protected void runStreamProcessor(SamzaResourceRequest request, String preferredHost) {
    CommandBuilder builder = getCommandBuilder(request.getProcessorId());
    // Get the available resource
    SamzaResource resource = peekAllocatedResource(preferredHost);
    if (resource == null) {
      throw new SamzaException("Expected resource for Processor ID: " + request.getProcessorId() + " was unavailable on host: " + preferredHost);
    }

    /**
     * If the allocated resource has expired then release the expired resource and re-request the resources from {@link ClusterResourceManager}
     */
    if (clusterResourceManager.isResourceExpired(resource)) {
      containerManager.handleExpiredResource(request, resource, preferredHost, resourceRequestState, this);
      return;
    }

    // Update state
    resourceRequestState.updateStateAfterAssignment(request, preferredHost, resource);
    String processorId = request.getProcessorId();

    // Run processor on resource
    LOG.info("Found Container ID: {} for Processor ID: {} on host: {} for request creation time: {}.",
        resource.getContainerId(), processorId, preferredHost, request.getRequestTimestamp());

    // Update processor state as "pending" and then issue a request to launch it. It's important to perform the state-update
    // prior to issuing the request. Otherwise, there's a race where the response callback may arrive sooner and not see
    // the processor as "pending" (SAMZA-2117)

    state.failedProcessors.remove(processorId);
    state.pendingProcessors.put(processorId, resource);

    clusterResourceManager.launchStreamProcessor(resource, builder);
  }

  /**
   * Called during initial request for resources
   *
   * @param processorToHostMapping A Map of [processorId, hostName], where processorId is the ID of the Samza processor
   *                               to run on the resource. hostName is the host on which the resource should be allocated.
   *                               The hostName value is null, either
   *                                - when host-affinity has never been enabled, or
   *                                - when host-affinity is enabled and job is run for the first time
   *                                - when the number of containers has been increased.
   */
  public void requestResources(Map<String, String> processorToHostMapping) {
    for (Map.Entry<String, String> entry : processorToHostMapping.entrySet()) {
      String processorId = entry.getKey();
      String preferredHost = entry.getValue();
      if (!hostAffinityEnabled) {
        preferredHost = ResourceRequestState.ANY_HOST;
      } else if (preferredHost == null) {
        LOG.info("No preferred host mapping found for Processor ID: {}. Requesting resource on ANY_HOST", processorId);
        preferredHost = ResourceRequestState.ANY_HOST;
      }
      requestResource(processorId, preferredHost);
    }
  }

  /**
   * Checks if this allocator has a pending resource request with a request timestamp equal to or earlier than the current
   * timestamp.
   * @return {@code true} if there is a pending request, {@code false} otherwise.
   */
  protected final boolean hasReadyPendingRequest() {
    return peekReadyPendingRequest().isPresent();
  }

  /**
   * Retrieves, but does not remove, the next pending request in the queue with the {@link SamzaResourceRequest#getRequestTimestamp()}
   * that is greater than the current timestamp.
   *
   * @return  the pending request or {@code null} if there is no pending request.
   */
  protected final Optional<SamzaResourceRequest> peekReadyPendingRequest() {
    SamzaResourceRequest pendingRequest = resourceRequestState.peekPendingRequest();
    return Optional.ofNullable(pendingRequest);
  }

  /**
   * Requests a resource from the cluster manager
   * @param processorId Samza processor ID that will be run when a resource is allocated for this request
   * @param preferredHost name of the host that you prefer to run the processor on
   */
  public final void requestResource(String processorId, String preferredHost) {
    requestResourceWithDelay(processorId, preferredHost, Duration.ZERO);
  }

  /**
   * Requests a resource from the cluster manager
   * @param processorId Samza processor ID that will be run when a resource is allocated for this request
   * @param preferredHost name of the host that you prefer to run the processor on
   * @param faultDomains set of fault domains on which to schedule this resource
   */
  public final void requestResource(String processorId, String preferredHost, Set<FaultDomain> faultDomains) {
    requestResourceWithDelay(processorId, preferredHost, Duration.ZERO, faultDomains);
  }

  /**
   * Requests a resource from the cluster manager with a request timestamp of the current time plus the specified delay.
   * @param processorId Samza processor ID that will be run when a resource is allocated for this request
   * @param preferredHost name of the host that you prefer to run the processor on
   * @param delay the {@link Duration} to add to the request timestamp
   */
  public final void requestResourceWithDelay(String processorId, String preferredHost, Duration delay) {
    SamzaResourceRequest request = getResourceRequestWithDelay(processorId, preferredHost, delay);
    issueResourceRequest(request);
  }

  /**
   * Requests a resource from the cluster manager with a request timestamp of the current time plus the specified delay.
   * @param processorId Samza processor ID that will be run when a resource is allocated for this request
   * @param preferredHost name of the host that you prefer to run the processor on
   * @param delay the {@link Duration} to add to the request timestamp
   * @param faultDomains set of fault domains on which to schedule this resource
   */
  public final void requestResourceWithDelay(String processorId, String preferredHost, Duration delay, Set<FaultDomain> faultDomains) {
    SamzaResourceRequest request = getResourceRequestWithDelay(processorId, preferredHost, delay, faultDomains);
    issueResourceRequest(request);
  }

  /**
   * Creates a {@link SamzaResourceRequest} to send to the cluster manager
   * @param processorId Samza processor ID that will be run when a resource is allocated for this request
   * @param preferredHost name of the host that you prefer to run the processor on
   * @return the created request
   */
  public final SamzaResourceRequest getResourceRequest(String processorId, String preferredHost) {
    return getResourceRequestWithDelay(processorId, preferredHost, Duration.ZERO);
  }

  /**
   * Creates a {@link SamzaResourceRequest} to send to the cluster manager
   * @param processorId Samza processor ID that will be run when a resource is allocated for this request
   * @param preferredHost name of the host that you prefer to run the processor on
   * @param faultDomains set of fault domains on which to schedule this resource
   * @return the created request
   */
  public final SamzaResourceRequest getResourceRequest(String processorId, String preferredHost, Set<FaultDomain> faultDomains) {
    return getResourceRequestWithDelay(processorId, preferredHost, Duration.ZERO, faultDomains);
  }

  /**
   * Creates a {@link SamzaResourceRequest} to send to the cluster manager with a request timestamp of the current time
   * plus the specified delay.
   * @param processorId Samza processor ID that will be run when a resource is allocated for this request
   * @param preferredHost name of the host that you prefer to run the processor on
   * @param delay the {@link Duration} to add to the request timestamp
   * @return the created request
   */
  public final SamzaResourceRequest getResourceRequestWithDelay(String processorId, String preferredHost, Duration delay) {
    return new SamzaResourceRequest(this.containerNumCpuCores, this.containerMemoryMb, preferredHost, processorId, Instant.now().plus(delay));
  }

  /**
   * Creates a {@link SamzaResourceRequest} to send to the cluster manager with a request timestamp of the current time
   * plus the specified delay.
   * @param processorId Samza processor ID that will be run when a resource is allocated for this request
   * @param preferredHost name of the host that you prefer to run the processor on
   * @param delay the {@link Duration} to add to the request timestamp
   * @param faultDomains set of fault domains on which to schedule this resource
   * @return the created request
   */
  public final SamzaResourceRequest getResourceRequestWithDelay(String processorId, String preferredHost, Duration delay, Set<FaultDomain> faultDomains) {
    return new SamzaResourceRequest(this.containerNumCpuCores, this.containerMemoryMb, preferredHost, processorId, Instant.now().plus(delay), faultDomains);
  }

  public final void issueResourceRequest(SamzaResourceRequest request) {
    resourceRequestState.addResourceRequest(request);
    state.containerRequests.incrementAndGet();
    if (ResourceRequestState.ANY_HOST.equals(request.getPreferredHost())) {
      state.anyHostRequests.incrementAndGet();
    } else {
      state.preferredHostRequests.incrementAndGet();
    }
    if (!request.getFaultDomains().isEmpty()) {
      state.faultDomainAwareContainerRequests.incrementAndGet();
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
   * Returns a command builder with the build environment configured with the processorId.
   * @param processorId to configure the builder with.
   * @return the constructed builder object
   */
  private CommandBuilder getCommandBuilder(String processorId) {
    String cmdBuilderClassName = taskConfig.getCommandClass(ShellCommandBuilder.class.getName());
    CommandBuilder cmdBuilder = ReflectionUtil.getObj(cmdBuilderClassName, CommandBuilder.class);

    cmdBuilder.setConfig(config).setId(processorId).setUrl(state.jobModelManager.server().getUrl());
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
   * Releases a single resource based on containerId.
   * @param containerId container ID
   */
  public final void releaseResource(String containerId) {
    resourceRequestState.releaseResource(containerId);
  }

  /**
   * Stops the Allocator. Setting this flag to false exits the allocator loop.
   */
  public void stop() {
    isRunning = false;
  }


  /**
   * Checks if a request has expired.
   *
   * @param request the request to check
   * @return true if request has expired
   */
  protected boolean isRequestExpired(SamzaResourceRequest request) {
    long currTime = Instant.now().toEpochMilli();
    boolean requestExpired =  currTime - request.getRequestTimestamp().toEpochMilli() > getRequestTimeout(request).toMillis();
    if (requestExpired) {
      LOG.info("Request for Processor ID: {} on host: {} with creation time: {} has expired at current time: {} after timeout: {} ms.",
          request.getProcessorId(), request.getPreferredHost(), request.getRequestTimestamp(), currTime, getRequestTimeout(request).toMillis());
    }
    return requestExpired;
  }

  /**
   * Determines the request expiry timeout. Container placement actions like move, restarts can optionally override
   * request expiry timeout. Otherwise it defaults to { @code configuredRequestExpiryTimeout }
   */
  private Duration getRequestTimeout(SamzaResourceRequest request) {
    Optional<Duration> controlActionRequestExpiryTimeout = containerManager.getActionExpiryTimeout(request);
    return controlActionRequestExpiryTimeout.orElse(Duration.ofMillis(configuredRequestExpiryTimeout));
  }

  private void updateExpiryMetrics(SamzaResourceRequest request) {
    String preferredHost = request.getPreferredHost();
    if (ResourceRequestState.ANY_HOST.equals(preferredHost)) {
      state.expiredAnyHostRequests.incrementAndGet();
    } else {
      state.expiredPreferredHostRequests.incrementAndGet();
    }
    if (!request.getFaultDomains().isEmpty()) {
      state.expiredFaultDomainAwareContainerRequests.incrementAndGet();
    }
  }
}
