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
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.metrics.ContainerProcessManagerMetrics;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * ContainerProcessManager is responsible for requesting containers, handling failures, and notifying the application master that the
 * job is done.
 *
 * The following main threads are involved in the execution of the ContainerProcessManager :
 *  - The main thread (defined in SamzaAppMaster) that sends requests to the cluster manager.
 *  - The callback handler thread that receives the responses from cluster manager and handles:
 *      - Populating a buffer when a container is allocated by the cluster manager
 *        (allocatedContainers in {@link ResourceRequestState}
 *      - Identifying the cause of container failure and re-request containers from the cluster manager by adding request to the
 *        internal requestQueue in {@link ResourceRequestState}
 *  - The allocator thread defined here assigns the allocated containers to pending requests
 *    (See {@link org.apache.samza.clustermanager.ContainerAllocator} or {@link org.apache.samza.clustermanager.HostAwareContainerAllocator})
 *
 */

public class ContainerProcessManager implements ClusterResourceManager.Callback   {

  private static final Logger log = LoggerFactory.getLogger(ContainerProcessManager.class);
  /**
   * Does this Samza Job need hostAffinity when containers are allocated.
   */
  private final boolean hostAffinityEnabled;

  /**
   * State variables tracking containers allocated, freed, running, released.
   */
  private final SamzaApplicationState state;

  /**
   * Config for this Samza job
   */
  private final ClusterManagerConfig clusterManagerConfig;
  private final JobConfig jobConfig;

  /**
   * The Allocator matches requests to resources and executes processes.
   */
  private final AbstractContainerAllocator containerAllocator;
  private final Thread allocatorThread;

  /**
   * A standard interface to request resources.
   */
  private final ClusterResourceManager clusterResourceManager;

  /**
   * If there are too many failed container failures (configured by job.container.retry.count) for a
   * container, the job exits.
   */
  private volatile boolean tooManyFailedContainers = false;

  /**
   * Exception thrown in callbacks, such as {@code containerAllocator}
   */
  private volatile Throwable exceptionOccurred = null;

  /**
   * A map that keeps track of how many times each container failed. The key is the container ID, and the
   * value is the {@link ResourceFailure} object that has a count of failures.
   *
   */
  private final Map<String, ResourceFailure> containerFailures = new HashMap<>();

  /**
   * Metrics for {@link ContainerProcessManager}
   */
  private final ContainerProcessManagerMetrics metrics;

  //for testing
  ContainerProcessManager(Config config, SamzaApplicationState state, MetricsRegistryMap registry, AbstractContainerAllocator allocator, ClusterResourceManager manager) {
    this.state = state;
    this.clusterManagerConfig = new ClusterManagerConfig(config);
    this.jobConfig = new JobConfig(config);
    this.hostAffinityEnabled = clusterManagerConfig.getHostAffinityEnabled();
    this.clusterResourceManager = manager;
    this.metrics = new ContainerProcessManagerMetrics(config, state, registry);
    this.containerAllocator = allocator;
    this.allocatorThread = new Thread(this.containerAllocator, "Container Allocator Thread");
  }

  public ContainerProcessManager(Config config,
                                 SamzaApplicationState state,
                                 MetricsRegistryMap registry) {
    this.state = state;
    this.clusterManagerConfig = new ClusterManagerConfig(config);
    this.jobConfig = new JobConfig(config);

    this.hostAffinityEnabled = clusterManagerConfig.getHostAffinityEnabled();

    ResourceManagerFactory factory = getContainerProcessManagerFactory(clusterManagerConfig);
    this.clusterResourceManager = checkNotNull(factory.getClusterResourceManager(this, state));
    this.metrics = new ContainerProcessManagerMetrics(config, state, registry);

    if (this.hostAffinityEnabled) {
      this.containerAllocator = new HostAwareContainerAllocator(clusterResourceManager, clusterManagerConfig.getContainerRequestTimeout(), config, state);
    } else {
      this.containerAllocator = new ContainerAllocator(clusterResourceManager, config, state);
    }

    this.allocatorThread = new Thread(this.containerAllocator, "Container Allocator Thread");
    log.info("finished initialization of samza task manager");

  }

  //package private, used only in tests
  ContainerProcessManager(Config config,
                          SamzaApplicationState state,
                          MetricsRegistryMap registry,
                          ClusterResourceManager resourceManager) {
    JobModelManager jobModelManager = state.jobModelManager;
    this.state = state;
    this.clusterManagerConfig = new ClusterManagerConfig(config);
    this.jobConfig = new JobConfig(config);

    this.hostAffinityEnabled = clusterManagerConfig.getHostAffinityEnabled();

    this.clusterResourceManager = resourceManager;
    this.metrics = new ContainerProcessManagerMetrics(config, state, registry);


    if (this.hostAffinityEnabled) {
      this.containerAllocator = new HostAwareContainerAllocator(clusterResourceManager, clusterManagerConfig.getContainerRequestTimeout(), config, state);
    } else {
      this.containerAllocator = new ContainerAllocator(clusterResourceManager, config, state);
    }

    this.allocatorThread = new Thread(this.containerAllocator, "Container Allocator Thread");
    log.info("finished initialization of samza task manager");
  }

  public boolean shouldShutdown() {
    log.debug(" TaskManager state: Completed containers: {}, Configured containers: {}, Is there too many FailedContainers: {}, Is AllocatorThread alive: {} "
      , new Object[]{state.completedContainers.get(), state.containerCount, tooManyFailedContainers ? "yes" : "no", allocatorThread.isAlive() ? "yes" : "no"});

    if (exceptionOccurred != null) {
      log.error("Exception in ContainerProcessManager", exceptionOccurred);
      throw new SamzaException(exceptionOccurred);
    }
    return tooManyFailedContainers || state.completedContainers.get() == state.containerCount.get() || !allocatorThread.isAlive();
  }

  public void start() {
    metrics.start();

    log.info("Starting Container Process Manager");
    clusterResourceManager.start();

    log.info("Starting the Samza task manager");
    final int containerCount = jobConfig.getContainerCount();

    state.containerCount.set(containerCount);
    state.neededContainers.set(containerCount);

    // Request initial set of containers
    Map<String, String> containerToHostMapping = state.jobModelManager.jobModel().getAllContainerLocality();

    containerAllocator.requestResources(containerToHostMapping);

    // Start container allocator thread
    log.info("Starting the container allocator thread");
    allocatorThread.start();
  }

  public void stop() {
    log.info("Invoked stop of the Samza container process manager");

    // Shutdown allocator thread
    containerAllocator.stop();
    try {
      allocatorThread.join();
      log.info("Stopped container allocator");
    } catch (InterruptedException ie) {
      log.error("Allocator Thread join() threw an interrupted exception", ie);
      Thread.currentThread().interrupt();
    }

    if (metrics != null) {
      try {
        metrics.stop();
        log.info("Stopped metrics reporters");
      } catch (Throwable e) {
        log.error("Exception while stopping metrics {}", e);
      }
    }

    try {
      clusterResourceManager.stop(state.status);
      log.info("Stopped cluster resource manager");
    } catch (Throwable e) {
      log.error("Exception while stopping cluster resource manager {}", e);
    }

    log.info("Finished stop of Container process manager");

  }

  public void onResourceAllocated(SamzaResource container) {
    log.info("Container allocated from RM on " + container.getHost());
    containerAllocator.addResource(container);
  }

  /**
   * This methods handles the onResourceCompleted callback from the RM. Based on the ContainerExitStatus, it decides
   * whether a container that exited is marked as complete or failure.
   * @param containerStatus of the resource that completed
   */
  public void onResourceCompleted(SamzaResourceStatus containerStatus) {
    String containerIdStr = containerStatus.getResourceID();
    String containerId = null;
    for (Map.Entry<String, SamzaResource> entry: state.runningContainers.entrySet()) {
      if (entry.getValue().getResourceID().equals(containerStatus.getResourceID())) {
        log.info("Matching container ID found " + entry.getKey() + " " + entry.getValue());

        containerId = entry.getKey();
        break;
      }
    }
    if (containerId == null) {
      log.info("No matching container id found for " + containerStatus.toString());
      state.redundantNotifications.incrementAndGet();
      return;
    }
    state.runningContainers.remove(containerId);

    int exitStatus = containerStatus.getExitCode();
    switch (exitStatus) {
      case SamzaResourceStatus.SUCCESS:
        log.info("Container {} completed successfully.", containerIdStr);

        state.completedContainers.incrementAndGet();

        state.finishedContainers.incrementAndGet();
        containerFailures.remove(containerId);

        if (state.completedContainers.get() == state.containerCount.get()) {
          log.info("Setting job status to SUCCEEDED, since all containers have been marked as completed.");
          state.status = SamzaApplicationState.SamzaAppStatus.SUCCEEDED;
        }
        break;

      case SamzaResourceStatus.DISK_FAIL:
      case SamzaResourceStatus.ABORTED:
      case SamzaResourceStatus.PREEMPTED:
        log.info("Got an exit code of {}. This means that container {} was "
                  + "killed by YARN, either due to being released by the application "
                  + "master or being 'lost' due to node failures etc. or due to preemption by the RM",
                exitStatus,
                containerIdStr);

        state.releasedContainers.incrementAndGet();

        // If this container was assigned some partitions (a containerId), then
        // clean up, and request a new container for the tasks. This only
        // should happen if the container was 'lost' due to node failure, not
        // if the AM released the container.
        log.info("Released container {} was assigned task group ID {}. Requesting a new container for the task group.", containerIdStr, containerId);

        state.neededContainers.incrementAndGet();
        state.jobHealthy.set(false);

          // request a container on new host
        containerAllocator.requestResource(containerId, ResourceRequestState.ANY_HOST);
        break;

      default:
        // TODO: Handle failure more intelligently. Should track NodeFailures!
        log.info("Container failed for some reason. Let's start it again");
        log.info("Container " + containerIdStr + " failed with exit code . " + exitStatus + " - " + containerStatus.getDiagnostics() + " containerID is " + containerId);

        state.failedContainers.incrementAndGet();
        state.failedContainersStatus.put(containerIdStr, containerStatus);
        state.jobHealthy.set(false);

        state.neededContainers.incrementAndGet();
        // Find out previously running container location
        String lastSeenOn = state.jobModelManager.jobModel().getContainerToHostValue(containerId, SetContainerHostMapping.HOST_KEY);
        if (!hostAffinityEnabled || lastSeenOn == null) {
          lastSeenOn = ResourceRequestState.ANY_HOST;
        }
        log.info("Container was last seen on " + lastSeenOn);
        // A container failed for an unknown reason. Let's check to see if
        // we need to shutdown the whole app master if too many container
        // failures have happened. The rules for failing are that the
        // failure count for a task group id must be > the configured retry
        // count, and the last failure (the one prior to this one) must have
        // happened less than retry window ms ago. If retry count is set to
        // 0, the app master will fail on any container failure. If the
        // retry count is set to a number < 0, a container failure will
        // never trigger an app master failure.
        int retryCount = clusterManagerConfig.getContainerRetryCount();
        int retryWindowMs = clusterManagerConfig.getContainerRetryWindowMs();

        if (retryCount == 0) {
          log.error("Container ID {} ({}) failed, and retry count is set to 0, so shutting down the application master, and marking the job as failed.", containerId, containerIdStr);

          tooManyFailedContainers = true;
        } else if (retryCount > 0) {
          int currentFailCount;
          long lastFailureTime;
          if (containerFailures.containsKey(containerId)) {
            ResourceFailure failure = containerFailures.get(containerId);
            currentFailCount = failure.getCount() + 1;
            lastFailureTime = failure.getLastFailure();
          } else {
            currentFailCount = 1;
            lastFailureTime = 0L;
          }
          if (currentFailCount >= retryCount) {
            long lastFailureMsDiff = System.currentTimeMillis() - lastFailureTime;

            if (lastFailureMsDiff < retryWindowMs) {
              log.error("Container ID " + containerId + "(" + containerIdStr + ") has failed " + currentFailCount +
                      " times, with last failure " + lastFailureMsDiff + "ms ago. This is greater than retry count of " +
                      retryCount + " and window of " + retryWindowMs + "ms , so shutting down the application master, and marking the job as failed.");

              // We have too many failures, and we're within the window
              // boundary, so reset shut down the app master.
              tooManyFailedContainers = true;
              state.status = SamzaApplicationState.SamzaAppStatus.FAILED;
            } else {
              log.info("Resetting fail count for container ID {} back to 1, since last container failure ({}) for " +
                      "this container ID was outside the bounds of the retry window.", containerId, containerIdStr);

              // Reset counter back to 1, since the last failure for this
              // container happened outside the window boundary.
              containerFailures.put(containerId, new ResourceFailure(1, System.currentTimeMillis()));
            }
          } else {
            log.info("Current fail count for container ID {} is {}.", containerId, currentFailCount);
            containerFailures.put(containerId, new ResourceFailure(currentFailCount, System.currentTimeMillis()));
          }
        }

        if (!tooManyFailedContainers) {
          log.info("Requesting a new container ");
          // Request a new container
          containerAllocator.requestResource(containerId, lastSeenOn);
        }

    }
  }

  @Override
  public void onResourcesAvailable(List<SamzaResource> resources) {
    for (SamzaResource resource : resources) {
      onResourceAllocated(resource);
    }

  }

  @Override
  public void onResourcesCompleted(List<SamzaResourceStatus> resourceStatuses) {
    for (SamzaResourceStatus resourceStatus : resourceStatuses) {
      onResourceCompleted(resourceStatus);
    }
  }

  @Override
  public void onStreamProcessorLaunchSuccess(SamzaResource resource) {

    // 1. Obtain the Samza container Id for the pending container on this resource.
    String containerId = getPendingContainerId(resource.getResourceID());
    log.info("Successfully started container ID: {} on resource: {}", containerId, resource);

    // 2. Remove the container from the pending buffer and add it to the running buffer. Additionally, update the
    // job-health metric.
    if (containerId != null) {
      log.info("Moving containerID: {} on resource: {} from pending to running state", containerId, resource);
      state.pendingContainers.remove(containerId);
      state.runningContainers.put(containerId, resource);

      if (state.neededContainers.decrementAndGet() == 0) {
        state.jobHealthy.set(true);
      }
    } else {
      log.warn("SamzaResource {} was not in pending state. Got an invalid callback for a launch request that " +
          "was not issued", resource);
    }
  }

  @Override
  public void onStreamProcessorLaunchFailure(SamzaResource resource, Throwable t) {
    log.error("Got a launch failure for SamzaResource {} with exception {}", resource, t);
    // 1. Release resources for containers that failed back to YARN
    log.info("Releasing unstartable container {}", resource.getResourceID());
    clusterResourceManager.releaseResources(resource);

    // 2. Obtain the Samza container Id for the pending container on this resource.
    String containerId = getPendingContainerId(resource.getResourceID());
    log.info("Failed container ID: {} for resourceId: {}", containerId, resource.getResourceID());

    // 3. Re-request resources on ANY_HOST in case of launch failures on the preferred host.
    if (containerId != null) {
      log.info("Launch of container ID: {} failed on host: {}. Falling back to ANY_HOST", containerId, resource.getHost());
      containerAllocator.requestResource(containerId, ResourceRequestState.ANY_HOST);
    } else {
      log.warn("SamzaResource {} was not in pending state. Got an invalid callback for a launch request that was " +
          "not issued", resource);
    }
  }

  /**
   * An error in the callback terminates the JobCoordinator
   * @param e the underlying exception/error
   */
  @Override
  public void onError(Throwable e) {
    log.error("Exception occured in callbacks in the Container Manager : {}", e);
    exceptionOccurred = e;
  }

  /**
   * Returns an instantiated {@link ResourceManagerFactory} from a {@link ClusterManagerConfig}. The
   * {@link ResourceManagerFactory} is used to return an implementation of a {@link ClusterResourceManager}
   *
   * @param clusterManagerConfig, the cluster manager config to parse.
   *
   */
  private ResourceManagerFactory getContainerProcessManagerFactory(final ClusterManagerConfig clusterManagerConfig) {
    final String containerManagerFactoryClass = clusterManagerConfig.getContainerManagerClass();
    final ResourceManagerFactory factory;

    try {
      factory = Util.getObj(containerManagerFactoryClass, ResourceManagerFactory.class);
    } catch (Exception e) {
      log.error("Exception when creating ContainerManager", e);
      throw new SamzaException(e);
    }
    return factory;
  }

  /**
   * Obtains the ID of the Samza container pending launch on the provided resource.
   *
   * @param resourceId the Id of the resource
   * @return the Id of the Samza container on this resource
   */
  private String getPendingContainerId(String resourceId) {
    for (Map.Entry<String, SamzaResource> entry: state.pendingContainers.entrySet()) {
      if (entry.getValue().getResourceID().equals(resourceId)) {
        log.info("Matching container ID found " + entry.getKey() + " " + entry.getValue());
        return entry.getKey();
      }
    }
    return null;
  }


}