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
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.diagnostics.DiagnosticsManager;
import org.apache.samza.metrics.ContainerProcessManagerMetrics;
import org.apache.samza.metrics.JvmMetrics;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.reporter.MetricsSnapshotReporter;
import org.apache.samza.util.DiagnosticsUtil;
import org.apache.samza.util.MetricsReporterLoader;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import scala.Option;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * ContainerProcessManager is responsible for requesting containers, handling failures, and notifying the application master that the
 * job is done.
 *
 * The following threads are involved in the execution of the ContainerProcessManager :
 *  - The main thread (defined in SamzaAppMaster) that sends requests to the cluster manager.
 *  - The callback handler thread that receives the responses from cluster manager and handles:
 *      - Populating a buffer when a container is allocated by the cluster manager
 *        (allocatedContainers in {@link ResourceRequestState}
 *      - Identifying the cause of container failure and re-requesting containers from the cluster manager by adding request to the
 *        internal requestQueue in {@link ResourceRequestState}
 *  - The allocator thread that assigns the allocated containers to pending requests
 *    (See {@link org.apache.samza.clustermanager.ContainerAllocator} or {@link org.apache.samza.clustermanager.HostAwareContainerAllocator})
 *
 */
public class ContainerProcessManager implements ClusterResourceManager.Callback   {

  private static final Logger log = LoggerFactory.getLogger(ContainerProcessManager.class);

  /**
   * Metrics for the {@link ContainerProcessManager}
   */
  private final static String METRICS_SOURCE_NAME = "ApplicationMaster";
  private final static String EXEC_ENV_CONTAINER_ID_SYS_PROPERTY = "CONTAINER_ID";


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

  // The StandbyContainerManager manages standby-aware allocation and failover of containers
  private final Optional<StandbyContainerManager> standbyContainerManager;

  private final Option<DiagnosticsManager> diagnosticsManager;

  /**
   * A standard interface to request resources.
   */
  private final ClusterResourceManager clusterResourceManager;

  /**
   * If there are too many failed container failures (configured by job.container.retry.count) for a
   * processor, the job exits.
   */
  private volatile boolean tooManyFailedContainers = false;

  /**
   * Exception thrown in callbacks, such as {@code containerAllocator}
   */
  private volatile Throwable exceptionOccurred = null;

  /**
   * A map that keeps track of how many times each processor failed. The key is the processor ID, and the
   * value is the {@link ProcessorFailure} object that has a count of failures.
   */
  private final Map<String, ProcessorFailure> processorFailures = new HashMap<>();
  private ContainerProcessManagerMetrics containerProcessManagerMetrics;
  private JvmMetrics jvmMetrics;
  private Map<String, MetricsReporter> metricsReporters;

  public ContainerProcessManager(Config config, SamzaApplicationState state, MetricsRegistryMap registry,
      ClassLoader classLoader) {
    this.state = state;
    this.clusterManagerConfig = new ClusterManagerConfig(config);
    this.jobConfig = new JobConfig(config);

    this.hostAffinityEnabled = clusterManagerConfig.getHostAffinityEnabled();

    ResourceManagerFactory factory = getContainerProcessManagerFactory(clusterManagerConfig, classLoader);
    this.clusterResourceManager = checkNotNull(factory.getClusterResourceManager(this, state));

    // Initialize metrics
    this.containerProcessManagerMetrics = new ContainerProcessManagerMetrics(config, state, registry);
    this.jvmMetrics = new JvmMetrics(registry);
    this.metricsReporters =
        MetricsReporterLoader.getMetricsReporters(new MetricsConfig(config), METRICS_SOURCE_NAME, classLoader);

    // Creating diagnostics manager and reporter, and wiring it respectively
    String jobName = new JobConfig(config).getName().get();
    String jobId = new JobConfig(config).getJobId();
    Optional<String> execEnvContainerId = Optional.ofNullable(System.getenv(EXEC_ENV_CONTAINER_ID_SYS_PROPERTY));
    Optional<Pair<DiagnosticsManager, MetricsSnapshotReporter>> diagnosticsManagerReporterPair =
        DiagnosticsUtil.buildDiagnosticsManager(jobName, jobId, METRICS_SOURCE_NAME, execEnvContainerId, config);

    if (diagnosticsManagerReporterPair.isPresent()) {
      diagnosticsManager = Option.apply(diagnosticsManagerReporterPair.get().getKey());
      metricsReporters.put(MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS(), diagnosticsManagerReporterPair.get().getValue());
    } else {
      diagnosticsManager = Option.empty();
    }

    // Wire all metrics to all reporters
    this.metricsReporters.values().forEach(reporter -> reporter.register(METRICS_SOURCE_NAME, registry));

    // Enable standby container manager if required
    if (jobConfig.getStandbyTasksEnabled()) {
      this.standbyContainerManager = Optional.of(new StandbyContainerManager(state, clusterResourceManager));
    } else {
      this.standbyContainerManager = Optional.empty();
    }

    this.containerAllocator =
        buildContainerAllocator(this.hostAffinityEnabled, this.clusterResourceManager, this.clusterManagerConfig,
            config, this.standbyContainerManager, state, classLoader);
    this.allocatorThread = new Thread(this.containerAllocator, "Container Allocator Thread");
    log.info("Finished container process manager initialization.");
  }

  @VisibleForTesting
  ContainerProcessManager(Config config,
      SamzaApplicationState state,
      MetricsRegistryMap registry,
      ClusterResourceManager resourceManager,
      Optional<AbstractContainerAllocator> allocator,
      ClassLoader classLoader) {
    JobModelManager jobModelManager = state.jobModelManager;
    this.state = state;
    this.clusterManagerConfig = new ClusterManagerConfig(config);
    this.jobConfig = new JobConfig(config);

    this.hostAffinityEnabled = clusterManagerConfig.getHostAffinityEnabled();

    this.clusterResourceManager = resourceManager;
    this.standbyContainerManager = Optional.empty();
    this.diagnosticsManager = Option.empty();

    if (allocator.isPresent()) {
      this.containerAllocator = allocator.get();
    } else if (this.hostAffinityEnabled) {
      this.containerAllocator =
          new HostAwareContainerAllocator(clusterResourceManager, clusterManagerConfig.getContainerRequestTimeout(),
              config, this.standbyContainerManager, state, classLoader);
    } else {
      this.containerAllocator = new ContainerAllocator(clusterResourceManager, config, state, classLoader);
    }

    this.allocatorThread = new Thread(this.containerAllocator, "Container Allocator Thread");
    log.info("Finished container process manager initialization");
  }

  private static AbstractContainerAllocator buildContainerAllocator(boolean hostAffinityEnabled,
      ClusterResourceManager clusterResourceManager, ClusterManagerConfig clusterManagerConfig, Config config,
      Optional<StandbyContainerManager> standbyContainerManager, SamzaApplicationState state, ClassLoader classLoader) {
    if (hostAffinityEnabled) {
      return new HostAwareContainerAllocator(clusterResourceManager, clusterManagerConfig.getContainerRequestTimeout(),
          config, standbyContainerManager, state, classLoader);
    } else {
      return new ContainerAllocator(clusterResourceManager, config, state, classLoader);
    }
  }

  public boolean shouldShutdown() {
    log.debug("ContainerProcessManager state: Completed containers: {}, Configured containers: {}, Are there too many failed containers: {}, Is allocator thread alive: {}",
      state.completedProcessors.get(), state.processorCount, tooManyFailedContainers ? "yes" : "no", allocatorThread.isAlive() ? "yes" : "no");

    if (exceptionOccurred != null) {
      log.error("Exception in container process manager", exceptionOccurred);
      throw new SamzaException(exceptionOccurred);
    }
    return tooManyFailedContainers || state.completedProcessors.get() == state.processorCount.get() || !allocatorThread.isAlive();
  }

  public void start() {
    log.info("Starting the container process manager");

    if (jvmMetrics != null) {
      jvmMetrics.start();
    }

    if (this.metricsReporters != null) {
      this.metricsReporters.values().forEach(reporter -> reporter.start());
    }

    if (this.diagnosticsManager.isDefined()) {
      this.diagnosticsManager.get().start();
    }

    log.info("Starting the cluster resource manager");
    clusterResourceManager.start();

    state.processorCount.set(state.jobModelManager.jobModel().getContainers().size());
    state.neededProcessors.set(state.jobModelManager.jobModel().getContainers().size());

    // Request initial set of containers
    Map<String, String> processorToHostMapping = state.jobModelManager.jobModel().getAllContainerLocality();
    containerAllocator.requestResources(processorToHostMapping);

    // Start container allocator thread
    log.info("Starting the container allocator thread");
    allocatorThread.start();
    log.info("Starting the container process manager");
  }

  public void stop() {
    log.info("Stopping the container process manager");

    // Shutdown allocator thread
    containerAllocator.stop();
    try {
      allocatorThread.join();
      log.info("Stopped container allocator");
    } catch (InterruptedException ie) {
      log.error("Allocator thread join threw an interrupted exception", ie);
      Thread.currentThread().interrupt();
    }

    if (this.diagnosticsManager.isDefined()) {
      try {
        this.diagnosticsManager.get().stop();
      } catch (InterruptedException e) {
        log.error("InterruptedException while stopping diagnosticsManager", e);
      }
    }

    try {

      if (this.metricsReporters != null) {
        this.metricsReporters.values().forEach(reporter -> reporter.stop());
      }

      if (this.jvmMetrics != null) {
        jvmMetrics.stop();
      }

      log.info("Stopped containerProcessManagerMetrics reporters");
    } catch (Throwable e) {
      log.error("Exception while stopping containerProcessManagerMetrics", e);
    }

    try {
      clusterResourceManager.stop(state.status);
      log.info("Stopped the cluster resource manager");
    } catch (Throwable e) {
      log.error("Exception while stopping cluster resource manager", e);
    }

    log.info("Stopped the container process manager");
  }

  public void onResourceAllocated(SamzaResource resource) {
    log.info("Container ID: {} allocated from RM on host: {}", resource.getContainerId(), resource.getHost());
    containerAllocator.addResource(resource);
  }

  /**
   * This methods handles the onResourceCompleted callback from the RM. Based on the ContainerExitStatus, it decides
   * whether the container that exited is marked as complete or failure.
   * @param resourceStatus status of the resource that completed
   */
  public void onResourceCompleted(SamzaResourceStatus resourceStatus) {
    String containerId = resourceStatus.getContainerId();
    String processorId = null;
    for (Map.Entry<String, SamzaResource> entry: state.runningProcessors.entrySet()) {
      if (entry.getValue().getContainerId().equals(resourceStatus.getContainerId())) {
        log.info("Container ID: {} matched running Processor ID: {} on host: {}", containerId, entry.getKey(), entry.getValue().getHost());

        processorId = entry.getKey();
        break;
      }
    }
    if (processorId == null) {
      log.info("No running Processor ID found for Container ID: {} with Status: {}. Ignoring redundant notification.", containerId, resourceStatus.toString());
      state.redundantNotifications.incrementAndGet();
      return;
    }
    state.runningProcessors.remove(processorId);

    int exitStatus = resourceStatus.getExitCode();
    switch (exitStatus) {
      case SamzaResourceStatus.SUCCESS:
        log.info("Container ID: {} for Processor ID: {} completed successfully.", containerId, processorId);

        state.completedProcessors.incrementAndGet();

        state.finishedProcessors.incrementAndGet();
        processorFailures.remove(processorId);

        if (state.completedProcessors.get() == state.processorCount.get()) {
          log.info("Setting job status to SUCCEEDED since all containers have been marked as completed.");
          state.status = SamzaApplicationState.SamzaAppStatus.SUCCEEDED;
        }
        break;

      case SamzaResourceStatus.DISK_FAIL:
      case SamzaResourceStatus.ABORTED:
      case SamzaResourceStatus.PREEMPTED:
        log.info("Container ID: {} for Processor ID: {} was released with an exit code: {}. This means that " +
                "the container was killed by YARN, either due to being released by the application master " +
                "or being 'lost' due to node failures etc. or due to preemption by the RM." +
                "Requesting a new container for the processor.",
                containerId, processorId, exitStatus);

        state.releasedContainers.incrementAndGet();

        // If this container was assigned some partitions (a processorId), then
        // clean up, and request a new container for the processor. This only
        // should happen if the container was 'lost' due to node failure, not
        // if the AM released the container.
        state.neededProcessors.incrementAndGet();
        state.jobHealthy.set(false);

        // handle container stop due to node fail
        this.handleContainerStop(processorId, resourceStatus.getContainerId(), ResourceRequestState.ANY_HOST, exitStatus);
        break;

      default:
        log.info("Container ID: {} for Processor ID: {} failed with exit code: {}.", containerId, processorId, exitStatus);

        state.failedContainers.incrementAndGet();
        state.failedContainersStatus.put(containerId, resourceStatus);
        state.jobHealthy.set(false);

        state.neededProcessors.incrementAndGet();
        // Find out previously running container location
        String lastSeenOn = state.jobModelManager.jobModel().getContainerToHostValue(processorId, SetContainerHostMapping.HOST_KEY);
        if (!hostAffinityEnabled || lastSeenOn == null) {
          lastSeenOn = ResourceRequestState.ANY_HOST;
        }
        log.info("Container ID: {} for Processor ID: {} was last seen on host {}.", containerId, processorId, lastSeenOn);
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
          log.error("Processor ID: {} (current Container ID: {}) failed, and retry count is set to 0, " +
              "so shutting down the application master and marking the job as failed.", processorId, containerId);

          tooManyFailedContainers = true;
        } else if (retryCount > 0) {
          int currentFailCount;
          long lastFailureTime;
          if (processorFailures.containsKey(processorId)) {
            ProcessorFailure failure = processorFailures.get(processorId);
            currentFailCount = failure.getCount() + 1;
            lastFailureTime = failure.getLastFailure();
          } else {
            currentFailCount = 1;
            lastFailureTime = 0L;
          }
          if (currentFailCount >= retryCount) {
            long lastFailureMsDiff = System.currentTimeMillis() - lastFailureTime;

            if (lastFailureMsDiff < retryWindowMs) {
              log.error("Processor ID: {} (current Container ID: {}) has failed {} times, with last failure {} ms ago. " +
                  "This is greater than retry count of {} and window of {} ms, " +
                  "so shutting down the application master and marking the job as failed.",
                  processorId, containerId, currentFailCount, lastFailureMsDiff, retryCount, retryWindowMs);

              // We have too many failures, and we're within the window
              // boundary, so reset shut down the app master.
              tooManyFailedContainers = true;
              state.status = SamzaApplicationState.SamzaAppStatus.FAILED;
            } else {
              log.info("Resetting failure count for Processor ID: {} back to 1, since last failure " +
                  "(for Container ID: {}) was outside the bounds of the retry window.", processorId, containerId);

              // Reset counter back to 1, since the last failure for this
              // container happened outside the window boundary.
              processorFailures.put(processorId, new ProcessorFailure(1, System.currentTimeMillis()));
            }
          } else {
            log.info("Current failure count for Processor ID: {} is {}.", processorId, currentFailCount);
            processorFailures.put(processorId, new ProcessorFailure(currentFailCount, System.currentTimeMillis()));
          }
        }

        if (!tooManyFailedContainers) {
          handleContainerStop(processorId, resourceStatus.getContainerId(), lastSeenOn, exitStatus);
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
    String containerId = resource.getContainerId();
    String containerHost = resource.getHost();

    // 1. Obtain the processor ID for the pending container on this resource.
    String processorId = getPendingProcessorId(containerId);
    log.info("Successfully started Processor ID: {} on Container ID: {} on host: {}",
        processorId, containerId, containerHost);

    // 2. Remove the container from the pending buffer and add it to the running buffer. Additionally, update the
    // job-health metric.
    if (processorId != null) {
      log.info("Moving Processor ID: {} on Container ID: {} on host: {} from pending to running state.",
          processorId, containerId, containerHost);
      state.pendingProcessors.remove(processorId);
      state.runningProcessors.put(processorId, resource);

      if (state.neededProcessors.decrementAndGet() == 0) {
        state.jobHealthy.set(true);
      }
    } else {
      log.warn("Did not find a pending Processor ID for Container ID: {} on host: {}. " +
          "Ignoring invalid/redundant notification.", containerId, containerHost);
    }
  }

  @Override
  public void onStreamProcessorLaunchFailure(SamzaResource resource, Throwable t) {
    String containerId = resource.getContainerId();
    String containerHost = resource.getHost();

    // 1. Obtain the pending Samza processor ID for this container ID.
    String processorId = getPendingProcessorId(containerId);
    log.error("Launch failed for pending Processor ID: {} on Container ID: {} on host: {} with exception: {}",
        processorId, containerId, containerHost, t);

    // 2. Release resources for containers that failed back to YARN
    log.info("Releasing un-startable Container ID: {} for pending Processor ID: {}", containerId, processorId);
    clusterResourceManager.releaseResources(resource);

    // 3. Re-request resources on ANY_HOST in case of launch failures on the preferred host, if standby are not enabled
    // otherwise calling standbyContainerManager
    if (processorId != null && standbyContainerManager.isPresent()) {
      this.standbyContainerManager.get().handleContainerLaunchFail(processorId, containerId, containerAllocator);
    } else if (processorId != null) {
      log.info("Falling back to ANY_HOST for Processor ID: {} since launch failed for Container ID: {} on host: {}",
          processorId, containerId, containerHost);
      containerAllocator.requestResource(processorId, ResourceRequestState.ANY_HOST);
    } else {
      log.warn("Did not find a pending Processor ID for Container ID: {} on host: {}. " +
          "Ignoring invalid/redundant notification.", containerId, containerHost);
    }
  }

  /**
   * An error in the callback terminates the JobCoordinator
   * @param e the underlying exception/error
   */
  @Override
  public void onError(Throwable e) {
    log.error("Exception occurred in callbacks in the Cluster Resource Manager", e);
    exceptionOccurred = e;
  }

  /**
   * Returns an instantiated {@link ResourceManagerFactory} from a {@link ClusterManagerConfig}. The
   * {@link ResourceManagerFactory} is used to return an implementation of a {@link ClusterResourceManager}
   *
   * @param clusterManagerConfig, the cluster manager config to parse.
   *
   */
  private ResourceManagerFactory getContainerProcessManagerFactory(final ClusterManagerConfig clusterManagerConfig,
      ClassLoader classLoader) {
    final String containerManagerFactoryClass = clusterManagerConfig.getContainerManagerClass();
    final ResourceManagerFactory factory;

    try {
      factory = ReflectionUtil.getObj(classLoader, containerManagerFactoryClass, ResourceManagerFactory.class);
    } catch (Exception e) {
      log.error("Error creating the cluster resource manager.", e);
      throw new SamzaException(e);
    }
    return factory;
  }

  /**
   * Obtains the ID of the Samza processor pending launch on the provided resource (container).
   *
   * @param resourceId the ID of the resource (container)
   * @return the ID of the Samza processor on this resource
   */
  private String getPendingProcessorId(String resourceId) {
    for (Map.Entry<String, SamzaResource> entry: state.pendingProcessors.entrySet()) {
      if (entry.getValue().getContainerId().equals(resourceId)) {
        log.info("Container ID: {} matched pending Processor ID: {} on host: {}", resourceId, entry.getKey(), entry.getValue().getHost());
        return entry.getKey();
      }
    }
    return null;
  }

  private void handleContainerStop(String processorId, String resourceID, String preferredHost, int exitStatus) {
    if (standbyContainerManager.isPresent()) {
      standbyContainerManager.get().handleContainerStop(processorId, resourceID, preferredHost, exitStatus, containerAllocator);
    } else {
      // If StandbyTasks are not enabled, we simply make a request for the preferredHost
      containerAllocator.requestResource(processorId, preferredHost);
    }
  }
}
