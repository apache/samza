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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementMetadataStore;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.apache.samza.diagnostics.DiagnosticsManager;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.LocalityModel;
import org.apache.samza.metrics.ContainerProcessManagerMetrics;
import org.apache.samza.metrics.JvmMetrics;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.util.DiagnosticsUtil;
import org.apache.samza.util.MetricsReporterLoader;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *    (See {@link org.apache.samza.clustermanager.ContainerAllocator} or {@link org.apache.samza.clustermanager.ContainerAllocator})
 *
 */
public class ContainerProcessManager implements ClusterResourceManager.Callback   {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerProcessManager.class);

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
  private final ContainerAllocator containerAllocator;
  private final Thread allocatorThread;

  // The ContainerManager manages control actions for both active & standby containers
  private final ContainerManager containerManager;

  private final Optional<DiagnosticsManager> diagnosticsManager;

  private final LocalityManager localityManager;

  /**
   * A standard interface to request resources.
   */
  private final ClusterResourceManager clusterResourceManager;

  /**
   * If there are more than job.container.retry.count failures of a container within a job.container.retry.window period,
   * then the ContainerProcessManager will indicate to the ClusterBasedJobCoordinator that the job should shutdown.
   */
  private volatile boolean jobFailureCriteriaMet = false;

  /**
   * Exception thrown in callbacks, such as {@code containerAllocator}
   */
  private volatile Throwable exceptionOccurred = null;

  /**
   * A map that keeps track of how many times each processor failed. The key is the processor ID, and the
   * value is the {@link ProcessorFailure} object that has a count of failures.
   */
  private final Map<String, ProcessorFailure> processorFailures = new HashMap<>();

  private final boolean restartContainers;

  private ContainerProcessManagerMetrics containerProcessManagerMetrics;
  private JvmMetrics jvmMetrics;
  private Map<String, MetricsReporter> metricsReporters;

  public ContainerProcessManager(Config config, SamzaApplicationState state, MetricsRegistryMap registry,
      ContainerPlacementMetadataStore metadataStore, LocalityManager localityManager, boolean restartContainers) {
    Preconditions.checkNotNull(localityManager, "Locality manager cannot be null");
    this.state = state;
    this.clusterManagerConfig = new ClusterManagerConfig(config);
    this.jobConfig = new JobConfig(config);

    this.hostAffinityEnabled = clusterManagerConfig.getHostAffinityEnabled();

    ResourceManagerFactory factory = getContainerProcessManagerFactory(clusterManagerConfig);
    this.clusterResourceManager = checkNotNull(factory.getClusterResourceManager(this, state));

    FaultDomainManagerFactory faultDomainManagerFactory = getFaultDomainManagerFactory(clusterManagerConfig);
    FaultDomainManager faultDomainManager = checkNotNull(faultDomainManagerFactory.getFaultDomainManager(config, registry));

    // Initialize metrics
    this.containerProcessManagerMetrics = new ContainerProcessManagerMetrics(config, state, registry);
    this.jvmMetrics = new JvmMetrics(registry);
    this.metricsReporters = MetricsReporterLoader.getMetricsReporters(new MetricsConfig(config), METRICS_SOURCE_NAME);

    // Creating diagnostics manager and reporter, and wiring it respectively
    String jobName = new JobConfig(config).getName().get();
    String jobId = new JobConfig(config).getJobId();
    Optional<String> execEnvContainerId = Optional.ofNullable(System.getenv(EXEC_ENV_CONTAINER_ID_SYS_PROPERTY));
    this.diagnosticsManager =
        DiagnosticsUtil.buildDiagnosticsManager(jobName, jobId, state.jobModelManager.jobModel(), METRICS_SOURCE_NAME,
            execEnvContainerId, config);

    this.localityManager = localityManager;
    // Wire all metrics to all reporters
    this.metricsReporters.values().forEach(reporter -> reporter.register(METRICS_SOURCE_NAME, registry));

    this.containerManager = new ContainerManager(metadataStore, state, clusterResourceManager,
            hostAffinityEnabled, jobConfig.getStandbyTasksEnabled(), localityManager, faultDomainManager, config);

    this.containerAllocator = new ContainerAllocator(this.clusterResourceManager, config, state, hostAffinityEnabled, this.containerManager);
    this.allocatorThread = new Thread(this.containerAllocator, "Container Allocator Thread");
    this.restartContainers = restartContainers;
    LOG.info("Finished container process manager initialization.");
  }

  @VisibleForTesting
  ContainerProcessManager(ClusterManagerConfig clusterManagerConfig,
      SamzaApplicationState state,
      MetricsRegistryMap registry,
      ClusterResourceManager resourceManager,
      Optional<ContainerAllocator> allocator,
      ContainerManager containerManager,
      LocalityManager localityManager,
      boolean restartContainers) {
    this.state = state;
    this.clusterManagerConfig = clusterManagerConfig;
    this.jobConfig = new JobConfig(clusterManagerConfig);

    this.hostAffinityEnabled = clusterManagerConfig.getHostAffinityEnabled();

    this.clusterResourceManager = resourceManager;
    this.containerManager = containerManager;
    this.diagnosticsManager = Optional.empty();
    this.localityManager = localityManager;
    this.containerAllocator = allocator.orElseGet(
      () -> new ContainerAllocator(this.clusterResourceManager, clusterManagerConfig, state,
          hostAffinityEnabled, this.containerManager));
    this.allocatorThread = new Thread(this.containerAllocator, "Container Allocator Thread");
    this.restartContainers = restartContainers;
    LOG.info("Finished container process manager initialization");
  }

  public boolean shouldShutdown() {
    LOG.debug("ContainerProcessManager state: Completed containers: {}, Configured containers: {}, Are there too many failed containers: {}, Is allocator thread alive: {}",
      state.completedProcessors.get(), state.processorCount, jobFailureCriteriaMet ? "yes" : "no", allocatorThread.isAlive() ? "yes" : "no");

    if (exceptionOccurred != null) {
      LOG.error("Exception in container process manager", exceptionOccurred);
      throw new SamzaException(exceptionOccurred);
    }
    return jobFailureCriteriaMet || state.completedProcessors.get() == state.processorCount.get() || !allocatorThread.isAlive();
  }

  public void start() {
    LOG.info("Starting the container process manager");

    int containerRetryCount = clusterManagerConfig.getContainerRetryCount();
    if (containerRetryCount > -1) {
      LOG.info("Max retries on restarting failed containers: {}", containerRetryCount);
    } else {
      LOG.info("Infinite retries on restarting failed containers");
    }

    if (jvmMetrics != null) {
      jvmMetrics.start();
    }

    if (metricsReporters != null) {
      metricsReporters.values().forEach(reporter -> reporter.start());
    }

    if (diagnosticsManager.isPresent()) {
      diagnosticsManager.get().start();
    }

    // In AM-HA, clusterResourceManager receives already running containers
    // and invokes onStreamProcessorLaunchSuccess which inturn updates state
    // hence state has to be set prior to starting clusterResourceManager.
    state.processorCount.set(state.jobModelManager.jobModel().getContainers().size());
    state.neededProcessors.set(state.jobModelManager.jobModel().getContainers().size());

    LOG.info("Starting the cluster resource manager");
    clusterResourceManager.start();

    // Request initial set of containers
    LocalityModel localityModel = localityManager.readLocality();
    Map<String, String> processorToHost = new HashMap<>();
    state.jobModelManager.jobModel().getContainers().keySet().forEach((processorId) -> {
      String host = Optional.ofNullable(localityModel.getProcessorLocality(processorId))
          .map(ProcessorLocality::host)
          .filter(StringUtils::isNotBlank)
          .orElse(null);
      processorToHost.put(processorId, host);
    });
    if (jobConfig.getApplicationMasterHighAvailabilityEnabled()) {
      // don't request resource for container that is already running
      state.runningProcessors.forEach((processorId, samzaResource) -> {
        LOG.info("Not requesting container for processorId: {} since its already running as containerId: {}",
            processorId, samzaResource.getContainerId());
        processorToHost.remove(processorId);
        if (restartContainers) {
          clusterResourceManager.stopStreamProcessor(samzaResource);
        }
      });
    }
    containerAllocator.requestResources(processorToHost);

    // Start container allocator thread
    LOG.info("Starting the container allocator thread");
    allocatorThread.start();
    LOG.info("Starting the container process manager");
  }

  public void stop() {
    LOG.info("Stopping the container process manager");

    // Shutdown allocator thread
    containerAllocator.stop();
    try {
      allocatorThread.join();
      LOG.info("Stopped container allocator");
    } catch (InterruptedException ie) {
      LOG.error("Allocator thread join threw an interrupted exception", ie);
      Thread.currentThread().interrupt();
    }

    if (diagnosticsManager.isPresent()) {
      try {
        diagnosticsManager.get().stop();
      } catch (InterruptedException e) {
        LOG.error("InterruptedException while stopping diagnosticsManager", e);
      }
    }

    try {

      if (metricsReporters != null) {
        metricsReporters.values().forEach(reporter -> reporter.stop());
      }

      if (jvmMetrics != null) {
        jvmMetrics.stop();
      }

      LOG.info("Stopped containerProcessManagerMetrics reporters");
    } catch (Throwable e) {
      LOG.error("Exception while stopping containerProcessManagerMetrics", e);
    }

    try {
      clusterResourceManager.stop(state.status);
      LOG.info("Stopped the cluster resource manager");
    } catch (Throwable e) {
      LOG.error("Exception while stopping cluster resource manager", e);
    }

    LOG.info("Stopped the container process manager");
  }

  public void onResourceAllocated(SamzaResource resource) {
    LOG.info("Container ID: {} allocated from RM on host: {}", resource.getContainerId(), resource.getHost());
    containerAllocator.addResource(resource);
  }

  /**
   * This methods handles the onResourceCompleted callback from the RM. Based on the ContainerExitStatus, it decides
   * whether the container that exited is marked as complete or failure.
   * @param resourceStatus status of the resource that completed
   */
  public void onResourceCompleted(SamzaResourceStatus resourceStatus) {
    String containerId = resourceStatus.getContainerId();
    Pair<String, String> runningProcessorIdHostname = getRunningProcessor(containerId);
    String processorId = runningProcessorIdHostname.getKey();
    String hostName = runningProcessorIdHostname.getValue();

    if (processorId == null) {
      LOG.info("No running Processor ID found for Container ID: {} with Status: {}. Ignoring redundant notification.", containerId, resourceStatus.toString());
      state.redundantNotifications.incrementAndGet();

      if (resourceStatus.getExitCode() != SamzaResourceStatus.SUCCESS) {
        // the requested container failed before assigning the request to it.
        // Remove from the buffer if it is there
        containerAllocator.releaseResource(containerId);
      }
      return;
    }
    state.runningProcessors.remove(processorId);
    int exitStatus = resourceStatus.getExitCode();
    switch (exitStatus) {
      case SamzaResourceStatus.SUCCESS:
        LOG.info("Container ID: {} for Processor ID: {} completed successfully.", containerId, processorId);

        state.completedProcessors.incrementAndGet();

        state.finishedProcessors.incrementAndGet();
        processorFailures.remove(processorId);

        if (state.completedProcessors.get() == state.processorCount.get()) {
          LOG.info("Setting job status to SUCCEEDED since all containers have been marked as completed.");
          state.status = SamzaApplicationState.SamzaAppStatus.SUCCEEDED;
        }
        break;

      case SamzaResourceStatus.DISK_FAIL:
      case SamzaResourceStatus.ABORTED:
      case SamzaResourceStatus.PREEMPTED:
        LOG.info("Container ID: {} for Processor ID: {} was released with an exit code: {}. This means that " +
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
        handleContainerStop(processorId, resourceStatus.getContainerId(), ResourceRequestState.ANY_HOST, exitStatus, Duration.ZERO);
        break;

      default:
        onResourceCompletedWithUnknownStatus(resourceStatus, containerId, processorId, exitStatus);
    }

    if (diagnosticsManager.isPresent()) {
      diagnosticsManager.get().addProcessorStopEvent(processorId, resourceStatus.getContainerId(), hostName, exitStatus);
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
    LOG.info("Successfully started Processor ID: {} on Container ID: {} on host: {}",
        processorId, containerId, containerHost);

    // 2. Remove the container from the pending buffer and add it to the running buffer. Additionally, update the
    // job-health metric.
    if (processorId != null) {
      LOG.info("Moving Processor ID: {} on Container ID: {} on host: {} from pending to running state.",
          processorId, containerId, containerHost);
      state.pendingProcessors.remove(processorId);
      state.runningProcessors.put(processorId, resource);
      if (state.neededProcessors.decrementAndGet() == 0) {
        state.jobHealthy.set(true);
      }
      containerManager.handleContainerLaunchSuccess(processorId, containerHost);
    } else {
      LOG.warn("Did not find a pending Processor ID for Container ID: {} on host: {}. " +
          "Ignoring invalid/redundant notification.", containerId, containerHost);
    }
  }

  @Override
  public void onStreamProcessorLaunchFailure(SamzaResource resource, Throwable t) {
    String containerId = resource.getContainerId();
    String containerHost = resource.getHost();

    // 1. Obtain the pending Samza processor ID for this container ID.
    String processorId = getPendingProcessorId(containerId);
    LOG.error("Launch failed for pending Processor ID: {} on Container ID: {} on host: {} with exception: {}",
        processorId, containerId, containerHost, t);

    // 2. Release resources for containers that failed back to YARN
    LOG.info("Releasing un-startable Container ID: {} for pending Processor ID: {}", containerId, processorId);
    clusterResourceManager.releaseResources(resource);

    // 3. Re-request resources on ANY_HOST in case of launch failures on the preferred host, if standby are not enabled
    // otherwise calling standbyContainerManager
    containerManager.handleContainerLaunchFail(processorId, containerId, containerHost, containerAllocator);
  }

  @Override
  public void onStreamProcessorStopFailure(SamzaResource resource, Throwable t) {
    String containerId = resource.getContainerId();
    String containerHost = resource.getHost();
    String processorId = getRunningProcessor(containerId).getKey();
    LOG.warn("Stop failed for running Processor ID: {} on Container ID: {} on host: {} with exception: {}",
        processorId, containerId, containerHost, t);

    // Notify container-manager of the failed container-stop request
    containerManager.handleContainerStopFail(processorId, containerId, containerHost, containerAllocator);
  }

  /**
   * An error in the callback terminates the JobCoordinator
   * @param e the underlying exception/error
   */
  @Override
  public void onError(Throwable e) {
    LOG.error("Exception occurred in callbacks in the Cluster Resource Manager", e);
    exceptionOccurred = e;
  }

  @VisibleForTesting
  boolean getJobFailureCriteriaMet() {
    return jobFailureCriteriaMet;
  }

  @VisibleForTesting
  Map<String, ProcessorFailure> getProcessorFailures() {
    return processorFailures;
  }

  /**
   * Called within {@link #onResourceCompleted(SamzaResourceStatus)} for unknown exit statuses. These exit statuses
   * correspond to container completion other than container run-to-completion, abort or preemption, or disk failure
   * (e.g., detected by YARN's NM healthchecks).
   * @param resourceStatus reported resource status.
   * @param containerId container ID
   * @param processorId processor ID (aka. logical container ID)
   * @param exitStatus exit status from the {@link #onResourceCompleted(SamzaResourceStatus)} callback.
   */
  @VisibleForTesting
  void onResourceCompletedWithUnknownStatus(SamzaResourceStatus resourceStatus, String containerId, String processorId,
      int exitStatus) {
    LOG.info("Container ID: {} for Processor ID: {} failed with exit code: {}.", containerId, processorId, exitStatus);
    Instant now = Instant.now();
    state.failedContainers.incrementAndGet();
    state.jobHealthy.set(false);

    state.neededProcessors.incrementAndGet();
    // Find out previously running container location
    String lastSeenOn = Optional.ofNullable(localityManager.readLocality().getProcessorLocality(processorId))
        .map(ProcessorLocality::host)
        .orElse(null);
    if (!hostAffinityEnabled || StringUtils.isBlank(lastSeenOn)) {
      lastSeenOn = ResourceRequestState.ANY_HOST;
    }
    LOG.info("Container ID: {} for Processor ID: {} was last seen on host {}.", containerId, processorId, lastSeenOn);
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
    int currentFailCount;
    boolean retryContainerRequest = true;

    if (retryCount == 0) {
      // Failure criteria met only if failed containers can fail the job.
      jobFailureCriteriaMet = clusterManagerConfig.shouldFailJobAfterContainerRetries();
      if (jobFailureCriteriaMet) {
        LOG.error("Processor ID: {} (current Container ID: {}) failed, and retry count is set to 0, " +
            "so shutting down the application master and marking the job as failed.", processorId, containerId);
      } else {
        LOG.error("Processor ID: {} (current Container ID: {}) failed, and retry count is set to 0, " +
            "but the job will continue to run with the failed container.", processorId, containerId);
        state.failedProcessors.put(processorId, resourceStatus);
      }
      retryContainerRequest = false;
    } else if (retryCount > 0) {
      long durationSinceLastRetryMs;
      if (processorFailures.containsKey(processorId)) {
        ProcessorFailure failure = processorFailures.get(processorId);
        currentFailCount = failure.getCount() + 1;
        Duration lastRetryDelay = getRetryDelay(processorId);
        Instant retryAttemptedAt = failure.getLastFailure().plus(lastRetryDelay);
        durationSinceLastRetryMs = now.toEpochMilli() - retryAttemptedAt.toEpochMilli();
        if (durationSinceLastRetryMs < 0) {
          // This should never happen without changes to the system clock or time travel. Log a warning just in case.
          LOG.warn("Last failure at: {} with a retry attempted at: {} which is supposed to be before current time of: {}",
              failure.getLastFailure(), retryAttemptedAt, now);
        }
      } else {
        currentFailCount = 1;
        durationSinceLastRetryMs = 0;
      }

      if (durationSinceLastRetryMs >= retryWindowMs) {
        LOG.info("Resetting failure count for Processor ID: {} back to 1, since last failure " +
            "(for Container ID: {}) was outside the bounds of the retry window.", processorId, containerId);

        // Reset counter back to 1, since the last failure for this
        // container happened outside the window boundary.
        currentFailCount = 1;
      }

      // if fail count is (1 initial failure + max retries) then fail job.
      if (currentFailCount > retryCount) {
        LOG.error("Processor ID: {} (current Container ID: {}) has failed {} times, with last failure {} ms ago. " +
                "This is greater than retry count of {} and window of {} ms, ",
            processorId, containerId, currentFailCount, durationSinceLastRetryMs, retryCount, retryWindowMs);

        // We have too many failures, and we're within the window
        // boundary, so reset shut down the app master.
        retryContainerRequest = false;
        if (clusterManagerConfig.shouldFailJobAfterContainerRetries()) {
          jobFailureCriteriaMet = true;
          LOG.error("Shutting down the application master and marking the job as failed after max retry attempts.");
          state.status = SamzaApplicationState.SamzaAppStatus.FAILED;
        } else {
          LOG.warn("Processor ID: {} with Container ID: {} failed after all retry attempts. Job will continue to run without this container.",
              processorId, containerId);
          state.failedProcessors.put(processorId, resourceStatus);
        }
      } else {
        LOG.info("Current failure count for Processor ID: {} is {}.", processorId, currentFailCount);
        Duration retryDelay = Duration.ZERO;
        if (!ResourceRequestState.ANY_HOST.equals(lastSeenOn) && currentFailCount == retryCount) {
          // Add the preferred host last retry delay on the last retry
          retryDelay = Duration.ofMillis(clusterManagerConfig.getContainerPreferredHostLastRetryDelayMs());
        }
        processorFailures.put(processorId, new ProcessorFailure(currentFailCount, now, retryDelay));
        retryContainerRequest = true;
      }
    }

    if (retryContainerRequest) {
      Duration retryDelay = getRetryDelay(processorId);
      if (!retryDelay.isZero()) {
        LOG.info("Adding a delay of: {} seconds on the last container retry request for preferred host: {}",
            retryDelay.getSeconds(), lastSeenOn);
      }
      handleContainerStop(processorId, resourceStatus.getContainerId(), lastSeenOn, exitStatus, retryDelay);
    }
  }

  /**
   * Registers a ContainerPlacement action, this method is invoked by ContainerPlacementRequestAllocator. {@link ContainerProcessManager}
   * needs to intercept container placement actions between ContainerPlacementRequestAllocator and {@link ContainerManager} to avoid
   * cyclic dependency between {@link ContainerManager} and {@link ContainerAllocator} on each other
   *
   * @param requestMessage request containing details of the desited container placement action
   */
  public void registerContainerPlacementAction(ContainerPlacementRequestMessage requestMessage) {
    containerManager.registerContainerPlacementAction(requestMessage, containerAllocator);
  }

  private Duration getRetryDelay(String processorId) {
    return processorFailures.containsKey(processorId)
        ? processorFailures.get(processorId).getLastRetryDelay()
        : Duration.ZERO;
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
      factory = ReflectionUtil.getObj(containerManagerFactoryClass, ResourceManagerFactory.class);
    } catch (Exception e) {
      LOG.error("Error creating the cluster resource manager.", e);
      throw new SamzaException(e);
    }
    return factory;
  }

  /**
   * Returns an instantiated {@link FaultDomainManagerFactory} from a {@link ClusterManagerConfig}. The
   * {@link FaultDomainManagerFactory} is used to return an implementation of a {@link FaultDomainManager}
   *
   * @param clusterManagerConfig, the cluster manager config to parse.
   *
   */
  private FaultDomainManagerFactory getFaultDomainManagerFactory(final ClusterManagerConfig clusterManagerConfig) {
    final String faultDomainManagerFactoryClass = clusterManagerConfig.getFaultDomainManagerClass();
    final FaultDomainManagerFactory factory;

    try {
      factory = ReflectionUtil.getObj(faultDomainManagerFactoryClass, FaultDomainManagerFactory.class);
    } catch (Exception e) {
      LOG.error("Error creating the fault domain manager.", e);
      throw new SamzaException(e);
    }
    return factory;
  }

  /**
   * Obtains the ID of the Samza processor pending launch on the provided resource (container).
   *
   * ContainerProcessManager [INFO] Container ID: container_e66_1569376389369_0221_01_000049 matched pending Processor ID: 0 on host: ltx1-app0772.stg.linkedin.com
   *
   * @param containerId last known id of the container deployed
   * @return the logical processorId of the processor (e.g., 0, 1, 2 ...)
   */
  private String getPendingProcessorId(String containerId) {
    for (Map.Entry<String, SamzaResource> entry: state.pendingProcessors.entrySet()) {
      if (entry.getValue().getContainerId().equals(containerId)) {
        LOG.info("Container ID: {} matched pending Processor ID: {} on host: {}", containerId, entry.getKey(), entry.getValue().getHost());
        return entry.getKey();
      }
    }
    return null;
  }

  private Pair<String, String> getRunningProcessor(String containerId) {
    for (Map.Entry<String, SamzaResource> entry: state.runningProcessors.entrySet()) {
      if (entry.getValue().getContainerId().equals(containerId)) {
        LOG.info("Container ID: {} matched running Processor ID: {} on host: {}", containerId, entry.getKey(), entry.getValue().getHost());

        String processorId = entry.getKey();
        String hostName = entry.getValue().getHost();
        return new ImmutablePair<>(processorId, hostName);
      }
    }

    return new ImmutablePair<>(null, null);
  }

  /**
   * Request {@link ContainerManager#handleContainerStop} to determine next step of actions for the stopped container
   */
  private void handleContainerStop(String processorId, String containerId, String preferredHost, int exitStatus, Duration preferredHostRetryDelay) {
    containerManager.handleContainerStop(processorId, containerId, preferredHost, exitStatus, preferredHostRetryDelay, containerAllocator);
  }
}