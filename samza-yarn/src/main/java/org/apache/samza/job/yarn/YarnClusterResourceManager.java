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

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.*;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.*;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.clustermanager.ProcessorLaunchException;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.YarnConfig;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.util.Util;
import org.apache.samza.util.hadoop.HttpFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * An {@link YarnClusterResourceManager} implements a ClusterResourceManager using Yarn as the underlying
 * resource manager. This class is as an adaptor between Yarn and translates Yarn callbacks into
 * Samza specific callback methods as specified in Callback.
 *
 * Thread-safety:
 * 1.Start and stop methods should  NOT be called from multiple threads.
 * 2.ALL callbacks from the YarnContainerManager are invoked from a single Callback thread of the AMRMClient.
 * 3.Stop should not be called more than once.
 *
 */

public class YarnClusterResourceManager extends ClusterResourceManager implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

  private static final int PREFERRED_HOST_PRIORITY = 0;
  private static final int ANY_HOST_PRIORITY = 1;

  private static final String INVALID_PROCESSOR_ID = "-1";

  /**
   * The AMClient instance to request resources from yarn.
   */
  private final AMRMClientAsync<AMRMClient.ContainerRequest> amClient;

  /**
   * Configuration and state specific to Yarn.
   */
  private final YarnConfiguration yarnConfiguration;
  private final YarnAppState state;

  /**
   * SamzaYarnAppMasterLifecycle is responsible for registering, unregistering the AM client.
   */
  private final SamzaYarnAppMasterLifecycle lifecycle;

  /**
   * SamzaAppMasterService is responsible for hosting an AM web UI. This picks up data from both
   * SamzaAppState and YarnAppState.
   */
  private final SamzaYarnAppMasterService service;

  private final YarnConfig yarnConfig;

  /**
   * State variables to map Yarn specific callbacks into Samza specific callbacks.
   */
  private final ConcurrentHashMap<SamzaResource, Container> allocatedResources = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<SamzaResourceRequest, AMRMClient.ContainerRequest> requestsMap = new ConcurrentHashMap<>();

  private final SamzaAppMasterMetrics metrics;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Object lock = new Object();
  private final NMClientAsync nmClientAsync;

  private static final Logger log = LoggerFactory.getLogger(YarnClusterResourceManager.class);
  private final Config config;

  YarnClusterResourceManager(AMRMClientAsync amClientAsync, NMClientAsync nmClientAsync, Callback callback,
      YarnAppState yarnAppState, SamzaYarnAppMasterLifecycle lifecycle, SamzaYarnAppMasterService service,
      SamzaAppMasterMetrics metrics, YarnConfiguration yarnConfiguration, Config config) {
    super(callback);
    this.yarnConfiguration  = yarnConfiguration;
    this.metrics = metrics;
    this.yarnConfig = new YarnConfig(config);
    this.config = config;
    this.amClient = amClientAsync;
    this.state = yarnAppState;
    this.lifecycle = lifecycle;
    this.service = service;
    this.nmClientAsync = nmClientAsync;
  }

  /**
   * Creates an YarnClusterResourceManager from config, a jobModelReader and a callback.
   * @param config to instantiate the cluster manager with
   * @param jobModelManager the jobModel manager to get the job model (mostly for the UI)
   * @param callback the callback to receive events from Yarn.
   * @param samzaAppState samza app state for display in the UI
   */
  public YarnClusterResourceManager(Config config, JobModelManager jobModelManager,
      ClusterResourceManager.Callback callback, SamzaApplicationState samzaAppState) {
    super(callback);
    yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.http.impl", HttpFileSystem.class.getName());

    // Use the Samza job config "fs.<scheme>.impl" and "fs.<scheme>.impl.*" for YarnConfiguration
    FileSystemImplConfig fsImplConfig = new FileSystemImplConfig(config);
    fsImplConfig.getSchemes().forEach(
      scheme -> {
        fsImplConfig.getSchemeConfig(scheme).forEach(
          (confKey, confValue) -> yarnConfiguration.set(confKey, confValue)
        );
      }
    );

    MetricsRegistryMap registry = new MetricsRegistryMap();
    metrics = new SamzaAppMasterMetrics(config, samzaAppState, registry);

    // parse configs from the Yarn environment
    String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString());
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    String nodeHostString = System.getenv(ApplicationConstants.Environment.NM_HOST.toString());
    String nodePortString = System.getenv(ApplicationConstants.Environment.NM_PORT.toString());
    String nodeHttpPortString = System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.toString());

    int nodePort = Integer.parseInt(nodePortString);
    int nodeHttpPort = Integer.parseInt(nodeHttpPortString);
    YarnConfig yarnConfig = new YarnConfig(config);
    this.yarnConfig = yarnConfig;
    this.config = config;
    int interval = yarnConfig.getAMPollIntervalMs();

    //Instantiate the AM Client.
    this.amClient = AMRMClientAsync.createAMRMClientAsync(interval, this);

    this.state = new YarnAppState(-1, containerId, nodeHostString, nodePort, nodeHttpPort);

    log.info("Initialized YarnAppState: {}", state.toString());
    this.service = new SamzaYarnAppMasterService(config, samzaAppState, this.state, registry, yarnConfiguration);

    log.info("Container ID: {}, Nodehost:  {} , Nodeport : {} , NodeHttpport: {}", containerIdStr, nodeHostString, nodePort, nodeHttpPort);
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.lifecycle = new SamzaYarnAppMasterLifecycle(
        clusterManagerConfig.getContainerMemoryMb(),
        clusterManagerConfig.getNumCores(),
        samzaAppState,
        state,
        amClient,
        new JobConfig(config).getApplicationMasterHighAvailabilityEnabled()
    );
    this.nmClientAsync = NMClientAsync.createNMClientAsync(this);

  }

  /**
   * Starts the YarnClusterResourceManager and initialize all its sub-systems.
   * Attempting to start an already started cluster manager will return immediately.
   */
  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      log.info("Attempting to start an already started YarnClusterResourceManager");
      return;
    }
    metrics.start();
    service.onInit();
    log.info("Starting YarnClusterResourceManager.");
    amClient.init(yarnConfiguration);
    amClient.start();
    nmClientAsync.init(yarnConfiguration);
    nmClientAsync.start();
    Set<ContainerId> previousAttemptsContainers = lifecycle.onInit();
    metrics.setContainersFromPreviousAttempts(previousAttemptsContainers.size());

    if (new JobConfig(config).getApplicationMasterHighAvailabilityEnabled()) {
      log.info("Received running containers from previous attempt. Invoking launch success for them.");
      previousAttemptsContainers.forEach(this::handleOnContainerStarted);
    }

    if (lifecycle.shouldShutdown()) {
      clusterManagerCallback.onError(new SamzaException("Invalid resource request."));
    }

    log.info("Finished starting YarnClusterResourceManager");
  }

  /**
   * Request resources for running container processes.
   */
  @Override
  public void requestResources(SamzaResourceRequest resourceRequest) {
    String processorId = resourceRequest.getProcessorId();
    String requestId = resourceRequest.getRequestId();
    String preferredHost = resourceRequest.getPreferredHost();
    String[] racks = resourceRequest.getFaultDomains().stream().map(FaultDomain::getId).toArray(String[]::new);
    int memoryMb = resourceRequest.getMemoryMB();
    int cpuCores = resourceRequest.getNumCores();
    Resource capability = Resource.newInstance(memoryMb, cpuCores);
    String nodeLabelsExpression = yarnConfig.getContainerLabel();

    AMRMClient.ContainerRequest issuedRequest;

    /*
     * Yarn enforces these two checks:
     *   1. ANY_HOST requests should always be made with relax-locality = true
     *   2. A request with relax-locality = false should not be in the same priority as another with relax-locality = true
     *
     * Since the Samza AM makes preferred-host requests with relax-locality = false, it follows that ANY_HOST requests
     * should specify a different priority-level. We can safely set priority of preferred-host requests to be higher than
     * any-host requests since data-locality is critical.
     */
    if (preferredHost.equals("ANY_HOST")) {
      Priority priority = Priority.newInstance(ANY_HOST_PRIORITY);
      boolean relaxLocality = true;
      log.info("Requesting resources for Processor ID: {} on nodes: {} on racks: {} with capability: {}, priority: {}, relaxLocality: {}, nodeLabelsExpression: {}",
          processorId, null, Arrays.toString(racks), capability, priority, relaxLocality, nodeLabelsExpression);
      issuedRequest = new AMRMClient.ContainerRequest(capability, null, null, priority, relaxLocality, nodeLabelsExpression);
    } else {
      String[] nodes = {preferredHost};
      Priority priority = Priority.newInstance(PREFERRED_HOST_PRIORITY);
      boolean relaxLocality = false;
      log.info("Requesting resources for Processor ID: {} on nodes: {} on racks: {} with capability: {}, priority: {}, relaxLocality: {}, nodeLabelsExpression: {}",
          processorId, Arrays.toString(nodes), Arrays.toString(racks), capability, priority, relaxLocality, nodeLabelsExpression);
      issuedRequest = new AMRMClient.ContainerRequest(capability, nodes, racks, priority, relaxLocality, nodeLabelsExpression);
    }
    // ensure that updating the state and making the request are done atomically.
    synchronized (lock) {
      requestsMap.put(resourceRequest, issuedRequest);
      amClient.addContainerRequest(issuedRequest);
    }
  }

  /**
   * Requests the YarnContainerManager to release a resource. If the app cannot use the resource or wants to give up
   * the resource, it can release them.
   *
   * @param resource to be released
   */
  @Override
  public void releaseResources(SamzaResource resource) {
    log.info("Releasing Container ID: {} on host: {}", resource.getContainerId(), resource.getHost());
    // ensure that updating state and removing the request are done atomically
    synchronized (lock) {
      Container container = allocatedResources.get(resource);
      if (container == null) {
        log.info("Container ID: {} on host: {} was already released.", resource.getContainerId(), resource.getHost());
        return;
      }
      amClient.releaseAssignedContainer(container.getId());
      allocatedResources.remove(resource);
      metrics.decrementAllocatedContainersInBuffer();
    }
  }

  /**
   *
   * Requests the launch of a StreamProcessor with the specified ID on the resource
   * @param resource the SamzaResource on which to launch the StreamProcessor
   * @param builder the builder to build the resource launch command from
   *
   * TODO: Support non-builder methods to launch resources. Maybe, refactor into a ContainerLaunchStrategy interface
   */
  @Override
  public void launchStreamProcessor(SamzaResource resource, CommandBuilder builder) {
    String processorId = builder.buildEnvironment().get(ShellCommandConfig.ENV_CONTAINER_ID);
    String containerId = resource.getContainerId();
    String host = resource.getHost();
    log.info("Starting Processor ID: {} on Container ID: {} on host: {}", processorId, containerId, host);
    synchronized (lock) {
      try {
        Container container = allocatedResources.get(resource);
        if (container == null) {
          log.info("Container ID: {} on host: {} was already allocated / released.", containerId, host);
          return;
        }

        runProcessor(processorId, container, builder);
      } catch (Throwable t) {
        log.info("Error starting Processor ID: {} on Container ID: {} on host: {}", processorId, containerId, host, t);
        clusterManagerCallback.onStreamProcessorLaunchFailure(resource, t);
      }
    }
  }

  public void stopStreamProcessor(SamzaResource resource) {
    synchronized (lock) {
      Container container = allocatedResources.get(resource);
      String containerId = resource.getContainerId();
      String containerHost = resource.getHost();
      /*
       * 1. Stop the container through NMClient if the container was instantiated as part of NMClient lifecycle.
       * 2. Stop the container through AMClient by release the assigned container if the container was from the previous
       *    attempt and managed by the AM due to AM-HA
       * 3. Ignore the request if the container associated with the resource isn't present in the book keeping.
       */
      if (container != null) {
        log.info("Stopping Container ID: {} on host: {}", containerId, containerHost);
        this.nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
      } else {
        YarnContainer yarnContainer = state.runningProcessors.get(getRunningProcessorId(containerId));
        if (yarnContainer != null) {
          log.info("Stopping container from previous attempt with Container ID: {} on host: {}",
              containerId, containerHost);
          amClient.releaseAssignedContainer(yarnContainer.id());
        } else {
          log.info("No container with Container ID: {} exists. Ignoring the stop request", containerId);
        }
      }
    }
  }

  /**
   * Given a containerId from Yarn (for example: containerId_app_12345, this method returns the processor ID
   * in the range [0,N-1] that maps to it.
   * @param containerId  the Yarn container ID.
   * @return  the Samza processor ID.
   */
  //TODO: Get rid of the YarnContainer object and just use Container in state.runningProcessors hashmap.
  //In that case, this scan will turn into a lookup. This change will require changes/testing in the UI files because
  //those UI stub templates operate on the YarnContainer object.
  private String getRunningProcessorId(String containerId) {
    for (Map.Entry<String, YarnContainer> entry : state.runningProcessors.entrySet()) {
      String key = entry.getKey();
      YarnContainer yarnContainer = entry.getValue();
      String yarnContainerId = yarnContainer.id().toString();
      if (yarnContainerId.equals(containerId)) {
        return key;
      }
    }
    return INVALID_PROCESSOR_ID;
  }


  /**
   *
   * Remove a previously submitted resource request. The previous container request may have
   * been submitted. Even after the remove request, a Callback implementation must
   * be prepared to receive an allocation for the previous request. This is merely a best effort cancellation.
   *
   * @param request the request to be cancelled
   */
  @Override
  public void cancelResourceRequest(SamzaResourceRequest request) {
    String processorId = request.getProcessorId();
    String preferredHost = request.getPreferredHost();
    String requestId = request.getRequestId();
    log.info("Cancelling resource request for Processor ID: {} on host: {} with Request ID: {}",
        processorId, preferredHost, requestId);
    //ensure that removal and cancellation are done atomically.
    synchronized (lock) {
      AMRMClient.ContainerRequest containerRequest = requestsMap.get(request);
      if (containerRequest == null) {
        log.info("Resource request for Processor ID: {} on host: {} with Request ID: {} already cancelled.",
            processorId, preferredHost, requestId);
        return;
      }
      requestsMap.remove(request);
      amClient.removeContainerRequest(containerRequest);
    }
  }


  /**
   * Stops the YarnContainerManager and all its sub-components.
   * Stop should NOT be called from multiple threads.
   * TODO: fix this to make stop idempotent?.
   */
  @Override
  public void stop(SamzaApplicationState.SamzaAppStatus status) {
    log.info("Stopping the AM client on shutdown request.");
    lifecycle.onShutdown(status);
    amClient.stop();
    log.info("Stopping the NM client on shutdown request.");
    nmClientAsync.stop();
    log.info("Stopping the SamzaYarnAppMasterService service on shutdown request.");
    service.onShutdown();
    log.info("Stopping SamzaAppMasterMetrics on shutdown request.");
    metrics.stop();

    if (status != SamzaApplicationState.SamzaAppStatus.UNDEFINED) {
      cleanupStagingDir();
    }
  }

  /**
   * Cleans up the staging directory of the job. All exceptions during the cleanup
   * are swallowed.
   */
  private void cleanupStagingDir() {
    String yarnJobStagingDirectory = yarnConfig.getYarnJobStagingDirectory();
    if (yarnJobStagingDirectory != null) {
      JobContext context = new JobContext();
      context.setAppStagingDir(new Path(yarnJobStagingDirectory));

      FileSystem fs = null;
      try {
        fs = FileSystem.get(yarnConfiguration);
      } catch (IOException e) {
        log.error("Unable to clean up file system.", e);
        return;
      }
      if (fs != null) {
        YarnJobUtil.cleanupStagingDir(context, fs);
      }
    }
  }

  /**
   * Callback invoked from Yarn when containers complete. This translates the yarn callbacks into Samza specific
   * ones.
   *
   * @param statuses the YarnContainerStatus callbacks from Yarn.
   */
  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    List<SamzaResourceStatus> samzaResourceStatuses = new ArrayList<>();

    for (ContainerStatus status : statuses) {
      log.info("Got completion notification for Container ID: {} with status: {} and state: {}. Diagnostics information: {}.",
          status.getContainerId(), status.getExitStatus(), status.getState(), status.getDiagnostics());

      SamzaResourceStatus samzaResourceStatus = new SamzaResourceStatus(status.getContainerId().toString(), status.getDiagnostics(), status.getExitStatus());
      samzaResourceStatuses.add(samzaResourceStatus);

      String completedProcessorID = getRunningProcessorId(status.getContainerId().toString());
      log.info("Completed Container ID: {} had Processor ID: {}", status.getContainerId(), completedProcessorID);

      //remove the container from the list of running containers, if failed with a non-zero exit code, add it to the list of
      //failed containers.
      if (!completedProcessorID.equals(INVALID_PROCESSOR_ID)) {
        if (state.runningProcessors.containsKey(completedProcessorID)) {
          log.info("Removing Processor ID: {} from YarnClusterResourceManager running processors.", completedProcessorID);
          state.runningProcessors.remove(completedProcessorID);

          if (status.getExitStatus() != ContainerExitStatus.SUCCESS)
            state.failedContainersStatus.put(status.getContainerId().toString(), status);
        }
      }
    }
    clusterManagerCallback.onResourcesCompleted(samzaResourceStatuses);
  }

  /**
   * Callback invoked from Yarn when containers are allocated. This translates the yarn callbacks into Samza
   * specific ones.
   * @param containers the list of {@link Container} returned by Yarn.
   */
  @Override
  public void onContainersAllocated(List<Container> containers) {
    List<SamzaResource> resources = new ArrayList<SamzaResource>();
    for (Container container : containers) {
      log.info("Got allocation notification for Container ID: {} on host: {}", container.getId(),
          container.getNodeId().getHost());
      String containerId = container.getId().toString();
      String host = container.getNodeId().getHost();
      int memory = container.getResource().getMemory();
      int numCores = container.getResource().getVirtualCores();

      SamzaResource resource = new SamzaResource(numCores, memory, host, containerId);
      allocatedResources.put(resource, container);
      resources.add(resource);
      metrics.incrementAllocatedContainersInBuffer();
    }
    clusterManagerCallback.onResourcesAvailable(resources);
  }

  //The below methods are specific to the Yarn AMRM Client. We currently don't handle scenarios where there are
  //nodes being updated. We always return 0 when asked for progress by Yarn.
  @Override
  public void onShutdownRequest() {
    stop(SamzaApplicationState.SamzaAppStatus.FAILED);
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    //not implemented currently.
  }

  @Override
  public float getProgress() {
    //not implemented currently.
    return 0;
  }

  /**
   * Callback invoked when there is an error in the Yarn client. This delegates the callback handling to
   * the {@link org.apache.samza.clustermanager.ClusterResourceManager.Callback} instance.
   *
   */
  @Override
  public void onError(Throwable e) {
    log.error("Exception in the Yarn callback", e);
    clusterManagerCallback.onError(e);
  }

  @Override
  public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
    handleOnContainerStarted(containerId);
  }

  @Override
  public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
    log.info("Got status notification for Container ID: {} for Processor ID: {}. Status: {}",
        containerId, getRunningProcessorId(containerId.toString()), containerStatus.getState());
  }

  @Override
  public void onContainerStopped(ContainerId containerId) {
    log.info("Got stop notification for Container ID: {} for Processor ID: {}",
        containerId, getRunningProcessorId(containerId.toString()));
  }

  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    String processorId = getPendingProcessorId(containerId);

    if (processorId != null) {
      log.info("Got start error notification for Container ID: {} for Processor ID: {} ", containerId, processorId, t);
      YarnContainer container = state.pendingProcessors.remove(processorId);
      SamzaResource resource = new SamzaResource(container.resource().getVirtualCores(),
          container.resource().getMemory(), container.nodeId().getHost(), containerId.toString());
      clusterManagerCallback.onStreamProcessorLaunchFailure(resource, new ProcessorLaunchException(t));
    } else {
      log.warn("Did not find the pending Processor ID for the start error notification for Container ID: {}. " +
          "Ignoring notification", containerId);
    }
  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    log.info("Got status error notification for Container ID: {} for Processor ID: {}",
        containerId, getRunningProcessorId(containerId.toString()), t);
  }

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    String processorId = getRunningProcessorId(containerId.toString());

    if (processorId != null) {
      log.info("Got stop error notification for Container ID: {} for Processor ID: {}", containerId, processorId, t);
      YarnContainer container = state.runningProcessors.get(processorId);
      SamzaResource resource = new SamzaResource(container.resource().getVirtualCores(),
          container.resource().getMemory(), container.nodeId().getHost(), containerId.toString());
      clusterManagerCallback.onStreamProcessorStopFailure(resource, t);
    } else {
      log.warn("Did not find the running Processor ID for the stop error notification for Container ID: {}. " +
          "Ignoring notification", containerId);
    }
  }

  @Override
  public boolean isResourceExpired(SamzaResource resource) {
    // Time from which resource was allocated > Yarn Expiry Timeout - 30 sec (to account for clock skew)
    Duration yarnAllocatedResourceExpiry =
        Duration.ofMillis(YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS)
            .minus(Duration.ofSeconds(30));
    return System.currentTimeMillis() - resource.getTimestamp() > yarnAllocatedResourceExpiry.toMillis();
  }

  /**
   * Runs a process as specified by the command builder on the container.
   * @param processorId id of the samza processor to run (passed as a command line parameter to the process)
   * @param container the yarn container to run the processor on.
   * @param cmdBuilder the command builder that encapsulates the command, and the context
   * @throws IOException on IO exceptions running the container
   */
  public void runProcessor(String processorId, Container container, CommandBuilder cmdBuilder) throws IOException {
    String containerIdStr = ConverterUtils.toString(container.getId());
    String cmdPath = "./__package/";
    cmdBuilder.setCommandPath(cmdPath);
    String command = cmdBuilder.buildCommand();

    Map<String, String> env = getEscapedEnvironmentVariablesMap(cmdBuilder);
    env.put(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID, Util.envVarEscape(container.getId().toString()));

    Path packagePath = new Path(yarnConfig.getPackagePath());
    String formattedCommand =
        getFormattedCommand(ApplicationConstants.LOG_DIR_EXPANSION_VAR, command, ApplicationConstants.STDOUT,
            ApplicationConstants.STDERR);

    log.info("Running Processor ID: {} on Container ID: {} on host: {} using command: {} and env: {} and package path: {}",
        processorId, containerIdStr, container.getNodeHttpAddress(), formattedCommand, env, packagePath);
    state.pendingProcessors.put(processorId, new YarnContainer(container));

    startContainer(packagePath, container, env, formattedCommand);

    log.info("Made start request for Processor ID: {} on Container ID: {} on host: {} (http://{}/node/containerlogs/{}).",
        processorId, containerIdStr, container.getNodeId().getHost(), container.getNodeHttpAddress(), containerIdStr);
  }

  /**
   * Runs a command as a process on the container. All binaries needed by the physical process are packaged in the URL
   * specified by packagePath.
   */
  private void startContainer(Path packagePath,
                              Container container,
                              Map<String, String> env,
                              final String cmd) throws IOException {
    LocalResource packageResource = Records.newRecord(LocalResource.class);
    URL packageUrl = ConverterUtils.getYarnUrlFromPath(packagePath);
    FileStatus fileStatus;
    fileStatus = packagePath.getFileSystem(yarnConfiguration).getFileStatus(packagePath);
    packageResource.setResource(packageUrl);
    log.debug("Set package resource in YarnContainerRunner for {}", packageUrl);
    packageResource.setSize(fileStatus.getLen());
    packageResource.setTimestamp(fileStatus.getModificationTime());
    packageResource.setType(LocalResourceType.ARCHIVE);
    packageResource.setVisibility(LocalResourceVisibility.APPLICATION);

    ByteBuffer allTokens;
    // copy tokens to start the container
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);

    // now remove the AM->RM token so that containers cannot access it
    Iterator iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      TokenIdentifier token = ((org.apache.hadoop.security.token.Token) iter.next()).decodeIdentifier();
      if (token != null && token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    Map<String, LocalResource> localResourceMap = new HashMap<>();
    localResourceMap.put("__package", packageResource);

    // include the resources from the universal resource configurations
    LocalizerResourceMapper resourceMapper = new LocalizerResourceMapper(new LocalizerResourceConfig(config), yarnConfiguration);
    localResourceMap.putAll(resourceMapper.getResourceMap());

    ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
    context.setEnvironment(env);
    context.setTokens(allTokens.duplicate());
    context.setCommands(new ArrayList<String>() {
      {
        add(cmd);
      }
    });
    context.setLocalResources(localResourceMap);

    if (UserGroupInformation.isSecurityEnabled()) {
      Map<ApplicationAccessType, String> acls = yarnConfig.getYarnApplicationAcls();
      if (!acls.isEmpty()) {
        context.setApplicationACLs(acls);
      }
    }

    log.debug("Setting localResourceMap to {}", localResourceMap);
    log.debug("Setting context to {}", context);

    StartContainerRequest startContainerRequest = Records.newRecord(StartContainerRequest.class);
    startContainerRequest.setContainerLaunchContext(context);

    log.info("Making an async start request for Container ID: {} on host: {} with local resource map: {} and context: {}",
        container.getId(), container.getNodeHttpAddress(), localResourceMap.toString(), context);
    nmClientAsync.startContainerAsync(container, context);
  }

  /**
   * Gets the environment variables from the specified {@link CommandBuilder} and escapes certain characters.
   *
   * @param cmdBuilder        the command builder containing the environment variables.
   * @return                  the map containing the escaped environment variables.
   */
  private Map<String, String> getEscapedEnvironmentVariablesMap(CommandBuilder cmdBuilder) {
    Map<String, String> env = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : cmdBuilder.buildEnvironment().entrySet()) {
      String escapedValue = Util.envVarEscape(entry.getValue());
      env.put(entry.getKey(), escapedValue);
    }
    return env;
  }


  private String getFormattedCommand(String logDirExpansionVar, String command, String stdOut, String stdErr) {
    return String.format("export SAMZA_LOG_DIR=%s && ln -sfn %s logs && exec %s 1>logs/%s 2>logs/%s",
        logDirExpansionVar, logDirExpansionVar, command, stdOut, stdErr);
  }

  /**
   * Returns the Id of the Samza container that corresponds to the provided Yarn {@link ContainerId}
   * @param containerId the Yarn ContainerId
   * @return the id of the Samza container corresponding to the {@link ContainerId} that is pending launch
   */
  private String getPendingProcessorId(ContainerId containerId) {
    for (String pendingProcessorId: state.pendingProcessors.keySet()) {
      YarnContainer yarnContainer = state.pendingProcessors.get(pendingProcessorId);
      if (yarnContainer != null && yarnContainer.id().equals(containerId)) {
        return pendingProcessorId;
      }
    }
    return null;
  }

  /**
   * Handles container started call back for a yarn container.
   * updates the YarnAppState's pendingProcessors and runningProcessors
   * and also invokes clusterManagerCallback.s stream processor launch success
   * @param containerId yarn container id which has started
   */
  private void handleOnContainerStarted(ContainerId containerId) {
    String processorId = getPendingProcessorId(containerId);
    if (processorId != null) {
      log.info("Got start notification for Container ID: {} for Processor ID: {}", containerId, processorId);
      // 1. Move the processor from pending to running state
      final YarnContainer container = state.pendingProcessors.remove(processorId);

      state.runningProcessors.put(processorId, container);

      // 2. Invoke the success callback.
      SamzaResource resource = new SamzaResource(container.resource().getVirtualCores(),
          container.resource().getMemory(), container.nodeId().getHost(), containerId.toString());
      clusterManagerCallback.onStreamProcessorLaunchSuccess(resource);
    } else {
      log.warn("Did not find the Processor ID for the start notification for Container ID: {}. " +
          "Ignoring notification.", containerId);
    }
  }

  @VisibleForTesting
  ConcurrentHashMap<SamzaResource, Container> getAllocatedResources() {
    return allocatedResources;
  }
}
