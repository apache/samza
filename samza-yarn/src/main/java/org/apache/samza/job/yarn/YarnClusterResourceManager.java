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
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.*;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.clustermanager.SamzaContainerLaunchException;
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

  private final String INVALID_YARN_CONTAINER_ID = "-1";

  /**
   * The containerProcessManager instance to request resources from yarn.
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

  private final ConcurrentHashMap<ContainerId, Container> containersPendingStartup = new ConcurrentHashMap<>();

  private final SamzaAppMasterMetrics metrics;

  final AtomicBoolean started = new AtomicBoolean(false);
  private final Object lock = new Object();
  private final NMClientAsync nmClientAsync;

  private static final Logger log = LoggerFactory.getLogger(YarnClusterResourceManager.class);
  private final Config config;

  /**
   * Creates an YarnClusterResourceManager from config, a jobModelReader and a callback.
   * @param config to instantiate the container manager with
   * @param jobModelManager the jobModel manager to get the job model (mostly for the UI)
   * @param callback the callback to receive events from Yarn.
   * @param samzaAppState samza app state for display in the UI
   */
  public YarnClusterResourceManager(Config config, JobModelManager jobModelManager, ClusterResourceManager.Callback callback, SamzaApplicationState samzaAppState ) {
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

    log.info("ContainerID str {}, Nodehost  {} , Nodeport  {} , NodeHttpport {}", new Object [] {containerIdStr, nodeHostString, nodePort, nodeHttpPort});
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.lifecycle = new SamzaYarnAppMasterLifecycle(
        clusterManagerConfig.getContainerMemoryMb(),
        clusterManagerConfig.getNumCores(),
        samzaAppState,
        state,
        amClient
    );
    this.nmClientAsync = NMClientAsync.createNMClientAsync(this);

  }

  /**
   * Starts the YarnContainerManager and initialize all its sub-systems.
   * Attempting to start an already started container manager will return immediately.
   */
  @Override
  public void start() {
    if(!started.compareAndSet(false, true)) {
      log.info("Attempting to start an already started ContainerManager");
      return;
    }
    metrics.start();
    service.onInit();
    log.info("Starting YarnContainerManager.");
    amClient.init(yarnConfiguration);
    amClient.start();
    nmClientAsync.init(yarnConfiguration);
    nmClientAsync.start();
    lifecycle.onInit();

    if(lifecycle.shouldShutdown()) {
      clusterManagerCallback.onError(new SamzaException("Invalid resource request."));
    }

    log.info("Finished starting YarnContainerManager");
  }

  /**
   * Request resources for running container processes.
   */
  @Override
  public void requestResources(SamzaResourceRequest resourceRequest) {
    final int DEFAULT_PRIORITY = 0;
    log.info("Requesting resources on  " + resourceRequest.getPreferredHost() + " for container " + resourceRequest.getContainerID());

    int memoryMb = resourceRequest.getMemoryMB();
    int cpuCores = resourceRequest.getNumCores();
    String containerLabel = yarnConfig.getContainerLabel();
    String preferredHost = resourceRequest.getPreferredHost();
    Resource capability = Resource.newInstance(memoryMb, cpuCores);
    Priority priority =  Priority.newInstance(DEFAULT_PRIORITY);

    AMRMClient.ContainerRequest issuedRequest;

    if (preferredHost.equals("ANY_HOST"))
    {
      log.info("Making a request for ANY_HOST " + preferredHost );
      issuedRequest = new AMRMClient.ContainerRequest(capability, null, null, priority, true, containerLabel);
    }
    else
    {
      log.info("Making a preferred host request on " + preferredHost);
      issuedRequest = new AMRMClient.ContainerRequest(
              capability,
              new String[]{preferredHost},
              null,
              priority,
              true,
              containerLabel);
    }
    //ensure that updating the state and making the request are done atomically.
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
    log.info("Release resource invoked {} ", resource);
    //ensure that updating state and removing the request are done atomically
    synchronized (lock) {
      Container container = allocatedResources.get(resource);
      if (container == null) {
        log.info("Resource {} already released. ", resource);
        return;
      }
      amClient.releaseAssignedContainer(container.getId());
      allocatedResources.remove(resource);
    }
  }

  /**
   *
   * Requests the launch of a StreamProcessor with the specified ID on the resource
   * @param resource , the SamzaResource on which to launch the StreamProcessor
   * @param builder, the builder to build the resource launch command from
   *
   * TODO: Support non-builder methods to launch resources. Maybe, refactor into a ContainerLaunchStrategy interface
   */

  @Override
  public void launchStreamProcessor(SamzaResource resource, CommandBuilder builder)  {
    String containerIDStr = builder.buildEnvironment().get(ShellCommandConfig.ENV_CONTAINER_ID());
    log.info("Received launch request for {} on hostname {}", containerIDStr , resource.getHost());

    synchronized (lock) {
      Container container = allocatedResources.get(resource);
      if (container == null) {
        log.info("Resource {} already released. ", resource);
        return;
      }

      runContainer(containerIDStr, container, builder);
    }
  }

  /**
   * Given a lookupContainerId from Yarn (for example: containerId_app_12345, this method returns the SamzaContainer ID
   * in the range [0,N-1] that maps to it.
   * @param lookupContainerId  the Yarn container ID.
   * @return  the samza container ID.
   */

  //TODO: Get rid of the YarnContainer object and just use Container in state.runningYarnContainers hashmap.
  //In that case, this scan will turn into a lookup. This change will require changes/testing in the UI files because
  //those UI stub templates operate on the YarnContainer object.

  private String getIDForContainer(String lookupContainerId) {
    String samzaContainerID = INVALID_YARN_CONTAINER_ID;
    for(Map.Entry<String, YarnContainer> entry : state.runningYarnContainers.entrySet()) {
      String key = entry.getKey();
      YarnContainer yarnContainer = entry.getValue();
      String yarnContainerId = yarnContainer.id().toString();
      if(yarnContainerId.equals(lookupContainerId)) {
        return key;
      }
    }
    return samzaContainerID;
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
    log.info("Cancelling request {} ", request);
    //ensure that removal and cancellation are done atomically.
    synchronized (lock) {
      AMRMClient.ContainerRequest containerRequest = requestsMap.get(request);
      if (containerRequest == null) {
        log.info("Cancellation of {} already done. ", request);
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
    log.info("Stopping AM client " );
    lifecycle.onShutdown(status);
    amClient.stop();
    log.info("Stopping the AM service " );
    nmClientAsync.stop();
    log.info("Stopping the NM service " );
    service.onShutdown();
    metrics.stop();

    if(status != SamzaApplicationState.SamzaAppStatus.UNDEFINED) {
      cleanupStagingDir();
    }
  }

  /**
   * Cleans up the staging directory of the job. All exceptions during the cleanup
   * are swallowed.
   */
  private void cleanupStagingDir() {
    String yarnJobStagingDirectory = yarnConfig.getYarnJobStagingDirectory();
    if(yarnJobStagingDirectory != null) {
      JobContext context = new JobContext();
      context.setAppStagingDir(new Path(yarnJobStagingDirectory));

      FileSystem fs = null;
      try {
        fs = FileSystem.get(yarnConfiguration);
      } catch (IOException e) {
        log.error("Unable to clean up file system: {}", e);
        return;
      }
      if(fs != null) {
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
    List<SamzaResourceStatus> samzaResrcStatuses = new ArrayList<>();

    for(ContainerStatus status: statuses) {
      log.info("Container completed from RM " + status);

      SamzaResourceStatus samzaResrcStatus = new SamzaResourceStatus(status.getContainerId().toString(), status.getDiagnostics(), status.getExitStatus());
      samzaResrcStatuses.add(samzaResrcStatus);

      String completedContainerID = getIDForContainer(status.getContainerId().toString());
      log.info("Completed container had ID: {}", completedContainerID);

      //remove the container from the list of running containers, if failed with a non-zero exit code, add it to the list of
      //failed containers.
      if(!completedContainerID.equals(INVALID_YARN_CONTAINER_ID)){
        if(state.runningYarnContainers.containsKey(completedContainerID)) {
          log.info("Removing container ID {} from completed containers", completedContainerID);
          state.runningYarnContainers.remove(completedContainerID);

          if(status.getExitStatus() != ContainerExitStatus.SUCCESS)
            state.failedContainersStatus.put(status.getContainerId().toString(), status);
        }
      }
    }
    clusterManagerCallback.onResourcesCompleted(samzaResrcStatuses);
  }

  /**
   * Callback invoked from Yarn when containers are allocated. This translates the yarn callbacks into Samza
   * specific ones.
   * @param containers the list of {@link Container} returned by Yarn.
   */
  @Override
  public void onContainersAllocated(List<Container> containers) {
      List<SamzaResource> resources = new ArrayList<SamzaResource>();
      for(Container container : containers) {
          log.info("Container allocated from RM on " + container.getNodeId().getHost());
          final String id = container.getId().toString();
          String host = container.getNodeId().getHost();
          int memory = container.getResource().getMemory();
          int numCores = container.getResource().getVirtualCores();

          SamzaResource resource = new SamzaResource(numCores, memory, host, id);
          allocatedResources.put(resource, container);
          resources.add(resource);
      }
      clusterManagerCallback.onResourcesAvailable(resources);
  }

  //The below methods are specific to the Yarn AMRM Client. We currently don't handle scenarios where there are
  //nodes being updated. We always return 0 when asked for progress by Yarn.
  @Override
  public void onShutdownRequest() {
    //not implemented currently.
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
   * Callback invoked when there is an error in the Yarn client. This delegates the
   * callback handling to the {@link ClusterResourceManager.Callback} instance.
   *
   */
  @Override
  public void onError(Throwable e) {
    log.error("Exception in the Yarn callback {}", e);
    clusterManagerCallback.onError(e);
  }

  @Override
  public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
    log.info("Received a containerStart notification from the NodeManager for container: {} ", containerId);
    String samzaContainerId = getPendingSamzaContainerId(containerId);

    if (samzaContainerId != null) {
      // 1. Move the container from pending to running state
      final YarnContainer container = state.pendingYarnContainers.remove(samzaContainerId);
      log.info("Samza containerId:{} has started", samzaContainerId);

      state.runningYarnContainers.put(samzaContainerId, container);

      // 2. Invoke the success callback.
      SamzaResource resource = new SamzaResource(container.resource().getVirtualCores(),
          container.resource().getMemory(), container.nodeId().getHost(), containerId.toString());
      clusterManagerCallback.onStreamProcessorLaunchSuccess(resource);
    } else {
      log.info("Got an invalid notification from YARN for container: {}", containerId);
    }
  }

  @Override
  public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
    log.info("Got a status from the NodeManager. Container: {} Status: {}", containerId, containerStatus.getState());
  }

  @Override
  public void onContainerStopped(ContainerId containerId) {
    log.info("Got a notification from the NodeManager for a stopped container. ContainerId: {}", containerId);
  }

  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    log.error(String.format("Container: %s could not start.", containerId), t);

    Container container = containersPendingStartup.remove(containerId);

    if (container != null) {
      SamzaResource resource = new SamzaResource(container.getResource().getVirtualCores(),
          container.getResource().getMemory(), container.getNodeId().getHost(), containerId.toString());
      log.info("Invoking failure callback for container: {}", containerId);
      clusterManagerCallback.onStreamProcessorLaunchFailure(resource, new SamzaContainerLaunchException(t));
    } else {
      log.info("Got an invalid notification for container: {}", containerId);
    }
  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    log.info("Got an error on getContainerStatus from the NodeManager. ContainerId: {}. Error: {}", containerId, t);
  }

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    log.info("Got an error when stopping container from the NodeManager. ContainerId: {}. Error: {}", containerId, t);
  }

  /**
   * Runs a process as specified by the command builder on the container.
   * @param samzaContainerId id of the samza Container to run (passed as a command line parameter to the process)
   * @param container the samza container to run.
   * @param cmdBuilder the command builder that encapsulates the command, and the context
   *
   */
  public void runContainer(String samzaContainerId, Container container, CommandBuilder cmdBuilder)  {
    String containerIdStr = ConverterUtils.toString(container.getId());
    log.info("Got available container ID ({}) for container: {}", samzaContainerId, container);

    // check if we have framework path specified. If yes - use it, if not use default ./__package/
    String jobLib = ""; // in case of separate framework, this directory will point at the job's libraries
    String cmdPath = "./__package/";

    String fwkPath = JobConfig.getFwkPath(this.config);
    if(fwkPath != null && (! fwkPath.isEmpty())) {
      cmdPath = fwkPath;
      jobLib = "export JOB_LIB_DIR=./__package/lib";
    }
    log.info("In runContainer in util: fwkPath= " + fwkPath + ";cmdPath=" + cmdPath + ";jobLib=" + jobLib);
    cmdBuilder.setCommandPath(cmdPath);


    String command = cmdBuilder.buildCommand();
    log.info("Container ID {} using command {}", samzaContainerId, command);

    Map<String, String> env = getEscapedEnvironmentVariablesMap(cmdBuilder);
    env.put(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID(), Util.envVarEscape(container.getId().toString()));
    printContainerEnvironmentVariables(samzaContainerId, env);

    log.info("Samza FWK path: " + command + "; env=" + env);

    Path packagePath = new Path(yarnConfig.getPackagePath());
    log.info("Starting container ID {} using package path {}", samzaContainerId, packagePath);
    state.pendingYarnContainers.put(samzaContainerId, new YarnContainer(container));

    startContainer(
        packagePath,
        container,
        env,
        getFormattedCommand(
            ApplicationConstants.LOG_DIR_EXPANSION_VAR,
            jobLib,
            command,
            ApplicationConstants.STDOUT,
            ApplicationConstants.STDERR)
    );


    log.info("Claimed container ID {} for container {} on node {} (http://{}/node/containerlogs/{}).",
        new Object[]{
            samzaContainerId,
            containerIdStr,
            container.getNodeId().getHost(),
            container.getNodeHttpAddress(),
            containerIdStr}
    );

    log.info("Started container ID {}", samzaContainerId);
  }

  /**
   *    Runs a command as a process on the container. All binaries needed by the physical process are packaged in the URL
   *    specified by packagePath.
   */
  private void startContainer(Path packagePath,
                              Container container,
                              Map<String, String> env,
                              final String cmd)  {
    log.info("Starting container {} {} {} {}",
        new Object[]{packagePath, container, env, cmd});

    LocalResource packageResource = Records.newRecord(LocalResource.class);
    URL packageUrl = ConverterUtils.getYarnUrlFromPath(packagePath);
    FileStatus fileStatus;
    try {
      fileStatus = packagePath.getFileSystem(yarnConfiguration).getFileStatus(packagePath);
    } catch (IOException ioe) {
      log.error("IO Exception when accessing the package status from the filesystem", ioe);
      SamzaResource resource = new SamzaResource(container.getResource().getVirtualCores(),
          container.getResource().getMemory(), container.getNodeId().getHost(), container.getId().toString());
      clusterManagerCallback.onStreamProcessorLaunchFailure(resource, new SamzaContainerLaunchException("IO Exception " +
          "when accessing the package status from the filesystem", ioe));
      return;
    }

    packageResource.setResource(packageUrl);
    log.info("Set package resource in YarnContainerRunner for {}", packageUrl);
    packageResource.setSize(fileStatus.getLen());
    packageResource.setTimestamp(fileStatus.getModificationTime());
    packageResource.setType(LocalResourceType.ARCHIVE);
    packageResource.setVisibility(LocalResourceVisibility.APPLICATION);

    ByteBuffer allTokens;
    // copy tokens to start the container
    try {
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

    } catch (IOException ioe) {
      log.error("IOException when writing credentials.", ioe);
      log.error("IO Exception when accessing the package status from the filesystem", ioe);
      SamzaResource resource = new SamzaResource(container.getResource().getVirtualCores(),
          container.getResource().getMemory(), container.getNodeId().getHost(), container.getId().toString());
      clusterManagerCallback.onStreamProcessorLaunchFailure(resource, new SamzaContainerLaunchException("IO Exception " +
          "when writing credentials", ioe));
      return;
    }

    Map<String, LocalResource> localResourceMap = new HashMap<>();
    localResourceMap.put("__package", packageResource);

    // include the resources from the universal resource configurations
    LocalizerResourceMapper resourceMapper = new LocalizerResourceMapper(new LocalizerResourceConfig(config), yarnConfiguration);
    localResourceMap.putAll(resourceMapper.getResourceMap());

    ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
    context.setEnvironment(env);
    context.setTokens(allTokens.duplicate());
    context.setCommands(new ArrayList<String>() {{add(cmd);}});
    context.setLocalResources(localResourceMap);

    log.debug("Setting localResourceMap to {}", localResourceMap);
    log.debug("Setting context to {}", context);

    StartContainerRequest startContainerRequest = Records.newRecord(StartContainerRequest.class);
    startContainerRequest.setContainerLaunchContext(context);

    log.info("Making an async start request for container {}", container);
    nmClientAsync.startContainerAsync(container, context);
  }

  /**
   * @param samzaContainerId  the Samza container Id for logging purposes.
   * @param env               the Map of environment variables to their respective values.
   */
  private void printContainerEnvironmentVariables(String samzaContainerId, Map<String, String> env) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : env.entrySet()) {
      sb.append(String.format("\n%s=%s", entry.getKey(), entry.getValue()));
    }
    log.info("Container ID {} using environment variables: {}", samzaContainerId, sb.toString());
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


  private String getFormattedCommand(String logDirExpansionVar,
                                     String jobLib,
                                     String command,
                                     String stdOut,
                                     String stdErr) {
    if (!jobLib.isEmpty()) {
      jobLib = "&& " + jobLib; // add job's libraries exported to an env variable
    }

    return String
        .format("export SAMZA_LOG_DIR=%s %s && ln -sfn %s logs && exec %s 1>logs/%s 2>logs/%s", logDirExpansionVar,
            jobLib, logDirExpansionVar, command, stdOut, stdErr);
  }

  /**
   * Returns the Id of the Samza container that corresponds to the provided Yarn {@link ContainerId}
   * @param containerId the Yarn ContainerId
   * @return the id of the Samza container corresponding to the {@link ContainerId} that is pending launch
   */
  private String getPendingSamzaContainerId(ContainerId containerId) {
    for (String samzaContainerId: state.pendingYarnContainers.keySet()) {
      YarnContainer yarnContainer = state.pendingYarnContainers.get(samzaContainerId);
      if (yarnContainer.id().equals(containerId)) {
        return samzaContainerId;
      }
    }
    return null;
  }


}
