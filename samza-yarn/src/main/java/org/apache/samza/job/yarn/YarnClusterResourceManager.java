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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.*;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.clustermanager.SamzaContainerLaunchException;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.YarnConfig;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.util.hadoop.HttpFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
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

public class YarnClusterResourceManager extends ClusterResourceManager implements AMRMClientAsync.CallbackHandler {

  private final int INVALID_YARN_CONTAINER_ID = -1;

  /**
   * The containerProcessManager instance to request resources from yarn.
   */
  private final AMRMClientAsync<AMRMClient.ContainerRequest> amClient;

  /**
   * A helper class to launch Yarn containers.
   */
  private final YarnContainerRunner yarnContainerRunner;

  /**
   * Configuration and state specific to Yarn.
   */
  private final YarnConfiguration hConfig;
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

  final AtomicBoolean started = new AtomicBoolean(false);
  private final Object lock = new Object();

  private static final Logger log = LoggerFactory.getLogger(YarnClusterResourceManager.class);

  /**
   * Creates an YarnClusterResourceManager from config, a jobModelReader and a callback.
   * @param config to instantiate the container manager with
   * @param jobModelManager the jobModel manager to get the job model (mostly for the UI)
   * @param callback the callback to receive events from Yarn.
   * @param samzaAppState samza app state for display in the UI
   */
  public YarnClusterResourceManager(Config config, JobModelManager jobModelManager, ClusterResourceManager.Callback callback, SamzaApplicationState samzaAppState ) {
    super(callback);
    hConfig = new YarnConfiguration();
    hConfig.set("fs.http.impl", HttpFileSystem.class.getName());

    // Use the Samza job config "fs.<scheme>.impl" to override YarnConfiguration
    FileSystemImplConfig fsImplConfig = new FileSystemImplConfig(config);
    fsImplConfig.getSchemes().forEach(
        scheme -> hConfig.set(fsImplConfig.getFsImplKey(scheme), fsImplConfig.getFsImplClassName(scheme))
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
    int interval = yarnConfig.getAMPollIntervalMs();

    //Instantiate the AM Client.
    this.amClient = AMRMClientAsync.createAMRMClientAsync(interval, this);

    this.state = new YarnAppState(-1, containerId, nodeHostString, nodePort, nodeHttpPort);

    log.info("Initialized YarnAppState: {}", state.toString());
    this.service = new SamzaYarnAppMasterService(config, samzaAppState, this.state, registry, hConfig);

    log.info("ContainerID str {}, Nodehost  {} , Nodeport  {} , NodeHttpport {}", new Object [] {containerIdStr, nodeHostString, nodePort, nodeHttpPort});
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.lifecycle = new SamzaYarnAppMasterLifecycle(
        clusterManagerConfig.getContainerMemoryMb(),
        clusterManagerConfig.getNumCores(),
        samzaAppState,
        state,
        amClient
    );

    yarnContainerRunner = new YarnContainerRunner(config, hConfig);
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
    amClient.init(hConfig);
    amClient.start();
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
  public void launchStreamProcessor(SamzaResource resource, CommandBuilder builder) throws SamzaContainerLaunchException {
    String containerIDStr = builder.buildEnvironment().get(ShellCommandConfig.ENV_CONTAINER_ID());
    int containerID = Integer.parseInt(containerIDStr);
    log.info("Received launch request for {} on hostname {}", containerID , resource.getHost());

    synchronized (lock) {
      Container container = allocatedResources.get(resource);
      if (container == null) {
        log.info("Resource {} already released. ", resource);
        return;
      }

      state.runningYarnContainers.put(containerID, new YarnContainer(container));
      yarnContainerRunner.runContainer(containerID, container, builder);
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

  private int getIDForContainer(String lookupContainerId) {
    int samzaContainerID = INVALID_YARN_CONTAINER_ID;
    for(Map.Entry<Integer, YarnContainer> entry : state.runningYarnContainers.entrySet()) {
      Integer key = entry.getKey();
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
        log.info("Cancellation of {} already done. ", containerRequest);
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
        fs = FileSystem.get(hConfig);
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

      int completedContainerID = getIDForContainer(status.getContainerId().toString());
      log.info("Completed container had ID: {}", completedContainerID);

      //remove the container from the list of running containers, if failed with a non-zero exit code, add it to the list of
      //failed containers.
      if(completedContainerID != INVALID_YARN_CONTAINER_ID){
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

}
