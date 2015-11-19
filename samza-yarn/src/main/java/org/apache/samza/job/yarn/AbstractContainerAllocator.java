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
import org.apache.samza.config.YarnConfig;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is responsible for making requests for containers to the AM and also, assigning a container to run on an allocated resource.
 *
 * Since we are using a simple thread based allocation of a container to an allocated resource, the subclasses should implement {@link java.lang.Runnable} interface.
 * The allocator thread follows the lifecycle of the {@link org.apache.samza.job.yarn.SamzaTaskManager}. Depending on whether host-affinity is enabled or not, the allocation model varies.
 *
 * See {@link org.apache.samza.job.yarn.ContainerAllocator} and {@link org.apache.samza.job.yarn.HostAwareContainerAllocator}
 */
public abstract class AbstractContainerAllocator implements Runnable {
  public static final String ANY_HOST = ContainerRequestState.ANY_HOST;
  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_CONTAINER_MEM = 1024;
  public static final int DEFAULT_CPU_CORES = 1;

  protected final AMRMClientAsync<AMRMClient.ContainerRequest> amClient;
  protected final int ALLOCATOR_SLEEP_TIME;
  protected final ContainerUtil containerUtil;
  protected final int containerMaxMemoryMb;
  protected final int containerMaxCpuCore;

  @Override
  public abstract void run();

  // containerRequestState indicate the state of all unfulfilled container requests and allocated containers
  protected final ContainerRequestState containerRequestState;

  // state that controls the lifecycle of the allocator thread
  protected AtomicBoolean isRunning = new AtomicBoolean(true);

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
   * Method that adds allocated container to a synchronized buffer of allocated containers list
   * See allocatedContainers in {@link org.apache.samza.job.yarn.ContainerRequestState}
   *
   * @param container Container resource returned by the RM
   */
  public final void addContainer(Container container) {
    containerRequestState.addContainer(container);
  }

  public final void setIsRunning(boolean state) {
    isRunning.set(state);
  }

}
