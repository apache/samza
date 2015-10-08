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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * This is the default allocator thread that will be used by SamzaTaskManager.
 *
 * When host-affinity is not enabled, this thread periodically wakes up to assign a container to an allocated resource.
 * If there aren't enough containers, it waits by sleeping for {@code ALLOCATOR_SLEEP_TIME} milliseconds.
 */
public class ContainerAllocator extends AbstractContainerAllocator {
  private static final Logger log = LoggerFactory.getLogger(ContainerAllocator.class);

  public ContainerAllocator(AMRMClientAsync<AMRMClient.ContainerRequest> amClient,
                            ContainerUtil containerUtil,
                            YarnConfig yarnConfig) {
    super(amClient, containerUtil, new ContainerRequestState(amClient, false), yarnConfig);
  }

  /**
   * During the run() method, the thread sleeps for ALLOCATOR_SLEEP_TIME ms. It tries to allocate any unsatisfied
   * request that is still in the request queue (See requests in {@link org.apache.samza.job.yarn.ContainerRequestState})
   * with allocated containers, if any.
   *
   * Since host-affinity is not enabled, all allocated container resources are buffered in the list keyed by "ANY_HOST".
   * */
  @Override
  public void run() {
    while(isRunning.get()) {
      try {
        List<Container> allocatedContainers = containerRequestState.getContainersOnAHost(ANY_HOST);
        while (!containerRequestState.getRequestsQueue().isEmpty() && allocatedContainers != null && allocatedContainers.size() > 0) {
          SamzaContainerRequest request = containerRequestState.getRequestsQueue().peek();
          Container container = allocatedContainers.get(0);

          // Update state
          containerRequestState.updateStateAfterAssignment(request, ANY_HOST, container);

          // Cancel request and run container
          log.info("Running {} on {}", request.expectedContainerId, container.getId());
          containerUtil.runContainer(request.expectedContainerId, container);
        }

        // If requestQueue is empty, all extra containers in the buffer should be released.
        containerRequestState.releaseExtraContainers();

        Thread.sleep(ALLOCATOR_SLEEP_TIME);
      } catch (InterruptedException e) {
        log.info("Got InterruptedException in AllocatorThread. Pending Container request(s) cannot be fulfilled!!", e);
      }
    }
  }
}
