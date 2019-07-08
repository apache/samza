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

import java.util.Map;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the default allocator that will be used by ContainerProcessManager.
 *
 * When host-affinity is not enabled, this periodically wakes up to assign a processor to *ANY* allocated resource.
 * If there aren't enough resources, it waits by sleeping for {@code allocatorSleepIntervalMs} milliseconds.
 *
 * This class is instantiated by the ContainerProcessManager (which in turn is created by the JC from run-jc.sh),
 * when host-affinity is off. Otherwise, the HostAwareContainerAllocator is instantiated.
 */
public class ContainerAllocator extends AbstractContainerAllocator {
  private static final Logger log = LoggerFactory.getLogger(ContainerAllocator.class);

  public ContainerAllocator(ClusterResourceManager manager,
      Config config,
      SamzaApplicationState state,
      ClassLoader pluginClassloader) {
    super(manager, new ResourceRequestState(false, manager), config, state, pluginClassloader);
  }

  /**
   * During the run() method, the thread sleeps for allocatorSleepIntervalMs ms. It then invokes assignResourceRequests,
   * and tries to allocate any unsatisfied request that is still in the request queue {@link ResourceRequestState})
   * with allocated resources, if any.
   *
   * Since host-affinity is not enabled, all allocated resources are buffered in the list keyed by "ANY_HOST".
   * */
  @Override
  public void assignResourceRequests() {
    while (hasReadyPendingRequest() && hasAllocatedResource(ResourceRequestState.ANY_HOST)) {
      SamzaResourceRequest request = peekReadyPendingRequest();
      runStreamProcessor(request, ResourceRequestState.ANY_HOST);
    }
  }

  /**
   * Since host-affinity is not enabled, the processor id to host mappings will be ignored and all resources will be
   * matched to any available host.
   *
   * @param processorToHostMapping A Map of [processorId, hostName] ID of the processor to run on the resource.
   *                               The hostName will be ignored and each processor will be matched to any available host.
   */
  @Override
  public void requestResources(Map<String, String> processorToHostMapping)  {
    for (Map.Entry<String, String> entry : processorToHostMapping.entrySet()) {
      String processorId = entry.getKey();
      requestResource(processorId, ResourceRequestState.ANY_HOST);
    }
  }

}
