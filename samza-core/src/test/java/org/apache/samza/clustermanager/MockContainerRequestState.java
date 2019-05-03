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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;


public class MockContainerRequestState extends ResourceRequestState {
  private final List<MockContainerListener> mockContainerListeners = new ArrayList<MockContainerListener>();
  private int numAddedContainers = 0;
  private int numReleasedContainers = 0;
  private int numAssignedContainers = 0;
  public Queue<SamzaResourceRequest> assignedRequests = new LinkedList<>();

  public MockContainerRequestState(ClusterResourceManager manager,
                                   boolean hostAffinityEnabled) {
    super(hostAffinityEnabled, manager);
  }

  @Override
  public synchronized void updateStateAfterAssignment(SamzaResourceRequest request, String assignedHost, SamzaResource resource) {
    super.updateStateAfterAssignment(request, assignedHost, resource);

    numAssignedContainers++;
    assignedRequests.add(request);

    for (MockContainerListener listener : mockContainerListeners) {
      listener.postUpdateRequestStateAfterAssignment(numAssignedContainers);
    }
  }

  @Override
  public synchronized void addResource(SamzaResource container) {
    super.addResource(container);

    numAddedContainers++;
    for (MockContainerListener listener : mockContainerListeners) {
      listener.postAddContainer(numAddedContainers);
    }
  }

  @Override
  public synchronized int releaseExtraResources() {
    numReleasedContainers += super.releaseExtraResources();

    for (MockContainerListener listener : mockContainerListeners) {
      listener.postReleaseContainers(numReleasedContainers);
    }

    return numAddedContainers;
  }

  @Override
  public void releaseUnstartableContainer(SamzaResource container, String preferredHost) {
    super.releaseUnstartableContainer(container, preferredHost);

    numReleasedContainers += 1;
    for (MockContainerListener listener : mockContainerListeners) {
      listener.postReleaseContainers(numReleasedContainers);
    }
  }


  public void registerContainerListener(MockContainerListener listener) {
    mockContainerListeners.add(listener);
  }

  public void clearContainerListeners() {
    mockContainerListeners.clear();
  }

}
