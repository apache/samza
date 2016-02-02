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
package org.apache.samza.job.yarn.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.samza.job.yarn.ContainerRequestState;


public class MockContainerRequestState extends ContainerRequestState {
  private final List<MockContainerListener> _mockContainerListeners = new ArrayList<MockContainerListener>();
  private int numAddedContainers = 0;
  private int numReleasedContainers = 0;

  public MockContainerRequestState(AMRMClientAsync<AMRMClient.ContainerRequest> amClient,
      boolean hostAffinityEnabled) {
    super(amClient, hostAffinityEnabled);
  }


  @Override
  public synchronized void addContainer(Container container) {
    super.addContainer(container);

    numAddedContainers++;
    for (MockContainerListener listener : _mockContainerListeners) {
      listener.postAddContainer(container, numAddedContainers);
    }
  }

  @Override
  public synchronized int releaseExtraContainers() {
    numReleasedContainers += super.releaseExtraContainers();

    for (MockContainerListener listener : _mockContainerListeners) {
      listener.postReleaseContainers(numReleasedContainers);
    }

    return numAddedContainers;
  }

  public void registerContainerListener(MockContainerListener listener) {
    _mockContainerListeners.add(listener);
  }

  public void clearContainerListeners() {
    _mockContainerListeners.clear();
  }

}
