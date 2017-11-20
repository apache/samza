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

import org.apache.samza.job.CommandBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class MockClusterResourceManager extends ClusterResourceManager {
  Set<SamzaResource> releasedResources = new HashSet<>();
  List<SamzaResource> resourceRequests = new ArrayList<>();
  List<SamzaResourceRequest> cancelledRequests = new ArrayList<>();
  List<SamzaResource> launchedResources = new ArrayList<>();
  List<MockContainerListener> mockContainerListeners = new ArrayList<MockContainerListener>();
  Throwable nextException = null;

  public MockClusterResourceManager(ClusterResourceManager.Callback callback) {
    super(callback);
  }

  @Override
  public void start() {

  }

  @Override
  public void requestResources(SamzaResourceRequest resourceRequest) {
    SamzaResource resource = new SamzaResource(resourceRequest.getNumCores(), resourceRequest.getMemoryMB(), resourceRequest.getPreferredHost(), UUID.randomUUID().toString());
    List<SamzaResource> resources = Collections.singletonList(resource);
    resourceRequests.addAll(resources);
  }

  @Override
  public void cancelResourceRequest(SamzaResourceRequest request) {
    cancelledRequests.add(request);
  }

  @Override
  public void releaseResources(SamzaResource resource) {
    releasedResources.add(resource);
  }

  @Override
  public void launchStreamProcessor(SamzaResource resource, CommandBuilder builder)  {
    if (nextException != null) {
      clusterManagerCallback.onStreamProcessorLaunchFailure(resource, new SamzaContainerLaunchException(nextException));
    }
    launchedResources.add(resource);
    clusterManagerCallback.onStreamProcessorLaunchSuccess(resource);
    for (MockContainerListener listener : mockContainerListeners) {
      listener.postRunContainer(launchedResources.size());
    }

  }

  @Override
  public void stop(SamzaApplicationState.SamzaAppStatus status) {

  }

  public void registerContainerListener(MockContainerListener listener) {
    mockContainerListeners.add(listener);
  }

  public void clearContainerListeners() {
    mockContainerListeners.clear();
  }

}
