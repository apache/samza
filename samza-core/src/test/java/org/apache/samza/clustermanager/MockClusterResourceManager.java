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

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import org.apache.samza.job.CommandBuilder;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class MockClusterResourceManager extends ClusterResourceManager {
  final Set<SamzaResource> releasedResources = Collections.synchronizedSet(new HashSet<>());
  final List<SamzaResource> resourceRequests = Collections.synchronizedList(new ArrayList<>());
  final List<SamzaResourceRequest> cancelledRequests = Collections.synchronizedList(new ArrayList<>());
  final List<SamzaResource> launchedResources = Collections.synchronizedList(new ArrayList<>());
  final List<MockContainerListener> mockContainerListeners = Collections.synchronizedList(new ArrayList<>());

  private final Semaphore requestCountSemaphore = new Semaphore(0);
  private final Semaphore launchCountSemaphore = new Semaphore(0);
  private final SamzaApplicationState state;

  Throwable nextException = null;

  MockClusterResourceManager(ClusterResourceManager.Callback callback, SamzaApplicationState state) {
    super(callback);
    this.state = state;
  }

  @Override
  public void start() {

  }

  @Override
  public void requestResources(SamzaResourceRequest resourceRequest) {
    SamzaResource resource = new SamzaResource(resourceRequest.getNumCores(), resourceRequest.getMemoryMB(),
        resourceRequest.getPreferredHost(), UUID.randomUUID().toString());
    resourceRequests.add(resource);
    requestCountSemaphore.release();
    clusterManagerCallback.onResourcesAvailable(ImmutableList.of(resource));
  }

  @Override
  public void cancelResourceRequest(SamzaResourceRequest request) {
    cancelledRequests.add(request);
  }

  public boolean awaitResourceRequests(int numExpectedRequests, long val, TimeUnit unit) throws Exception  {
    return requestCountSemaphore.tryAcquire(numExpectedRequests, val, unit);
  }

  public boolean awaitContainerLaunch(int numExpectedContainers, long val, TimeUnit unit) throws Exception {
    return launchCountSemaphore.tryAcquire(numExpectedContainers, val, unit);
  }

  @Override
  public void releaseResources(SamzaResource resource) {
    releasedResources.add(resource);
  }

  @Override
  public void launchStreamProcessor(SamzaResource resource, CommandBuilder builder)  {
    // assert that the resource is in "pending" state prior to invoking this method
    Assert.assertTrue(state.pendingProcessors.values().contains(resource));

    if (nextException != null) {
      clusterManagerCallback.onStreamProcessorLaunchFailure(resource, new ProcessorLaunchException(nextException));
    } else {
      launchedResources.add(resource);
      clusterManagerCallback.onStreamProcessorLaunchSuccess(resource);
    }

    for (MockContainerListener listener : mockContainerListeners) {
      listener.postRunContainer(launchedResources.size());
    }
    launchCountSemaphore.release();
  }

  @Override
  public void stopStreamProcessor(SamzaResource resource) {
    SamzaResourceStatus status = new SamzaResourceStatus(resource.getContainerId(), "diagnostics", SamzaResourceStatus.PREEMPTED);
    List<SamzaResourceStatus> statList = new ArrayList<>();
    statList.add(status);
    clusterManagerCallback.onResourcesCompleted(statList);
  }

  @Override
  public boolean isResourceExpired(SamzaResource resource) {
    Duration yarnAllocatedResourceExpiry = Duration.ofMinutes(10).minus(Duration.ofSeconds(30));
    return System.currentTimeMillis() - resource.getTimestamp() > yarnAllocatedResourceExpiry.toMillis();
  }


  public void registerContainerListener(MockContainerListener listener) {
    mockContainerListeners.add(listener);
  }

  public void clearContainerListeners() {
    mockContainerListeners.clear();
  }

  public boolean containsReleasedResource(SamzaResource resource) {
    return releasedResources.contains(resource);
  }

  @Override
  public void stop(SamzaApplicationState.SamzaAppStatus status) {

  }
}
