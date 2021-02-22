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

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.config.Config;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.samza.config.MapConfig;


public class MockContainerAllocatorWithoutHostAffinity extends ContainerAllocator {
  public int requestedContainers = 0;
  private Semaphore semaphore = new Semaphore(0);

  private Semaphore expiredRequestSemaphore = new Semaphore(0);
  private AtomicInteger expiredRequestCallCount = new AtomicInteger(0);
  private volatile boolean overrideIsRequestExpired = false;

  // Create a MockContainerAllocator with certain config overrides
  public static MockContainerAllocatorWithoutHostAffinity createContainerAllocatorWithConfigOverride(
      ClusterResourceManager resourceManager, Config config, SamzaApplicationState state,
      ContainerManager containerManager, Config overrideConfig) {
    Map<String, String> mergedConfig = new HashMap<>();
    mergedConfig.putAll(config);
    mergedConfig.putAll(overrideConfig);
    return new MockContainerAllocatorWithoutHostAffinity(resourceManager, new MapConfig(mergedConfig), state, containerManager);
  }

  public MockContainerAllocatorWithoutHostAffinity(ClusterResourceManager resourceManager,
                                Config config, SamzaApplicationState state, ContainerManager containerManager) {
    super(resourceManager, config, state, false, containerManager);
  }

  /**
   * Causes the current thread to block until the expected number of containers have started.
   *
   * @param numExpectedContainers the number of containers expected to start
   * @param timeout the maximum time to wait
   * @param unit the time unit of the {@code timeout} argument
   *
   * @return a boolean that specifies whether containers started within the timeout.
   * @throws InterruptedException  if the current thread is interrupted while waiting
   */
  boolean awaitContainersStart(int numExpectedContainers, long timeout, TimeUnit unit) throws InterruptedException {
    return semaphore.tryAcquire(numExpectedContainers, timeout, unit);
  }

  @Override
  public void requestResources(Map<String, String> processorToHostMapping) {
    requestedContainers += processorToHostMapping.size();
    super.requestResources(processorToHostMapping);
  }

  public void setOverrideIsRequestExpired() {
    overrideIsRequestExpired = true;
  }

  public int getExpiredRequestCallCount() {
    return expiredRequestCallCount.get();
  }

  @Override
  protected boolean isRequestExpired(SamzaResourceRequest request) {
    if (!overrideIsRequestExpired) {
      // if not set to override, then return the original result
      return super.isRequestExpired(request);
    }
    expiredRequestSemaphore.release();
    expiredRequestCallCount.incrementAndGet();
    return true;
  }

  public boolean awaitIsRequestExpiredCall(long timeoutMs) throws InterruptedException {
    return expiredRequestSemaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
  }

  public ResourceRequestState getContainerRequestState() throws Exception {
    Field field = ContainerAllocator.class.getDeclaredField("resourceRequestState");
    field.setAccessible(true);

    return (ResourceRequestState) field.get(this);
  }

  @Override
  protected void runStreamProcessor(SamzaResourceRequest request, String preferredHost) {
    super.runStreamProcessor(request, preferredHost);
    semaphore.release();
  }
}
