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
package org.apache.samza.clustermanager.container.placements;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.samza.clustermanager.SamzaResourceRequest;
import org.apache.samza.container.placements.ContainerPlacementMessage;
import org.apache.samza.container.placements.ContainerPlacementRequestMessage;


/**
 * Maintains the state for any control action like move or restart container issued externally, this action metadata
 * is accessed by multiple threads i.e the ContainerAllocator thread and the thread running the {@link org.apache.samza.clustermanager.ClusterResourceManager.Callback}
 * i.e ContainerProcessManager. Allocator thread issues a stop on active container and waits for {@code containerStatus}
 * to be changed by the {@link org.apache.samza.clustermanager.ClusterResourceManager.Callback} thread.
 *
 * This class is thread safe
 */
public class ContainerPlacementMetadata {
  /**
   * State to track container failover
   */
  public enum ContainerStatus { RUNNING, STOP_IN_PROGRESS, STOPPED }
  // Container Placement request message
  private final ContainerPlacementRequestMessage requestMessage;
  // Host where the container is actively running
  private final String sourceHost;
  // Resource requests issued during this failover
  private final Set<SamzaResourceRequest> resourceRequests;
  // State of the control action
  private ContainerPlacementMessage.StatusCode actionStatus;
  // State of the active container to track failover
  private ContainerStatus containerStatus;
  // Represents information on current status of action
  private String responseMessage;

  public ContainerPlacementMetadata(ContainerPlacementRequestMessage requestMessage, String sourceHost) {
    this.requestMessage = requestMessage;
    this.sourceHost = sourceHost;
    this.resourceRequests = new HashSet<>();
    this.containerStatus = ContainerStatus.RUNNING;
    this.actionStatus = ContainerPlacementMessage.StatusCode.ACCEPTED;
  }

  // Add the samzaResourceRequest to the list of resource requests associated with this failover
  public synchronized void recordResourceRequest(SamzaResourceRequest samzaResourceRequest) {
    this.resourceRequests.add(samzaResourceRequest);
  }

  public synchronized boolean containsResourceRequest(SamzaResourceRequest samzaResourceRequest) {
    return resourceRequests.contains(samzaResourceRequest);
  }

  public synchronized void setActionStatus(ContainerPlacementMessage.StatusCode statusCode, String responseMessage) {
    this.actionStatus = statusCode;
    this.responseMessage = responseMessage;
  }

  public synchronized ContainerStatus getContainerStatus() {
    return containerStatus;
  }

  public synchronized void setContainerStatus(ContainerStatus containerStatus) {
    this.containerStatus = containerStatus;
  }

  public synchronized ContainerPlacementMessage.StatusCode getActionStatus() {
    return actionStatus;
  }

  public synchronized String getResponseMessage() {
    return responseMessage;
  }

  public String getSourceHost() {
    return sourceHost;
  }

  public Optional<Duration> getRequestActionExpiryTimeout() {
    return requestMessage.getRequestExpiryTimeout();
  }

  public UUID getUuid() {
    return requestMessage.getUuid();
  }

  @Override
  public String toString() {
    return "ContainerPlacementMetadata{" + "requestMessage=" + requestMessage + ", sourceHost='" + sourceHost + '\''
        + ", resourceRequests=" + resourceRequests + ", actionStatus=" + actionStatus + ", containerStatus="
        + containerStatus + ", responseMessage='" + responseMessage + '\'' + '}';
  }
}