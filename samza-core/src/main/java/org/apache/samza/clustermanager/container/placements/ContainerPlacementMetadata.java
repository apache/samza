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

import java.util.HashSet;
import java.util.Set;
import org.apache.samza.clustermanager.SamzaResourceRequest;

/**
 * Maintains the state for any control action like move or restart container issued externally
 */
public class ContainerPlacementMetadata {
  // Logical container id 0,1,2,3,
  private final String processorId;
  // Last known deployment id of the container
  private final String containerId;
  private final String sourceHost;
  private final String destinationHost;
  // Expiry timeout for current request
  private final Long requestExpiryTimeout;
  // Resource requests issued during this failover
  private final Set<SamzaResourceRequest> resourceRequests;
  private ContainerPlacementStatus actionStatus;
  /**
   * State to track container failover
   */
  private final Object activeContainerStopped;

  public ContainerPlacementMetadata(String processorId, String containerId, String sourceHost, String destinationHost,
      ContainerPlacementStatus actionStatus, Long requestExiryTimeout) {
    this.containerId = containerId;
    this.processorId = processorId;
    this.sourceHost = sourceHost;
    this.destinationHost = destinationHost;
    this.resourceRequests = new HashSet<>();
    this.activeContainerStopped = false;
    this.requestExpiryTimeout = requestExiryTimeout;
    this.actionStatus = actionStatus;
  }

  // Add the samzaResourceRequest to the list of resource requests associated with this failover
  public synchronized void recordResourceRequest(SamzaResourceRequest samzaResourceRequest) {
    this.resourceRequests.add(samzaResourceRequest);
  }

  public synchronized Object getActiveContainerStopped() {
    return activeContainerStopped;
  }

  public synchronized void setActionStatus(ContainerPlacementStatus.StatusCode statusCode) {
    this.actionStatus.status = statusCode;
  }

  public synchronized void setActionStatus(ContainerPlacementStatus.StatusCode statusCode, String responseMessage) {
    this.actionStatus.status = statusCode;
    this.actionStatus.responseMessage = responseMessage;
  }

  public String getSourceHost() {
    return sourceHost;
  }

  public Long getRequestActionExpiryTimeout() {
    return requestExpiryTimeout;
  }

  @Override
  public String toString() {
    return "ContainerPlacementMetaData{" + "processorId='" + processorId + '\'' + ", containerId='" + containerId + '\''
        + ", sourceHost='" + sourceHost + '\'' + ", destinationHost='" + destinationHost + '\'' + ", actionStatus="
        + actionStatus + ", resourceRequests=" + resourceRequests + '}';
  }
}