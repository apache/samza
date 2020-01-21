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
package org.apache.samza.container.placement;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates the request or response payload information between the ContainerPlacementService and external
 * controllers issuing placement actions
 */
public abstract class ContainerPlacementMessage {

  public enum StatusCode {
    /**
     * Indicates that the container placement action is created
     */
    CREATED,

    /**
     * Indicates that the container placement action was rejected because request was deemed invalid
     */
    BAD_REQUEST,

    /**
     * Indicates that the container placement action is accepted and waiting to be processed
     */
    ACCEPTED,

    /**
     * Indicates that the container placement action is in progress
     */
    IN_PROGRESS,

    /**
     * Indicates that the container placement action is in progress
     */
    SUCCEEDED,

    /**
     * Indicates that the container placement action is in failed
     */
    FAILED;
  }

  /**
   * Unique identifier of a request or response message. Helps in identifying duplicate request messages.
   */
  protected final UUID uuid;
  /**
   * Unique identifier for a deployment so messages can be invalidated across a job restarts
   * for ex yarn bases cluster manager can be set to app attempt id
   */
  protected final String deploymentId;
  // Logical container Id 0, 1, 2
  protected final String processorId;
  // Destination host where container is desired to be moved
  protected final String destinationHost;
  // Optional request expiry which acts as a timeout for any resource request to cluster resource manager
  protected final Duration requestExpiry;
  // Status of the current request
  protected final StatusCode statusCode;
  // Timestamp of the request or response message
  protected final long timestamp;

  protected ContainerPlacementMessage(UUID uuid, String deploymentId, String processorId, String destinationHost,
      Duration requestExpiry, StatusCode statusCode, long timestamp) {
    this.uuid = Preconditions.checkNotNull(uuid, "uuid cannot be null");
    this.deploymentId = Preconditions.checkNotNull(deploymentId, "deploymentId cannot be null");
    this.processorId = Preconditions.checkNotNull(processorId, "processorId cannot be null");
    this.destinationHost = Preconditions.checkNotNull(destinationHost, "destinationHost cannot be null");
    this.statusCode = Preconditions.checkNotNull(statusCode, "statusCode cannot be null");
    this.timestamp = Preconditions.checkNotNull(timestamp, "Timestamp of the message cannot be null");
    this.requestExpiry = requestExpiry;
  }

  protected ContainerPlacementMessage(UUID uuid, String deploymentId, String processorId, String destinationHost,
      StatusCode statusCode, long timestamp) {
    this(uuid, deploymentId, processorId, destinationHost, null, statusCode, timestamp);
  }

  public String getDeploymentId() {
    return deploymentId;
  }

  public String getProcessorId() {
    return processorId;
  }

  public String getDestinationHost() {
    return destinationHost;
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }

  public Duration getRequestExpiry() {
    return requestExpiry;
  }

  public UUID getUuid() {
    return uuid;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContainerPlacementMessage message = (ContainerPlacementMessage) o;
    return getTimestamp() == message.getTimestamp() && getUuid().equals(message.getUuid()) && getDeploymentId().equals(
        message.getDeploymentId()) && getProcessorId().equals(message.getProcessorId()) && getDestinationHost().equals(
        message.getDestinationHost()) && Objects.equals(getRequestExpiry(), message.getRequestExpiry())
        && getStatusCode() == message.getStatusCode();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getUuid(), getDeploymentId(), getProcessorId(), getDestinationHost(), getRequestExpiry(),
        getStatusCode(), getTimestamp());
  }
}
