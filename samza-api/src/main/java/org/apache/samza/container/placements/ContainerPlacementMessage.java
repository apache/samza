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
package org.apache.samza.container.placements;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.UUID;


/**
 * Encapsulates the request or response payload information between the ContainerPlacementHandler service and external
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
   * UUID attached to a message which helps in identifying duplicate request messages written to metastore and not
   * retake actions even if metastore is eventually consistent
   */
  protected final UUID uuid;
  /**
   * Unique identifier for a deployment so messages can be invalidated across a job restarts
   * for ex yarn bases cluster manager should set this to app attempt id
   */
  protected final String applicationId;
  // Logical container Id 0, 1, 2
  protected final String processorId;
  // Destination host where container is desired to be moved
  protected final String destinationHost;
  // Optional request expiry which acts as a timeout for any resource request to
  protected final Long requestExpiry;
  // Status of the current request
  protected final StatusCode statusCode;

  protected ContainerPlacementMessage(UUID uuid, String applicationId, String processorId, String destinationHost,
      Long requestExpiry, StatusCode statusCode) {
    Preconditions.checkNotNull(uuid, "uuid cannot be null");
    Preconditions.checkNotNull(applicationId, "applicationId cannot be null");
    Preconditions.checkNotNull(processorId, "processorId cannot be null");
    Preconditions.checkNotNull(destinationHost, "destinationHost cannot be null");
    Preconditions.checkNotNull(statusCode, "statusCode cannot be null");
    this.uuid = uuid;
    this.applicationId = applicationId;
    this.processorId = processorId;
    this.destinationHost = destinationHost;
    this.requestExpiry = requestExpiry;
    this.statusCode = statusCode;
  }

  protected ContainerPlacementMessage(UUID uuid, String applicationId, String processorId, String destinationHost,
      StatusCode statusCode) {
    this(uuid, applicationId, processorId, destinationHost, null, statusCode);
  }

  public String getApplicationId() {
    return applicationId;
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

  public Long getRequestExpiry() {
    return requestExpiry;
  }

  public UUID getUuid() {
    return uuid;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContainerPlacementMessage that = (ContainerPlacementMessage) o;
    return uuid.equals(that.uuid) && getApplicationId().equals(that.getApplicationId()) && getProcessorId().equals(
        that.getProcessorId()) && getDestinationHost().equals(that.getDestinationHost())
        && getStatusCode() == that.getStatusCode();
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid, getApplicationId(), getProcessorId(), getDestinationHost(), getStatusCode());
  }
}
