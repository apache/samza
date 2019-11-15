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
import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates the response sent from the JobCoordinator for a container placement action
 */
public class ContainerPlacementResponseMessage extends ContainerPlacementMessage {
  // Returned status of the request
  private String responseMessage;

  public ContainerPlacementResponseMessage(UUID uuid, String applicationId, String processorId, String destinationHost,
      Long requestExpiry, StatusCode statusCode, String responseMessage) {
    super(uuid, applicationId, processorId, destinationHost, requestExpiry, statusCode);
    Preconditions.checkNotNull(responseMessage);
    this.responseMessage = responseMessage;
  }

  public ContainerPlacementResponseMessage(UUID uuid, String applicationId, String processorId, String destinationHost,
      StatusCode statusCode, String responseMessage) {
    this(uuid, applicationId, processorId, destinationHost, null, statusCode, responseMessage);
  }

  public static ContainerPlacementResponseMessage fromContainerPlacementRequestMessage(
      ContainerPlacementRequestMessage requestMessage, StatusCode statusCode, String responseMessage) {
    return new ContainerPlacementResponseMessage(requestMessage.getUuid(), requestMessage.getApplicationId(), requestMessage.getProcessorId(),
        requestMessage.getDestinationHost(), requestMessage.getRequestExpiry(), statusCode, responseMessage);
  }

  public String getResponseMessage() {
    return responseMessage;
  }

  @Override
  public String toString() {
    return "ContainerPlacementResponseMessage{" + "responseMessage='" + responseMessage + '\'' + ", uuid=" + uuid
        + ", applicationId='" + applicationId + '\'' + ", processorId='" + processorId + '\'' + ", destinationHost='"
        + destinationHost + '\'' + ", requestExpiry=" + requestExpiry + ", statusCode=" + statusCode + "} "
        + super.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ContainerPlacementResponseMessage that = (ContainerPlacementResponseMessage) o;
    return getResponseMessage().equals(that.getResponseMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getResponseMessage());
  }
}
