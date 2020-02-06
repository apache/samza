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
 * Encapsulates the value for the response sent from the JobCoordinator for a container placement action
 */
public class ContainerPlacementResponseMessage extends ContainerPlacementMessage {
  // Returned status of the request
  private String responseMessage;

  public ContainerPlacementResponseMessage(UUID uuid, String deploymentId, String processorId, String destinationHost,
      Duration requestExpiry, StatusCode statusCode, String responseMessage, long timestamp) {
    super(uuid, deploymentId, processorId, destinationHost, requestExpiry, statusCode, timestamp);
    Preconditions.checkNotNull(responseMessage);
    this.responseMessage = responseMessage;
  }

  public ContainerPlacementResponseMessage(UUID uuid, String deploymentId, String processorId, String destinationHost,
      StatusCode statusCode, String responseMessage, long timestamp) {
    this(uuid, deploymentId, processorId, destinationHost, null, statusCode, responseMessage, timestamp);
  }

  public static ContainerPlacementResponseMessage fromContainerPlacementRequestMessage(
      ContainerPlacementRequestMessage requestMessage, StatusCode statusCode, String responseMessage, long timestamp) {
    return new ContainerPlacementResponseMessage(requestMessage.getUuid(), requestMessage.getDeploymentId(), requestMessage.getProcessorId(),
        requestMessage.getDestinationHost(), requestMessage.getRequestExpiry(), statusCode, responseMessage, timestamp);
  }

  public String getResponseMessage() {
    return responseMessage;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ContainerPlacementResponseMessage{");
    sb.append(" UUID: ").append(uuid);
    sb.append(", Processor ID: ").append(processorId);
    sb.append(", deploymentId='").append(deploymentId).append('\'');
    sb.append(", destinationHost='").append(destinationHost).append('\'');
    sb.append(", requestExpiry=").append(requestExpiry);
    sb.append(", statusCode=").append(statusCode);
    sb.append(", timestamp=").append(timestamp);
    sb.append(", responseMessage='").append(responseMessage).append('\'');
    sb.append('}');
    return sb.toString();
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
