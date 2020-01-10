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

import java.time.Duration;
import java.util.UUID;

/**
 * Encapsulates the request sent from the external controller to the JobCoordinator to take a container placement action
 */
public class ContainerPlacementRequestMessage extends ContainerPlacementMessage {

  public ContainerPlacementRequestMessage(UUID uuid, String deploymentId, String processorId, String destinationHost,
      Duration requestExpiry, long timestamp) {
    super(uuid, deploymentId, processorId, destinationHost, requestExpiry, StatusCode.CREATED, timestamp);
  }

  public ContainerPlacementRequestMessage(UUID uuid, String deploymentId, String processorId, String destinationHost,
      long timestamp) {
    super(uuid, deploymentId, processorId, destinationHost, StatusCode.CREATED, timestamp);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ContainerPlacementRequestMessage{");
    sb.append(" UUID: ").append(uuid);
    sb.append(", Processor ID: ").append(processorId);
    sb.append(", deploymentId='").append(deploymentId).append('\'');
    sb.append(", destinationHost='").append(destinationHost).append('\'');
    sb.append(", requestExpiry=").append(requestExpiry);
    sb.append(", statusCode=").append(statusCode);
    sb.append(", timestamp=").append(timestamp);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
}
