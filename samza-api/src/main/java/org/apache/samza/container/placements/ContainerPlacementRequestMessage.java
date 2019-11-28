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

import java.util.UUID;


/**
 * Encapsulates the request sent from the external controller to the JobCoordinator to take a container placement action
 */
public class ContainerPlacementRequestMessage extends ContainerPlacementMessage {

  public ContainerPlacementRequestMessage(UUID uuid, String applicationId, String processorId, String destinationHost, Long requestExpiry) {
    super(uuid, applicationId, processorId, destinationHost, requestExpiry, StatusCode.CREATED);
  }

  public ContainerPlacementRequestMessage(UUID uuid, String applicationId, String processorId, String destinationHost) {
    super(uuid, applicationId, processorId, destinationHost, StatusCode.CREATED);
  }

  @Override
  public String toString() {
    return "ContainerPlacementRequestMessage{" + "uuid=" + uuid + ", applicationId='" + applicationId + '\''
        + ", processorId='" + processorId + '\'' + ", destinationHost='" + destinationHost + '\'' + ", requestExpiry="
        + requestExpiry + ", statusCode=" + statusCode + "} " + super.toString();
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
