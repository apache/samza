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

package org.apache.samza.clustermanager.container.placement;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.container.placement.ContainerPlacementMessage;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.apache.samza.container.placement.ContainerPlacementResponseMessage;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestContainerPlacementObjectMapper {

  @Test
  public void testIncomingContainerMessageSerDe() throws IOException {
    testContainerPlacementRequestMessage(
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "4", "ANY_HOST", System.currentTimeMillis()));
    testContainerPlacementRequestMessage(
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "4", "ANY_HOST",
            Duration.ofSeconds(10), System.currentTimeMillis()));
  }

  @Test
  public void testOutgoingContainerMessageSerDe() throws IOException {
    testContainerPlacementResponseMessage(new ContainerPlacementResponseMessage(UUID.randomUUID(), "app-attempt-001",
        Integer.toString(new Random().nextInt(5)), "ANY_HOST", ContainerPlacementMessage.StatusCode.BAD_REQUEST,
        "Request ignored redundant", System.currentTimeMillis()));
    testContainerPlacementResponseMessage(new ContainerPlacementResponseMessage(UUID.randomUUID(), "app-attempt-001",
        Integer.toString(new Random().nextInt(5)), "ANY_HOST", Duration.ofSeconds(10),
        ContainerPlacementMessage.StatusCode.IN_PROGRESS, "Request is in progress", System.currentTimeMillis()));
  }

  private void testContainerPlacementRequestMessage(ContainerPlacementRequestMessage requestMessage)
      throws IOException {
    ObjectMapper objectMapper = ContainerPlacementMessageObjectMapper.getObjectMapper();
    ContainerPlacementMessage message =
        objectMapper.readValue(objectMapper.writeValueAsString(requestMessage), ContainerPlacementMessage.class);
    assertTrue(message instanceof ContainerPlacementRequestMessage);
    ContainerPlacementRequestMessage deserializedRequest = (ContainerPlacementRequestMessage) message;
    assertEquals(requestMessage, deserializedRequest);
  }

  private void testContainerPlacementResponseMessage(ContainerPlacementResponseMessage responseMessage)
      throws IOException {
    ObjectMapper objectMapper = ContainerPlacementMessageObjectMapper.getObjectMapper();
    ContainerPlacementMessage message =
        objectMapper.readValue(objectMapper.writeValueAsString(responseMessage), ContainerPlacementMessage.class);
    assertTrue(message instanceof ContainerPlacementResponseMessage);
    ContainerPlacementResponseMessage deserializedResponse = (ContainerPlacementResponseMessage) message;
    assertEquals(responseMessage, deserializedResponse);
  }
}
