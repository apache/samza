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

package org.apache.samza.container;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.stream.Collectors;
import org.apache.samza.util.Util;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Issues a heartbeat to the coordinator and returns a
 * {@link ContainerHeartbeatResponse}.
 * Here's the description of the protocol between the
 * container and the coordinator:
 *
 * The heartbeat request contains a <code> executionContainerId
 * </code> that identifies the container from which the
 * request is made. The coordinator validates the provided
 * executionContainerId against its list of containers that should be
 * running and returns a {@link ContainerHeartbeatResponse}.
 *
 * The returned {@link ContainerHeartbeatResponse#isAlive()} is
 * <code> true </code> iff. the coordinator has determined
 * that the container is valid and should continue running.
 */
public class ContainerHeartbeatClient {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerHeartbeatClient.class);
  private static final int NUM_RETRIES = 3;
  private static final int TIMEOUT_MS = 5000;
  private static final int BACKOFF_MULTIPLIER = 2;
  private final String heartbeatEndpoint;

  public ContainerHeartbeatClient(String coordinatorUrl, String executionEnvContainerId) {
    this.heartbeatEndpoint =
        String.format("%scontainerHeartbeat?executionContainerId=%s", coordinatorUrl, executionEnvContainerId);
  }

  /**
   * Issues a heartbeat request to the coordinator and
   * returns the corresponding {@link ContainerHeartbeatResponse}.
   */
  public ContainerHeartbeatResponse requestHeartbeat() {
    ObjectMapper mapper = new ObjectMapper();
    ContainerHeartbeatResponse response;
    String reply = "";
    try {
      reply = httpGet(new URL(heartbeatEndpoint));
      LOG.debug("Container Heartbeat got response {}", reply);
      response = mapper.readValue(reply, ContainerHeartbeatResponse.class);
      return response;
    } catch (IOException e) {
      LOG.error("Error in container heart beat protocol. Query url: {} response: {}", heartbeatEndpoint, reply);
    }
    response = new ContainerHeartbeatResponse(false);
    return response;
  }

  String httpGet(URL url) throws IOException {
    HttpURLConnection conn;
    int delayMillis = 1000;

    for (int currentTry = 0; currentTry < NUM_RETRIES; currentTry++) {
      conn = Util.getHttpConnection(url, TIMEOUT_MS);
      try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
          throw new IOException(String.format("HTTP error fetching url %s. Returned status code %d", url.toString(),
              conn.getResponseCode()));
        } else {
          return br.lines().collect(Collectors.joining());
        }
      } catch (Exception e) {
        LOG.error("Error in heartbeat request", e);
        sleepUninterruptibly(delayMillis);
        delayMillis = delayMillis * BACKOFF_MULTIPLIER;
      }
    }
    throw new IOException(String.format("Error fetching url: %s. Tried %d time(s).", url.toString(), NUM_RETRIES));
  }

  private void sleepUninterruptibly(int delayMillis) {
    try {
      Thread.sleep(delayMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}

