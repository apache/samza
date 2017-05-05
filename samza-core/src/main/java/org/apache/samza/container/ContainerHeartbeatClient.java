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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.samza.util.Util;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The client responsible to make requests to the heartbeat endpoint on the JobCoordinator side
 * and receives a response stating if the container is valid.
 * The expected endpoint on the JobCoordinator side is `/containerHeartbeat`
 * which takes `executionContainerId` (eg: YARN container Id) as a query parameter to validate against.
 */
public class ContainerHeartbeatClient {
  private static Logger log = LoggerFactory.getLogger(ContainerHeartbeatClient.class);
  private final String coordinatorUrl;
  private final String executionEnvContainerId;
  private final String heartbeatEndpoint = "%scontainerHeartbeat?executionContainerId=%s";
  private static final int NUMBER_OF_RETRIES = 3;
  private static final int TIMEOUT = 5000;
  private static final int HTTP_OK = 200;
  private static final int BACKOFF_MULTIPLIER = 2;

  public ContainerHeartbeatClient(String coordinatorUrl, String executionEnvContainerId) {
    this.coordinatorUrl = coordinatorUrl;
    this.executionEnvContainerId = executionEnvContainerId;
  }

  public ContainerHeartbeatResponse requestHeartbeat() {
    String queryUrl = String.format(heartbeatEndpoint, coordinatorUrl, executionEnvContainerId);
    ObjectMapper mapper = new ObjectMapper();
    ContainerHeartbeatResponse response;
    String reply = "";
    try {
      reply = httpGet(new URL(queryUrl));
      log.debug("Container Heartbeat got response {}", reply);
      response = mapper.readValue(reply, ContainerHeartbeatResponse.class);
      return response;
    } catch (IOException e) {
      log.error("Error in container heart beat protocol. Query url: {} response: {}", queryUrl, reply);
      response = new ContainerHeartbeatResponse();
      response.setAlive(false);
      return response;
    }
  }



  private String httpGet(URL url) throws IOException {
    int currentTry;
    HttpURLConnection conn;
    int delaySeconds = 1;

    for (currentTry = 0; currentTry < NUMBER_OF_RETRIES; currentTry++) {
      conn = Util.getHttpConnection(url, TIMEOUT);
      try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        if (conn.getResponseCode() != HTTP_OK) {
          log.warn("HTTP error fetching url {}. Returned status code {}", url.toString(), conn.getResponseCode());
          throw new IOException(String.format("HTTP error fetching url %s. Returned status code %d", url.toString(), conn.getResponseCode()));
        } else {
          return br.lines().collect(Collectors.joining());
        }
      } catch (Exception e) {
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(delaySeconds));
          delaySeconds = delaySeconds * BACKOFF_MULTIPLIER;
        } catch (InterruptedException ignored) { }
      }
    }
    throw new IOException("Could not get a response from the container heartbeat endpoint");
  }
}

