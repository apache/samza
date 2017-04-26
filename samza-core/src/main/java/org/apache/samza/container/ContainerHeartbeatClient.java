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
import java.util.Random;
import java.util.stream.Collectors;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContainerHeartbeatClient {
  private static Logger log = LoggerFactory.getLogger(ContainerHeartbeatClient.class);
  private final String coordinatorUrl;
  private final String executionEnvContainerId;
  private final int numberOfRetries = 3;
  private final int timeout = 5000;

  public ContainerHeartbeatClient(String coordinatorUrl, String executionEnvContainerId) {
    this.coordinatorUrl = coordinatorUrl;
    this.executionEnvContainerId = executionEnvContainerId;
  }

  public boolean isAlive() {
    String queryUrl = coordinatorUrl + "containerHeartbeat?executionContainerId=" + executionEnvContainerId;
    ObjectMapper mapper = new ObjectMapper();
    ContainerHeartbeatClient.ContainerHeartbeatResponse response;
    String reply = "";
    try {
      reply = retryableHTTPGetWithBackOff(new URL(queryUrl), numberOfRetries, timeout);
      log.debug("Container Heartbeat got response {}", reply);
      response = mapper.readValue(reply, ContainerHeartbeatClient.ContainerHeartbeatResponse.class);
      return response.isAlive();
    } catch (IOException e) {
      log.error("Error in container heartbeat protocol got endpoint: {} and reply: {}", queryUrl, reply);
      return false;
    }
  }

  public static class ContainerHeartbeatResponse {
    private boolean alive;

    public boolean isAlive() {
      return alive;
    }

    public void setAlive(boolean alive) {
      this.alive = alive;
    }
  }

  private String retryableHTTPGetWithBackOff(URL url, int numRetry, int timeout) throws IOException {
    int currentTry;
    Random r = new Random();
    HttpURLConnection conn = getHttpConnection(url, timeout);

    for (currentTry = 0; currentTry < numRetry; currentTry++) {
      try {
        if (conn.getResponseCode() != 200) {
          log.warn("error: fetching url {} returned code {}", url.toString(), conn.getResponseCode());
          conn = getHttpConnection(url, timeout);
          throw new IOException("Http Error during heartbeat");
        } else {
          BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
          String body = br.lines().collect(Collectors.joining());
          br.close();
          return body;
        }
      } catch (Exception e) {
        try {
          Thread.sleep((int) Math.round(Math.pow(2, currentTry)) * 1000);
        } catch (InterruptedException ignored) { }
      }
    }
    throw new IOException("Could not get a response from the container heartbeat endpoint");
  }

  private HttpURLConnection getHttpConnection(URL url, int timeout) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setConnectTimeout(timeout);
    conn.setReadTimeout(timeout);
    return conn;
  }
}
