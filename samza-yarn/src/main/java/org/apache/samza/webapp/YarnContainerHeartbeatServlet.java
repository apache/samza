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

package org.apache.samza.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.samza.job.yarn.YarnAppState;
import org.apache.samza.job.yarn.YarnContainer;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class YarnContainerHeartbeatServlet extends HttpServlet {

  private static final String YARN_CONTAINER_ID = "executionContainerId";
  private static final Logger log = LoggerFactory.getLogger(YarnContainerHeartbeatServlet.class);
  public static final String APPLICATION_JSON = "application/json";
  public static final String GROUP = "ContainerHeartbeat";
  private final Counter heartbeatInvalidCount;

  private YarnAppState yarnAppState;
  private JSONObject jsonResponse;

  public YarnContainerHeartbeatServlet(YarnAppState yarnAppState, ReadableMetricsRegistry registry) {
    this.yarnAppState = yarnAppState;
    this.jsonResponse = new JSONObject();
    this.heartbeatInvalidCount = registry.newCounter(GROUP, "heartbeats-invalid");
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    PrintWriter printWriter = resp.getWriter();
    String containerIdParam = req.getParameter(YARN_CONTAINER_ID);
    resp.setContentType(APPLICATION_JSON);
    boolean alive = false;
    ContainerId yarnContainerId;
    try {
      yarnContainerId = ContainerId.fromString(containerIdParam);
      for (YarnContainer yarnContainer : yarnAppState.runningYarnContainers.values()) {
        if (yarnContainer.id().compareTo(yarnContainerId) == 0) {
          alive = true;
        }
      }
      if (!alive) {
        heartbeatInvalidCount.inc();
      }
      this.jsonResponse.put("alive", alive);
      printWriter.write(jsonResponse.toString());
    } catch (IllegalArgumentException e) {
      log.error("Container ID {} passed is invalid", containerIdParam);
      resp.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
    } catch (JSONException e) {
      log.error("Unable to serialize JSON heartbeat response for container ID {} with status {}", containerIdParam,
          alive);
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }
}
