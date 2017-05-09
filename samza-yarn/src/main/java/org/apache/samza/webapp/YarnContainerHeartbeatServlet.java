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
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.samza.container.ContainerHeartbeatResponse;
import org.apache.samza.job.yarn.YarnAppState;
import org.apache.samza.job.yarn.YarnContainer;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Responds to heartbeat requests from the containers with a {@link ContainerHeartbeatResponse}.
 * The heartbeat request contains the <code> executionContainerId </code>
 * which in YARN's case is the YARN container Id.
 * This servlet validates the container Id against the list
 * of running containers maintained in the {@link YarnAppState}.
 * The returned {@link ContainerHeartbeatResponse#isAlive()} is
 * <code> true </code> iff. the container Id exists in {@link YarnAppState#runningYarnContainers}.
 */
public class YarnContainerHeartbeatServlet extends HttpServlet {

  private static final String YARN_CONTAINER_ID = "executionContainerId";
  private static final Logger log = LoggerFactory.getLogger(YarnContainerHeartbeatServlet.class);
  public static final String APPLICATION_JSON = "application/json";
  public static final String GROUP = "ContainerHeartbeat";
  private final Counter heartbeatsInvalidCount;

  private YarnAppState yarnAppState;
  private ObjectMapper mapper;

  public YarnContainerHeartbeatServlet(YarnAppState yarnAppState, ReadableMetricsRegistry registry) {
    this.yarnAppState = yarnAppState;
    this.mapper = new ObjectMapper();
    this.heartbeatsInvalidCount = registry.newCounter(GROUP, "heartbeats-invalid");
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    ContainerId yarnContainerId;
    PrintWriter printWriter = resp.getWriter();
    String containerIdParam = req.getParameter(YARN_CONTAINER_ID);
    ContainerHeartbeatResponse response;
    resp.setContentType(APPLICATION_JSON);
    boolean alive = false;
    try {
      yarnContainerId = ContainerId.fromString(containerIdParam);
      for (YarnContainer yarnContainer : yarnAppState.runningYarnContainers.values()) {
        if (yarnContainer.id().compareTo(yarnContainerId) == 0) {
          alive = true;
        }
      }
      if (!alive) {
        heartbeatsInvalidCount.inc();
      }
      response = new ContainerHeartbeatResponse(alive);
      printWriter.write(mapper.writeValueAsString(response));
    } catch (IllegalArgumentException e) {
      log.error("Container ID {} passed is invalid", containerIdParam);
      resp.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
    }
  }
}
