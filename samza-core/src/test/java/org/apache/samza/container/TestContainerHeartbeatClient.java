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

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import junit.framework.Assert;
import org.apache.samza.coordinator.server.HttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestContainerHeartbeatClient {

  private String output;
  private HttpServer webApp;
  private ContainerHeartbeatClient client;

  @Before
  public void setup() {
    webApp = new HttpServer("/", 0, "", new ServletHolder(new DefaultServlet()));
    HttpServlet servlet = new HttpServlet() {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse resp)
          throws ServletException, IOException {
        PrintWriter printWriter = resp.getWriter();
        printWriter.write(output);
      }
    };
    webApp.addServlet("/containerHeartbeat", servlet);
    webApp.start();
    client = new ContainerHeartbeatClient(webApp.getUrl().toString(), "FAKE_CONTAINER_ID");
  }

  @After
  public void teardown() {
    webApp.stop();
  }

  @Test
  public void testClientResponseForHeartbeatAlive() {
    output = "{\"alive\": true}";
    ContainerHeartbeatResponse response = client.requestHeartbeat();
    Assert.assertTrue(response.isAlive());
  }

  @Test
  public void testClientResponseForHeartbeatDead() {
    output = "{\"alive\": false}";
    ContainerHeartbeatResponse response = client.requestHeartbeat();
    Assert.assertFalse(response.isAlive());
  }

  @Test
  public void testClientResponseOnBadRequest() {
    HttpServer webApp = new HttpServer("/", 0, "", new ServletHolder(new DefaultServlet()));
    HttpServlet servlet = new HttpServlet() {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse resp)
          throws ServletException, IOException {
        resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "BAD_REQUEST");
      }
    };
    webApp.addServlet("/containerHeartbeat", servlet);
    webApp.start();
    ContainerHeartbeatClient client = new ContainerHeartbeatClient(webApp.getUrl().toString(), "FAKE_CONTAINER_ID");
    ContainerHeartbeatResponse response = client.requestHeartbeat();
    Assert.assertFalse(response.isAlive());
    webApp.stop();
  }
}
