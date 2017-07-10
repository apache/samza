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
import java.net.URL;
import junit.framework.Assert;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.samza.container.ContainerHeartbeatResponse;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.job.yarn.YarnAppState;
import org.apache.samza.job.yarn.YarnContainer;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.util.Util;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestYarnContainerHeartbeatServlet {

  private YarnContainer container;
  private YarnAppState yarnAppState;
  private HttpServer webApp;
  private ObjectMapper mapper;

  private ContainerHeartbeatResponse heartbeat;

  @Before
  public void setup()
      throws Exception {
    container = mock(YarnContainer.class);
    ReadableMetricsRegistry registry = new MetricsRegistryMap("test-registry");

    yarnAppState =
        new YarnAppState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "testHost", 1, 1);
    webApp = new HttpServer("/", 0, "", new ServletHolder(new DefaultServlet()));
    webApp.addServlet("/", new YarnContainerHeartbeatServlet(yarnAppState, registry));
    webApp.start();
    mapper = new ObjectMapper();
  }

  @After
  public void cleanup()
      throws Exception {
    webApp.stop();
  }

  @Test
  public void testContainerHeartbeatWhenValid()
      throws IOException {
    String VALID_CONTAINER_ID = "container_1350670447861_0003_01_000002";
    when(container.id()).thenReturn(ConverterUtils.toContainerId(VALID_CONTAINER_ID));
    yarnAppState.runningYarnContainers.put(VALID_CONTAINER_ID, container);
    URL url = new URL(webApp.getUrl().toString() + "containerHeartbeat?executionContainerId=" + VALID_CONTAINER_ID);
    String response = Util.read(url, 1000);
    heartbeat = mapper.readValue(response, ContainerHeartbeatResponse.class);
    Assert.assertTrue(heartbeat.isAlive());
  }

  @Test
  public void testContainerHeartbeatWhenInvalid()
      throws IOException {
    String VALID_CONTAINER_ID = "container_1350670447861_0003_01_000003";
    String INVALID_CONTAINER_ID = "container_1350670447861_0003_01_000002";
    when(container.id()).thenReturn(ConverterUtils.toContainerId(VALID_CONTAINER_ID));
    yarnAppState.runningYarnContainers.put(VALID_CONTAINER_ID, container);
    URL url = new URL(webApp.getUrl().toString() + "containerHeartbeat?executionContainerId=" + INVALID_CONTAINER_ID);
    String response = Util.read(url, 1000);
    heartbeat = mapper.readValue(response, ContainerHeartbeatResponse.class);
    Assert.assertFalse(heartbeat.isAlive());
  }
}
