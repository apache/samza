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
package org.apache.samza.rest;

import com.google.common.collect.ImmutableMap;
import junit.framework.TestCase;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSamzaRestService extends TestCase {

  private final Server server = Mockito.spy(new Server());

  private final ReadableMetricsRegistry metricsRegistry = Mockito.mock(ReadableMetricsRegistry.class);

  private final ServletContextHandler contextHandler = Mockito.mock(ServletContextHandler.class);

  private final MetricsReporter metricsReporter = Mockito.mock(MetricsReporter.class);

  private SamzaRestService samzaRestService;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    samzaRestService = new SamzaRestService(server, metricsRegistry,
                                            ImmutableMap.of("testReporter", metricsReporter), contextHandler);
  }

  @Test
  public void testStartShouldStartTheMetricsReportersAndServer() throws Exception {
    Connector connector = Mockito.mock(Connector.class);
    int testServerPort = 100;
    Mockito.doReturn(testServerPort).when(connector).getPort();
    Mockito.when(server.getConnectors()).thenReturn(new Connector[]{connector});
    Mockito.doNothing().when(server).start();
    samzaRestService.start();
    Mockito.verify(metricsReporter).start();
    Mockito.verify(metricsReporter).register("SamzaRest", metricsRegistry);
    Mockito.verify(server).start();
  }

  @Test
  public void testStopShouldStopTheMetricsReportersAndStopTheServer() throws Exception {
    samzaRestService.stop();
    Mockito.verify(metricsReporter).stop();
    Mockito.verify(server).stop();
  }
}
