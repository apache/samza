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
package org.apache.samza.coordinator.communication;

import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.coordinator.server.HttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;


/**
 * Implements HTTP-based communication between the coordinator and workers.
 */
public class HttpCoordinatorToWorkerCommunicationFactory implements CoordinatorToWorkerCommunicationFactory {
  @Override
  public CoordinatorCommunication coordinatorCommunication(CoordinatorCommunicationContext context) {
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(context.getConfigForFactory());
    JobModelHttpServlet jobModelHttpServlet = new JobModelHttpServlet(context.getJobInfoProvider(),
        new JobModelHttpServlet.Metrics(context.getMetricsRegistry()));
    HttpServer httpServer = new HttpServer("/", clusterManagerConfig.getCoordinatorUrlPort(), null,
        new ServletHolder(DefaultServlet.class));
    httpServer.addServlet("/", jobModelHttpServlet);
    return new HttpCoordinatorCommunication(httpServer);
  }
}
