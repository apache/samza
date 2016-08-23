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

import joptsimple.OptionSet;
import org.apache.samza.config.MapConfig;
import org.apache.samza.monitor.SamzaMonitorService;
import org.apache.samza.monitor.ScheduledExecutorSchedulingProvider;
import org.apache.samza.util.CommandLine;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


/**
 * The main Java class for the Samza REST API. It runs an embedded Jetty server so it can be deployed as a Jar file.
 *
 * This class can be started from the command line by providing the --config-path parameter to a Samza REST config file
 * which will be used to configure the default resources exposed by the API.
 *
 * It can also be managed programmatically using the
 * {@link org.apache.samza.rest.SamzaRestService#addServlet(javax.servlet.Servlet, String)},
 * {@link #start()} and {@link #stop()} methods.
 */
public class SamzaRestService {

  private static final Logger log = LoggerFactory.getLogger(SamzaRestService.class);

  private final Server server;
  private final ServletContextHandler context;


  public SamzaRestService(SamzaRestConfig config) {
    log.info("Creating new SamzaRestService with config: {}", config);
    server = new Server(config.getPort());

    context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);
  }

  /**
   * Command line interface to run the server.
   *
   * @param args arguments supported by {@link org.apache.samza.util.CommandLine}.
   *             In particular, --config-path and --config-factory are used to read the Samza REST config file.
   * @throws Exception if the server could not be successfully started.
   */
  public static void main(String[] args)
      throws Exception {
    try {
      SamzaRestConfig config = parseConfig(args);
      SamzaRestService restService = new SamzaRestService(config);

      // Add applications
      SamzaRestApplication samzaRestApplication = new SamzaRestApplication(config);
      ServletContainer container = new ServletContainer(samzaRestApplication);
      restService.addServlet(container, "/*");

      // Schedule monitors to run
      ScheduledExecutorService schedulingService = Executors.newScheduledThreadPool(1);
      ScheduledExecutorSchedulingProvider schedulingProvider = new ScheduledExecutorSchedulingProvider(schedulingService);
      SamzaMonitorService monitorService = new SamzaMonitorService(config, schedulingProvider);
      monitorService.start();

      restService.runBlocking();
      monitorService.stop();
    } catch (Throwable t) {
      log.error("Exception in main.", t);
    }
  }

  /**
   * Reads a {@link org.apache.samza.config.Config} from command line parameters.
   * @param args  the command line parameters supported by {@link org.apache.samza.util.CommandLine}.
   * @return      the parsed {@link org.apache.samza.config.Config}.
   */
  private static SamzaRestConfig parseConfig(String[] args) {
    CommandLine cmd = new CommandLine();
    OptionSet options = cmd.parser().parse(args);
    MapConfig cfg = cmd.loadConfig(options);
    return new SamzaRestConfig(new MapConfig(cfg));
  }

  /**
   * Adds the specified {@link javax.servlet.Servlet} to the server at the specified path.
   * @param servlet the {@link javax.servlet.Servlet} to be added.
   * @param path    the path for the servlet.
   */
  public void addServlet(Servlet servlet, String path) {
    log.info("Adding servlet {} for path {}", servlet, path);
    ServletHolder holder = new ServletHolder(servlet);
    context.addServlet(holder, path);
    holder.setInitOrder(0);
  }

  /**
   * Runs the server and waits for it to finish.
   *
   * @throws Exception if the server could not be successfully started.
   */
  private void runBlocking()
      throws Exception {
    try {
      start();
      server.join();
    } finally {
      server.destroy();
      log.info("Server terminated.");
    }
  }

  /**
   * Starts the server asynchronously. To stop the server, see {@link #stop()}.
   *
   * @throws Exception if the server could not be successfully started.
   */
  public void start()
      throws Exception {
    log.info("Starting server on port {}", server.getConnectors()[0].getPort());
    server.start();
    log.info("Server is running");
  }

  /**
   * Stops the server.
   *
   * @throws Exception if the server could not be successfully stopped.
   */
  public void stop()
      throws Exception {
    log.info("Stopping server");
    server.stop();
    log.info("Server is stopped");
  }
}
