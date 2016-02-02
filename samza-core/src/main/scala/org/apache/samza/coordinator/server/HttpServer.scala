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

package org.apache.samza.coordinator.server;

import javax.servlet.Servlet
import org.apache.samza.SamzaException
import org.eclipse.jetty.server.Connector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import java.net.URL
import org.apache.samza.util.{Util, Logging}


/**
 * <p>A Jetty-based HTTP server. The server allows arbitrary servlets to be added
 * with the addServlet() method. The server is configured to automatically
 * serve static CSS and JS from the /css and /js directories if a
 * resourceBasePath is specified.</p>
 */
class HttpServer(
  /**
   * All servlet paths will be served out of the rootPath. If rootPath is set
   * to /foo, then all servlet paths will be served underneath /foo.
   */
  rootPath: String = "/",

  /**
   * The port that Jetty should bind to. If set to 0, Jetty will bind to a
   * dynamically allocated free port on the machine it's running on. The port
   * can be retrieved by calling .getUrl.
   */
  port: Int = 0,

  /**
   * If specified, tells Jetty where static resources are located inside
   * WEB-INF. This allows HttpServer to serve arbitrary static files that are
   * embedded in a JAR.
   */
  resourceBasePath: String = null,

  /**
   * The SevletHolder to use for static file (CSS/JS) serving.
   */
  defaultHolder: ServletHolder = new ServletHolder(classOf[DefaultServlet])) extends Logging {

  var running = false
  var servlets = Map[String, Servlet]()
  val server = new Server(port)
  val context = new ServletContextHandler(ServletContextHandler.SESSIONS)

  defaultHolder.setName("default")

  /**
   * <p>
   * Add a servlet to the Jetty container. Path can be wild-carded (e.g. /\*
   * or /foo/\*), and is relative to the rootPath specified in the constructor.
   * </p>
   *
   * <p>
   * Servlets with path /bar/\* and rootPath /foo will result in a location of
   * http://localhost/foo/bar.
   * </p>
   */
  def addServlet(path: String, servlet: Servlet) {
    debug("Adding servlet %s to path %s" format (servlet, path))
    servlets += path -> servlet
  }

  /**
   * Start the Jetty server, and begin serving content.
   */
  def start {
    debug("Starting server with rootPath=%s port=%s resourceBasePath=%s" format (rootPath, port, resourceBasePath))
    context.setContextPath(rootPath)
    server.setHandler(context)
    context.addServlet(defaultHolder, "/css/*")
    context.addServlet(defaultHolder, "/js/*")

    // TODO This is where you'd add Hadoop's Kerberos security filters.
    // context.addFilter(classOf[YourApplicationEndpointFilter], "/*", 0)

    if (resourceBasePath != null) {
      context.setResourceBase(getClass.getClassLoader.getResource(resourceBasePath).toExternalForm())
    }

    servlets.foreach {
      case (path, servlet) =>
        context.addServlet(new ServletHolder(servlet), path);
    }

    debug("Starting HttpServer.")
    server.start()
    running = true
    info("Started HttpServer on: %s" format getUrl)
  }

  /**
   * Shutdown the Jetty server.
   */
  def stop {
    running = false
    debug("Stopping server")
    context.stop()
    server.stop()
    info("Stopped server")
  }

  /**
   * Returns the URL for the root of the HTTP server. This method
   */
  def getUrl = {
    if (running) {
      val runningPort = server.getConnectors()(0).asInstanceOf[Connector].getLocalPort()

      new URL("http://" + Util.getLocalHost.getHostName + ":" + runningPort + rootPath)
    } else {
      throw new SamzaException("HttpServer is not currently running, so URLs are not available for it.")
    }
  }
}
