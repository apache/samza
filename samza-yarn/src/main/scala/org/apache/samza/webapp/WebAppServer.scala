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

package org.apache.samza.webapp

import javax.servlet.Servlet
import org.eclipse.jetty.server.{ Connector, Server }
import org.eclipse.jetty.servlet.{ DefaultServlet, ServletHolder }
import org.eclipse.jetty.webapp.WebAppContext
import org.apache.samza.SamzaException

class WebAppServer(rootPath: String) {
  val server = new Server(0)
  val context = new WebAppContext
  var port: Int = 0

  // add a default holder to deal with static files
  val defaultHolder = new ServletHolder(classOf[DefaultServlet])
  defaultHolder.setName("default")
  context.setContextPath(rootPath)
  context.addServlet(defaultHolder, "/css/*")
  context.addServlet(defaultHolder, "/js/*")

  // TODO This is where you'd add Hadoop's Kerberos security filters.
  // context.addFilter(classOf[YourApplicationEndpointFilter], "/*", 0)

  def addServlet(subPath: String, servlet: Servlet) {
    context.addServlet(new ServletHolder(servlet), subPath)
  }

  def start {
    context.setContextPath("/")
    context.setResourceBase(getClass.getClassLoader.getResource("scalate").toExternalForm)
    server.setHandler(context)
    server.start
    // retrieve the real port
    try {
      val connector : Connector = server.getConnectors()(0).asInstanceOf[Connector]
      port = connector.getLocalPort
    } catch {
      case e: Throwable => {
        throw new SamzaException("Error when getting the port", e)
      }
    }
  }
}
