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

package org.apache.samza.metrics

import org.apache.samza.util.{Util, Logging}
import java.rmi.server.RMIServerSocketFactory
import java.net.ServerSocket
import java.rmi.registry.LocateRegistry
import management.ManagementFactory
import java.util
import javax.management.remote.{ JMXConnectorServerFactory, JMXServiceURL }

/**
 * Programmatically start the JMX server and its accompanying RMI server. This is necessary in order to reliably
 * and - heh - simply request and know a dynamic port such that processes on the same machine do not collide when
 * opening JMX servers on the same port. Server will start upon instantiation.
 *
 * Note: This server starts the JMX server, which runs in a separate thread and must be stopped or it will prevent
 * the process from ending.
 *
 * @param requestedPort Port on which to start JMX server, 0 for ephemeral
 */
class JmxServer(requestedPort: Int) extends Logging {
  val hostname = Util.getLocalHost.getHostName

  def this() = this(0)

  // Instance construction
  val (jmxServer, url, registryPort, serverPort) = {
    // An RMIServerSocketFactory that will tell what port it opened up.  Imagine that.
    class UpfrontRMIServerSocketFactory extends RMIServerSocketFactory {
      var lastSS: ServerSocket = null
      def createServerSocket(port: Int): ServerSocket = {
        lastSS = new ServerSocket(port)
        lastSS
      }
    }

    // Check if the system property has been set and, if not, set it to what we need. Warn otherwise.
    def updateSystemProperty(prop: String, value: String) = {
      val existingProp = System.getProperty(prop)
      if (existingProp == null) {
        debug("Setting new system property of %s to %s" format (prop, value))
        System.setProperty(prop, value)
      } else {
        info("Not overriding system property %s as already has value %s" format (prop, existingProp))
      }
    }

    if (System.getProperty("com.sun.management.jmxremote") != null) {
      warn("System property com.sun.management.jmxremote has been specified, starting the JVM's JMX server as well. " +
        "This behavior is not well defined and our values will collide with any set on command line.")
    }

    info("According to Util.getLocalHost.getHostName we are " + hostname)
    updateSystemProperty("com.sun.management.jmxremote.authenticate", "false")
    updateSystemProperty("com.sun.management.jmxremote.ssl", "false")
    updateSystemProperty("java.rmi.server.hostname", hostname)

    val ssFactory = new UpfrontRMIServerSocketFactory
    LocateRegistry.createRegistry(requestedPort, null, ssFactory)
    val registryPort = ssFactory.lastSS.getLocalPort
    val serverPort = registryPort + 1 // In comparison to the registry port. Tiny chance of collision.  Sigh.
    val mbs = ManagementFactory.getPlatformMBeanServer
    val env = new util.HashMap[String, Object]()
    val url = new JMXServiceURL("service:jmx:rmi://localhost:" + serverPort + "/jndi/rmi://localhost:" + registryPort + "/jmxrmi")
    val jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs)

    (jmxServer, url.toString, registryPort, serverPort)
  }

  jmxServer.start
  info("Started " + toString)
  info("If you are tunneling, you might want to try " + toString.replaceAll("localhost", hostname))

  /**
   * Get RMI registry port
   * @return RMI port
   */
  def getRegistryPort = registryPort

  /**
   * Get JMX server port
   */
  def getServerPort = serverPort


  /**
   * Get Jmx URL for this server
   * @return Jmx-Style URL string
   */
  def getJmxUrl = url

  /**
   * Stop the JMX server. Must be called at program end or will prevent termination.
   */
  def stop = jmxServer.stop

  override def toString = "JmxServer registry port=%d server port=%d url=%s" format (getRegistryPort, getServerPort, getJmxUrl)

  def getTunnelingJmxUrl = getJmxUrl.replaceAll("localhost", hostname)
}
