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

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.util.Logging
import javax.management.remote.{JMXConnector, JMXConnectorFactory, JMXServiceURL}
import java.io.IOException


class TestJmxServer extends Logging {
  @Test
  def serverStartsUp {
    var jmxServer:JmxServer = null

    try {
      jmxServer = new JmxServer

      println("JmxServer = %s" format jmxServer)
      println("Got jmxServer on port " + jmxServer.getRegistryPort)

      val jmxURL = new JMXServiceURL(jmxServer.getJmxUrl)
      var jmxConnector:JMXConnector = null
      try {
        jmxConnector = JMXConnectorFactory.connect(jmxURL, null)
        val connection = jmxConnector.getMBeanServerConnection()
        assertTrue("Connected but mbean count is somehow 0", connection.getMBeanCount.intValue() > 0)
      } catch {
        case ioe:IOException => fail("Couldn't open connection to local JMX server")
      }finally {
        if(jmxConnector != null) jmxConnector.close
      }

    } finally {
      if (jmxServer != null) jmxServer.stop
    }

  }

}
