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

package org.apache.samza.coordinator.server

import org.apache.samza.util.Util
import org.junit.Assert._
import org.junit.Test
import java.net.URL
import org.eclipse.jetty.server.Connector

class TestHttpServer {
  @Test
  def testHttpServerDynamicPort {
    val server = new HttpServer("/test", resourceBasePath = "scalate")
    try {
      server.addServlet("/basic", new BasicServlet())
      server.start
      val body = Util.read(new URL(server.getUrl + "/basic"))
      assertEquals("{\"foo\":\"bar\"}", body)
      val css = Util.read(new URL(server.getUrl + "/css/ropa-sans.css"))
      assertTrue(css.contains("RopaSans"))
    } finally {
      server.stop
    }
  }

  @Test
  def testHttpServerUrl {
    val server = new HttpServer("/test", resourceBasePath = "scalate")
    try {
      server.addServlet("/basic", new BasicServlet())
      server.start
      assertTrue(server.getUrl.getHost == Util.getLocalHost.getHostName)
    } finally {
      server.stop
    }
  }
}

class BasicServlet extends ServletBase {
  def getObjectToWrite = {
    val map = new java.util.HashMap[String, String]()
    map.put("foo", "bar")
    map
  }
}