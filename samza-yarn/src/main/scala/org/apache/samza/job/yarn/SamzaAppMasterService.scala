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

package org.apache.samza.job.yarn

import grizzled.slf4j.Logging
import org.apache.samza.webapp._
import org.apache.samza.config.Config
import org.apache.samza.metrics.ReadableMetricsRegistry
import org.apache.samza.SamzaException

/**
 * Samza's application master runs a very basic HTTP/JSON service to allow
 * dashboards to check on the status of a job. SamzaAppMasterService starts
 * up the web service when initialized.
 */
class SamzaAppMasterService(config: Config, state: SamzaAppMasterState, registry: ReadableMetricsRegistry, clientHelper: ClientHelper) extends YarnAppMasterListener with Logging {
  var rpcApp: WebAppServer = null
  var webApp: WebAppServer = null

  override def onInit() {
    // try starting the samza AM dashboard at a random rpc and tracking port
    info("Starting webapp at a random rpc and tracking port")

    rpcApp = new WebAppServer("/")
    rpcApp.addServlet("/*", new ApplicationMasterRestServlet(config, state, registry))
    rpcApp.start

    webApp = new WebAppServer("/")
    webApp.addServlet("/*", new ApplicationMasterWebServlet(config, state))
    webApp.start

    state.rpcPort = rpcApp.port
    state.trackingPort = webApp.port
    if (state.rpcPort > 0 && state.trackingPort > 0) {
      info("Webapp is started at rpc %d, tracking port %d" format (state.rpcPort, state.trackingPort))
    } else {
      throw new SamzaException("Unable to start webapp, since the host is out of ports")
    }
  }

  override def onShutdown() {
    if (rpcApp != null) {
      rpcApp.context.stop
      rpcApp.server.stop
    }

    if (webApp != null) {
      webApp.context.stop
      webApp.server.stop
    }
  }
}
