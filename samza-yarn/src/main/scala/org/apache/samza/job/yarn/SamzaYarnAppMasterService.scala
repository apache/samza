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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.samza.clustermanager.SamzaApplicationState
import org.apache.samza.config.Config
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.coordinator.stream.CoordinatorStreamWriter
import org.apache.samza.coordinator.stream.messages.SetConfig
import org.apache.samza.metrics.ReadableMetricsRegistry
import org.apache.samza.util.Logging
import org.apache.samza.webapp.{ApplicationMasterRestServlet, ApplicationMasterWebServlet, YarnContainerHeartbeatServlet}

/**
  * Samza's application master runs a very basic HTTP/JSON service to allow
  * dashboards to check on the status of a job. SamzaYarnAppMasterService starts
  * up the web service when initialized.
  */
//This class is used in the refactored code path as called by run-jc.sh

class SamzaYarnAppMasterService(config: Config, samzaAppState: SamzaApplicationState, state: YarnAppState, registry: ReadableMetricsRegistry, yarnConfiguration: YarnConfiguration) extends  Logging {
  var rpcApp: HttpServer = null
  var webApp: HttpServer = null
  val SERVER_URL_OPT: String = "samza.autoscaling.server.url"
  var securityManager: Option[SamzaAppMasterSecurityManager] = None

  def onInit() {
    // try starting the samza AM dashboard at a random rpc and tracking port
    info("Starting webapp at a random rpc and tracking port")

    rpcApp = new HttpServer(resourceBasePath = "scalate")

    rpcApp.addServlet("/*", new ApplicationMasterRestServlet(config, samzaAppState, state, registry))
    rpcApp.start

    webApp = new HttpServer(resourceBasePath = "scalate")
    webApp.addServlet("/*", new ApplicationMasterWebServlet(config, samzaAppState, state))
    webApp.start

    samzaAppState.jobModelManager.server.addServlet("/containerHeartbeat", new YarnContainerHeartbeatServlet(state, registry))
    samzaAppState.jobModelManager.start
    state.rpcUrl = rpcApp.getUrl
    state.trackingUrl = webApp.getUrl
    state.coordinatorUrl = samzaAppState.jobModelManager.server.getUrl

    //write server url to coordinator stream
    val coordinatorStreamWriter: CoordinatorStreamWriter = new CoordinatorStreamWriter(config)
    coordinatorStreamWriter.start()
    coordinatorStreamWriter.sendMessage(SetConfig.TYPE, SERVER_URL_OPT, state.coordinatorUrl.toString)
    coordinatorStreamWriter.stop()
    debug("Sent server url message with value: %s " format state.coordinatorUrl.toString)

    info("Webapp is started at (rpc %s, tracking %s, coordinator %s)" format(state.rpcUrl, state.trackingUrl, state.coordinatorUrl))

    // start YarnSecurityManger for a secure cluster
    if (UserGroupInformation.isSecurityEnabled) {
      securityManager = Option {
        val securityManager = new SamzaAppMasterSecurityManager(config, yarnConfiguration)
        securityManager.start
        securityManager
      }
    }

  }

  def onShutdown() {
    if (rpcApp != null) {
      rpcApp.stop
    }

    if (webApp != null) {
      webApp.stop
    }

    samzaAppState.jobModelManager.stop

    securityManager.map {
      securityManager => securityManager.stop
    }

  }
}
