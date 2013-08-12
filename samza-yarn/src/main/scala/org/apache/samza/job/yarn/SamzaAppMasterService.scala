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

import org.apache.samza.util.Util
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
  override def onInit() {
    // try starting the samza AM dashboard. try ten times, just in case we 
    // pick a port that's already in use.
    for (i <- 0 until 10) {
      val rpcPort = Util.randomBetween(10000, 50000)
      val trackingPort = Util.randomBetween(10000, 50000)
      info("Starting webapp at rpc %d, tracking port %d" format (rpcPort, trackingPort))

      try {
        val rpcapp = new WebAppServer("/", rpcPort)
        rpcapp.addServlet("/*", new ApplicationMasterRestServlet(config, state, registry))
        rpcapp.start

        val webapp = new WebAppServer("/", trackingPort)
        webapp.addServlet("/*", new ApplicationMasterWebServlet(config, state))
        webapp.start

        state.rpcPort = rpcPort
        state.trackingPort = trackingPort
        return
      } catch {
        case e: Exception => {
          warn("Unable to start webapp on rpc port %d, tracking port %d .. retrying" format (rpcPort, trackingPort))
        }
      }
    }

    if (state.rpcPort == 0 || state.trackingPort == 0) {
      throw new SamzaException("Giving up trying to start the webapp, since we keep getting ports that are already in use")
    }
  }
}
