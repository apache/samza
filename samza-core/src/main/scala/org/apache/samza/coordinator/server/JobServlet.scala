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


import java.util.concurrent.atomic.AtomicReference

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

/**
 * Serves the JSON serialized job model for the job.
 */
class JobServlet(jobModelRef: AtomicReference[Array[Byte]]) extends HttpServlet {
  override protected def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    val jobModel = jobModelRef.get()

    // This should never happen because JobServlet is instantiated only after a jobModel is generated and its reference is updated
    if (jobModel == null) {
      throw new IllegalStateException("No JobModel to serve in the JobCoordinator.")
    }

    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)
    response.getOutputStream.write(jobModel)
  }
}