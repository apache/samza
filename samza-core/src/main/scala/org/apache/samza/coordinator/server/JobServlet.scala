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

import org.apache.samza.SamzaException
import org.apache.samza.job.model.JobModel
import org.apache.samza.util.Logging

/**
 * A servlet that dumps the job model for a Samza job.
 */
class JobServlet(jobModelRef: AtomicReference[JobModel]) extends ServletBase with Logging {
  protected def getObjectToWrite() = {
    val jobModel = jobModelRef.get()
    if (jobModel == null) { // This should never happen because JobServlet is instantiated only after a jobModel is generated and its reference is updated
      throw new SamzaException("Job Model is not defined in the JobCoordinator. This indicates that the Samza job is unstable. Exiting...")
    }
    jobModel
  }
}