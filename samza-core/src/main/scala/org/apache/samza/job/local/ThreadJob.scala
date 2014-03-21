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

package org.apache.samza.job.local

import grizzled.slf4j.Logging
import org.apache.samza.job.StreamJob
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus.New
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.job.ApplicationStatus.SuccessfulFinish
import org.apache.samza.job.ApplicationStatus.UnsuccessfulFinish

class ThreadJob(runnable: Runnable) extends StreamJob with Logging {
  @volatile var jobStatus: Option[ApplicationStatus] = None
  var thread: Thread = null

  def submit: StreamJob = {
    jobStatus = Some(New)

    // create a non-daemon thread to make job runner block until the job finishes.
    // without this, the proc dies when job runner ends.
    thread = new Thread {
      override def run {
        try {
          runnable.run
          jobStatus = Some(SuccessfulFinish)
        } catch {
          case e: Exception => {
            error("Failing job with exception.", e)
            jobStatus = Some(UnsuccessfulFinish)
            throw e
          }
        }
      }
    }
    thread.setName("ThreadJob")
    thread.start
    jobStatus = Some(Running)

    ThreadJob.this
  }

  def kill: StreamJob = {
    thread.interrupt
    ThreadJob.this
  }

  def waitForFinish(timeoutMs: Long) = {
    thread.join(timeoutMs)
    jobStatus.getOrElse(null)
  }

  def waitForStatus(status: ApplicationStatus, timeoutMs: Long) = {
    val start = System.currentTimeMillis

    while (System.currentTimeMillis - start < timeoutMs && !status.equals(jobStatus.getOrElse(null))) {
      Thread.sleep(500)
    }

    jobStatus.getOrElse(null)
  }

  def getStatus = jobStatus.getOrElse(null)
}
