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

import java.util.concurrent.CountDownLatch

import org.apache.samza.coordinator.JobModelManager
import org.apache.samza.job.ApplicationStatus.{New, Running, SuccessfulFinish, UnsuccessfulFinish}
import org.apache.samza.job.{ApplicationStatus, CommandBuilder, StreamJob}
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

object ProcessJob {
  private def createProcessBuilder(commandBuilder: CommandBuilder): ProcessBuilder = {
    val processBuilder = new ProcessBuilder(commandBuilder.buildCommand.split(" ").toList.asJava)
    processBuilder.environment.putAll(commandBuilder.buildEnvironment)

    // Pipe all output to this process's streams.
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
    processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)

    processBuilder
  }
}

class ProcessJob(commandBuilder: CommandBuilder, val jobModelManager: JobModelManager) extends StreamJob with Logging {

  import ProcessJob._

  val lock = new Object
  val processBuilder: ProcessBuilder = createProcessBuilder(commandBuilder)
  var jobStatus: ApplicationStatus = New
  var processThread: Option[Thread] = None


  def submit: StreamJob = {
    val threadStartCountDownLatch = new CountDownLatch(1)

    // Create a non-daemon thread to make job runner block until the job finishes.
    // Without this, the proc dies when job runner ends.
    processThread = Some(new Thread {
      override def run {
        var processExitCode = -1
        var process: Option[Process] = None

        setStatus(Running)

        try {
          threadStartCountDownLatch.countDown
          process = Some(processBuilder.start)
          processExitCode = process.get.waitFor
        } catch {
          case _: InterruptedException => process foreach { p => p.destroyForcibly }
          case e: Exception => error("Encountered an error during job start: %s".format(e.getMessage))
        } finally {
          jobModelManager.stop
          setStatus(if (processExitCode == 0) SuccessfulFinish else UnsuccessfulFinish)
        }
      }
    })

    processThread.get.start
    threadStartCountDownLatch.await
    ProcessJob.this
  }

  def kill: StreamJob = {
    if (getStatus == Running) {
      processThread foreach { thread =>
        thread.interrupt
        thread.join
      }
    }
    ProcessJob.this
  }

  def waitForFinish(timeoutMs: Long): ApplicationStatus = {
    require(timeoutMs >= 0, "Timeout values must be non-negative.")

    processThread foreach { thread => thread.join(timeoutMs) }
    getStatus
  }

  def waitForStatus(status: ApplicationStatus, timeoutMs: Long): ApplicationStatus = lock.synchronized {
    require(timeoutMs >= 0, "Timeout values must be non-negative.")

    timeoutMs match {
      case 0 => while (getStatus != status) lock.wait(0)
      case _ => {
        val startTimeMs = System.currentTimeMillis
        var remainingTimeoutMs = timeoutMs

        while (getStatus != status && remainingTimeoutMs > 0) {
          lock.wait(remainingTimeoutMs)

          val elapsedWaitTimeMs = System.currentTimeMillis - startTimeMs
          remainingTimeoutMs = timeoutMs - elapsedWaitTimeMs
        }
      }
    }
    getStatus
  }

  def getStatus: ApplicationStatus = lock.synchronized {
    jobStatus
  }

  private def setStatus(status: ApplicationStatus): Unit = lock.synchronized {
    jobStatus = status
    lock.notify
  }
}
