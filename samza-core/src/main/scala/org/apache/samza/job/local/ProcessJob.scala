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

import java.io.{InputStream, OutputStream}
import java.util.concurrent.CountDownLatch

import org.apache.samza.SamzaException
import org.apache.samza.coordinator.JobCoordinator
import org.apache.samza.job.ApplicationStatus.{New, Running, UnsuccessfulFinish}
import org.apache.samza.job.{ApplicationStatus, CommandBuilder, StreamJob}
import org.apache.samza.util.Logging

import scala.collection.JavaConversions._

class ProcessJob(commandBuilder: CommandBuilder, jobCoordinator: JobCoordinator) extends StreamJob with Logging {
  var jobStatus: Option[ApplicationStatus] = None
  var process: Process = null

  def submit: StreamJob = {
    jobStatus = Some(New)
    val waitForThreadStart = new CountDownLatch(1)
    val processBuilder = new ProcessBuilder(commandBuilder.buildCommand.split(" ").toList)

    processBuilder
      .environment
      .putAll(commandBuilder.buildEnvironment)

    // create a non-daemon thread to make job runner block until the job finishes.
    // without this, the proc dies when job runner ends.
    val procThread = new Thread {
      override def run {
        process = processBuilder.start

        // pipe all output to this process's streams
        val outThread = new Thread(new Piper(process.getInputStream, System.out))
        val errThread = new Thread(new Piper(process.getErrorStream, System.err))
        outThread.setDaemon(true)
        errThread.setDaemon(true)
        outThread.start
        errThread.start
        waitForThreadStart.countDown
        process.waitFor
        jobCoordinator.stop
      }
    }

    procThread.start
    waitForThreadStart.await
    jobStatus = Some(Running)
    ProcessJob.this
  }

  def kill: StreamJob = {
    process.destroy
    jobStatus = Some(UnsuccessfulFinish);
    ProcessJob.this
  }

  def waitForFinish(timeoutMs: Long) = {
    val thread = new Thread {
      setDaemon(true)
      override def run {
        try {
          process.waitFor
        } catch {
          case e: InterruptedException => info("Got interrupt.", e)
        }
      }
    }

    thread.start
    thread.join(timeoutMs)
    thread.interrupt
    jobStatus.getOrElse(null)
  }

  def waitForStatus(status: ApplicationStatus, timeoutMs: Long) = {
    val start = System.currentTimeMillis

    while (System.currentTimeMillis - start < timeoutMs && status != jobStatus) {
      Thread.sleep(500)
    }

    jobStatus.getOrElse(null)
  }

  def getStatus = jobStatus.getOrElse(null)
}

/**
 * Silly class to forward bytes from one stream to another. Using this to pipe
 * output from subprocess to this process' stdout/stderr.
 */
class Piper(in: InputStream, out: OutputStream) extends Runnable {
  def run() {
    try {
      val b = new Array[Byte](512)
      var read = 1;
      while (read > -1) {
        read = in.read(b, 0, b.length)
        if (read > -1) {
          out.write(b, 0, read)
          out.flush()
        }
      }
    } catch {
      case e: Exception => throw new SamzaException("Broken pipe", e);
    } finally {
      in.close()
      out.close()
    }
  }
}
