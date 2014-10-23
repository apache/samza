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

import org.apache.samza.util.Logging
import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.config.ShellCommandConfig._
import org.apache.samza.config.TaskConfig._
import org.apache.samza.container.{TaskNamesToSystemStreamPartitions, SamzaContainer}
import org.apache.samza.job.{StreamJob, StreamJobFactory}
import org.apache.samza.util.Util
import org.apache.samza.config.JobConfig._

/**
 * Creates a new Thread job with the given config
 */
class ThreadJobFactory extends StreamJobFactory with Logging {
  def getJob(config: Config): StreamJob = {
    // Since we're local, there will only be a single task into which all the SSPs will be processed
    val taskToTaskNames: Map[Int, TaskNamesToSystemStreamPartitions] = Util.assignContainerToSSPTaskNames(config, 1)
    if(taskToTaskNames.size != 1) {
      throw new SamzaException("Should only have a single task count but somehow got more " + taskToTaskNames.size)
    }

    // So pull out that single TaskNamesToSystemStreamPartitions
    val sspTaskName: TaskNamesToSystemStreamPartitions = taskToTaskNames.getOrElse(0, throw new SamzaException("Should have a 0 task number for the SSPs but somehow do not: " + taskToTaskNames))
    if (sspTaskName.size <= 0) {
      throw new SamzaException("No SystemStreamPartitions to process were detected for your input streams. It's likely that the system(s) specified don't know about the input streams: %s" format config.getInputStreams)
    }

    val taskNameToChangeLogPartitionMapping = Util.getTaskNameToChangeLogPartitionMapping(config, taskToTaskNames)
    info("got taskName for job %s" format sspTaskName)
    info("Creating a ThreadJob, which is only meant for debugging.")

    // Give developers a nice friendly warning if they've specified task.opts and are using a threaded job.
    config.getTaskOpts match {
      case Some(taskOpts) => warn("%s was specified in config, but is not being used because job is being executed with ThreadJob. You probably want to run %s=%s." format (TASK_JVM_OPTS, STREAM_JOB_FACTORY_CLASS, classOf[ThreadJobFactory].getName))
      case _ => None
    }

    // No command class was specified, so execute the job in this process
    // using a threaded job.
    new ThreadJob(SamzaContainer(0, sspTaskName, taskNameToChangeLogPartitionMapping, config))
  }
}