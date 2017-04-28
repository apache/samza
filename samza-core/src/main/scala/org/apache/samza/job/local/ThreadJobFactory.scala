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


import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig._
import org.apache.samza.config.ShellCommandConfig._
import org.apache.samza.container.SamzaContainer
import org.apache.samza.coordinator.JobModelManager
import org.apache.samza.job.{StreamJob, StreamJobFactory}
import org.apache.samza.metrics.{JmxServer, MetricsReporter}
import org.apache.samza.processor.SamzaContainerListener
import org.apache.samza.runtime.LocalContainerRunner
import org.apache.samza.task.TaskFactoryUtil
import org.apache.samza.util.Logging

/**
 * Creates a new Thread job with the given config
 */
class ThreadJobFactory extends StreamJobFactory with Logging {
  def getJob(config: Config): StreamJob = {
    info("Creating a ThreadJob, which is only meant for debugging.")
    val coordinator = JobModelManager(config)
    val jobModel = coordinator.jobModel
    val containerModel = jobModel.getContainers.get("0")
    val jmxServer = new JmxServer
    val streamApp = TaskFactoryUtil.createStreamApplication(config)
    val appRunner = new LocalContainerRunner(jobModel, "0")
    val taskFactory = TaskFactoryUtil.createTaskFactory(config, streamApp, appRunner)

    // Give developers a nice friendly warning if they've specified task.opts and are using a threaded job.
    config.getTaskOpts match {
      case Some(taskOpts) => warn("%s was specified in config, but is not being used because job is being executed with ThreadJob. You probably want to run %s=%s." format (TASK_JVM_OPTS, STREAM_JOB_FACTORY_CLASS, classOf[ProcessJobFactory].getName))
      case _ => None
    }

    val containerListener = new SamzaContainerListener {
      override def onContainerFailed(t: Throwable): Unit = {
        throw t
      }

      override def onContainerStop(pausedOrNot: Boolean): Unit = {
      }

      override def onContainerStart(): Unit = {

      }
    }
    try {
      coordinator.start
      val container = SamzaContainer(
        containerModel,
        config,
        jobModel.maxChangeLogStreamPartitions,
        jmxServer,
        Map[String, MetricsReporter](),
        taskFactory)
      container.setContainerListener(containerListener)

      val threadJob = new ThreadJob(container)
      threadJob
    } finally {
      coordinator.stop
      jmxServer.stop
    }
  }
}
