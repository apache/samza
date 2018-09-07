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

import org.apache.samza.application.{ApplicationDescriptorUtil, ApplicationUtil}
import org.apache.samza.config.{Config, TaskConfigJava}
import org.apache.samza.config.JobConfig._
import org.apache.samza.config.ShellCommandConfig._
import org.apache.samza.container.{SamzaContainer, SamzaContainerListener, TaskName}
import org.apache.samza.coordinator.JobModelManager
import org.apache.samza.coordinator.stream.CoordinatorStreamManager
import org.apache.samza.job.{StreamJob, StreamJobFactory}
import org.apache.samza.metrics.{JmxServer, MetricsRegistryMap, MetricsReporter}
import org.apache.samza.runtime.ProcessorContext
import org.apache.samza.storage.ChangelogStreamManager
import org.apache.samza.task.TaskFactory
import org.apache.samza.task.TaskFactoryUtil
import org.apache.samza.util.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Creates a new Thread job with the given config
 */
class ThreadJobFactory extends StreamJobFactory with Logging {
  def getJob(config: Config): StreamJob = {
    info("Creating a ThreadJob, which is only meant for debugging.")

    val metricsRegistry = new MetricsRegistryMap()
    val coordinatorStreamManager = new CoordinatorStreamManager(config, metricsRegistry)
    coordinatorStreamManager.register(getClass.getSimpleName)
    coordinatorStreamManager.start
    coordinatorStreamManager.bootstrap
    val changelogStreamManager = new ChangelogStreamManager(coordinatorStreamManager)

    val coordinator = JobModelManager(coordinatorStreamManager, changelogStreamManager.readPartitionMapping())
    val jobModel = coordinator.jobModel

    val taskPartitionMappings: mutable.Map[TaskName, Integer] = mutable.Map[TaskName, Integer]()
    for (containerModel <- jobModel.getContainers.values) {
      for (taskModel <- containerModel.getTasks.values) {
        taskPartitionMappings.put(taskModel.getTaskName, taskModel.getChangelogPartition.getPartitionId)
      }
    }

    changelogStreamManager.writePartitionMapping(taskPartitionMappings)

    //create necessary checkpoint and changelog streams
    val checkpointManager = new TaskConfigJava(jobModel.getConfig).getCheckpointManager(metricsRegistry)
    if (checkpointManager != null) {
      checkpointManager.createResources()
    }
    ChangelogStreamManager.createChangelogStreams(jobModel.getConfig, jobModel.maxChangeLogStreamPartitions)

    val containerId = "0"
    val jmxServer = new JmxServer

    val appDesc = ApplicationDescriptorUtil.getAppDescriptor(ApplicationUtil.fromConfig(config), config)
    val taskFactory : TaskFactory[_] = TaskFactoryUtil.getTaskFactory(appDesc)

    // Give developers a nice friendly warning if they've specified task.opts and are using a threaded job.
    config.getTaskOpts match {
      case Some(taskOpts) => warn("%s was specified in config, but is not being used because job is being executed with ThreadJob. " +
        "You probably want to run %s=%s." format (TASK_JVM_OPTS, STREAM_JOB_FACTORY_CLASS, classOf[ProcessJobFactory].getName))
      case _ => None
    }

    val containerListener = {
      val processorLifecycleListener = appDesc.getProcessorLifecycleListenerFactory().createInstance(new ProcessorContext() { }, config)
      new SamzaContainerListener {
        override def afterFailure(t: Throwable): Unit = {
          processorLifecycleListener.afterFailure(t)
          throw t
        }

        override def afterStart(): Unit = {
          processorLifecycleListener.afterStart()
        }

        override def afterStop(): Unit = {
          processorLifecycleListener.afterStop()
        }

        override def beforeStart(): Unit = {
          processorLifecycleListener.beforeStart()
        }

      }
    }

    try {
      coordinator.start
      val container = SamzaContainer(
        containerId,
        jobModel,
        config,
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
