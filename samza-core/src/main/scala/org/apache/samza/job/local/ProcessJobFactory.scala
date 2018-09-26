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

import java.util
import org.apache.samza.SamzaException
import org.apache.samza.config.{Config, JobConfig, TaskConfigJava}
import org.apache.samza.config.TaskConfig._
import org.apache.samza.container.TaskName
import org.apache.samza.coordinator.JobModelManager
import org.apache.samza.coordinator.stream.CoordinatorStreamManager
import org.apache.samza.job.{CommandBuilder, ShellCommandBuilder, StreamJob, StreamJobFactory}
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.storage.ChangelogStreamManager
import org.apache.samza.util.{Logging, Util}
import scala.collection.JavaConversions._

/**
 * Creates a stand alone ProcessJob with the specified config.
 */
class ProcessJobFactory extends StreamJobFactory with Logging {
  def getJob(config: Config): StreamJob = {
    val containerCount = JobConfig.Config2Job(config).getContainerCount

    if (containerCount > 1) {
      throw new SamzaException("Container count larger than 1 is not supported for ProcessJobFactory")
    }

    val metricsRegistry = new MetricsRegistryMap()
    val coordinatorStreamManager = new CoordinatorStreamManager(config, metricsRegistry)
    coordinatorStreamManager.register(getClass.getSimpleName)
    coordinatorStreamManager.start
    coordinatorStreamManager.bootstrap
    val changelogStreamManager = new ChangelogStreamManager(coordinatorStreamManager)

    val coordinator = JobModelManager(coordinatorStreamManager.getConfig, changelogStreamManager.readPartitionMapping())
    val jobModel = coordinator.jobModel

    val taskPartitionMappings: util.Map[TaskName, Integer] = new util.HashMap[TaskName, Integer]
    for (containerModel <- jobModel.getContainers.values) {
      for (taskModel <- containerModel.getTasks.values) {
        taskPartitionMappings.put(taskModel.getTaskName, taskModel.getChangelogPartition.getPartitionId)
      }
    }

    changelogStreamManager.writePartitionMapping(taskPartitionMappings)
    coordinatorStreamManager.stop()

    //create necessary checkpoint and changelog streams
    val checkpointManager = new TaskConfigJava(jobModel.getConfig).getCheckpointManager(metricsRegistry)
    if (checkpointManager != null) {
      checkpointManager.createResources()
    }
    ChangelogStreamManager.createChangelogStreams(jobModel.getConfig, jobModel.maxChangeLogStreamPartitions)

    val containerModel = coordinator.jobModel.getContainers.get(0)

    val fwkPath = JobConfig.getFwkPath(config) // see if split deployment is configured
    info("Process job. using fwkPath = " + fwkPath)

    val commandBuilder = {
      config.getCommandClass match {
        case Some(cmdBuilderClassName) => {
          // A command class was specified, so we need to use a process job to
          // execute the command in its own process.
          Util.getObj(cmdBuilderClassName, classOf[CommandBuilder])
        }
        case _ => {
          info("Defaulting to ShellCommandBuilder")
          new ShellCommandBuilder
        }
      }
    }
    // JobCoordinator is stopped by ProcessJob when it exits
    coordinator.start

    commandBuilder
            .setConfig(config)
            .setId("0")
            .setUrl(coordinator.server.getUrl)
            .setCommandPath(fwkPath)

    new ProcessJob(commandBuilder, coordinator)
  }
}
