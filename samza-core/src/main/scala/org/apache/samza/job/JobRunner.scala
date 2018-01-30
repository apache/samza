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

package org.apache.samza.job


import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.coordinator.stream.{CoordinatorStreamSystemConsumer, CoordinatorStreamSystemProducer}
import org.apache.samza.coordinator.stream.messages.{Delete, SetConfig}
import org.apache.samza.job.ApplicationStatus.{Running, SuccessfulFinish}
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.runtime.ApplicationRunnerMain.ApplicationRunnerCommandLine
import org.apache.samza.runtime.ApplicationRunnerOperation
import org.apache.samza.system.StreamSpec
import org.apache.samza.util.{Logging, Util}

import scala.collection.JavaConverters._


object JobRunner extends Logging {
  val SOURCE = "job-runner"

  def main(args: Array[String]) {
    val cmdline = new ApplicationRunnerCommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    val operation = cmdline.getOperation(options)

    val runner = new JobRunner(Util.rewriteConfig(config))
    doOperation(runner, operation)
  }

  def doOperation(runner: JobRunner, operation: ApplicationRunnerOperation): Unit = {
    operation match {
      case ApplicationRunnerOperation.RUN => runner.run()
      case ApplicationRunnerOperation.KILL => runner.kill()
      case ApplicationRunnerOperation.STATUS => println(runner.status())
      case _ =>
        throw new SamzaException("Invalid job runner operation: %s" format operation)
    }
  }
}

/**
 * ConfigRunner is a helper class that sets up and executes a Samza job based
 * on a config URI. The configFactory is instantiated, fed the configPath,
 * and returns a Config, which is used to execute the job.
 */
class JobRunner(val config: Config) extends Logging {

  var streamJob: StreamJob = null

  /**
   * This function submits the samza job.
   * @param resetJobConfig This flag indicates whether or not to reset the job configurations when submitting the job.
   *                       If this value is set to true, all previously written configs to coordinator stream will be
   *                       deleted, and only the configs in the input config file will have an affect. Otherwise, any
   *                       config that is not deleted will have an affect.
   *                       By default this value is set to true.
   * @return The job submitted
   */
  def run(resetJobConfig: Boolean = true) = {
    debug("config: %s" format (config))
    if (streamJob != null) {
      throw new SamzaException("The job is already running")
    }

    val jobFactory: StreamJobFactory = getJobFactory
    val coordinatorSystemConsumer = new CoordinatorStreamSystemConsumer(config, new MetricsRegistryMap)
    val coordinatorSystemProducer = new CoordinatorStreamSystemProducer(config, new MetricsRegistryMap)

    // Create the coordinator stream if it doesn't exist
    info("Creating coordinator stream")
    val coordinatorSystemStream = Util.getCoordinatorSystemStream(config)
    val systemFactory = Util.getCoordinatorSystemFactory(config)
    val systemAdmin = systemFactory.getAdmin(coordinatorSystemStream.getSystem, config)
    val streamName = coordinatorSystemStream.getStream
    val coordinatorSpec = StreamSpec.createCoordinatorStreamSpec(streamName, coordinatorSystemStream.getSystem)
    systemAdmin.start()
    if (systemAdmin.createStream(coordinatorSpec)) {
      info("Created coordinator stream %s." format streamName)
    } else {
      info("Coordinator stream %s already exists." format streamName)
    }
    systemAdmin.stop()

    if (resetJobConfig) {
      info("Storing config in coordinator stream.")
      coordinatorSystemProducer.register(JobRunner.SOURCE)
      coordinatorSystemProducer.start()
      coordinatorSystemProducer.writeConfig(JobRunner.SOURCE, config)
    }
    info("Loading old config from coordinator stream.")
    coordinatorSystemConsumer.register()
    coordinatorSystemConsumer.start()
    coordinatorSystemConsumer.bootstrap()
    coordinatorSystemConsumer.stop()

    val oldConfig = coordinatorSystemConsumer.getConfig
    if (resetJobConfig) {
      val keysToRemove = oldConfig.keySet.asScala.toSet.diff(config.keySet.asScala)
      info("Deleting old configs that are no longer defined: %s".format(keysToRemove))
      keysToRemove.foreach(key => { coordinatorSystemProducer.send(new Delete(JobRunner.SOURCE, key, SetConfig.TYPE)) })
    }
    coordinatorSystemProducer.stop()

    // Create the actual job, and submit it.
    streamJob = jobFactory.getJob(config).submit

    info("waiting for job to start")

    // Wait until the job has started, then exit.
    Option(streamJob.waitForStatus(Running, 500)) match {
      case Some(appStatus) => {
        if (Running.equals(appStatus)) {
          info("job started successfully - " + appStatus)
        } else {
          warn("unable to start job successfully. job has status %s" format (appStatus))
        }
      }
      case _ => warn("unable to start job successfully.")
    }

    info("exiting")
    streamJob
  }

  def kill(): Unit = {
    val jobFactory: StreamJobFactory = getJobFactory

    // Create the actual job, and kill it.
    streamJob.kill()

    info("waiting for job to terminate")

    // Wait until the job has terminated, then exit.
    Option(streamJob.waitForFinish(5000)) match {
      case Some(appStatus) => {
        if (SuccessfulFinish.equals(appStatus)) {
          info("job terminated successfully - " + appStatus)
        } else {
          warn("unable to terminate job successfully. job has status %s" format (appStatus))
        }
      }
      case _ => warn("unable to terminate job successfully.")
    }

    info("exiting")
  }

  def status(): ApplicationStatus = {
    // Create the actual job, and get its status.
    streamJob.getStatus
  }

  private def getJobFactory: StreamJobFactory = {
    val jobFactoryClass = config.getStreamJobFactoryClass match {
      case Some(factoryClass) => factoryClass
      case _ => throw new SamzaException("no job factory class defined")
    }
    val jobFactory = Class.forName(jobFactoryClass).newInstance.asInstanceOf[StreamJobFactory]
    info("job factory: %s" format (jobFactoryClass))
    jobFactory
  }
}
