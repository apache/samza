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
import org.apache.samza.config.{ConfigRewriter, Config}
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.coordinator.stream.messages.{Delete, SetConfig}
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.migration.JobRunnerMigration
import org.apache.samza.util.CommandLine
import org.apache.samza.util.Logging
import org.apache.samza.util.Util
import scala.collection.JavaConversions._
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory


object JobRunner extends Logging {
  val SOURCE = "job-runner"

  /**
   * Re-writes configuration using a ConfigRewriter, if one is defined. If
   * there is no ConfigRewriter defined for the job, then this method is a
   * no-op.
   *
   * @param config The config to re-write.
   */
  def rewriteConfig(config: Config): Config = {
    def rewrite(c: Config, rewriterName: String): Config = {
      val klass = config
        .getConfigRewriterClass(rewriterName)
        .getOrElse(throw new SamzaException("Unable to find class config for config rewriter %s." format rewriterName))
      val rewriter = Util.getObj[ConfigRewriter](klass)
      info("Re-writing config with " + rewriter)
      rewriter.rewrite(rewriterName, c)
    }

    config.getConfigRewriters match {
      case Some(rewriters) => rewriters.split(",").foldLeft(config)(rewrite(_, _))
      case _ => config
    }
  }

  def main(args: Array[String]) {
    val cmdline = new CommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    new JobRunner(rewriteConfig(config)).run()
  }
}

/**
 * ConfigRunner is a helper class that sets up and executes a Samza job based
 * on a config URI. The configFactory is instantiated, fed the configPath,
 * and returns a Config, which is used to execute the job.
 */
class JobRunner(config: Config) extends Logging {

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
    val jobFactoryClass = config.getStreamJobFactoryClass match {
      case Some(factoryClass) => factoryClass
      case _ => throw new SamzaException("no job factory class defined")
    }
    val jobFactory = Class.forName(jobFactoryClass).newInstance.asInstanceOf[StreamJobFactory]
    info("job factory: %s" format (jobFactoryClass))
    val factory = new CoordinatorStreamSystemFactory
    val coordinatorSystemConsumer = factory.getCoordinatorStreamSystemConsumer(config, new MetricsRegistryMap)
    val coordinatorSystemProducer = factory.getCoordinatorStreamSystemProducer(config, new MetricsRegistryMap)

    // Create the coordinator stream if it doesn't exist
    info("Creating coordinator stream")
    val (coordinatorSystemStream, systemFactory) = Util.getCoordinatorSystemStreamAndFactory(config)
    val systemAdmin = systemFactory.getAdmin(coordinatorSystemStream.getSystem, config)
    systemAdmin.createCoordinatorStream(coordinatorSystemStream.getStream)

    if (resetJobConfig) {
      info("Storing config in coordinator stream.")
      coordinatorSystemProducer.register(JobRunner.SOURCE)
      coordinatorSystemProducer.start
      coordinatorSystemProducer.writeConfig(JobRunner.SOURCE, config)
    }
    info("Loading old config from coordinator stream.")
    coordinatorSystemConsumer.register
    coordinatorSystemConsumer.start
    coordinatorSystemConsumer.bootstrap
    coordinatorSystemConsumer.stop

    val oldConfig = coordinatorSystemConsumer.getConfig()
    if (resetJobConfig) {
      info("Deleting old configs that are no longer defined: %s".format(oldConfig.keySet -- config.keySet))
      (oldConfig.keySet -- config.keySet).foreach(key => {
        coordinatorSystemProducer.send(new Delete(JobRunner.SOURCE, key, SetConfig.TYPE))
      })
    }
    coordinatorSystemProducer.stop

    // Perform any migration plan to run in job runner
    JobRunnerMigration(config)

    // Create the actual job, and submit it.
    val job = jobFactory.getJob(config).submit

    info("waiting for job to start")

    // Wait until the job has started, then exit.
    Option(job.waitForStatus(Running, 500)) match {
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
    job
  }
}
