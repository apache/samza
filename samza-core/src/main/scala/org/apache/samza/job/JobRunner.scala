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
import org.apache.samza.config.{Config, ConfigRewriter}
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.factories.PropertiesConfigFactory
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.util.Util
import org.apache.samza.util.CommandLine
import grizzled.slf4j.Logging
import scala.collection.JavaConversions._

object JobRunner extends Logging {
  def main(args: Array[String]) {
    val cmdline = new CommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    new JobRunner(config).run
  }
}

/**
 * ConfigRunner is a helper class that sets up and executes a Samza job based
 * on a config URI. The configFactory is instantiated, fed the configPath,
 * and returns a Config, which is used to execute the job.
 */
class JobRunner(config: Config) extends Logging with Runnable {

  def run() {
    val conf = rewriteConfig(config)

    val jobFactoryClass = conf.getStreamJobFactoryClass match {
      case Some(factoryClass) => factoryClass
      case _ => throw new SamzaException("no job factory class defined")
    }

    val jobFactory = Class.forName(jobFactoryClass).newInstance.asInstanceOf[StreamJobFactory]

    info("job factory: %s" format (jobFactoryClass))
    debug("config: %s" format (conf))

    // Create the actual job, and submit it.
    val job = jobFactory.getJob(conf).submit

    info("waiting for job to start")

    // Wait until the job has started, then exit.
    Option(job.waitForStatus(Running, 500)) match {
      case Some(appStatus) => {
        if (Running.equals(appStatus)) {
          info("job started successfully")
        } else {
          warn("unable to start job successfully. job has status %s" format (appStatus))
        }
      }
      case _ => warn("unable to start job successfully.")
    }

    info("exiting")
  }

  // Apply any and all config re-writer classes that the user has specified
  def rewriteConfig(config: Config): Config = {
    def rewrite(c: Config, rewriterName: String): Config = {
      val klass = config
        .getConfigRewriterClass(rewriterName)
        .getOrElse(throw new SamzaException("Unable to find class config for config rewriter %s." format rewriterName))
      val rewriter = Util.getObj[ConfigRewriter](klass)
      info("Re-writing config file with " + rewriter)
      rewriter.rewrite(rewriterName, c)
    }

    config.getConfigRewriters match {
      case Some(rewriters) => rewriters.split(",").foldLeft(config)(rewrite(_, _))
      case None => config
    }
  }
}
