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

import java.lang.String
import java.net.URI
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config._
import grizzled.slf4j.Logging
import joptsimple.OptionParser
import joptsimple.util.KeyValuePair
import org.apache.samza.SamzaException
import org.apache.samza.config.factories.PropertiesConfigFactory
import scala.Some
import org.apache.samza.util.Util
import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._

object JobRunner extends Logging {
  def main(args: Array[String]) {
    // Define parameters.
    var parser = new OptionParser()
    val configFactoryOpt = 
      parser.accepts("config-factory", "The config factory to use to read your config file.")
            .withRequiredArg
            .ofType(classOf[java.lang.String])
            .describedAs("com.foo.bar.ClassName")
            .defaultsTo(classOf[PropertiesConfigFactory].getName)
    val configPathOpt =
      parser.accepts("config-path", "URI location to a config file (e.g. file:///some/local/path.properties). " + 
                                    "If multiple files are given they are all used with later files overriding any values that appear in earlier files.")
            .withRequiredArg
            .ofType(classOf[URI])
            .describedAs("path")
    val configOverrideOpt = 
      parser.accepts("config", "A configuration value in the form key=value. Command line properties override any configuration values given.")
            .withRequiredArg
            .ofType(classOf[KeyValuePair])
            .describedAs("key=value")
    var options = parser.parse(args: _*)

    // Verify legitimate parameters.
    if (!options.has(configPathOpt)) {
      parser.printHelpOn(System.err)
      System.exit(-1)
    }

    // Set up the job parameters.
    val configFactoryClass = options.valueOf(configFactoryOpt)
    val configPaths = options.valuesOf(configPathOpt)
    val configFactory = Class.forName(configFactoryClass).newInstance.asInstanceOf[ConfigFactory]
    val configOverrides = options.valuesOf(configOverrideOpt).map(kv => (kv.key, kv.value)).toMap
    
    val configs: Buffer[java.util.Map[String, String]] = configPaths.map(configFactory.getConfig)
    configs += configOverrides

    new JobRunner(new MapConfig(configs)).run
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
