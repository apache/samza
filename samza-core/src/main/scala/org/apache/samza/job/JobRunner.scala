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


import joptsimple.{OptionSet, OptionSpec}
import org.apache.samza.SamzaException
import org.apache.samza.config._
import org.apache.samza.runtime.ApplicationRunnerOperation
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals
import org.apache.samza.util._


object JobRunner extends Logging {
  val SOURCE = "job-runner"

  class JobRunnerCommandLine extends CommandLine {
    var operationOpt: OptionSpec[String] =
      parser.accepts("operation", "The operation to perform; run, status, kill.")
        .withRequiredArg
        .ofType(classOf[String])
        .describedAs("operation=run")
        .defaultsTo("run")

    def getOperation(options: OptionSet): ApplicationRunnerOperation = {
      val rawOp = options.valueOf(operationOpt)
      ApplicationRunnerOperation.fromString(rawOp)
    }
  }

  def main(args: Array[String]) {
    val cmdline = new JobRunnerCommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    val operation = cmdline.getOperation(options)

    val runner = new JobRunner(ConfigUtil.rewriteConfig(config))
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
class JobRunner(config: Config) extends Logging {

  /**
   * This function persist config in coordinator stream, create diagnostics stream if applicable and
   * then submits the samza job.
   * @param resetJobConfig This flag indicates whether or not to reset the job configurations in coordinator stream
   *                       when submitting the job.
   *                       If this value is set to true, all previously written configs to coordinator stream will be
   *                       deleted, and only the configs in the input config file will have an affect. Otherwise, any
   *                       config that is not deleted will have an affect.
   *                       By default this value is set to true.
   * @return The job submitted
   */
  def run(resetJobConfig: Boolean = true): StreamJob = {
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(config, resetJobConfig)
    DiagnosticsUtil.createDiagnosticsStream(config)
    submit()
  }

  /**
   * This function submits the samza job.
   *
   * @return The job submitted
   */
  def submit(): StreamJob = {
    // Create the actual job, and submit it.
    val job = getJobFactory.getJob(config)

    job.submit()

    info("Job submitted. Check status to determine when it is running.")
    job
  }

  def kill(): Unit = {
    val jobFactory: StreamJobFactory = getJobFactory

    // Create the actual job, and kill it.
    val job = jobFactory.getJob(config).kill()

    info("Kill command executed. Check status to determine when it is terminated.")
  }

  def status(): ApplicationStatus = {
    val jobFactory: StreamJobFactory = getJobFactory

    // Create the actual job, and get its status.
    jobFactory.getJob(config).getStatus
  }

  private def getJobFactory: StreamJobFactory = {
    val jobConfig = new JobConfig(config)
    val jobFactoryClass = JavaOptionals.toRichOptional(jobConfig.getStreamJobFactoryClass).toOption match {
      case Some(factoryClass) => factoryClass
      case _ => throw new SamzaException("no job factory class defined")
    }
    val jobFactory = Class.forName(jobFactoryClass).newInstance.asInstanceOf[StreamJobFactory]
    info("job factory: %s" format (jobFactoryClass))
    jobFactory
  }
}
