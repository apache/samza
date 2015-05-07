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

package org.apache.samza.config

object ShellCommandConfig {
  /**
   * This environment variable is used to store a JSON serialized map of all coordinator system configs.
   */
  val ENV_COORDINATOR_SYSTEM_CONFIG = "SAMZA_COORDINATOR_SYSTEM_CONFIG"

  /**
   * The ID for a container. This is an integer number between 0 and
   * &lt;number of containers&gt;.
   */
  val ENV_CONTAINER_ID = "SAMZA_CONTAINER_ID"

  /**
   * The URL location of the job coordinator's HTTP server.
   */
  val ENV_COORDINATOR_URL = "SAMZA_COORDINATOR_URL"

  /**
   * Arguments to be passed to the processing running the TaskRunner (or equivalent, for non JVM languages).
   */
  val ENV_JAVA_OPTS = "JAVA_OPTS"

  /**
   * The JAVA_HOME path for running the task
   */
  val ENV_JAVA_HOME = "JAVA_HOME"

  /*
   * The base directory for storing logged data stores used in Samza. This has to be set on all machine running Samza
   * containers. For example, when using YARN, it has to be set in all NMs and passed to the containers.
   * If this environment variable is not set, the path defaults to current working directory (which is the same as the
   * path for persisting non-logged data stores)
   */
  val ENV_LOGGED_STORE_BASE_DIR = "LOGGED_STORE_BASE_DIR"

  val COMMAND_SHELL_EXECUTE = "task.execute"
  val TASK_JVM_OPTS = "task.opts"
  val TASK_JAVA_HOME = "task.java.home"

  implicit def Config2ShellCommand(config: Config) = new ShellCommandConfig(config)
}

class ShellCommandConfig(config: Config) extends ScalaMapConfig(config) {
  def getCommand = getOption(ShellCommandConfig.COMMAND_SHELL_EXECUTE).getOrElse("bin/run-container.sh")

  def getTaskOpts = getOption(ShellCommandConfig.TASK_JVM_OPTS)

  def getJavaHome = getOption(ShellCommandConfig.TASK_JAVA_HOME)
}
