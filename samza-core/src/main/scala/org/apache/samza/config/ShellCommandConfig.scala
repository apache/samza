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
   * This environment variable is used to store a JSON serialized map of all configuration.
   */
  val ENV_CONFIG = "SAMZA_CONFIG"

  /**
   * A CSV list of partition IDs that a TaskRunner is responsible for (e.g. 0,2,4,6).
   */
  val ENV_PARTITION_IDS = "SAMZA_PARTITION_IDS"

  /**
   * The name for a container (either a YARN AM or SamzaContainer)
   */
  val ENV_CONTAINER_NAME = "SAMZA_CONTAINER_NAME"

  /**
   * Arguments to be passed to the processing running the TaskRunner (or equivalent, for non JVM languages).
   */
  val ENV_SAMZA_OPTS = "SAMZA_OPTS"

  val COMMAND_SHELL_EXECUTE = "task.execute"
  val TASK_JVM_OPTS = "task.opts"

  implicit def Config2ShellCommand(config: Config) = new ShellCommandConfig(config)
}

class ShellCommandConfig(config: Config) extends ScalaMapConfig(config) {
  def getCommand = getOption(ShellCommandConfig.COMMAND_SHELL_EXECUTE).getOrElse("bin/run-task.sh")

  def getTaskOpts = getOption(ShellCommandConfig.TASK_JVM_OPTS)
}
