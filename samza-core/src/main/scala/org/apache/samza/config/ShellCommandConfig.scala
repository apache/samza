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
   * An encoded list of the streams and partitions this container is responsible for. Encoded by 
   * {@link org.apache.samza.util.Util#createStreamPartitionString}
   */
  val ENV_SYSTEM_STREAMS = "SAMZA_SYSTEM_STREAMS"

  /**
   * The name for a container (either a YARN AM or SamzaContainer)
   */
  val ENV_CONTAINER_NAME = "SAMZA_CONTAINER_NAME"

  /**
   * Arguments to be passed to the processing running the TaskRunner (or equivalent, for non JVM languages).
   */
  val ENV_JAVA_OPTS = "JAVA_OPTS"

  /**
   * Specifies whether the config for ENV_CONFIG and ENV_SYSTEM_STREAMS are compressed or not.
   */
  val ENV_COMPRESS_CONFIG = "SAMZA_COMPRESS_CONFIG"

  val COMMAND_SHELL_EXECUTE = "task.execute"
  val TASK_JVM_OPTS = "task.opts"
  val COMPRESS_ENV_CONFIG = "task.config.compress"

  implicit def Config2ShellCommand(config: Config) = new ShellCommandConfig(config)
}

class ShellCommandConfig(config: Config) extends ScalaMapConfig(config) {
  def getCommand = getOption(ShellCommandConfig.COMMAND_SHELL_EXECUTE).getOrElse("bin/run-container.sh")

  def getTaskOpts = getOption(ShellCommandConfig.TASK_JVM_OPTS)

  def isEnvConfigCompressed = getBoolean(ShellCommandConfig.COMPRESS_ENV_CONFIG, false)
}
