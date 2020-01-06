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
   * This enviorment variable is used to pass a JSON serialized map of configs provided during job submission.
   */
  val ENV_SUBMISSION_CONFIG = "SAMZA_SUBMISSION_CONFIG"

  /**
   * The ID for a container. This is a string representation that is unique to the runtime environment.
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

  /**
   * The ID assigned to the container by the execution environment (eg: YARN Container Id)
   */
  val ENV_EXECUTION_ENV_CONTAINER_ID = "EXECUTION_ENV_CONTAINER_ID"

  /**
   * Set to "true" if cluster-based job coordinator dependency isolation is enabled. Otherwise, will be considered
   * false.
   *
   * The launch process for the cluster-based job coordinator depends on the value of this, since it needs to be known
   * if the cluster-based job coordinator should be launched in an isolated mode. This needs to be an environment
   * variable, because the value needs to be known before the full configs can be read from the metadata store (full
   * configs are only read after launch is complete).
   */
  val ENV_CLUSTER_BASED_JOB_COORDINATOR_DEPENDENCY_ISOLATION_ENABLED =
    "CLUSTER_BASED_JOB_COORDINATOR_DEPENDENCY_ISOLATION_ENABLED"

  /**
   * When running the cluster-based job coordinator in an isolated mode, it uses JARs and resources from a lib directory
   * which is provided by the framework. In some cases, it is necessary to use some resources specified by the
   * application as well. This environment variable can be set to a directory which is different from the framework lib
   * directory in order to tell Samza where application resources live.
   * This is an environment variable because it is needed in order to launch the cluster-based job coordinator Java
   * process, which means access to full configs is not available yet.
   * For example, this is used to set a system property for the location of an application-specified log4j configuration
   * file when launching the cluster-based job coordinator Java process.
   */
  val ENV_APPLICATION_LIB_DIR = "APPLICATION_LIB_DIR"

  /*
   * The base directory for storing logged data stores used in Samza. This has to be set on all machine running Samza
   * containers. For example, when using YARN, it has to be set in all NMs and passed to the containers.
   * If this environment variable is not set, the path defaults to current working directory (which is the same as the
   * path for persisting non-logged data stores)
   */
  val ENV_LOGGED_STORE_BASE_DIR = "LOGGED_STORE_BASE_DIR"

  /**
   * The directory path that contains the execution plan
   */
  val EXECUTION_PLAN_DIR = "EXECUTION_PLAN_DIR"

  /**
   * Points to the lib directory of the localized resources(other than the framework dependencies).
   */
  val ENV_ADDITIONAL_CLASSPATH_DIR = "ADDITIONAL_CLASSPATH_DIR"

  val COMMAND_SHELL_EXECUTE = "task.execute"
  val TASK_JVM_OPTS = "task.opts"
  val TASK_JAVA_HOME = "task.java.home"

  /**
   * SamzaContainer uses JARs from the lib directory of the framework in it classpath. In some cases, it is necessary to include
   * the jars from lib directories of the resources that are localized along with the framework dependencies. These resources are logically
   * independent of the framework and cannot be bundled with the framework dependencies. The URI of these resources are set dynamically at
   * run-time before launching the SamzaContainer. This environment variable can be set to a lib directory of the localized resource and
   * it will be included in the java classpath of the SamzaContainer.
   */
  val ADDITIONAL_CLASSPATH_DIR = "additional.classpath.dir"

  implicit def Config2ShellCommand(config: Config) = new ShellCommandConfig(config)
}

class ShellCommandConfig(config: Config) extends ScalaMapConfig(config) {
  def getCommand = getOption(ShellCommandConfig.COMMAND_SHELL_EXECUTE).getOrElse("bin/run-container.sh")

  def getTaskOpts = getOption(ShellCommandConfig.TASK_JVM_OPTS)

  def getJavaHome = getOption(ShellCommandConfig.TASK_JAVA_HOME)

  def getAdditionalClasspathDir(): Option[String] = getOption(ShellCommandConfig.ADDITIONAL_CLASSPATH_DIR)
}
