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

object YarnConfig {
  // yarn job config
  val PACKAGE_PATH = "yarn.package.path"
  val CONTAINER_MAX_MEMORY_MB = "yarn.container.memory.mb"
  val CONTAINER_MAX_CPU_CORES = "yarn.container.cpu.cores"
  val CONTAINER_RETRY_COUNT = "yarn.container.retry.count"
  val CONTAINER_RETRY_WINDOW_MS = "yarn.container.retry.window.ms"
  val TASK_COUNT = "yarn.container.count"
  val AM_JVM_OPTIONS = "yarn.am.opts"
  val AM_JMX_ENABLED = "yarn.am.jmx.enabled"
  val AM_CONTAINER_MAX_MEMORY_MB = "yarn.am.container.memory.mb"
  val AM_POLL_INTERVAL_MS = "yarn.am.poll.interval.ms"
  val AM_JAVA_HOME = "yarn.am.java.home"
  val QUEUE = "yarn.queue"

  implicit def Config2Yarn(config: Config) = new YarnConfig(config)
}

class YarnConfig(config: Config) extends ScalaMapConfig(config) {
  def getContainerMaxMemoryMb: Option[Int] = getOption(YarnConfig.CONTAINER_MAX_MEMORY_MB).map(_.toInt)

  def getContainerMaxCpuCores: Option[Int] = getOption(YarnConfig.CONTAINER_MAX_CPU_CORES).map(_.toInt)

  def getContainerRetryCount: Option[Int] = getOption(YarnConfig.CONTAINER_RETRY_COUNT).map(_.toInt)

  def getContainerRetryWindowMs: Option[Int] = getOption(YarnConfig.CONTAINER_RETRY_WINDOW_MS).map(_.toInt)

  def getPackagePath = getOption(YarnConfig.PACKAGE_PATH)

  def getTaskCount: Option[Int] = getOption(YarnConfig.TASK_COUNT).map(_.toInt)

  def getAmOpts = getOption(YarnConfig.AM_JVM_OPTIONS)

  def getAMContainerMaxMemoryMb: Option[Int] = getOption(YarnConfig.AM_CONTAINER_MAX_MEMORY_MB).map(_.toInt)

  def getAMPollIntervalMs: Option[Int] = getOption(YarnConfig.AM_POLL_INTERVAL_MS).map(_.toInt)

  def getJmxServerEnabled = getBoolean(YarnConfig.AM_JMX_ENABLED, true)

  def getAMJavaHome = getOption(YarnConfig.AM_JAVA_HOME)

  def getQueue = getOption(YarnConfig.QUEUE)
}
