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

import org.apache.samza.system.SystemStream
import org.apache.samza.util.{Logging, Util}

object TaskConfig {
  // task config constants
  val INPUT_STREAMS = "task.inputs" // streaming.input-streams
  val WINDOW_MS = "task.window.ms" // window period in milliseconds
  val COMMIT_MS = "task.commit.ms" // commit period in milliseconds
  val SHUTDOWN_MS = "task.shutdown.ms" // how long to wait for a clean shutdown
  val TASK_CLASS = "task.class" // streaming.task-factory-class
  val COMMAND_BUILDER = "task.command.class" // streaming.task-factory-class
  val LIFECYCLE_LISTENERS = "task.lifecycle.listeners" // li-generator,foo
  val LIFECYCLE_LISTENER = "task.lifecycle.listener.%s.class" // task.lifecycle.listener.li-generator.class
  val CHECKPOINT_MANAGER_FACTORY = "task.checkpoint.factory" // class name to use when sending offset checkpoints
  val MESSAGE_CHOOSER_CLASS_NAME = "task.chooser.class"
  val DROP_DESERIALIZATION_ERROR = "task.drop.deserialization.errors" // define whether drop the messages or not when deserialization fails
  val DROP_SERIALIZATION_ERROR = "task.drop.serialization.errors" // define whether drop the messages or not when serialization fails
  val IGNORED_EXCEPTIONS = "task.ignored.exceptions" // exceptions to ignore in process and window
  val GROUPER_FACTORY = "task.name.grouper.factory" // class name for task grouper

  /**
   * Samza's container polls for more messages under two conditions. The first
   * condition arises when there are simply no remaining buffered messages to
   * process for any input SystemStreamPartition. The second condition arises
   * when some input SystemStreamPartitions have empty buffers, but some do
   * not. In the latter case, a polling interval is defined to determine how
   * often to refresh the empty SystemStreamPartition buffers. By default,
   * this interval is 50ms, which means that any empty SystemStreamPartition
   * buffer will be refreshed at least every 50ms. A higher value here means
   * that empty SystemStreamPartitions will be refreshed less often, which
   * means more latency is introduced, but less CPU and network will be used.
   * Decreasing this value means that empty SystemStreamPartitions are
   * refreshed more frequently, thereby introducing less latency, but
   * increasing CPU and network utilization.
   */
  val POLL_INTERVAL_MS = "task.poll.interval.ms"

  implicit def Config2Task(config: Config) = new TaskConfig(config)
}

class TaskConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  def getInputStreams = getOption(TaskConfig.INPUT_STREAMS) match {
    case Some(streams) => if (streams.length > 0) {
      streams.split(",").map(systemStreamNames => {
        Util.getSystemStreamFromNames(systemStreamNames.trim)
      }).toSet
    } else {
      Set[SystemStream]()
    }
    case _ => Set[SystemStream]()
  }

  def getWindowMs: Option[Long] = getOption(TaskConfig.WINDOW_MS) match {
    case Some(ms) => Some(ms.toLong)
    case _ => None
  }

  def getCommitMs: Option[Long] = getOption(TaskConfig.COMMIT_MS) match {
    case Some(ms) => Some(ms.toLong)
    case _ => None
  }

  def getShutdownMs: Option[Long] = getOption(TaskConfig.SHUTDOWN_MS) match {
    case Some(ms) => Some(ms.toLong)
    case _ => None
  }

  def getLifecycleListeners(): Option[String] = getOption(TaskConfig.LIFECYCLE_LISTENERS)

  def getLifecycleListenerClass(name: String): Option[String] = getOption(TaskConfig.LIFECYCLE_LISTENER format name)

  def getTaskClass = getOption(TaskConfig.TASK_CLASS)

  def getCommandClass = getOption(TaskConfig.COMMAND_BUILDER)

  def getCheckpointManagerFactory() = getOption(TaskConfig.CHECKPOINT_MANAGER_FACTORY)

  def getMessageChooserClass = getOption(TaskConfig.MESSAGE_CHOOSER_CLASS_NAME)

  def getDropDeserialization = getOption(TaskConfig.DROP_DESERIALIZATION_ERROR)

  def getDropSerialization = getOption(TaskConfig.DROP_SERIALIZATION_ERROR)

  def getPollIntervalMs = getOption(TaskConfig.POLL_INTERVAL_MS)

  def getIgnoredExceptions = getOption(TaskConfig.IGNORED_EXCEPTIONS)

  def getTaskNameGrouperFactory = {
    getOption(TaskConfig.GROUPER_FACTORY) match {
      case Some(grouperFactory) => grouperFactory
      case _ =>
        info("No %s configuration, using 'org.apache.samza.container.grouper.task.GroupByContainerCountFactory'" format TaskConfig.GROUPER_FACTORY)
        "org.apache.samza.container.grouper.task.GroupByContainerCountFactory"
    }
  }

}
