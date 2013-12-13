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

import org.apache.samza.util.Util
import org.apache.samza.system.SystemStream

object TaskConfig {
  // task config constants
  val INPUT_STREAMS = "task.inputs" // streaming.input-streams
  val WINDOW_MS = "task.window.ms" // window period in milliseconds
  val COMMIT_MS = "task.commit.ms" // commit period in milliseconds
  val TASK_CLASS = "task.class" // streaming.task-factory-class
  val COMMAND_BUILDER = "task.command.class" // streaming.task-factory-class
  val LIFECYCLE_LISTENERS = "task.lifecycle.listeners" // li-generator,foo
  val LIFECYCLE_LISTENER = "task.lifecycle.listener.%s.class" // task.lifecycle.listener.li-generator.class
  val CHECKPOINT_MANAGER_FACTORY = "task.checkpoint.factory" // class name to use when sending offset checkpoints
  val TASK_JMX_ENABLED = "task.jmx.enabled" // Start up a JMX server for this task?
  val MESSAGE_CHOOSER_CLASS_NAME = "task.chooser.class"

  implicit def Config2Task(config: Config) = new TaskConfig(config)
}

class TaskConfig(config: Config) extends ScalaMapConfig(config) {
  def getInputStreams = getOption(TaskConfig.INPUT_STREAMS) match {
    case Some(streams) => if (streams.length > 0) {
      streams.split(",").map(systemStreamNames => {
        Util.getSystemStreamFromNames(systemStreamNames)
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

  def getLifecycleListeners(): Option[String] = getOption(TaskConfig.LIFECYCLE_LISTENERS)

  def getLifecycleListenerClass(name: String): Option[String] = getOption(TaskConfig.LIFECYCLE_LISTENER format name)

  def getTaskClass = getOption(TaskConfig.TASK_CLASS)

  def getCommandClass = getOption(TaskConfig.COMMAND_BUILDER)

  def getCheckpointManagerFactory() = getOption(TaskConfig.CHECKPOINT_MANAGER_FACTORY)

  def getJmxServerEnabled = getBoolean(TaskConfig.TASK_JMX_ENABLED, true)
  
  def getMessageChooserClass = getOption(TaskConfig.MESSAGE_CHOOSER_CLASS_NAME)
}
