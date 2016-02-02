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

package org.apache.samza.test.performance

import org.apache.samza.task.TaskContext
import org.apache.samza.task.InitableTask
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.task.TaskCoordinator.RequestScope
import org.apache.samza.config.Config
import org.apache.samza.util.{Util, Logging}
import org.apache.samza.system.SystemStream
import org.apache.samza.system.OutgoingMessageEnvelope


object TestPerformanceTask {
  // No thread safety is needed for these variables because they're mutated in 
  // the process method, which is single threaded.
  var messagesProcessed = 0
  var startTime = 0L
}

/**
 * A little test task that prints how many messages a SamzaContainer has
 * received, and over what period of time. The messages-processed count is
 * stored statically, so that all tasks in a single SamzaContainer increment
 * the same counter.
 *
 * The log interval is configured with task.log.interval, which defines how
 * many messages to process before printing a log line. The task will continue
 * running until task.max.messages have been processed, at which point it will
 * shut itself down.
 *
 * This task can also be configured to take incoming messages, and send them
 * to an output stream. If the task is configured to do this, the outgoing
 * message will have the same key and value as the incoming message. The
 * output stream is configured with task.outputs=[system].[stream]. For
 * example:
 *
 * <pre>
 *   task.outputs=kafka.MyOutputTopic
 * <pre>
 * 
 * If undefined, the task simply drops incoming messages, rather than
 * forwarding them to the output stream.
 */
class TestPerformanceTask extends StreamTask with InitableTask with Logging {
  import TestPerformanceTask._

  /**
   * How many messages to process before a log message is printed.
   */
  var logInterval = 10000

  /**
   * How many messages to process before shutting down.
   */
  var maxMessages = 10000000

  /**
   * If defined, incoming messages will be forwarded to this SystemStream. If
   * undefined, the task will not output messages.
   */
  var outputSystemStream: Option[SystemStream] = None

  def init(config: Config, context: TaskContext) {
    logInterval = config.getInt("task.log.interval", 10000)
    maxMessages = config.getInt("task.max.messages", 10000000)
    outputSystemStream = Option(config.get("task.outputs", null)).map(Util.getSystemStreamFromNames(_))
  }

  def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    if (startTime == 0) {
      startTime = System.currentTimeMillis
    }

    if (outputSystemStream.isDefined) {
      collector.send(new OutgoingMessageEnvelope(outputSystemStream.get, envelope.getKey, envelope.getMessage))
    }

    messagesProcessed += 1

    if (messagesProcessed % logInterval == 0) {
      val seconds = (System.currentTimeMillis - startTime) / 1000
      info("Processed %s messages in %s seconds." format (messagesProcessed, seconds))
    }

    if (messagesProcessed >= maxMessages) {
      coordinator.shutdown(RequestScope.ALL_TASKS_IN_CONTAINER)
    }
  }
}