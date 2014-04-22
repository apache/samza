package org.apache.samza.test.performance

import org.apache.samza.task.TaskContext
import org.apache.samza.task.InitableTask
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.config.Config
import grizzled.slf4j.Logging

object TestPerformanceTask {
  var messagesProcessed = 0
  var startTime = 0L
}

/**
 * A little test task that prints how many messages a SamzaContainer has
 * received, and over what period of time. The messages-processed count is
 * stored statically, so that all tasks in a single SamzaContainer increment
 * the same counter.
 *
 * The log interval is configured with task.log.interval, which defines how many
 * messages to process before printing a log line. The task will continue running
 * until task.max.messages have been processed, at which point it will shut
 * itself down.
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
  var maxMessages = 100000

  def init(config: Config, context: TaskContext) {
    logInterval = config.getInt("task.log.interval", 10000)
    maxMessages = config.getInt("task.max.messages", 100000)
  }

  def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    if (startTime == 0) {
      startTime = System.currentTimeMillis
    }

    messagesProcessed += 1

    if (messagesProcessed % logInterval == 0) {
      val seconds = (System.currentTimeMillis - startTime) / 1000
      info("Processed %s messages in %s seconds." format (messagesProcessed, seconds))
    }

    if (messagesProcessed >= maxMessages) {
      coordinator.shutdown
    }
  }
}