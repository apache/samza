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

package org.apache.samza.container

import grizzled.slf4j.Logging
import org.apache.samza.Partition
import org.apache.samza.system.SystemConsumers
import org.apache.samza.task.ReadableCoordinator

/**
 * Each {@link SamzaContainer} uses a single-threaded execution model: activities for
 * all {@link TaskInstance}s within a container are multiplexed onto one execution
 * thread. Those activities include task callbacks (such as StreamTask.process and
 * WindowableTask.window), committing checkpoints, etc.
 *
 * <p>This class manages the execution of that run loop, determining what needs to
 * be done when.
 */
class RunLoop(
  val taskInstances: Map[Partition, TaskInstance],
  val consumerMultiplexer: SystemConsumers,
  val metrics: SamzaContainerMetrics,
  val windowMs: Long = -1,
  val commitMs: Long = 60000,
  val clock: () => Long = { System.currentTimeMillis }) extends Runnable with Logging {

  private var lastWindowMs = 0L
  private var lastCommitMs = 0L
  private var taskShutdownRequests: Set[Partition] = Set()
  private var taskCommitRequests: Set[Partition] = Set()
  private var shutdownNow = false


  /**
   * Starts the run loop. Blocks until either the tasks request shutdown, or an
   * unhandled exception is thrown.
   */
  def run {
    while (!shutdownNow) {
      process
      window
      send
      commit
    }
  }


  /**
   * Chooses a message from an input stream to process, and calls the
   * process() method on the appropriate StreamTask to handle it.
   */
  private def process {
    trace("Attempting to choose a message to process.")
    metrics.processes.inc

    val envelope = consumerMultiplexer.choose

    if (envelope != null) {
      val partition = envelope.getSystemStreamPartition.getPartition

      trace("Processing incoming message envelope for partition %s." format partition)
      metrics.envelopes.inc

      val coordinator = new ReadableCoordinator(partition)
      taskInstances(partition).process(envelope, coordinator)
      checkCoordinator(coordinator)
    } else {
      trace("No incoming message envelope was available.")
      metrics.nullEnvelopes.inc
    }
  }


  /**
   * Invokes WindowableTask.window on all tasks if it's time to do so.
   */
  private def window {
    if (windowMs >= 0 && lastWindowMs + windowMs < clock()) {
      trace("Windowing stream tasks.")
      lastWindowMs = clock()
      metrics.windows.inc

      taskInstances.foreach { case (partition, task) =>
        val coordinator = new ReadableCoordinator(partition)
        task.window(coordinator)
        checkCoordinator(coordinator)
      }
    }
  }


  /**
   * If task instances published any messages to output streams, this flushes
   * them to the underlying systems.
   */
  private def send {
    trace("Triggering send in task instances.")
    metrics.sends.inc
    taskInstances.values.foreach(_.send)
  }


  /**
   * Commits task state as a a checkpoint, if necessary.
   */
  private def commit {
    if (commitMs >= 0 && lastCommitMs + commitMs < clock()) {
      trace("Committing task instances because the commit interval has elapsed.")
      lastCommitMs = clock()
      metrics.commits.inc
      taskInstances.values.foreach(_.commit)
    } else if (!taskCommitRequests.isEmpty) {
      trace("Committing due to explicit commit request.")
      metrics.commits.inc
      taskCommitRequests.foreach(partition => {
        taskInstances(partition).commit
      })
    }

    taskCommitRequests = Set()
  }


  /**
   * A new TaskCoordinator object is passed to a task on every call to StreamTask.process
   * and WindowableTask.window. This method checks whether the task requested that we
   * do something that affects the run loop (such as commit or shut down), and updates
   * run loop state accordingly.
   */
  private def checkCoordinator(coordinator: ReadableCoordinator) {
    if (coordinator.requestedCommitTask) {
      debug("Task %s requested commit for current task only" format coordinator.partition)
      taskCommitRequests += coordinator.partition
    }

    if (coordinator.requestedCommitAll) {
      debug("Task %s requested commit for all tasks in the container" format coordinator.partition)
      taskCommitRequests ++= taskInstances.keys
    }

    if (coordinator.requestedShutdownOnConsensus) {
      taskShutdownRequests += coordinator.partition
      info("Shutdown has now been requested by tasks: %s" format taskShutdownRequests)
    }

    if (coordinator.requestedShutdownNow || taskShutdownRequests.size == taskInstances.size) {
      info("Shutdown requested.")
      shutdownNow = true
    }
  }

}