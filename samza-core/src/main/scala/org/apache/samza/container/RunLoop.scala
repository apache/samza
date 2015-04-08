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

import org.apache.samza.util.Logging
import org.apache.samza.system.{ SystemStreamPartition, SystemConsumers }
import org.apache.samza.task.ReadableCoordinator
import org.apache.samza.util.TimerUtils

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
  val taskInstances: Map[TaskName, TaskInstance],
  val consumerMultiplexer: SystemConsumers,
  val metrics: SamzaContainerMetrics,
  val windowMs: Long = -1,
  val commitMs: Long = 60000,
  val clock: () => Long = { System.currentTimeMillis },
  val shutdownMs: Long = 5000) extends Runnable with TimerUtils with Logging {

  private var lastWindowMs = 0L
  private var lastCommitMs = 0L
  private var taskShutdownRequests: Set[TaskName] = Set()
  private var taskCommitRequests: Set[TaskName] = Set()
  @volatile private var shutdownNow = false

  // Messages come from the chooser with no connection to the TaskInstance they're bound for.
  // Keep a mapping of SystemStreamPartition to TaskInstance to efficiently route them.
  val systemStreamPartitionToTaskInstance: Map[SystemStreamPartition, TaskInstance] = {
    // We could just pass in the SystemStreamPartitionMap during construction, but it's safer and cleaner to derive the information directly
    def getSystemStreamPartitionToTaskInstance(taskInstance: TaskInstance) = taskInstance.systemStreamPartitions.map(_ -> taskInstance).toMap

    taskInstances.values.map { getSystemStreamPartitionToTaskInstance }.flatten.toMap
  }


  /**
   * Starts the run loop. Blocks until either the tasks request shutdown, or an
   * unhandled exception is thrown.
   */
  def run {
    addShutdownHook(Thread.currentThread())

    while (!shutdownNow) {
      process
      window
      commit
    }
  }

  private def addShutdownHook(runLoopThread: Thread) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        info("Shutting down, will wait up to %s ms" format shutdownMs)
        shutdownNow = true
        runLoopThread.join(shutdownMs)
        if (runLoopThread.isAlive) {
          warn("Did not shut down within %s ms, exiting" format shutdownMs)
        } else {
          info("Shutdown complete")
        }
      }
    })
  }

  /**
   * Chooses a message from an input stream to process, and calls the
   * process() method on the appropriate StreamTask to handle it.
   */
  private def process {
    trace("Attempting to choose a message to process.")
    metrics.processes.inc

    updateTimer(metrics.processMs) {
      val envelope = updateTimer(metrics.chooseMs) {
        consumerMultiplexer.choose
      }

      if (envelope != null) {
        val ssp = envelope.getSystemStreamPartition

        trace("Processing incoming message envelope for SSP %s." format ssp)
        metrics.envelopes.inc

        val taskInstance = systemStreamPartitionToTaskInstance(ssp)
        val coordinator = new ReadableCoordinator(taskInstance.taskName)

        taskInstance.process(envelope, coordinator)
        checkCoordinator(coordinator)
      } else {
        trace("No incoming message envelope was available.")
        metrics.nullEnvelopes.inc
      }
    }
  }

  /**
   * Invokes WindowableTask.window on all tasks if it's time to do so.
   */
  private def window {
    updateTimer(metrics.windowMs) {
      if (windowMs >= 0 && lastWindowMs + windowMs < clock()) {
        trace("Windowing stream tasks.")
        lastWindowMs = clock()
        metrics.windows.inc

        taskInstances.foreach {
          case (taskName, task) =>
            val coordinator = new ReadableCoordinator(taskName)
            task.window(coordinator)
            checkCoordinator(coordinator)
        }
      }
    }
  }

  /**
   * Commits task state as a a checkpoint, if necessary.
   */
  private def commit {
    updateTimer(metrics.commitMs) {
      if (commitMs >= 0 && lastCommitMs + commitMs < clock()) {
        trace("Committing task instances because the commit interval has elapsed.")
        lastCommitMs = clock()
        metrics.commits.inc
        taskInstances.values.foreach(_.commit)
      } else if (!taskCommitRequests.isEmpty) {
        trace("Committing due to explicit commit request.")
        metrics.commits.inc
        taskCommitRequests.foreach(taskName => {
          taskInstances(taskName).commit
        })
      }

      taskCommitRequests = Set()
    }
  }

  /**
   * A new TaskCoordinator object is passed to a task on every call to StreamTask.process
   * and WindowableTask.window. This method checks whether the task requested that we
   * do something that affects the run loop (such as commit or shut down), and updates
   * run loop state accordingly.
   */
  private def checkCoordinator(coordinator: ReadableCoordinator) {
    if (coordinator.requestedCommitTask) {
      debug("Task %s requested commit for current task only" format coordinator.taskName)
      taskCommitRequests += coordinator.taskName
    }

    if (coordinator.requestedCommitAll) {
      debug("Task %s requested commit for all tasks in the container" format coordinator.taskName)
      taskCommitRequests ++= taskInstances.keys
    }

    if (coordinator.requestedShutdownOnConsensus) {
      taskShutdownRequests += coordinator.taskName
      info("Shutdown has now been requested by tasks: %s" format taskShutdownRequests)
    }

    if (coordinator.requestedShutdownNow || taskShutdownRequests.size == taskInstances.size) {
      info("Shutdown requested.")
      shutdownNow = true
    }
  }
}
