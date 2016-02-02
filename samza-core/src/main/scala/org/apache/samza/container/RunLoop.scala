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

import org.apache.samza.system.{SystemConsumers, SystemStreamPartition}
import org.apache.samza.task.ReadableCoordinator
import org.apache.samza.util.{Logging, TimerUtils}

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
  val clock: () => Long = { System.nanoTime },
  val shutdownMs: Long = 5000) extends Runnable with TimerUtils with Logging {

  private val metricsMsOffset = 1000000L
  private var lastWindowNs = clock()
  private var lastCommitNs = clock()
  private var activeNs = 0L
  private var taskShutdownRequests: Set[TaskName] = Set()
  private var taskCommitRequests: Set[TaskName] = Set()
  @volatile private var shutdownNow = false

  // Messages come from the chooser with no connection to the TaskInstance they're bound for.
  // Keep a mapping of SystemStreamPartition to TaskInstance to efficiently route them.
  val systemStreamPartitionToTaskInstances = getSystemStreamPartitionToTaskInstancesMapping

  def getSystemStreamPartitionToTaskInstancesMapping: Map[SystemStreamPartition, List[TaskInstance]] = {
    // We could just pass in the SystemStreamPartitionMap during construction, but it's safer and cleaner to derive the information directly
    def getSystemStreamPartitionToTaskInstance(taskInstance: TaskInstance) = taskInstance.systemStreamPartitions.map(_ -> taskInstance).toMap

    taskInstances.values.map { getSystemStreamPartitionToTaskInstance }.flatten.groupBy(_._1).map {
      case (ssp, ssp2taskInstance) => ssp -> ssp2taskInstance.map(_._2).toList
    }
  }

  /**
   * Starts the run loop. Blocks until either the tasks request shutdown, or an
   * unhandled exception is thrown.
   */
  def run {
    addShutdownHook(Thread.currentThread())

    while (!shutdownNow) {
      val loopStartTime = clock()
      process
      window
      commit
      val totalNs = clock() - loopStartTime
      metrics.utilization.set(activeNs.toFloat/totalNs)
      activeNs = 0L
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

    activeNs += updateTimerAndGetDuration(metrics.processNs) ((currentTimeNs: Long) => {
      val envelope = updateTimer(metrics.chooseNs) {
        consumerMultiplexer.choose
      }

      if (envelope != null) {
        val ssp = envelope.getSystemStreamPartition

        trace("Processing incoming message envelope for SSP %s." format ssp)
        metrics.envelopes.inc

        val taskInstances = systemStreamPartitionToTaskInstances(ssp)
        taskInstances.foreach {
          taskInstance =>
            {
              val coordinator = new ReadableCoordinator(taskInstance.taskName)
              taskInstance.process(envelope, coordinator)
              checkCoordinator(coordinator)
            }
        }
      } else {
        trace("No incoming message envelope was available.")
        metrics.nullEnvelopes.inc
      }
    })
  }

  /**
   * Invokes WindowableTask.window on all tasks if it's time to do so.
   */
  private def window {
    activeNs += updateTimerAndGetDuration(metrics.windowNs) ((currentTimeNs: Long) => {
      if (windowMs >= 0 && lastWindowNs + windowMs * metricsMsOffset < currentTimeNs) {
        trace("Windowing stream tasks.")
        lastWindowNs = currentTimeNs
        metrics.windows.inc

        taskInstances.foreach {
          case (taskName, task) =>
            val coordinator = new ReadableCoordinator(taskName)
            task.window(coordinator)
            checkCoordinator(coordinator)
        }
      }
    })
  }

  /**
   * Commits task state as a a checkpoint, if necessary.
   */
  private def commit {
    activeNs += updateTimerAndGetDuration(metrics.commitNs) ((currentTimeNs: Long) => {
      if (commitMs >= 0 && lastCommitNs + commitMs * metricsMsOffset < currentTimeNs) {
        trace("Committing task instances because the commit interval has elapsed.")
        lastCommitNs = currentTimeNs
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
    })
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
