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

import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.OffsetManager
import org.apache.samza.config.Config
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.metrics.MetricsReporter
import org.apache.samza.storage.TaskStorageManager
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.SystemConsumers
import org.apache.samza.task.TaskContext
import org.apache.samza.task.ClosableTask
import org.apache.samza.task.InitableTask
import org.apache.samza.task.WindowableTask
import org.apache.samza.task.StreamTask
import org.apache.samza.task.ReadableCoordinator
import org.apache.samza.task.TaskInstanceCollector
import org.apache.samza.util.Logging
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemAdmin

class TaskInstance(
  task: StreamTask,
  val taskName: TaskName,
  config: Config,
  metrics: TaskInstanceMetrics,
  systemAdmins: Map[String, SystemAdmin],
  consumerMultiplexer: SystemConsumers,
  collector: TaskInstanceCollector,
  containerContext: SamzaContainerContext,
  offsetManager: OffsetManager = new OffsetManager,
  storageManager: TaskStorageManager = null,
  reporters: Map[String, MetricsReporter] = Map(),
  val systemStreamPartitions: Set[SystemStreamPartition] = Set(),
  val exceptionHandler: TaskInstanceExceptionHandler = new TaskInstanceExceptionHandler) extends Logging {
  val isInitableTask = task.isInstanceOf[InitableTask]
  val isWindowableTask = task.isInstanceOf[WindowableTask]
  val isClosableTask = task.isInstanceOf[ClosableTask]
  val context = new TaskContext {
    def getMetricsRegistry = metrics.registry
    def getSystemStreamPartitions = systemStreamPartitions
    def getStore(storeName: String) = if (storageManager != null) {
      storageManager(storeName)
    } else {
      warn("No store found for name: %s" format storeName)

      null
    }
    def getTaskName = taskName
    def getSamzaContainerContext = containerContext

    override def setStartingOffset(ssp: SystemStreamPartition, offset: String): Unit = {
      val startingOffsets = offsetManager.startingOffsets
      offsetManager.startingOffsets += taskName -> (startingOffsets(taskName) + (ssp -> offset))
    }
  }
  // store the (ssp -> if this ssp is catched up) mapping. "catched up"
  // means the same ssp in other taskInstances have the same offset as
  // the one here.
  var ssp2catchedupMapping: scala.collection.mutable.Map[SystemStreamPartition, Boolean] = scala.collection.mutable.Map[SystemStreamPartition, Boolean]()
  systemStreamPartitions.foreach(ssp2catchedupMapping += _ -> false)

  def registerMetrics {
    debug("Registering metrics for taskName: %s" format taskName)

    reporters.values.foreach(_.register(metrics.source, metrics.registry))
  }

  def registerOffsets {
    debug("Registering offsets for taskName: %s" format taskName)

    offsetManager.register(taskName, systemStreamPartitions)
  }

  def startStores {
    if (storageManager != null) {
      debug("Starting storage manager for taskName: %s" format taskName)

      storageManager.init
    } else {
      debug("Skipping storage manager initialization for taskName: %s" format taskName)
    }
  }

  def initTask {
    if (isInitableTask) {
      debug("Initializing task for taskName: %s" format taskName)

      task.asInstanceOf[InitableTask].init(config, context)
    } else {
      debug("Skipping task initialization for taskName: %s" format taskName)
    }
  }

  def registerProducers {
    debug("Registering producers for taskName: %s" format taskName)

    collector.register
  }

  def registerConsumers {
    debug("Registering consumers for taskName: %s" format taskName)

    systemStreamPartitions.foreach(systemStreamPartition => {
      val offset = offsetManager.getStartingOffset(taskName, systemStreamPartition)
      .getOrElse(throw new SamzaException("No offset defined for SystemStreamPartition: %s" format systemStreamPartition))
      consumerMultiplexer.register(systemStreamPartition, offset)
      metrics.addOffsetGauge(systemStreamPartition, () => {
        offsetManager
          .getLastProcessedOffset(taskName, systemStreamPartition)
          .orNull
      })
    })
  }

  def process(envelope: IncomingMessageEnvelope, coordinator: ReadableCoordinator) {
    metrics.processes.inc

    if (!ssp2catchedupMapping.getOrElse(envelope.getSystemStreamPartition,
      throw new SamzaException(envelope.getSystemStreamPartition + " is not registered!"))) {
      checkCaughtUp(envelope)
    }

    if (ssp2catchedupMapping(envelope.getSystemStreamPartition)) {
      metrics.messagesActuallyProcessed.inc

      trace("Processing incoming message envelope for taskName and SSP: %s, %s" format (taskName, envelope.getSystemStreamPartition))

      exceptionHandler.maybeHandle {
        task.process(envelope, collector, coordinator)
      }

      trace("Updating offset map for taskName, SSP and offset: %s, %s, %s" format (taskName, envelope.getSystemStreamPartition, envelope.getOffset))

      offsetManager.update(taskName, envelope.getSystemStreamPartition, envelope.getOffset)
    }
  }

  def window(coordinator: ReadableCoordinator) {
    if (isWindowableTask) {
      trace("Windowing for taskName: %s" format taskName)

      metrics.windows.inc

      exceptionHandler.maybeHandle {
        task.asInstanceOf[WindowableTask].window(collector, coordinator)
      }
    }
  }

  def commit {
    trace("Flushing state stores for taskName: %s" format taskName)

    metrics.commits.inc

    if (storageManager != null) {
      storageManager.flush
    }

    trace("Flushing producers for taskName: %s" format taskName)

    collector.flush

    trace("Committing offset manager for taskName: %s" format taskName)

    offsetManager.checkpoint(taskName)
  }

  def shutdownTask {
    if (task.isInstanceOf[ClosableTask]) {
      debug("Shutting down stream task for taskName: %s" format taskName)

      task.asInstanceOf[ClosableTask].close
    } else {
      debug("Skipping stream task shutdown for taskName: %s" format taskName)
    }
  }

  def shutdownStores {
    if (storageManager != null) {
      debug("Shutting down storage manager for taskName: %s" format taskName)

      storageManager.stop
    } else {
      debug("Skipping storage manager shutdown for taskName: %s" format taskName)
    }
  }

  override def toString() = "TaskInstance for class %s and taskName %s." format (task.getClass.getName, taskName)

  def toDetailedString() = "TaskInstance [taskName = %s, windowable=%s, closable=%s]" format (taskName, isWindowableTask, isClosableTask)

  /**
   * From the envelope, check if this SSP has catched up with the starting offset of the SSP
   * in this TaskInstance. If the offsets are not comparable, default to true, which means
   * it's already catched-up.
   */
  private def checkCaughtUp(envelope: IncomingMessageEnvelope) = {
    systemAdmins match {
      case null => {
        warn("systemAdmin is null. Set all SystemStreamPartitions to catched-up")
        ssp2catchedupMapping(envelope.getSystemStreamPartition) = true
      }
      case others => {
        val startingOffset = offsetManager.getStartingOffset(taskName, envelope.getSystemStreamPartition)
            .getOrElse(throw new SamzaException("No offset defined for SystemStreamPartition: %s" format envelope.getSystemStreamPartition))
        val system = envelope.getSystemStreamPartition.getSystem
        others(system).offsetComparator(envelope.getOffset, startingOffset) match {
          case null => {
            info("offsets in " + system + " is not comparable. Set all SystemStreamPartitions to catched-up")
            ssp2catchedupMapping(envelope.getSystemStreamPartition) = true // not comparable
          }
          case result => {
            if (result >= 0) {
              info(envelope.getSystemStreamPartition.toString + " is catched up.")
              ssp2catchedupMapping(envelope.getSystemStreamPartition) = true
            }
          }
        }
      }
    }
  }
}
