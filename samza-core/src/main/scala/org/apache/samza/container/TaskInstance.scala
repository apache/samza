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


import java.util.concurrent.ScheduledExecutorService

import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.OffsetManager
import org.apache.samza.config.Config
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.job.model.JobModel
import org.apache.samza.metrics.MetricsReporter
import org.apache.samza.storage.{SideInputStorageManager, TaskStorageManager}
import org.apache.samza.system._
import org.apache.samza.table.TableManager
import org.apache.samza.task._
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.Map

class TaskInstance(
  val task: Any,
  val taskName: TaskName,
  config: Config,
  val metrics: TaskInstanceMetrics,
  systemAdmins: SystemAdmins,
  consumerMultiplexer: SystemConsumers,
  collector: TaskInstanceCollector,
  containerContext: SamzaContainerContext,
  val offsetManager: OffsetManager = new OffsetManager,
  storageManager: TaskStorageManager = null,
  tableManager: TableManager = null,
  reporters: Map[String, MetricsReporter] = Map(),
  val systemStreamPartitions: Set[SystemStreamPartition] = Set(),
  val exceptionHandler: TaskInstanceExceptionHandler = new TaskInstanceExceptionHandler,
  jobModel: JobModel = null,
  streamMetadataCache: StreamMetadataCache = null,
  timerExecutor : ScheduledExecutorService = null,
  sideInputStorageManager: SideInputStorageManager = null,
  val sideInputSSPs: Set[SystemStreamPartition] = Set()) extends Logging {
  val isInitableTask = task.isInstanceOf[InitableTask]
  val isWindowableTask = task.isInstanceOf[WindowableTask]
  val isEndOfStreamListenerTask = task.isInstanceOf[EndOfStreamListenerTask]
  val isClosableTask = task.isInstanceOf[ClosableTask]
  val isAsyncTask = task.isInstanceOf[AsyncStreamTask]

  val context = new TaskContextImpl(taskName, metrics, containerContext, systemStreamPartitions.asJava, offsetManager,
                                    storageManager, tableManager, jobModel, streamMetadataCache, timerExecutor)

  // store the (ssp -> if this ssp is catched up) mapping. "catched up"
  // means the same ssp in other taskInstances have the same offset as
  // the one here.
  var ssp2CaughtupMapping: scala.collection.mutable.Map[SystemStreamPartition, Boolean] =
    scala.collection.mutable.Map[SystemStreamPartition, Boolean]()
  systemStreamPartitions.foreach(ssp2CaughtupMapping += _ -> false)

  val intermediateStreams: Set[String] = config.getStreamIds.filter(config.getIsIntermediateStream).toSet

  val streamsToDeleteCommittedMessages: Set[String] = config.getStreamIds.filter(config.getDeleteCommittedMessages).map(config.getPhysicalName).toSet

  def registerMetrics {
    debug("Registering metrics for task name: %s" format taskName)

    reporters.values.foreach(_.register(metrics.source, metrics.registry))
  }

  def registerOffsets {
    debug("Registering offsets for task name: %s" format taskName)

    val sspsToRegister = systemStreamPartitions -- sideInputSSPs
    offsetManager.register(taskName, sspsToRegister)
  }

  def startStores {
    if (storageManager != null) {
      debug("Starting storage manager for task name: %s" format taskName)

      storageManager.init
    } else {
      debug("Skipping storage manager initialization for task name: %s" format taskName)
    }

    if (sideInputStorageManager != null) {
      debug("Starting side input storage manager for task name: %s" format taskName)
      sideInputStorageManager.init()
    }
  }

  def startTableManager {
    if (tableManager != null) {
      debug("Starting table manager for task name: %s" format taskName)

      tableManager.init(containerContext, context)
    } else {
      debug("Skipping table manager initialization for task name %s" format taskName)
    }
  }

  def initTask {
    if (isInitableTask) {
      debug("Initializing task for task name %s" format taskName)

      task.asInstanceOf[InitableTask].init(config, context)
    } else {
      debug("Skipping task initialization for task name %s" format taskName)
    }
  }

  def registerProducers {
    debug("Registering producers for task name %s" format taskName)

    collector.register
  }

  def registerConsumers {
    debug("Registering consumers for task name %s" format taskName)

    systemStreamPartitions.foreach(systemStreamPartition => {
      var metricsValueGetter: () => String = null
      var offset:String = null


      if (sideInputSSPs.contains(systemStreamPartition)) {
        offset = sideInputStorageManager.getStartingOffset(systemStreamPartition)
        metricsValueGetter = () => sideInputStorageManager.getLastProcessedOffset(systemStreamPartition)
      } else {
        offset = offsetManager.getStartingOffset(taskName, systemStreamPartition).orNull
        metricsValueGetter = () => offsetManager.getLastProcessedOffset(taskName, systemStreamPartition).orNull
      }

//      if (offset == null) {
//        throw new SamzaException("No offset defined for SystemStreamPartition: %s" format systemStreamPartition)
//      }

      consumerMultiplexer.register(systemStreamPartition, offset)
      metrics.addOffsetGauge(systemStreamPartition, metricsValueGetter)
    })
  }

  def process(envelope: IncomingMessageEnvelope, coordinator: ReadableCoordinator,
    callbackFactory: TaskCallbackFactory = null) {
    metrics.processes.inc

    val incomingMessageSsp = envelope.getSystemStreamPartition

    if (!ssp2CaughtupMapping.getOrElse(incomingMessageSsp,
      throw new SamzaException(incomingMessageSsp + " is not registered!"))) {
      checkCaughtUp(envelope)
    }

    if (ssp2CaughtupMapping(incomingMessageSsp)) {
      metrics.messagesActuallyProcessed.inc

      trace("Processing incoming message envelope for task name and SSP: %s, %s"
        format (taskName, incomingMessageSsp))

      if (sideInputSSPs.contains(incomingMessageSsp)) {
        sideInputStorageManager.process(envelope)
      } else {
        if (isAsyncTask) {
          exceptionHandler.maybeHandle {
            val callback = callbackFactory.createCallback()
            task.asInstanceOf[AsyncStreamTask].processAsync(envelope, collector, coordinator, callback)
          }
        } else {
          exceptionHandler.maybeHandle {
            task.asInstanceOf[StreamTask].process(envelope, collector, coordinator)
          }

          trace("Updating offset map for task name, SSP and offset: %s, %s, %s"
            format (taskName, incomingMessageSsp, envelope.getOffset))

          offsetManager.update(taskName, incomingMessageSsp, envelope.getOffset)
        }
      }
    }
  }

  def endOfStream(coordinator: ReadableCoordinator): Unit = {
    if (isEndOfStreamListenerTask) {
      exceptionHandler.maybeHandle {
        task.asInstanceOf[EndOfStreamListenerTask].onEndOfStream(collector, coordinator)
      }
    }
  }

  def window(coordinator: ReadableCoordinator) {
    if (isWindowableTask) {
      trace("Windowing for task name %s" format taskName)

      metrics.windows.inc

      exceptionHandler.maybeHandle {
        task.asInstanceOf[WindowableTask].window(collector, coordinator)
      }
    }
  }

  def timer(coordinator: ReadableCoordinator) {
    trace("Timer for task name %s" format taskName)

    exceptionHandler.maybeHandle {
      context.getTimerScheduler.removeReadyTimers().entrySet().foreach { entry =>
        entry.getValue.asInstanceOf[TimerCallback[Any]].onTimer(entry.getKey.getKey, collector, coordinator)
      }
    }
  }

  def commit {
    metrics.commits.inc

    val checkpoint = offsetManager.buildCheckpoint(taskName)

    trace("Flushing producers for task name %s" format taskName)
    collector.flush

    trace("Flushing state stores for task name %s" format taskName)
    if (storageManager != null) {
      storageManager.flush
    }

    trace ("Flushing side input stores for task name %s" format taskName)
    if (sideInputStorageManager != null) {
      sideInputStorageManager.flush
    }

    trace("Checkpointing offsets for task name %s" format taskName)
    offsetManager.writeCheckpoint(taskName, checkpoint)

    if (checkpoint != null) {
      checkpoint.getOffsets.asScala
        .filter { case (ssp, _) => streamsToDeleteCommittedMessages.contains(ssp.getStream) } // Only delete data of intermediate streams
        .groupBy { case (ssp, _) => ssp.getSystem }
        .foreach { case (systemName: String, offsets: Map[SystemStreamPartition, String]) =>
          systemAdmins.getSystemAdmin(systemName).deleteMessages(offsets.asJava)
        }
    }
  }

  def shutdownTask {
    if (task.isInstanceOf[ClosableTask]) {
      debug("Shutting down stream task for task name %s" format taskName)

      task.asInstanceOf[ClosableTask].close
    } else {
      debug("Skipping stream task shutdown for task name %s" format taskName)
    }
  }

  def shutdownStores {
    if (storageManager != null) {
      debug("Shutting down storage manager for task name: %s" format taskName)

      storageManager.stop
    } else {
      debug("Skipping storage manager shutdown for task name: %s" format taskName)
    }

    if (sideInputStorageManager != null) {
      debug("Shutting down side input storage manager for task name: %s" format taskName)
      sideInputStorageManager.stop
    }
  }

  def shutdownTableManager {
    if (tableManager != null) {
      debug("Shutting down table manager for task name %s" format taskName)

      tableManager.close
    } else {
      debug("Skipping table manager shutdown for task name %s" format taskName)
    }
  }

  override def toString() = "TaskInstance for class %s and task name %s." format (task.getClass.getName, taskName)

  def toDetailedString() = "TaskInstance [task name = %s, windowable=%s, closable=%s endofstreamlistener=%s]" format
    (taskName, isWindowableTask, isClosableTask, isEndOfStreamListenerTask)

  /**
   * From the envelope, check if this SSP has catched up with the starting offset of the SSP
   * in this TaskInstance. If the offsets are not comparable, default to true, which means
   * it's already catched-up.
   */
  private def checkCaughtUp(envelope: IncomingMessageEnvelope) = {
    val incomingMessageSsp = envelope.getSystemStreamPartition

    if (IncomingMessageEnvelope.END_OF_STREAM_OFFSET.equals(envelope.getOffset)) {
      ssp2CaughtupMapping(incomingMessageSsp) = true
    } else {
      systemAdmins match {
        case null => {
          warn("systemAdmin is null. Set all SystemStreamPartitions to catched-up")
          ssp2CaughtupMapping(incomingMessageSsp) = true
        }
        case others => {
          var startingOffset:String = null

          if (sideInputSSPs.contains(incomingMessageSsp)) {
            startingOffset = sideInputStorageManager.getStartingOffset(incomingMessageSsp)
          } else {
            startingOffset = offsetManager.getStartingOffset(taskName, incomingMessageSsp).orNull
          }

//          if (startingOffset == null) {
//            throw new SamzaException("No offset defined for SystemStreamPartition: %s" format incomingMessageSsp)
//          }

          val system = incomingMessageSsp.getSystem
          others.getSystemAdmin(system).offsetComparator(envelope.getOffset, startingOffset) match {
            case null => {
              info("offsets in " + system + " is not comparable. Set all SystemStreamPartitions to catched-up")
              ssp2CaughtupMapping(incomingMessageSsp) = true // not comparable
            }
            case result => {
              if (result >= 0) {
                info(incomingMessageSsp.toString + " is catched up.")
                ssp2CaughtupMapping(incomingMessageSsp) = true
              }
            }
          }
        }
      }
    }
  }
}
