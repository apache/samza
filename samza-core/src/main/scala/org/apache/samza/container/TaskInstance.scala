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


import java.util.{Objects, Optional}
import java.util.concurrent.ScheduledExecutorService

import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.OffsetManager
import org.apache.samza.config.Config
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.context._
import org.apache.samza.job.model.{JobModel, TaskModel}
import org.apache.samza.metrics.MetricsReporter
import org.apache.samza.scheduler.{CallbackSchedulerImpl, ScheduledCallback}
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.storage.TaskStorageManager
import org.apache.samza.system._
import org.apache.samza.table.TableManager
import org.apache.samza.task._
import org.apache.samza.util.{Logging, ScalaJavaUtil}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.Map

class TaskInstance(
  val task: Any,
  taskModel: TaskModel,
  val metrics: TaskInstanceMetrics,
  systemAdmins: SystemAdmins,
  consumerMultiplexer: SystemConsumers,
  collector: TaskInstanceCollector,
  val offsetManager: OffsetManager = new OffsetManager,
  storageManager: TaskStorageManager = null,
  tableManager: TableManager = null,
  val systemStreamPartitions: Set[SystemStreamPartition] = Set(),
  val exceptionHandler: TaskInstanceExceptionHandler = new TaskInstanceExceptionHandler,
  jobModel: JobModel = null,
  streamMetadataCache: StreamMetadataCache = null,
  timerExecutor : ScheduledExecutorService = null,
  jobContext: JobContext,
  containerContext: ContainerContext,
  applicationContainerContextOption: Option[ApplicationContainerContext],
  applicationTaskContextFactoryOption: Option[ApplicationTaskContextFactory[ApplicationTaskContext]],
  externalContextOption: Option[ExternalContext]
) extends Logging {

  val taskName: TaskName = taskModel.getTaskName
  val isInitableTask = task.isInstanceOf[InitableTask]
  val isWindowableTask = task.isInstanceOf[WindowableTask]
  val isEndOfStreamListenerTask = task.isInstanceOf[EndOfStreamListenerTask]
  val isClosableTask = task.isInstanceOf[ClosableTask]
  val isAsyncTask = task.isInstanceOf[AsyncStreamTask]

  val epochTimeScheduler: EpochTimeScheduler = EpochTimeScheduler.create(timerExecutor)

  private val kvStoreSupplier = ScalaJavaUtil.toJavaFunction(
    (storeName: String) => {
      if (storageManager != null && storageManager.getStore(storeName).isDefined) {
        storageManager.getStore(storeName).get.asInstanceOf[KeyValueStore[_, _]]
      } else {
        null
      }
    })
  private val taskContext = new TaskContextImpl(taskModel, metrics.registry, kvStoreSupplier, tableManager,
    new CallbackSchedulerImpl(epochTimeScheduler), offsetManager, jobModel, streamMetadataCache)
  // need separate field for this instead of using it through Context, since Context throws an exception if it is null
  private val applicationTaskContextOption = applicationTaskContextFactoryOption
    .map(_.create(externalContextOption.orNull, jobContext, containerContext, taskContext,
      applicationContainerContextOption.orNull))
  val context = new ContextImpl(jobContext, containerContext, taskContext,
    Optional.ofNullable(applicationContainerContextOption.orNull),
    Optional.ofNullable(applicationTaskContextOption.orNull), Optional.ofNullable(externalContextOption.orNull))

  // store the (ssp -> if this ssp has caught up) mapping. "caught up"
  // means the same ssp in other taskInstances have the same offset as
  // the one here.
  var ssp2CaughtupMapping: scala.collection.mutable.Map[SystemStreamPartition, Boolean] =
    scala.collection.mutable.Map[SystemStreamPartition, Boolean]()
  systemStreamPartitions.foreach(ssp2CaughtupMapping += _ -> false)

  private val config: Config = jobContext.getConfig

  val intermediateStreams: Set[String] = config.getStreamIds.filter(config.getIsIntermediateStream).toSet

  val streamsToDeleteCommittedMessages: Set[String] = config.getStreamIds.filter(config.getDeleteCommittedMessages).map(config.getPhysicalName).toSet

  def registerOffsets {
    debug("Registering offsets for taskName: %s" format taskName)
    offsetManager.register(taskName, systemStreamPartitions)
  }

  def startTableManager {
    if (tableManager != null) {
      debug("Starting table manager for taskName: %s" format taskName)

      tableManager.init(context)
    } else {
      debug("Skipping table manager initialization for taskName: %s" format taskName)
    }
  }

  def initTask {
    initCaughtUpMapping()

    if (isInitableTask) {
      debug("Initializing task for taskName: %s" format taskName)

      task.asInstanceOf[InitableTask].init(context)
    } else {
      debug("Skipping task initialization for taskName: %s" format taskName)
    }
    applicationTaskContextOption.foreach(applicationTaskContext => {
      debug("Starting application-defined task context for taskName: %s" format taskName)
      applicationTaskContext.start()
    })
  }

  def registerProducers {
    debug("Registering producers for taskName: %s" format taskName)

    collector.register
  }

  def registerConsumers {
    debug("Registering consumers for taskName: %s" format taskName)
    systemStreamPartitions.foreach(systemStreamPartition => {
      val startingOffset = getStartingOffset(systemStreamPartition)
      val startpoint = offsetManager.getStartpoint(taskName, systemStreamPartition).getOrElse(null)
      consumerMultiplexer.register(systemStreamPartition, startingOffset, startpoint)
      metrics.addOffsetGauge(systemStreamPartition, () =>
          offsetManager.getLastProcessedOffset(taskName, systemStreamPartition).orNull
        )
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

      trace("Processing incoming message envelope for taskName and SSP: %s, %s"
        format (taskName, incomingMessageSsp))

      if (isAsyncTask) {
        exceptionHandler.maybeHandle {
          val callback = callbackFactory.createCallback()
          task.asInstanceOf[AsyncStreamTask].processAsync(envelope, collector, coordinator, callback)
        }
      } else {
        exceptionHandler.maybeHandle {
          task.asInstanceOf[StreamTask].process(envelope, collector, coordinator)
        }

        trace("Updating offset map for taskName, SSP and offset: %s, %s, %s"
          format(taskName, incomingMessageSsp, envelope.getOffset))

        offsetManager.update(taskName, incomingMessageSsp, envelope.getOffset)
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
      trace("Windowing for taskName: %s" format taskName)

      metrics.windows.inc

      exceptionHandler.maybeHandle {
        task.asInstanceOf[WindowableTask].window(collector, coordinator)
      }
    }
  }

  def scheduler(coordinator: ReadableCoordinator) {
    trace("Scheduler for taskName: %s" format taskName)

    exceptionHandler.maybeHandle {
      epochTimeScheduler.removeReadyTimers().entrySet().foreach { entry =>
        entry.getValue.asInstanceOf[ScheduledCallback[Any]].onCallback(entry.getKey.getKey, collector, coordinator)
      }
    }
  }

  def commit {
    metrics.commits.inc

    val checkpoint = offsetManager.buildCheckpoint(taskName)

    trace("Flushing producers for taskName: %s" format taskName)
    collector.flush

    trace("Flushing state stores for taskName: %s" format taskName)
    if (storageManager != null) {
      storageManager.flush
    }

    trace("Flushing tables for taskName: %s" format taskName)
    if (tableManager != null) {
      tableManager.flush
    }

    trace("Checkpointing offsets for taskName: %s" format taskName)
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
    applicationTaskContextOption.foreach(applicationTaskContext => {
      debug("Stopping application-defined task context for taskName: %s" format taskName)
      applicationTaskContext.stop()
    })
    if (task.isInstanceOf[ClosableTask]) {
      debug("Shutting down stream task for taskName: %s" format taskName)

      task.asInstanceOf[ClosableTask].close
    } else {
      debug("Skipping stream task shutdown for taskName: %s" format taskName)
    }
  }

  def shutdownTableManager {
    if (tableManager != null) {
      debug("Shutting down table manager for taskName: %s" format taskName)

      tableManager.close
    } else {
      debug("Skipping table manager shutdown for taskName: %s" format taskName)
    }
  }

  override def toString() = "TaskInstance for class %s and taskName %s." format (task.getClass.getName, taskName)

  def toDetailedString() = "TaskInstance [taskName = %s, windowable=%s, closable=%s endofstreamlistener=%s]" format
    (taskName, isWindowableTask, isClosableTask, isEndOfStreamListenerTask)

  /**
   * From the envelope, check if this SSP has caught up with the starting offset of the SSP
   * in this TaskInstance. If the offsets are not comparable, default to true, which means
   * it's already caught up.
   */
  private def checkCaughtUp(envelope: IncomingMessageEnvelope) = {
    val incomingMessageSsp = envelope.getSystemStreamPartition

    if (IncomingMessageEnvelope.END_OF_STREAM_OFFSET.equals(envelope.getOffset)) {
      ssp2CaughtupMapping(incomingMessageSsp) = true
    } else {
      systemAdmins match {
        case null => {
          warn("systemAdmin is null. Set all SystemStreamPartitions to caught-up")
          ssp2CaughtupMapping(incomingMessageSsp) = true
        }
        case others => {
          val startingOffset = getStartingOffset(incomingMessageSsp)

          val system = incomingMessageSsp.getSystem
          others.getSystemAdmin(system).offsetComparator(envelope.getOffset, startingOffset) match {
            case null => {
              info("offsets in " + system + " is not comparable. Set all SystemStreamPartitions to caught-up")
              ssp2CaughtupMapping(incomingMessageSsp) = true // not comparable
            }
            case result => {
              if (result >= 0) {
                info(incomingMessageSsp.toString + " has caught up.")
                ssp2CaughtupMapping(incomingMessageSsp) = true
              }
            }
          }
        }
      }
    }
  }

  /**
    * Check each partition assigned to the task is caught to the last offset
    */
  def initCaughtUpMapping() {
    if (taskContext.getStreamMetadataCache != null) {
      systemStreamPartitions.foreach(ssp => {
        val partitionMetadata = taskContext
          .getStreamMetadataCache
          .getSystemStreamMetadata(ssp.getSystemStream, false)
          .getSystemStreamPartitionMetadata.get(ssp.getPartition)

        val upcomingOffset = partitionMetadata.getUpcomingOffset
        val startingOffset = offsetManager.getStartingOffset(taskName, ssp)
          .getOrElse(throw new SamzaException("No offset defined for SystemStreamPartition: %s" format ssp))

        // Mark ssp to be caught up if the starting offset is already the
        // upcoming offset, meaning the task has consumed all the messages
        // in this partition before and waiting for the future incoming messages.
        if(Objects.equals(upcomingOffset, startingOffset)) {
          ssp2CaughtupMapping(ssp) = true
        }
      })
    }
  }

  private def getStartingOffset(systemStreamPartition: SystemStreamPartition) = {
    val offset = offsetManager.getStartingOffset(taskName, systemStreamPartition)
    val startingOffset = offset.getOrElse(
      throw new SamzaException("No offset defined for SystemStreamPartition: %s" format systemStreamPartition))

    startingOffset
  }
}
